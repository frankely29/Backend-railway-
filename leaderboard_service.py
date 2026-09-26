from __future__ import annotations

import logging
import time
import sqlite3
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from core import DB_BACKEND, _db_exec, _db_query_all, _db_query_one, _db_run_in_transaction, _sql
from leaderboard_models import LeaderboardMetric, LeaderboardPeriod

_LOGGER = logging.getLogger(__name__)

NYC_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------- the economy
#
# Everything a driver does in the app earns XP, and the whole thing is
# calibrated against ONE benchmark: a driver who uses the app every day and is
# very active reaches the top rank in four months. Everything else falls out
# of that -- someone who drives less, or never posts, simply takes longer.
#
# The benchmark day, measured against what a hard NYC FHV shift actually looks
# like -- 10 hours, 150 miles, 25 saved trips -- plus an active social day:
#
#     miles      150 x  8  =  1,200
#     hours       10 x 30  =    300
#     trips       25 x 60  =  1,500   <- the largest single source, by design
#     posts        5 x 25  =    125
#     comments    20 x  8  =    160
#     likes       40 x  2  =     80
#                            -------
#                              3,365 XP/day  x 120 days = 403,800
#
# so the climb to the top rank is 405,000 XP. What that produces:
#
#     very active, posts daily      120 days   (the benchmark)
#     drives hard, never posts      135 days
#     committed full-time           173 days
#     steady part-time              349 days
#
# Social participation is worth about two weeks off the climb. That is the
# right weight for a DRIVER network: it is worth doing and it is not a second
# job, and a driver who only ever drives is never locked out of anything.
#
# The order the rates encode, deliberately: saving a trip beats posting, a
# post beats a reply, a reply beats a like. A saved trip is the thing the
# whole product is built on, so it pays the most per action.
PROGRESSION_XP_PER_MILE = 8
PROGRESSION_XP_PER_HOUR = 30
PROGRESSION_XP_PER_REPORTED_PICKUP = 60
PROGRESSION_XP_PER_POST = 25
PROGRESSION_XP_PER_COMMENT = 8
PROGRESSION_XP_PER_LIKE_GIVEN = 2

# Daily ceilings, per NYC day. Not anti-cheat theatre: without them the
# cheapest action sets the pace, and a driver farming likes out-earns one
# actually working. Every cap is set at "a genuinely heavy day" so nobody
# real ever touches it.
PROGRESSION_MAX_PICKUP_REPORTS_PER_DAY_FOR_XP = 25
PROGRESSION_MAX_POSTS_PER_DAY_FOR_XP = 5
PROGRESSION_MAX_COMMENTS_PER_DAY_FOR_XP = 20
PROGRESSION_MAX_LIKES_PER_DAY_FOR_XP = 40

# Thirty levels, one per rank on the ladder: ten prestiges of three. There is
# no second scale any more. The 1000-level curve this replaces was never
# calibrated against anything -- it wanted 34.3 MILLION XP for the top rank,
# which the benchmark driver above would have reached in 47 years -- and it
# forced every surface to translate between two numbers for the same thing.
MAX_LEVEL = 30
PROGRESSION_XP_TO_MAX_LEVEL = 405_000
_CURRENT_BADGES_REFRESH_LOCK = threading.Lock()
_CURRENT_BADGES_LAST_REFRESH_TS = 0
_CURRENT_BADGES_MIN_REFRESH_INTERVAL_SECONDS = 30
_CURRENT_BADGES_BY_USER_CACHE: Dict[int, Dict[str, Any]] = {}
_PROGRESSION_BY_USER_CACHE: Dict[int, Dict[str, Any]] = {}
_LEADERBOARD_RUNTIME_LOCK = threading.Lock()
_CURRENT_BADGES_CACHE_TTL_SECONDS = 10
_PROGRESSION_CACHE_TTL_SECONDS = 15
_CURRENT_BADGES_CACHE_MAX_ENTRIES = 2000
_PROGRESSION_CACHE_MAX_ENTRIES = 2000


def _prune_user_ttl_cache(cache: Dict[int, Dict[str, Any]], now: int, ttl_seconds: int, max_entries: int) -> None:
    expired_user_ids = [
        uid for uid, entry in cache.items() if (now - int(entry.get("cached_at_unix") or 0)) > int(ttl_seconds)
    ]
    for uid in expired_user_ids:
        cache.pop(uid, None)

    if len(cache) <= int(max_entries):
        return

    over_limit = len(cache) - int(max_entries)
    oldest_first = sorted(cache.items(), key=lambda item: int(item[1].get("cached_at_unix") or 0))
    for uid, _ in oldest_first[:over_limit]:
        cache.pop(uid, None)


# How much steeper each level is than the last. 1.25 was chosen by looking at
# what the pacing does to a benchmark driver rather than because it is a round
# number: level 2 inside the first session, the first prestige inside a day,
# the halfway mark around a month, and the last rank a nine-day climb. Flatter
# and the top ranks arrive too cheaply; steeper and the middle of the ladder
# stalls.
PROGRESSION_LEVEL_CURVE_EXPONENT = 1.25


def _build_level_xp_thresholds() -> List[int]:
    """Lifetime XP needed for each level, thresholds[0] = level 1 = 0 XP.

    The shape is chosen, the scale is solved: the steps are weighted L^1.25
    and then scaled so the total is exactly PROGRESSION_XP_TO_MAX_LEVEL. That
    way the benchmark -- four months -- is the input, and no step is a magic
    number somebody has to keep in sync by hand.
    """
    weights = [float(level_index) ** PROGRESSION_LEVEL_CURVE_EXPONENT
               for level_index in range(1, MAX_LEVEL)]
    scale = float(PROGRESSION_XP_TO_MAX_LEVEL) / sum(weights)
    steps = [int(round(scale * weight)) for weight in weights]
    # Rounding drift lands on the last step so the top of the ladder is the
    # exact number the benchmark produced, not a few XP either side of it.
    steps[-1] += PROGRESSION_XP_TO_MAX_LEVEL - sum(steps)

    thresholds = [0]
    for step_xp in steps:
        thresholds.append(thresholds[-1] + step_xp)
    return thresholds


LEVEL_XP_THRESHOLDS = _build_level_xp_thresholds()

# The ladder a driver climbs: ten prestiges of three ranks each, thirty in all.
#
# A driver finishing prestige 1 rank 3 moves to prestige 2 rank 1, and so on
# to the tenth prestige. So band_005 is prestige 2, rank 2 -- the band index
# is the only thing stored or sent, and the pair is derived from it:
#
#     prestige = ((band - 1) // RANKS_PER_PRESTIGE) + 1
#     rank     = ((band - 1) %  RANKS_PER_PRESTIGE) + 1
#
# Written against the constant rather than a literal 3, because this shape has
# already changed twice -- a hundred bands of ten, then fifty of five.
#
# A level IS a rank now. MAX_LEVEL is 30 and so is the band count, so
# LEVELS_PER_RANK_BAND is 1 and every level a driver gains is a rank-up worth
# a ceremony. Under the old 1000-level curve a driver crossed eight levels a
# day and got eight notifications for it, which is how a promotion came to
# feel like nothing.
PRESTIGE_COUNT = 10
RANKS_PER_PRESTIGE = 3
RANK_BAND_COUNT = PRESTIGE_COUNT * RANKS_PER_PRESTIGE
LEVELS_PER_RANK_BAND = MAX_LEVEL // RANK_BAND_COUNT


# The ten prestiges, in climbing order, each named for the creature on its
# badge. This table lives here rather than in the clients because the ladder
# lives here: a rank's name is part of what the rank IS, and a backend that
# sends only "band_007" forces every client to keep its own copy of this list
# and hope they agree. They did not -- the frontend printed "Band 034" at
# drivers for weeks because it had no name to use and fell back to the key.
PRESTIGE_NAMES = [
    "Wyvern", "Chimera", "Hydra", "Kraken", "Warlord",
    "Colossus", "Titan", "Celestial", "Phoenix", "Dragon",
]

# Ranks inside a prestige are numbered, not named: Wyvern I, Wyvern II,
# Wyvern III. Roman because the badge strikes the numeral on its nameplate and
# a numeral reads as an honour where a digit reads as a count.
RANK_NUMERALS = ["I", "II", "III", "IV", "V"]


def rank_title(prestige: int, rank: int) -> str:
    """The name a driver is shown: "Wyvern III", not "Band 003"."""
    p = max(1, min(len(PRESTIGE_NAMES), int(prestige)))
    r = max(1, min(len(RANK_NUMERALS), int(rank)))
    return f"{PRESTIGE_NAMES[p - 1]} {RANK_NUMERALS[r - 1]}"


def _prestige_and_rank(band_index: int):
    """The pair, without the clamping -- used while the ladder is being built,
    before RANK_LADDER exists for prestige_and_rank_for_band to read."""
    return (((band_index - 1) // RANKS_PER_PRESTIGE) + 1,
            ((band_index - 1) % RANKS_PER_PRESTIGE) + 1)


def _build_rank_ladder():
    """The thirty bands, each one covering a slice of the thousand XP levels.

    MAX_LEVEL does not divide evenly by the band count -- 1000 over 30 leaves
    ten levels spare -- and the obvious `band_index * LEVELS_PER_RANK_BAND`
    quietly drops them: the ladder would end at 990 and a driver at level 995
    would have no rank at all. So the last band runs to MAX_LEVEL and absorbs
    the remainder, which also means the top rank is the longest climb in the
    game. That is the right place for it.
    """
    rows = []
    for band_index in range(1, RANK_BAND_COUNT + 1):
        start = ((band_index - 1) * LEVELS_PER_RANK_BAND) + 1
        end = MAX_LEVEL if band_index == RANK_BAND_COUNT else band_index * LEVELS_PER_RANK_BAND
        pair = _prestige_and_rank(band_index)
        rows.append((start, end, rank_title(pair[0], pair[1]), f"band_{band_index:03d}"))
    return rows


RANK_LADDER = _build_rank_ladder()


def prestige_and_rank_for_band(band_index: int) -> Dict[str, int]:
    """The pair a driver actually sees, from the band index that is stored.

    Clamped rather than raising: a band outside the ladder means the ladder
    changed under stored data, and a driver should see the nearest real rank
    instead of an error.
    """
    band = max(1, min(RANK_BAND_COUNT, int(band_index)))
    return {
        "band": band,
        "prestige": ((band - 1) // RANKS_PER_PRESTIGE) + 1,
        "rank": ((band - 1) % RANKS_PER_PRESTIGE) + 1,
    }


def band_index_for_prestige_and_rank(prestige: int, rank: int) -> int:
    p = max(1, min(PRESTIGE_COUNT, int(prestige)))
    r = max(1, min(RANKS_PER_PRESTIGE, int(rank)))
    return ((p - 1) * RANKS_PER_PRESTIGE) + r


def get_rank_ladder() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for band_index, (start, end, rank_name, rank_icon_key) in enumerate(RANK_LADDER, start=1):
        pair = prestige_and_rank_for_band(band_index)
        rows.append(
            {
                "start_level": start,
                "end_level": end,
                "rank_name": rank_name,
                "rank_icon_key": rank_icon_key,
                # Sent rather than left to be re-derived. Four clients were
                # each parsing the key to get back to the pair, and one of
                # them printed the key itself when it could not.
                "band": pair["band"],
                "prestige": pair["prestige"],
                "rank": pair["rank"],
            }
        )
    return rows


def _bool_db_value(flag: bool):
    if DB_BACKEND == "postgres":
        return bool(flag)
    return 1 if flag else 0


def _leaderboard_active_user_where_sql(alias: str = "u") -> str:
    if DB_BACKEND == "postgres":
        return (
            f"COALESCE({alias}.is_disabled, FALSE) = FALSE "
            f"AND COALESCE({alias}.is_suspended, FALSE) = FALSE"
        )
    return (
        f"COALESCE(CAST({alias}.is_disabled AS INTEGER), 0) = 0 "
        f"AND COALESCE(CAST({alias}.is_suspended AS INTEGER), 0) = 0"
    )



@dataclass
class PeriodBounds:
    start_date: date
    end_date: date
    period_key: str


def _today_nyc() -> date:
    # Match leaderboard_tracker.py business-day logic:
    # the leaderboard “day” runs from 4 AM NYC to 3:59:59 AM NYC next day.
    return (datetime.now(timezone.utc).astimezone(NYC_TZ) - timedelta(hours=4)).date()


def current_period_bounds(period: LeaderboardPeriod) -> PeriodBounds:
    today = _today_nyc()
    if period == LeaderboardPeriod.daily:
        return PeriodBounds(today, today, today.isoformat())
    if period == LeaderboardPeriod.weekly:
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return PeriodBounds(start, end, f"{start.isoformat()}_{end.isoformat()}")
    if period == LeaderboardPeriod.monthly:
        start = today.replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(days=1)
        return PeriodBounds(start, end, start.strftime("%Y-%m"))
    start = today.replace(month=1, day=1)
    end = today.replace(month=12, day=31)
    return PeriodBounds(start, end, str(today.year))


def _metric_column(metric: LeaderboardMetric) -> str:
    return "miles_worked" if metric == LeaderboardMetric.miles else "hours_worked"


def _badge_for_rank(rank: int) -> Optional[str]:
    if rank == 1:
        return "crown"
    if rank == 2:
        return "silver"
    if rank == 3:
        return "bronze"
    return None


def _normalized_badge_code(rank_position: int, badge_code: Optional[str] = None) -> Optional[str]:
    # Always derive the podium badge from current rank position so stale legacy
    # values can never leak crown to rank #2/#3 and non-podium ranks are null.
    return _badge_for_rank(int(rank_position))


def _display_name(row: Dict) -> str:
    email = (row.get("email") or "Driver").strip()
    fallback = email.split("@")[0] if "@" in email else "Driver"
    return ((row.get("display_name") or "").strip() or fallback)[:28]


def _rank_for_level(level: int) -> Dict[str, str]:
    clamped = max(1, min(MAX_LEVEL, int(level)))
    for start, end, rank_name, rank_icon_key in RANK_LADDER:
        if start <= clamped <= end:
            return {"rank_name": rank_name, "rank_icon_key": rank_icon_key}
    # Only reachable if the ladder is empty or a level fell outside it, which
    # the boundary test forbids. The first real rank, not a key spelled out.
    return {"rank_name": rank_title(1, 1), "rank_icon_key": "band_001"}


def get_level_from_lifetime_xp(total_xp: int) -> int:
    xp = max(0, int(total_xp or 0))
    if xp >= LEVEL_XP_THRESHOLDS[-1]:
        return MAX_LEVEL
    level = 1
    for idx, threshold in enumerate(LEVEL_XP_THRESHOLDS):
        if xp >= threshold:
            level = idx + 1
        else:
            break
    return min(MAX_LEVEL, max(1, level))


def get_next_level_xp(level: int) -> Optional[int]:
    clamped = max(1, min(MAX_LEVEL, int(level)))
    if clamped >= MAX_LEVEL:
        return None
    return int(LEVEL_XP_THRESHOLDS[clamped])


def get_level_progress_from_lifetime_xp(total_xp: int) -> Dict[str, Any]:
    normalized_xp = max(0, int(total_xp or 0))
    level = get_level_from_lifetime_xp(normalized_xp)
    rank = _rank_for_level(level)
    current_level_xp = int(LEVEL_XP_THRESHOLDS[level - 1])
    next_level_xp = get_next_level_xp(level)
    xp_to_next_level = 0 if next_level_xp is None else max(0, int(next_level_xp) - normalized_xp)
    return {
        "level": level,
        "rank_name": rank["rank_name"],
        "rank_icon_key": rank["rank_icon_key"],
        "title": rank["rank_name"],
        "total_xp": normalized_xp,
        "current_level_xp": current_level_xp,
        "next_level_xp": next_level_xp,
        "xp_to_next_level": xp_to_next_level,
        "max_level_reached": level == MAX_LEVEL,
    }


def _social_xp_from_daily_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """XP from posting, replying and liking, capped per day.

    Rows are one per (user, NYC day) with the three counts on them. The caps
    have to be applied per DAY and not to a lifetime total, or a driver who
    posted forty times in one week would be paid for all of it while one
    posting twice a day for a month gets less -- which is backwards for a
    network that wants people to come back.
    """
    post_xp = 0
    comment_xp = 0
    like_xp = 0
    for raw_row in rows:
        row = dict(raw_row)
        posts = min(max(0, int(row.get("posts") or 0)), PROGRESSION_MAX_POSTS_PER_DAY_FOR_XP)
        comments = min(max(0, int(row.get("comments") or 0)), PROGRESSION_MAX_COMMENTS_PER_DAY_FOR_XP)
        likes = min(max(0, int(row.get("likes") or 0)), PROGRESSION_MAX_LIKES_PER_DAY_FOR_XP)
        post_xp += posts * PROGRESSION_XP_PER_POST
        comment_xp += comments * PROGRESSION_XP_PER_COMMENT
        like_xp += likes * PROGRESSION_XP_PER_LIKE_GIVEN
    return {"post_xp": post_xp, "comment_xp": comment_xp, "like_xp": like_xp}


def build_progression_from_daily_stats_rows(
    rows: List[Dict[str, Any]],
    game_xp: int = 0,
    social_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    lifetime_miles = 0.0
    lifetime_hours = 0.0
    lifetime_pickups_recorded = 0
    miles_xp = 0
    hours_xp = 0
    report_xp = 0
    normalized_game_xp = max(0, int(game_xp or 0))
    social = _social_xp_from_daily_counts(social_rows or [])

    for raw_row in rows:
        row = dict(raw_row)
        miles = float(row.get("miles_worked") or 0.0)
        hours = float(row.get("hours_worked") or 0.0)
        pickups_recorded = int(row.get("pickups_recorded") or 0)
        pickup_count_for_xp = min(max(0, pickups_recorded), PROGRESSION_MAX_PICKUP_REPORTS_PER_DAY_FOR_XP)

        lifetime_miles += miles
        lifetime_hours += hours
        lifetime_pickups_recorded += max(0, pickups_recorded)

        miles_xp += round(miles * PROGRESSION_XP_PER_MILE)
        hours_xp += round(hours * PROGRESSION_XP_PER_HOUR)
        report_xp += pickup_count_for_xp * PROGRESSION_XP_PER_REPORTED_PICKUP

    social_xp = int(social["post_xp"] + social["comment_xp"] + social["like_xp"])
    total_xp = int(miles_xp + hours_xp + report_xp + social_xp)
    total_xp += normalized_game_xp
    progression = get_level_progress_from_lifetime_xp(total_xp)
    progression["lifetime_miles"] = round(lifetime_miles, 4)
    progression["lifetime_hours"] = round(lifetime_hours, 4)
    progression["lifetime_pickups_recorded"] = int(lifetime_pickups_recorded)
    progression["xp_breakdown"] = {
        "miles_xp": int(miles_xp),
        "hours_xp": int(hours_xp),
        "report_xp": int(report_xp),
        "game_xp": int(normalized_game_xp),
        "post_xp": int(social["post_xp"]),
        "comment_xp": int(social["comment_xp"]),
        "like_xp": int(social["like_xp"]),
    }
    return progression


def get_lifetime_totals_for_user(user_id: int) -> Dict[str, float]:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked,
          COALESCE(SUM(pickups_recorded), 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=?
        """,
        (int(user_id),),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
        "pickups": int(row["pickups_recorded"] or 0),
    }


def get_progression_for_user(user_id: int) -> Dict[str, Any]:
    now = int(time.time())
    uid = int(user_id)
    with _LEADERBOARD_RUNTIME_LOCK:
        _prune_user_ttl_cache(
            _PROGRESSION_BY_USER_CACHE,
            now=now,
            ttl_seconds=_PROGRESSION_CACHE_TTL_SECONDS,
            max_entries=_PROGRESSION_CACHE_MAX_ENTRIES,
        )
        cached = _PROGRESSION_BY_USER_CACHE.get(uid)
        if cached and (now - int(cached.get("cached_at_unix") or 0)) <= _PROGRESSION_CACHE_TTL_SECONDS:
            return dict(cached.get("payload") or {})
    by_user = get_progression_for_users([int(user_id)])
    progression = by_user.get(uid, build_progression_from_daily_stats_rows([]))
    with _LEADERBOARD_RUNTIME_LOCK:
        _PROGRESSION_BY_USER_CACHE[uid] = {"payload": dict(progression), "cached_at_unix": now}
    return progression


def _social_rows_for_users(user_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """Posts, comments and likes per driver per NYC day, in three queries.

    Three rather than one join: a driver with many posts and many likes would
    have the counts multiplied by each other in a single joined GROUP BY, and
    a driver missing from one table would drop out of the others.

    Every row is counted, including content that was later deleted. That
    reads wrong at first and is deliberate on both sides. Counting only live
    rows would mean deleting an old post silently takes a rank back -- a
    driver demoted for tidying up, with nothing on screen to explain it. And
    it cannot be farmed, because deleting does not free the row: post, delete,
    post again is two rows and still runs into the same daily cap.

    Wrapped, because XP is decoration on top of a network that has to keep
    working. A social table that is missing or slow costs the social part of a
    driver's XP for one request; it must never cost them the whole
    progression payload.
    """
    clean_user_ids = [int(uid) for uid in user_ids]
    if not clean_user_ids:
        return {}
    placeholders = ",".join(["?" for _ in clean_user_ids])
    # The same NYC day the driving stats are bucketed by, so one calendar day
    # of work and one of posting land on the same row and share the same caps.
    if DB_BACKEND == "postgres":
        day = "to_char(to_timestamp(created_at) AT TIME ZONE 'America/New_York', 'YYYY-MM-DD')"
    else:
        day = "date(created_at, 'unixepoch', 'localtime')"

    by_user_day: Dict[int, Dict[str, Dict[str, int]]] = {uid: {} for uid in clean_user_ids}
    sources = (
        ("posts", "posts"),
        ("comments", "post_comments"),
        ("likes", "post_likes"),
    )
    for field, table in sources:
        try:
            rows = _db_query_all(
                f"""
                SELECT user_id, {day} AS day_key, COUNT(*) AS n
                FROM {table}
                WHERE user_id IN ({placeholders})
                GROUP BY user_id, {day}
                """,
                tuple(clean_user_ids),
            ) or []
        except Exception:
            _LOGGER.warning("Could not read %s for progression", table, exc_info=True)
            continue
        for row in rows:
            uid = int(row["user_id"])
            day_key = str(row["day_key"])
            bucket = by_user_day.setdefault(uid, {}).setdefault(
                day_key, {"posts": 0, "comments": 0, "likes": 0})
            bucket[field] = int(row["n"] or 0)

    return {uid: list(days.values()) for uid, days in by_user_day.items()}


def get_progression_for_users(user_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean_user_ids = [int(uid) for uid in user_ids]
    if not clean_user_ids:
        return {}

    placeholders = ",".join(["?" for _ in clean_user_ids])
    rows = _db_query_all(
        f"""
        SELECT user_id,
               COALESCE(miles_worked, 0) AS miles_worked,
               COALESCE(hours_worked, 0) AS hours_worked,
               COALESCE(pickups_recorded, 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id IN ({placeholders})
        """,
        tuple(clean_user_ids),
    )

    rows_by_user: Dict[int, List[Dict[str, Any]]] = {uid: [] for uid in clean_user_ids}
    for row in rows:
        rows_by_user.setdefault(int(row["user_id"]), []).append(dict(row))
    try:
        game_rows = _db_query_all(
            f"""
            SELECT user_id, COALESCE(SUM(xp_awarded), 0) AS xp_total
            FROM game_xp_awards
            WHERE user_id IN ({placeholders})
            GROUP BY user_id
            """,
            tuple(clean_user_ids),
        )
    except sqlite3.OperationalError:
        game_rows = []
    game_xp_by_user = {int(row["user_id"]): int(row["xp_total"] or 0) for row in game_rows}
    social_by_user = _social_rows_for_users(clean_user_ids)
    progression_by_user = {
        uid: build_progression_from_daily_stats_rows(
            rows_by_user.get(uid, []),
            game_xp=int(game_xp_by_user.get(uid, 0)),
            social_rows=social_by_user.get(uid, []),
        )
        for uid in clean_user_ids
    }
    return progression_by_user


def _enrich_rows_with_progression(rows: List[Dict]) -> None:
    if not rows:
        return
    progression_by_user = get_progression_for_users([int(row["user_id"]) for row in rows])
    for row in rows:
        progression = progression_by_user.get(int(row["user_id"])) or {}
        row["level"] = progression.get("level")
        row["rank_name"] = progression.get("rank_name")
        row["rank_icon_key"] = progression.get("rank_icon_key")
        row["title"] = progression.get("title")


def _aggregate_rows(metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    bounds = current_period_bounds(period)
    metric_col = _metric_column(metric)
    rows = _db_query_all(
        f"""
        SELECT s.user_id,
               u.display_name,
               u.email,
               COALESCE(SUM(s.{metric_col}), 0) AS metric_value
        FROM driver_daily_stats s
        JOIN users u ON u.id = s.user_id
        WHERE s.nyc_date >= ? AND s.nyc_date <= ?
          AND {_leaderboard_active_user_where_sql("u")}
        GROUP BY s.user_id, u.display_name, u.email
        ORDER BY metric_value DESC, s.user_id ASC
        """,
        (bounds.start_date.isoformat(), bounds.end_date.isoformat()),
    )
    ranked: List[Dict] = []
    for idx, row in enumerate(rows, start=1):
        badge_code = _badge_for_rank(idx)
        ranked.append(
            {
                "user_id": int(row["user_id"]),
                "display_name": _display_name(dict(row)),
                "metric_value": round(float(row["metric_value"] or 0.0), 4),
                "rank_position": idx,
                "badge_code": badge_code,
            }
        )
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "rows": ranked}


def get_leaderboard(metric: LeaderboardMetric, period: LeaderboardPeriod, limit: int = 10) -> Dict:
    board = _aggregate_rows(metric, period)
    board["rows"] = board["rows"][: max(1, min(100, int(limit)))]
    _enrich_rows_with_progression(board["rows"])
    return board


def get_my_rank(user_id: int, metric: LeaderboardMetric, period: LeaderboardPeriod) -> Dict:
    bounds = current_period_bounds(period)
    metric_col = _metric_column(metric)

    my_totals = _db_query_one(
        f"""
        SELECT s.user_id,
               u.display_name,
               u.email,
               COALESCE(SUM(s.{metric_col}), 0) AS metric_value
        FROM driver_daily_stats s
        JOIN users u ON u.id = s.user_id
        WHERE s.user_id = ? AND s.nyc_date >= ? AND s.nyc_date <= ?
          AND {_leaderboard_active_user_where_sql("u")}
        GROUP BY s.user_id, u.display_name, u.email
        LIMIT 1
        """,
        (int(user_id), bounds.start_date.isoformat(), bounds.end_date.isoformat()),
    )

    if not my_totals:
        return {"metric": metric, "period": period, "period_key": bounds.period_key, "row": None}

    metric_value = float(my_totals["metric_value"] or 0.0)
    better_count_row = _db_query_one(
        f"""
        SELECT COUNT(*) AS better_count
        FROM (
          SELECT s.user_id, COALESCE(SUM(s.{metric_col}), 0) AS metric_value
          FROM driver_daily_stats s
          JOIN users u ON u.id = s.user_id
          WHERE s.nyc_date >= ? AND s.nyc_date <= ?
            AND {_leaderboard_active_user_where_sql("u")}
          GROUP BY s.user_id
        ) ranked
        WHERE ranked.metric_value > ?
           OR (ranked.metric_value = ? AND ranked.user_id < ?)
        """,
        (bounds.start_date.isoformat(), bounds.end_date.isoformat(), metric_value, metric_value, int(user_id)),
    )
    rank_position = int((better_count_row["better_count"] or 0) + 1)

    row = {
        "user_id": int(my_totals["user_id"]),
        "display_name": _display_name(dict(my_totals)),
        "metric_value": round(metric_value, 4),
        "rank_position": rank_position,
        "badge_code": _badge_for_rank(rank_position),
    }
    progression = get_progression_for_users([int(my_totals["user_id"])])
    row_progression = progression.get(int(my_totals["user_id"])) or {}
    row["level"] = row_progression.get("level")
    row["rank_name"] = row_progression.get("rank_name")
    row["rank_icon_key"] = row_progression.get("rank_icon_key")
    row["title"] = row_progression.get("title")
    return {"metric": metric, "period": period, "period_key": bounds.period_key, "row": row}


def refresh_current_badges() -> None:
    now = int(time.time())
    daily_bounds = current_period_bounds(LeaderboardPeriod.daily)
    weekly_bounds = current_period_bounds(LeaderboardPeriod.weekly)
    monthly_bounds = current_period_bounds(LeaderboardPeriod.monthly)
    yearly_bounds = current_period_bounds(LeaderboardPeriod.yearly)

    def _run(conn, cur):
        cur.execute(
            _sql("DELETE FROM leaderboard_badges_current WHERE metric<>? OR period<>?"),
            (LeaderboardMetric.miles.value, LeaderboardPeriod.daily.value),
        )
        cur.execute(
            _sql(
                f"""
                SELECT s.user_id,
                       COALESCE(SUM(s.miles_worked), 0) AS metric_value
                FROM driver_daily_stats s
                JOIN users u ON u.id = s.user_id
                WHERE s.nyc_date >= ? AND s.nyc_date <= ?
                  AND {_leaderboard_active_user_where_sql("u")}
                GROUP BY s.user_id
                ORDER BY metric_value DESC, s.user_id ASC
                """
            ),
            (daily_bounds.start_date.isoformat(), daily_bounds.end_date.isoformat()),
        )
        ranked_rows = list(cur.fetchall())
        cur.execute(
            _sql("DELETE FROM leaderboard_badges_current WHERE metric=? AND period=?"),
            (LeaderboardMetric.miles.value, LeaderboardPeriod.daily.value),
        )
        for rank_position, row in enumerate(ranked_rows[:3], start=1):
            normalized_badge_code = _normalized_badge_code(rank_position)
            if not normalized_badge_code:
                continue
            cur.execute(
                _sql(
                    """
                    INSERT INTO leaderboard_badges_current(user_id, metric, period, period_key, rank_position, badge_code, awarded_at, is_current)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, metric, period, period_key) DO UPDATE SET
                      rank_position=excluded.rank_position,
                      badge_code=excluded.badge_code,
                      awarded_at=excluded.awarded_at,
                      is_current=excluded.is_current
                    """
                ),
                (
                    int(row["user_id"]),
                    LeaderboardMetric.miles.value,
                    LeaderboardPeriod.daily.value,
                    daily_bounds.period_key,
                    rank_position,
                    normalized_badge_code,
                    now,
                    _bool_db_value(True),
                ),
            )
        cur.execute(_sql("SELECT COALESCE(MAX(updated_at), 0) AS max_updated_at FROM driver_daily_stats"))
        source_row = cur.fetchone()
        source_updated_at = int(source_row["max_updated_at"] or 0) if source_row else 0
        cur.execute(
            _sql(
                """
                INSERT INTO leaderboard_badges_refresh_state(id, daily_period_key, weekly_period_key, monthly_period_key, yearly_period_key, source_updated_at, refreshed_at)
                VALUES(1,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  daily_period_key=excluded.daily_period_key,
                  weekly_period_key=excluded.weekly_period_key,
                  monthly_period_key=excluded.monthly_period_key,
                  yearly_period_key=excluded.yearly_period_key,
                  source_updated_at=excluded.source_updated_at,
                  refreshed_at=excluded.refreshed_at
                """
            ),
            (
                daily_bounds.period_key,
                weekly_bounds.period_key,
                monthly_bounds.period_key,
                yearly_bounds.period_key,
                source_updated_at,
                now,
            ),
        )

    _db_run_in_transaction(_run)
    global _CURRENT_BADGES_LAST_REFRESH_TS
    with _LEADERBOARD_RUNTIME_LOCK:
        _CURRENT_BADGES_LAST_REFRESH_TS = now
        _CURRENT_BADGES_BY_USER_CACHE.clear()


def refresh_current_badges_if_needed(max_staleness_seconds: int = 30) -> None:
    global _CURRENT_BADGES_LAST_REFRESH_TS
    now = int(time.time())
    refresh_interval_seconds = max(
        1,
        int(max_staleness_seconds),
        int(_CURRENT_BADGES_MIN_REFRESH_INTERVAL_SECONDS),
    )
    with _LEADERBOARD_RUNTIME_LOCK:
        if now - int(_CURRENT_BADGES_LAST_REFRESH_TS) <= refresh_interval_seconds:
            return
    expected_keys = {
        "daily_period_key": current_period_bounds(LeaderboardPeriod.daily).period_key,
        "weekly_period_key": current_period_bounds(LeaderboardPeriod.weekly).period_key,
        "monthly_period_key": current_period_bounds(LeaderboardPeriod.monthly).period_key,
        "yearly_period_key": current_period_bounds(LeaderboardPeriod.yearly).period_key,
    }

    state = _db_query_one(
        """
        SELECT daily_period_key, weekly_period_key, monthly_period_key, yearly_period_key, source_updated_at, refreshed_at
        FROM leaderboard_badges_refresh_state
        WHERE id=1
        LIMIT 1
        """
    )
    if state:
        state_dict = dict(state)
        keys_match = all((state_dict.get(key) or "") == value for key, value in expected_keys.items())
        recently_refreshed = now - int(state_dict.get("refreshed_at") or 0) <= refresh_interval_seconds
        if keys_match and recently_refreshed:
            with _LEADERBOARD_RUNTIME_LOCK:
                _CURRENT_BADGES_LAST_REFRESH_TS = int(state_dict.get("refreshed_at") or now)
            return
    with _CURRENT_BADGES_REFRESH_LOCK:
        now_in_lock = int(time.time())
        with _LEADERBOARD_RUNTIME_LOCK:
            if now_in_lock - int(_CURRENT_BADGES_LAST_REFRESH_TS) <= refresh_interval_seconds:
                return
        state_in_lock = _db_query_one(
            """
            SELECT daily_period_key, weekly_period_key, monthly_period_key, yearly_period_key, source_updated_at, refreshed_at
            FROM leaderboard_badges_refresh_state
            WHERE id=1
            LIMIT 1
            """
        )
        if state_in_lock:
            state_in_lock_dict = dict(state_in_lock)
            keys_match = all((state_in_lock_dict.get(key) or "") == value for key, value in expected_keys.items())
            recently_refreshed = now_in_lock - int(state_in_lock_dict.get("refreshed_at") or 0) <= refresh_interval_seconds
            if keys_match and recently_refreshed:
                with _LEADERBOARD_RUNTIME_LOCK:
                    _CURRENT_BADGES_LAST_REFRESH_TS = int(state_in_lock_dict.get("refreshed_at") or now_in_lock)
                return
        refresh_current_badges()


def get_current_badges_for_user(user_id: int, refresh_if_needed: bool = True) -> List[Dict]:
    now = int(time.time())
    uid = int(user_id)
    with _LEADERBOARD_RUNTIME_LOCK:
        _prune_user_ttl_cache(
            _CURRENT_BADGES_BY_USER_CACHE,
            now=now,
            ttl_seconds=_CURRENT_BADGES_CACHE_TTL_SECONDS,
            max_entries=_CURRENT_BADGES_CACHE_MAX_ENTRIES,
        )
        cached = _CURRENT_BADGES_BY_USER_CACHE.get(uid)
        if cached and (now - int(cached.get("cached_at_unix") or 0)) <= _CURRENT_BADGES_CACHE_TTL_SECONDS:
            return list(cached.get("payload") or [])
    if refresh_if_needed:
        refresh_current_badges_if_needed()
    rows = _db_query_all(
        """
        SELECT metric, period, period_key, rank_position, badge_code
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3)
        ORDER BY awarded_at DESC, rank_position ASC
        """,
        (
            uid,
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
        ),
    )
    normalized_rows: List[Dict] = []
    for row in rows:
        item = dict(row)
        item["badge_code"] = _normalized_badge_code(int(item.get("rank_position") or 0), item.get("badge_code"))
        if item["badge_code"]:
            normalized_rows.append(item)
    with _LEADERBOARD_RUNTIME_LOCK:
        _CURRENT_BADGES_BY_USER_CACHE[uid] = {"payload": list(normalized_rows), "cached_at_unix": now}
    return normalized_rows


def get_leaderboard_runtime_snapshot() -> Dict[str, Any]:
    with _LEADERBOARD_RUNTIME_LOCK:
        badges_cache_entries = len(_CURRENT_BADGES_BY_USER_CACHE)
        progression_cache_entries = len(_PROGRESSION_BY_USER_CACHE)
        last_refresh_ts = int(_CURRENT_BADGES_LAST_REFRESH_TS)
    return {
        "current_badges_last_refresh_ts": last_refresh_ts,
        "current_badges_refresh_interval_seconds": int(_CURRENT_BADGES_MIN_REFRESH_INTERVAL_SECONDS),
        "current_badges_refresh_lock_active": bool(_CURRENT_BADGES_REFRESH_LOCK.locked()),
        "leaderboard_badges_cache_entries": badges_cache_entries,
        "leaderboard_progression_cache_entries": progression_cache_entries,
    }



def get_best_current_badge_for_user(user_id: int, refresh_if_needed: bool = True) -> Dict:
    if refresh_if_needed:
        refresh_current_badges_if_needed()
    rows = _db_query_all(
        """
        SELECT user_id, metric, period, period_key, rank_position
        FROM leaderboard_badges_current
        WHERE user_id=? AND is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3)
        """,
        (
            int(user_id),
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
        ),
    )
    if not rows:
        return {"leaderboard_badge_code": None}
    best = min((dict(row) for row in rows), key=lambda item: int(item.get("rank_position") or 999))
    return {"leaderboard_badge_code": _badge_for_rank(int(best.get("rank_position") or 0))}


def get_best_current_badges_for_users(user_ids: List[int], refresh_if_needed: bool = True) -> Dict[int, Dict]:
    if refresh_if_needed:
        refresh_current_badges_if_needed()
    if not user_ids:
        return {}
    placeholders = ",".join(["?" for _ in user_ids])
    rows = _db_query_all(
        f"""
        SELECT user_id, metric, period, period_key, rank_position
        FROM leaderboard_badges_current
        WHERE is_current=? AND metric=? AND period=? AND rank_position IN (1,2,3) AND user_id IN ({placeholders})
        """,
        (
            _bool_db_value(True),
            LeaderboardMetric.miles.value,
            LeaderboardPeriod.daily.value,
            *[int(u) for u in user_ids],
        ),
    )

    by_user: Dict[int, List[Dict]] = {}
    for row in rows:
        item = dict(row)
        uid = int(item["user_id"])
        by_user.setdefault(uid, []).append(item)

    out: Dict[int, Dict] = {}
    for uid, badges in by_user.items():
        best = min(badges, key=lambda item: int(item.get("rank_position") or 999))
        out[uid] = {"leaderboard_badge_code": _badge_for_rank(int(best.get("rank_position") or 0))}
    return out


def _sum_for_user(user_id: int, start_date: date, end_date: date) -> Dict:
    row = _db_query_one(
        """
        SELECT
          COALESCE(SUM(miles_worked), 0) AS miles_worked,
          COALESCE(SUM(hours_worked), 0) AS hours_worked,
          COALESCE(SUM(pickups_recorded), 0) AS pickups_recorded
        FROM driver_daily_stats
        WHERE user_id=? AND nyc_date >= ? AND nyc_date <= ?
        """,
        (int(user_id), start_date.isoformat(), end_date.isoformat()),
    )
    return {
        "miles": round(float(row["miles_worked"] or 0.0), 4),
        "hours": round(float(row["hours_worked"] or 0.0), 4),
        "pickups": int(row["pickups_recorded"] or 0),
    }


def get_overview_for_user(user_id: int) -> Dict:
    out: Dict[str, Dict[str, float]] = {}
    for period in [LeaderboardPeriod.daily, LeaderboardPeriod.weekly, LeaderboardPeriod.monthly, LeaderboardPeriod.yearly]:
        bounds = current_period_bounds(period)
        out[period.value] = _sum_for_user(user_id, bounds.start_date, bounds.end_date)
    return out
