"""Tests for the city_events feature (Ticketmaster + keyless sports -> map).

Runs fully offline: the normalizers are pure, the DB path uses the conftest
SQLite tmpdir, and the network fetch paths are not exercised (Ticketmaster is
tested only in its unconfigured no-key form; the MLB/NHL/NBA sports feeds are
validated at the normalizer level with inline sample payloads).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import city_events as ce
from core import _db_exec


# Realistic Ticketmaster Discovery API event shapes (trimmed to fields used).
SAMPLE_CONCERT = {
    "id": "vvG1zZ-concert",
    "name": "Some Band — Live at the Garden",
    "url": "https://www.ticketmaster.com/event/vvG1zZ-concert",
    "dates": {"start": {"localDate": "2026-06-08", "localTime": "20:00:00",
                        "dateTime": "2026-06-09T00:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Madison Square Garden",
                              "location": {"latitude": "40.7505", "longitude": "-73.9934"}}]},
}

SAMPLE_SPORTS = {
    "id": "vvG1zZ-sports",
    "name": "Yankees vs Red Sox",
    "url": "https://www.ticketmaster.com/event/vvG1zZ-sports",
    "dates": {"start": {"localDate": "2026-06-08", "localTime": "19:05:00",
                        "dateTime": "2026-06-08T23:05:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nE", "name": "Sports"}}],
    "_embedded": {"venues": [{"name": "Yankee Stadium",
                              "location": {"latitude": "40.8296", "longitude": "-73.9262"}}]},
}

SAMPLE_THEATRE = {  # Arts & Theatre segment -> not one of our 3 -> skipped
    "id": "vvG1zZ-theatre",
    "name": "A Broadway Show",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7na", "name": "Arts & Theatre"}}],
    "_embedded": {"venues": [{"name": "Some Theatre",
                              "location": {"latitude": "40.76", "longitude": "-73.98"}}]},
}

SAMPLE_NO_COORDS = {  # Music but venue has no location and is unknown -> skipped
    "id": "vvG1zZ-nocoords",
    "name": "Festival TBA",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Unknown Pop-up Lot"}]},
}

SAMPLE_FALLBACK_COORDS = {  # no location, but venue name matches the fallback table
    "id": "vvG1zZ-fallback",
    "name": "Concert at Barclays",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Barclays Center"}]},
}


def test_normalize_concert_and_sports():
    c = ce.normalize_event(SAMPLE_CONCERT)
    assert c is not None
    assert c["category"] == "concert"
    assert c["name"] == "Some Band — Live at the Garden"
    assert c["venue"] == "Madison Square Garden"
    assert abs(c["lat"] - 40.7505) < 1e-6 and abs(c["lng"] - (-73.9934)) < 1e-6
    assert c["source"] == "ticketmaster" and c["source_id"] == "vvG1zZ-concert"
    # dateTime "2026-06-09T00:00:00Z" -> exact unix
    expected = int(datetime(2026, 6, 9, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    assert c["start_at"] == expected

    s = ce.normalize_event(SAMPLE_SPORTS)
    assert s is not None and s["category"] == "sports" and s["venue"] == "Yankee Stadium"


def test_normalize_skips_unwanted_or_coordless():
    assert ce.normalize_event(SAMPLE_THEATRE) is None       # segment not requested
    assert ce.normalize_event(SAMPLE_NO_COORDS) is None      # no coordinates
    assert ce.normalize_event({}) is None                    # garbage
    assert ce.normalize_event({"id": "x"}) is None           # no name


def test_normalize_venue_coordinate_fallback():
    e = ce.normalize_event(SAMPLE_FALLBACK_COORDS)
    assert e is not None
    assert abs(e["lat"] - 40.6826) < 1e-6 and abs(e["lng"] - (-73.9754)) < 1e-6


def test_parse_start_localdate_fallback():
    # No dateTime -> localDate + localTime interpreted as NYC.
    ts = ce._parse_start_unix({"localDate": "2026-06-08", "localTime": "20:00:00"})
    assert ts is not None
    assert datetime.fromtimestamp(ts, ce.NYC_TZ).hour == 20


def test_fetch_returns_empty_without_key():
    # The test env has no TICKETMASTER_API_KEY, so the fetcher is dormant.
    assert ce.events_api_is_configured() is False
    assert ce.fetch_nyc_events_today() == []


def test_schema_upsert_select_roundtrip():
    ce.ensure_city_events_schema()
    _db_exec("DELETE FROM city_events")  # isolate this test
    now = int(time.time())
    rows = [
        {**ce.normalize_event(SAMPLE_CONCERT), "start_at": now - 600},   # 10 min ago
        {**ce.normalize_event(SAMPLE_SPORTS), "start_at": now - 1200},   # 20 min ago
    ]
    stored = ce.upsert_events(rows)
    assert stored == 2

    out = ce.select_events_for_today()
    names = {e["name"] for e in out["events"]}
    assert "Some Band — Live at the Garden" in names
    assert "Yankees vs Red Sox" in names
    one = next(e for e in out["events"] if e["category"] == "concert")
    assert set(one.keys()) >= {"id", "name", "category", "venue", "lat", "lng", "startAt", "endAt", "url"}
    assert one["startAt"] == now - 600 and one["endAt"] is None

    # Idempotent upsert: same source ids -> updated in place, still 2 rows.
    ce.upsert_events(rows)
    assert len(ce.select_events_for_today()["events"]) == 2


def test_prune_old_events():
    ce.ensure_city_events_schema()
    _db_exec("DELETE FROM city_events")
    now = int(time.time())
    ce.upsert_events([
        {**ce.normalize_event(SAMPLE_CONCERT), "source_id": "old-1",
         "start_at": now - ce.MAX_EVENT_SPAN_SECONDS - 3600},  # well past
        {**ce.normalize_event(SAMPLE_SPORTS), "source_id": "fresh-1", "start_at": now - 300},
    ])
    ce.prune_old_events()
    names = {e["name"] for e in ce.select_events_for_today()["events"]}
    assert "Yankees vs Red Sox" in names           # fresh kept
    assert "Some Band — Live at the Garden" not in names  # old pruned


# ---------------------------------------------------------------------------
# Keyless sports feeds (MLB / NHL / NBA) — realistic trimmed payload shapes.
# ---------------------------------------------------------------------------
MLB_YANKEES_HOME = {
    "gamePk": 717465,
    "gameDate": "2026-06-09T23:05:00Z",
    "status": {"detailedState": "Scheduled"},
    "teams": {
        "away": {"team": {"id": 111, "name": "Boston Red Sox"}},
        "home": {"team": {"id": 147, "name": "New York Yankees"}},
    },
    "venue": {"name": "Yankee Stadium"},
}
MLB_METS_HOME = {
    "gamePk": 717470,
    "gameDate": "2026-06-09T23:10:00Z",
    "status": {"detailedState": "Pre-Game"},
    "teams": {
        "away": {"team": {"id": 144, "name": "Atlanta Braves"}},
        "home": {"team": {"id": 121, "name": "New York Mets"}},
    },
    "venue": {"name": "Citi Field"},
}
MLB_AWAY = {  # Yankees on the road (home is Boston) -> skip
    "gamePk": 717466,
    "gameDate": "2026-06-09T23:05:00Z",
    "status": {"detailedState": "Scheduled"},
    "teams": {
        "away": {"team": {"id": 147, "name": "New York Yankees"}},
        "home": {"team": {"id": 111, "name": "Boston Red Sox"}},
    },
    "venue": {"name": "Fenway Park"},
}
MLB_POSTPONED = {  # our home team but postponed -> skip
    "gamePk": 717467,
    "gameDate": "2026-06-09T23:05:00Z",
    "status": {"detailedState": "Postponed"},
    "teams": {
        "away": {"team": {"id": 111, "name": "Boston Red Sox"}},
        "home": {"team": {"id": 147, "name": "New York Yankees"}},
    },
    "venue": {"name": "Yankee Stadium"},
}
MLB_UNKNOWN_VENUE = {  # in-scope home team but a venue with no coords -> skip
    "gamePk": 717468,
    "gameDate": "2026-06-09T23:05:00Z",
    "status": {"detailedState": "Scheduled"},
    "teams": {
        "away": {"team": {"id": 111, "name": "Boston Red Sox"}},
        "home": {"team": {"id": 147, "name": "New York Yankees"}},
    },
    "venue": {"name": "Spring Training Backfield 3"},
}

NHL_RANGERS_HOME = {
    "id": 2025020500,
    "startTimeUTC": "2026-06-09T23:00:00Z",
    "gameScheduleState": "OK",
    "venue": {"default": "Madison Square Garden"},
    "homeTeam": {"abbrev": "NYR", "placeName": {"default": "New York"},
                 "commonName": {"default": "Rangers"}},
    "awayTeam": {"abbrev": "BOS", "placeName": {"default": "Boston"},
                 "commonName": {"default": "Bruins"}},
}
NHL_AWAY = {  # Rangers on the road (home BOS) -> skip
    "id": 2025020501,
    "startTimeUTC": "2026-06-09T23:00:00Z",
    "gameScheduleState": "OK",
    "venue": {"default": "TD Garden"},
    "homeTeam": {"abbrev": "BOS", "placeName": {"default": "Boston"},
                 "commonName": {"default": "Bruins"}},
    "awayTeam": {"abbrev": "NYR", "placeName": {"default": "New York"},
                 "commonName": {"default": "Rangers"}},
}
NHL_PPD = {  # our home team but postponed -> skip
    "id": 2025020502,
    "startTimeUTC": "2026-06-09T23:00:00Z",
    "gameScheduleState": "PPD",
    "venue": {"default": "UBS Arena"},
    "homeTeam": {"abbrev": "NYI", "placeName": {"default": "New York"},
                 "commonName": {"default": "Islanders"}},
    "awayTeam": {"abbrev": "BOS", "placeName": {"default": "Boston"},
                 "commonName": {"default": "Bruins"}},
}

NBA_KNICKS_HOME = {
    "gameId": "0022500456",
    "gameDateTimeUTC": "2026-06-09T23:30:00Z",
    "gameStatusText": "7:30 pm ET",
    "arenaName": "Madison Square Garden",
    "homeTeam": {"teamTricode": "NYK", "teamCity": "New York", "teamName": "Knicks"},
    "awayTeam": {"teamTricode": "BOS", "teamCity": "Boston", "teamName": "Celtics"},
}
NBA_NETS_HOME = {
    "gameId": "0022500457",
    "gameDateTimeUTC": "2026-06-09T23:00:00Z",
    "gameStatusText": "7:00 pm ET",
    "arenaName": "Barclays Center",
    "homeTeam": {"teamTricode": "BKN", "teamCity": "Brooklyn", "teamName": "Nets"},
    "awayTeam": {"teamTricode": "MIA", "teamCity": "Miami", "teamName": "Heat"},
}
NBA_AWAY = {  # Knicks on the road (home BOS) -> skip
    "gameId": "0022500458",
    "gameDateTimeUTC": "2026-06-09T23:30:00Z",
    "gameStatusText": "7:30 pm ET",
    "arenaName": "TD Garden",
    "homeTeam": {"teamTricode": "BOS", "teamCity": "Boston", "teamName": "Celtics"},
    "awayTeam": {"teamTricode": "NYK", "teamCity": "New York", "teamName": "Knicks"},
}
NBA_PPD = {  # our home team but postponed -> skip
    "gameId": "0022500459",
    "gameDateTimeUTC": "2026-06-09T23:30:00Z",
    "gameStatusText": "PPD",
    "arenaName": "Madison Square Garden",
    "homeTeam": {"teamTricode": "NYK", "teamCity": "New York", "teamName": "Knicks"},
    "awayTeam": {"teamTricode": "BOS", "teamCity": "Boston", "teamName": "Celtics"},
}


def test_parse_iso_utc():
    expected = int(datetime(2026, 6, 9, 23, 5, 0, tzinfo=timezone.utc).timestamp())
    assert ce._parse_iso_utc("2026-06-09T23:05:00Z") == expected
    assert ce._parse_iso_utc("2026-06-09T23:05:00.000Z") == expected     # fractional secs
    assert ce._parse_iso_utc("2026-06-09T19:05:00-04:00") == expected    # explicit offset
    assert ce._parse_iso_utc("garbage") is None
    assert ce._parse_iso_utc("") is None
    assert ce._parse_iso_utc(None) is None


def test_normalize_mlb_home_games():
    y = ce.normalize_mlb_game(MLB_YANKEES_HOME)
    assert y is not None
    assert y["source"] == "mlb" and y["source_id"] == "717465"
    assert y["category"] == "sports"
    assert y["name"] == "Boston Red Sox at New York Yankees"
    assert y["venue"] == "Yankee Stadium"
    assert abs(y["lat"] - 40.8296) < 1e-6 and abs(y["lng"] - (-73.9262)) < 1e-6
    assert y["start_at"] == int(datetime(2026, 6, 9, 23, 5, 0, tzinfo=timezone.utc).timestamp())
    assert y["end_at"] is None

    m = ce.normalize_mlb_game(MLB_METS_HOME)
    assert m is not None and m["venue"] == "Citi Field"
    assert m["name"] == "Atlanta Braves at New York Mets"


def test_normalize_mlb_skips():
    assert ce.normalize_mlb_game(MLB_AWAY) is None           # away game
    assert ce.normalize_mlb_game(MLB_POSTPONED) is None      # postponed
    assert ce.normalize_mlb_game(MLB_UNKNOWN_VENUE) is None  # venue not in fallback
    assert ce.normalize_mlb_game({}) is None                 # garbage


def test_normalize_nhl_home_and_skips():
    r = ce.normalize_nhl_game(NHL_RANGERS_HOME)
    assert r is not None
    assert r["source"] == "nhl" and r["source_id"] == "2025020500"
    assert r["category"] == "sports" and r["venue"] == "Madison Square Garden"
    assert r["name"] == "Boston Bruins at New York Rangers"
    assert abs(r["lat"] - 40.7505) < 1e-6 and abs(r["lng"] - (-73.9934)) < 1e-6
    assert r["start_at"] == int(datetime(2026, 6, 9, 23, 0, 0, tzinfo=timezone.utc).timestamp())

    assert ce.normalize_nhl_game(NHL_AWAY) is None    # away game
    assert ce.normalize_nhl_game(NHL_PPD) is None     # postponed
    assert ce.normalize_nhl_game({}) is None


def test_normalize_nba_home_and_skips():
    k = ce.normalize_nba_game(NBA_KNICKS_HOME)
    assert k is not None
    assert k["source"] == "nba" and k["source_id"] == "0022500456"
    assert k["category"] == "sports" and k["venue"] == "Madison Square Garden"
    assert k["name"] == "Boston Celtics at New York Knicks"

    n = ce.normalize_nba_game(NBA_NETS_HOME)
    assert n is not None and n["venue"] == "Barclays Center"
    assert abs(n["lat"] - 40.6826) < 1e-6 and abs(n["lng"] - (-73.9754)) < 1e-6

    assert ce.normalize_nba_game(NBA_AWAY) is None    # away game
    assert ce.normalize_nba_game(NBA_PPD) is None     # postponed
    assert ce.normalize_nba_game({}) is None


def test_sports_upsert_select_roundtrip():
    ce.ensure_city_events_schema()
    _db_exec("DELETE FROM city_events")
    now = int(time.time())
    rows = [
        {**ce.normalize_mlb_game(MLB_YANKEES_HOME), "start_at": now - 600},
        {**ce.normalize_nhl_game(NHL_RANGERS_HOME), "start_at": now - 1200},
        {**ce.normalize_nba_game(NBA_NETS_HOME), "start_at": now - 1800},
    ]
    assert ce.upsert_events(rows) == 3

    out = ce.select_events_for_today()
    assert {e["category"] for e in out["events"]} == {"sports"}
    names = {e["name"] for e in out["events"]}
    assert "Boston Red Sox at New York Yankees" in names
    assert "Boston Bruins at New York Rangers" in names
    # camelCase serialization contract holds for sports rows too.
    sample = out["events"][0]
    assert set(sample.keys()) >= {"id", "name", "category", "venue", "lat", "lng", "startAt", "endAt", "url"}
    assert sample["endAt"] is None
