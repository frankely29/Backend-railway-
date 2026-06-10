"""
NYC nightlife & dining DISTRICTS — pickup pulse on the map.

A parallel of long_trip_hotspot_builder.py, but for the after-dark crowd:
clusters of high-end restaurants AND bars/clubs that sit within a ~5-minute
walk of each other. The frontend draws one cocktail-glass pin per district and
pulses it during the **let-out window** — when diners finish dinner and the bar
crowd spills out (the best time to be parked nearby).

Same rules/characteristics as the dollar-flag system:
  - hand-curated POIs (no API key, no account — stable for years),
  - complete-link clustering at CLUSTER_RADIUS_MI (~5-min walk),
  - keep clusters of MIN_MEMBERS_PER_DISTRICT,
  - a per-district dim_schedule (peak/off/prime) recomputed at read time,
which differs in two ways:
  - a district must mix DINING and NIGHTLIFE (>=1 each) — not just 3 of one,
  - `prime` is the dinner-let-out -> last-call surge, with a `prime_weekend`
    that runs later on Fri/Sat. (dim_schedule hour ranges wrap past midnight.)

Build once via:  POST /admin/nightlife_districts/rebuild  (admin auth)
Read once via:   GET /nightlife_districts                 (user auth)
Refresh whenever you edit the POI list below.
"""

from __future__ import annotations

import json
import math
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

# (name, latitude, longitude, category, weight, address)
# Categories split into two groups (see _DINING / _NIGHTLIFE). A district must
# contain at least one of EACH to qualify. Weight is cosmetic (centroid + sort).
# Coordinates are picked tight: every venue in a district is within
# CLUSTER_RADIUS_MI of every other, so the district clusters cleanly.
NIGHTLIFE_POIS: List[Tuple[str, float, float, str, float, str]] = [
    # --- Meatpacking District (Manhattan) ---
    ("Pastis",                 40.7396, -74.0073, "upscale_restaurant", 2.4, "52 Gansevoort St"),
    ("Catch NYC",              40.7411, -74.0063, "upscale_restaurant", 2.4, "21 9th Ave"),
    ("STK Steakhouse",         40.7409, -74.0079, "upscale_restaurant", 2.2, "26 Little W 12th St"),
    ("RH Rooftop",             40.7404, -74.0066, "rooftop_bar",        2.1, "9 9th Ave"),
    ("Le Bain at The Standard",40.7409, -74.0081, "nightclub",          2.3, "444 W 13th St"),

    # --- Lower East Side (Manhattan) ---
    ("Dirty French",           40.7210, -73.9877, "upscale_restaurant", 2.3, "180 Ludlow St"),
    ("Beauty & Essex",         40.7201, -73.9868, "lounge",             2.2, "146 Essex St"),
    ("Double Chicken Please",  40.7192, -73.9896, "cocktail_bar",       2.4, "115 Allen St"),
    ("Tigre",                  40.7203, -73.9872, "cocktail_bar",       2.0, "143 Rivington St"),
    ("Mr. Purple",             40.7211, -73.9886, "rooftop_bar",        2.0, "180 Orchard St"),

    # --- SoHo / Nolita (Manhattan) ---
    ("Balthazar",              40.7227, -73.9980, "upscale_restaurant", 2.4, "80 Spring St"),
    ("Rubirosa",               40.7229, -73.9961, "upscale_restaurant", 2.2, "235 Mulberry St"),
    ("Peasant",                40.7233, -73.9951, "upscale_restaurant", 2.1, "194 Elizabeth St"),
    ("Mother's Ruin",          40.7212, -73.9954, "cocktail_bar",       2.0, "18 Spring St"),
    ("Spring Lounge",          40.7218, -73.9960, "cocktail_bar",       1.9, "48 Spring St"),

    # --- West Village (Manhattan) ---
    ("Via Carota",             40.7333, -74.0031, "upscale_restaurant", 2.4, "51 Grove St"),
    ("I Sodi",                 40.7338, -74.0049, "upscale_restaurant", 2.3, "314 Bleecker St"),
    ("Employees Only",         40.7340, -74.0065, "cocktail_bar",       2.3, "510 Hudson St"),
    ("The Garret",             40.7333, -74.0041, "cocktail_bar",       2.0, "296 Bleecker St"),
    ("Katana Kitten",          40.7350, -74.0064, "cocktail_bar",       2.2, "531 Hudson St"),

    # --- Flatiron / NoMad (Manhattan) ---
    ("Bazaar Meat by Jose Andres", 40.7447, -73.9886, "upscale_restaurant", 2.4, "1185 Broadway"),
    ("Zaytinya",               40.7449, -73.9885, "upscale_restaurant", 2.2, "1185 Broadway"),
    ("Nubeluz",                40.7448, -73.9886, "rooftop_bar",        2.2, "1185 Broadway"),
    ("230 Fifth Rooftop",      40.7445, -73.9882, "rooftop_bar",        2.2, "230 Fifth Ave"),
    ("Everdene",               40.7457, -73.9889, "cocktail_bar",       2.0, "1227 Broadway"),

    # --- Tribeca (Manhattan) ---
    ("Frenchette",             40.7196, -74.0052, "upscale_restaurant", 2.3, "241 W Broadway"),
    ("Batard",                 40.7195, -74.0053, "upscale_restaurant", 2.2, "239 W Broadway"),
    ("Tamarind Tribeca",       40.7188, -74.0058, "upscale_restaurant", 2.2, "99 Franklin St"),
    ("Brandy Library",         40.7202, -74.0067, "cocktail_bar",       2.2, "25 N Moore St"),
    ("Macao Trading Co.",      40.7194, -74.0040, "lounge",             2.1, "311 Church St"),

    # --- Williamsburg (Brooklyn) ---
    ("Le Crocodile",           40.7224, -73.9580, "upscale_restaurant", 2.3, "80 Wythe Ave"),
    ("Laser Wolf",             40.7218, -73.9588, "upscale_restaurant", 2.2, "97 Wythe Ave"),
    ("Bar Blondeau",           40.7224, -73.9580, "wine_bar",           2.2, "80 Wythe Ave"),
    ("The Ides Bar",           40.7224, -73.9580, "rooftop_bar",        2.1, "80 Wythe Ave"),
    ("Brooklyn Bowl",          40.7222, -73.9576, "nightclub",          2.0, "61 Wythe Ave"),
    ("Berry Park",             40.7232, -73.9569, "rooftop_bar",        2.0, "4 Berry St"),

    # --- Greenpoint (Brooklyn) ---
    ("Oxomoco",                40.7295, -73.9558, "upscale_restaurant", 2.4, "128 Greenpoint Ave"),
    ("Chez Ma Tante",          40.7275, -73.9560, "upscale_restaurant", 2.3, "90 Calyer St"),
    ("Ramona",                 40.7280, -73.9562, "cocktail_bar",       2.0, "113 Franklin St"),
    ("Diamond Lil",            40.7290, -73.9568, "cocktail_bar",       2.0, "179 Franklin St"),
    ("Black Rabbit",           40.7297, -73.9565, "cocktail_bar",       1.9, "91 Greenpoint Ave"),
]

# ~5-minute walk; same value the dollar-flag clusters use.
CLUSTER_RADIUS_MI = 0.25
# A district needs at least this many venues clustered together.
MIN_MEMBERS_PER_DISTRICT = 3

# The two groups. A qualifying district has >=1 from EACH (the user's
# "high-end restaurants AND nightclubs or bars").
_DINING = {"upscale_restaurant"}
_NIGHTLIFE = {"wine_bar", "cocktail_bar", "rooftop_bar", "lounge", "nightclub"}

# Dominant-category priority (low -> high). The dominant category is the
# highest-priority one present; it drives the label/rationale (the icon is the
# same magenta cocktail glass for every district).
_CATEGORY_PRIORITY = [
    "upscale_restaurant", "wine_bar", "cocktail_bar", "rooftop_bar",
    "lounge", "nightclub",
]

_CATEGORY_LABEL = {
    "upscale_restaurant": "upscale restaurant",
    "wine_bar": "wine bar",
    "cocktail_bar": "cocktail bar",
    "rooftop_bar": "rooftop bar",
    "lounge": "lounge",
    "nightclub": "nightclub",
}


# ---------------------------------------------------------------------------
# Time-of-day dim schedule — the let-out pulse window.
#
# Unlike the dollar-flag builder (one static schedule per category), a
# nightlife district's window is computed from its whole member set so the
# pulse spans dinner let-out -> the district's latest close. `prime` is the
# weeknight window; `prime_weekend` runs later (Fri/Sat). Hour ranges wrap
# past midnight (e.g. [20, 2] = 8pm-2am), which the frontend handles.
# ---------------------------------------------------------------------------
def _dim_schedule_for_members(categories: List[str]) -> Dict[str, Any]:
    cats = set(categories)
    if "nightclub" in cats:
        peak, off = [[18, 4]], [[5, 17]]
        prime, prime_weekend = [[20, 2]], [[20, 4]]
    elif cats & {"lounge", "rooftop_bar"}:
        peak, off = [[17, 2]], [[3, 16]]
        prime, prime_weekend = [[20, 2]], [[20, 3]]
    elif "cocktail_bar" in cats:
        peak, off = [[17, 1]], [[2, 16]]
        prime, prime_weekend = [[20, 1]], [[20, 2]]
    else:  # wine bar / restaurant only — dinner-forward
        peak, off = [[17, 0]], [[1, 16]]
        prime, prime_weekend = [[20, 23]], [[20, 0]]
    return {
        "peak": peak,
        "off": off,
        "weekday_only": False,
        "prime": prime,
        "prime_weekend": prime_weekend,
    }


def _best_hours_for_members(categories: List[str]) -> str:
    cats = set(categories)
    if "nightclub" in cats:
        return "Dinner let-out ~8pm; last call 1-3am (latest Fri/Sat)"
    if cats & {"lounge", "rooftop_bar"}:
        return "Dinner let-out ~8pm through ~2am (later Fri/Sat)"
    if "cocktail_bar" in cats:
        return "Dinner let-out ~8pm to ~1am (later Fri/Sat)"
    return "Dinner let-out ~8-11pm (later Fri/Sat)"


# ---------------------------------------------------------------------------
# Pure geo helpers (copied, like city_events, so the module is self-contained).
# ---------------------------------------------------------------------------
def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R_MI = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return R_MI * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _cluster_pois(
    pois: List[Tuple[str, float, float, str, float, str]],
    radius_mi: float = CLUSTER_RADIUS_MI,
) -> List[List[int]]:
    """Complete-link agglomerative clustering -> groups of POI indices.

    A POI joins a cluster only if it's within radius_mi of EVERY current
    member (max-distance constraint), preventing the chain failure mode where
    far-apart strips merge through an intermediate venue. O(N^2), fine here.
    """
    clusters: List[List[int]] = []
    for i in range(len(pois)):
        lat_i, lng_i = pois[i][1], pois[i][2]
        best_idx, best_max = -1, float("inf")
        for cidx, members in enumerate(clusters):
            ok, max_d = True, 0.0
            for m in members:
                d = haversine_miles(lat_i, lng_i, pois[m][1], pois[m][2])
                if d > radius_mi:
                    ok = False
                    break
                max_d = max(max_d, d)
            if ok and max_d < best_max:
                best_max, best_idx = max_d, cidx
        if best_idx >= 0:
            clusters[best_idx].append(i)
        else:
            clusters.append([i])
    return clusters


def _dominant_category(member_indices: List[int]) -> str:
    cats = {NIGHTLIFE_POIS[i][3] for i in member_indices}
    for c in reversed(_CATEGORY_PRIORITY):
        if c in cats:
            return c
    return "cocktail_bar"


def _district_label(member_indices: List[int]) -> str:
    """Highest-weight venue as the anchor name, '+N' for the rest."""
    members = sorted((NIGHTLIFE_POIS[i] for i in member_indices), key=lambda m: -m[4])
    anchor = members[0][0]
    extra = len(members) - 1
    return f"{anchor} +{extra}" if extra > 0 else anchor


def _category_summary(member_indices: List[int]) -> Tuple[Dict[str, int], str]:
    """Counts per category + a human 'N upscale restaurants + M cocktail bars'."""
    counts: Dict[str, int] = {}
    for i in member_indices:
        cat = NIGHTLIFE_POIS[i][3]
        counts[cat] = counts.get(cat, 0) + 1
    parts: List[str] = []
    for cat in _CATEGORY_PRIORITY:
        n = counts.get(cat, 0)
        if n:
            label = _CATEGORY_LABEL.get(cat, cat)
            parts.append(f"{n} {label}{'s' if n > 1 else ''}")
    return counts, " + ".join(parts)


def _qualifies(member_indices: List[int]) -> bool:
    """>= MIN_MEMBERS_PER_DISTRICT and a mix of dining AND nightlife."""
    if len(member_indices) < MIN_MEMBERS_PER_DISTRICT:
        return False
    cats = {NIGHTLIFE_POIS[i][3] for i in member_indices}
    return bool(cats & _DINING) and bool(cats & _NIGHTLIFE)


def _nearest_member_to_centroid(
    member_indices: List[int], lat_c: float, lng_c: float,
) -> Tuple[float, float]:
    best = member_indices[0]
    best_d = float("inf")
    for i in member_indices:
        d = haversine_miles(lat_c, lng_c, NIGHTLIFE_POIS[i][1], NIGHTLIFE_POIS[i][2])
        if d < best_d:
            best_d, best = d, i
    return NIGHTLIFE_POIS[best][1], NIGHTLIFE_POIS[best][2]


def district_runtime_meta(members: List[Dict[str, Any]]) -> Dict[str, Any]:
    """best_hours + dim_schedule + rationale + category_counts, recomputed at
    read time from the stored member list (pure function of the static tables
    above — so editing a schedule takes effect on the next GET, no rebuild)."""
    cats = [str(m.get("category", "")) for m in members if isinstance(m, dict)]
    counts: Dict[str, int] = {}
    for c in cats:
        counts[c] = counts.get(c, 0) + 1
    parts: List[str] = []
    for cat in _CATEGORY_PRIORITY:
        n = counts.get(cat, 0)
        if n:
            parts.append(f"{n} {_CATEGORY_LABEL.get(cat, cat)}{'s' if n > 1 else ''}")
    return {
        "best_hours": _best_hours_for_members(cats),
        "dim_schedule": _dim_schedule_for_members(cats),
        "rationale": " + ".join(parts),
        "category_counts": counts,
    }


def build_nightlife_districts() -> List[Dict[str, Any]]:
    """Cluster NIGHTLIFE_POIS into qualifying districts (>=3 members, mixed
    dining + nightlife). Returns dicts ready to store / serve."""
    groups = _cluster_pois(NIGHTLIFE_POIS, CLUSTER_RADIUS_MI)
    districts: List[Dict[str, Any]] = []
    next_id = 1
    for indices in groups:
        if not _qualifies(indices):
            continue
        total_w = lat_w = lng_w = 0.0
        members: List[Dict[str, Any]] = []
        for i in indices:
            name, lat, lng, cat, w, addr = NIGHTLIFE_POIS[i]
            total_w += w
            lat_w += lat * w
            lng_w += lng * w
            members.append({
                "name": name, "category": cat, "weight": w,
                "lat": lat, "lng": lng, "address": addr,
            })
        if total_w <= 0:
            continue
        members.sort(key=lambda m: -m["weight"])
        lat_centroid = lat_w / total_w
        lng_centroid = lng_w / total_w
        # Snap the pin to a real venue near the centroid (never mid-street).
        stand_lat, stand_lng = _nearest_member_to_centroid(indices, lat_centroid, lng_centroid)
        dom_cat = _dominant_category(indices)
        counts, rationale = _category_summary(indices)
        cats = [NIGHTLIFE_POIS[i][3] for i in indices]
        districts.append({
            "id": next_id,
            "lat": round(stand_lat, 6),
            "lng": round(stand_lng, 6),
            "centroid_lat": round(lat_centroid, 6),
            "centroid_lng": round(lng_centroid, 6),
            "label": _district_label(indices),
            "dominant_category": dom_cat,
            "member_count": len(indices),
            "total_weight": round(total_w, 3),
            "rationale": rationale,
            "category_counts": counts,
            "best_hours": _best_hours_for_members(cats),
            "dim_schedule": _dim_schedule_for_members(cats),
            "members": members,
        })
        next_id += 1
    districts.sort(key=lambda d: -d["total_weight"])
    return districts


def write_nightlife_districts(db_exec: Callable[..., Any]) -> Dict[str, Any]:
    """Build once, full-replace into the nightlife_districts table."""
    districts = build_nightlife_districts()
    now_unix = int(time.time())
    db_exec("DELETE FROM nightlife_districts")
    for d in districts:
        db_exec(
            """
            INSERT INTO nightlife_districts
                (id, lat, lng, label, dominant_category, member_count,
                 total_weight, members_json, generated_at_unix)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(d["id"]), float(d["lat"]), float(d["lng"]),
                str(d["label"]), str(d["dominant_category"]),
                int(d["member_count"]), float(d["total_weight"]),
                json.dumps(d["members"]), int(now_unix),
            ),
        )
    return {
        "districts_count": len(districts),
        "poi_count": len(NIGHTLIFE_POIS),
        "cluster_radius_mi": CLUSTER_RADIUS_MI,
        "generated_at_unix": now_unix,
    }
