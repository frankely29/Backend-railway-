"""
NYC long-trip generator POI dataset + zone scorer.

This module is the source of truth for which physical landmarks tend to
generate long trips (45+ min OR 10+ mi), and how strongly. The list is
hand-curated NYC knowledge — hospitals, airports, transit hubs, large
hotels, convention centers, and major event venues — with a weight per
category.

A one-time build pass walks every TLC pickup zone, finds the POIs within
a small radius of the zone centroid, and stores a single proximity score
per zone in Postgres. After that, the frontend just SELECTs the table to
overlay a "long-trip generator" indicator. No re-computation per request.

To refresh after editing the POI list:
  POST /admin/long_trip_generators/rebuild  (admin auth)
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# (name, latitude, longitude, category, weight)
# Weights reflect long-trip generation intensity by category type:
#   airport         5.0  — airports themselves are special-cased in
#                          the existing engine, but their PERIMETER
#                          zones still benefit from proximity
#   hospital        3.0  — long-distance medical transport, family
#                          visitors leaving outer-borough hospitals
#   transit_hub     2.5  — LIRR/Metro-North/PA terminals → long trips
#                          to outer boroughs and Long Island
#   hotel_luxury    2.5  — corporate guests with airport & long
#                          intercity transfers
#   convention      2.2  — Javits and similar attract long-trip
#                          attendees from the suburbs daily
#   stadium         1.8  — event-driven long-trip spikes
#   performance     1.5  — theater, opera, classical venues — Lincoln
#                          Center crowd often returns to suburbs
#   corporate       1.5  — financial / corporate hubs, mid-shift and
#                          end-of-day long trips
#   tourist         1.2  — flagship tourist destinations
NYC_LONG_TRIP_POIS: List[Tuple[str, float, float, str, float]] = [
    # ---------------- Airports ----------------
    ("JFK Airport (central)",     40.6413, -73.7781, "airport", 5.0),
    ("LaGuardia Airport",         40.7769, -73.8740, "airport", 5.0),
    ("Newark Liberty Airport",    40.6895, -74.1745, "airport", 5.0),

    # ---------------- Hospitals ----------------
    ("Mt Sinai Hospital (Main)",  40.7894, -73.9529, "hospital", 3.0),
    ("Mt Sinai West",             40.7706, -73.9876, "hospital", 3.0),
    ("Mt Sinai Morningside",      40.8044, -73.9609, "hospital", 3.0),
    ("Mt Sinai Beth Israel",      40.7325, -73.9824, "hospital", 3.0),
    ("NYU Langone Tisch",         40.7421, -73.9744, "hospital", 3.0),
    ("NYU Langone Brooklyn",      40.6928, -73.9787, "hospital", 3.0),
    ("Bellevue Hospital",         40.7392, -73.9759, "hospital", 3.0),
    ("Memorial Sloan Kettering",  40.7644, -73.9568, "hospital", 3.0),
    ("Hospital for Special Surgery", 40.7649, -73.9560, "hospital", 3.0),
    ("Lenox Hill Hospital",       40.7740, -73.9601, "hospital", 3.0),
    ("Columbia Presbyterian",     40.8418, -73.9419, "hospital", 3.0),
    ("NewYork-Presbyterian (Beekman/LM)", 40.7102, -74.0033, "hospital", 3.0),
    ("NewYork-Presbyterian Queens", 40.7656, -73.8268, "hospital", 3.0),
    ("Brooklyn Hospital Center",  40.6913, -73.9783, "hospital", 3.0),
    ("Maimonides Medical Center", 40.6363, -73.9931, "hospital", 3.0),
    ("Coney Island Hospital",     40.5832, -73.9534, "hospital", 3.0),
    ("Elmhurst Hospital",         40.7448, -73.8829, "hospital", 3.0),
    ("Queens Hospital Center",    40.7172, -73.7873, "hospital", 3.0),
    ("Cohen Children's Medical",  40.7626, -73.7212, "hospital", 3.0),
    ("Montefiore Medical (Bronx)", 40.8810, -73.8779, "hospital", 3.0),
    ("Lincoln Hospital (Bronx)",  40.8175, -73.9251, "hospital", 3.0),
    ("Staten Island University Hospital", 40.5832, -74.0884, "hospital", 3.0),

    # ---------------- Transit hubs ----------------
    ("Penn Station",              40.7506, -73.9935, "transit_hub", 2.5),
    ("Grand Central Terminal",    40.7527, -73.9772, "transit_hub", 2.5),
    ("Port Authority Bus Terminal", 40.7570, -73.9893, "transit_hub", 2.5),
    ("Atlantic Terminal (Brooklyn)", 40.6841, -73.9772, "transit_hub", 2.5),
    ("Jamaica Station (LIRR/AirTrain)", 40.7028, -73.8087, "transit_hub", 2.5),
    ("Hunters Point LIRR",        40.7421, -73.9396, "transit_hub", 2.0),
    ("Newark Penn Station",       40.7345, -74.1645, "transit_hub", 2.5),

    # ---------------- Hotels (large / luxury) ----------------
    ("Plaza Hotel",               40.7644, -73.9743, "hotel_luxury", 2.5),
    ("St Regis NY",               40.7615, -73.9742, "hotel_luxury", 2.5),
    ("Waldorf Astoria NY",        40.7560, -73.9744, "hotel_luxury", 2.5),
    ("The Pierre",                40.7676, -73.9719, "hotel_luxury", 2.3),
    ("Mandarin Oriental NY",      40.7686, -73.9819, "hotel_luxury", 2.5),
    ("Park Hyatt NY",             40.7659, -73.9817, "hotel_luxury", 2.3),
    ("Lotte NY Palace",           40.7585, -73.9742, "hotel_luxury", 2.3),
    ("Ritz-Carlton Central Park", 40.7659, -73.9776, "hotel_luxury", 2.5),
    ("Ritz-Carlton Battery Park", 40.7048, -74.0177, "hotel_luxury", 2.3),
    ("Four Seasons Tribeca",      40.7163, -74.0086, "hotel_luxury", 2.3),
    ("Marriott Marquis Times Sq", 40.7589, -73.9854, "hotel_luxury", 2.5),
    ("NY Hilton Midtown",         40.7621, -73.9789, "hotel_luxury", 2.5),
    ("Sheraton Times Square",     40.7625, -73.9826, "hotel_luxury", 2.3),
    ("Conrad NY Downtown",        40.7144, -74.0152, "hotel_luxury", 2.3),

    # ---------------- Convention / performance ----------------
    ("Javits Center",             40.7577, -74.0024, "convention", 2.2),
    ("Madison Square Garden",     40.7505, -73.9934, "performance", 1.8),
    ("Barclays Center",           40.6826, -73.9754, "stadium", 1.8),
    ("Lincoln Center",            40.7725, -73.9835, "performance", 1.5),
    ("Carnegie Hall",             40.7651, -73.9799, "performance", 1.5),
    ("Brooklyn Academy of Music", 40.6864, -73.9783, "performance", 1.5),
    ("Apollo Theater",            40.8102, -73.9505, "performance", 1.3),

    # ---------------- Stadiums ----------------
    ("Citi Field",                40.7571, -73.8458, "stadium", 1.8),
    ("Yankee Stadium",            40.8296, -73.9262, "stadium", 1.8),
    ("USTA Billie Jean King",     40.7500, -73.8458, "stadium", 1.8),

    # ---------------- Tourist / cultural ----------------
    ("Times Square",              40.7580, -73.9855, "tourist", 1.2),
    ("Metropolitan Museum of Art", 40.7794, -73.9632, "tourist", 1.2),
    ("MoMA",                      40.7614, -73.9776, "tourist", 1.2),
    ("Museum of Natural History", 40.7813, -73.9740, "tourist", 1.2),
    ("One World Trade Center",    40.7127, -74.0134, "tourist", 1.5),
    ("Brooklyn Bridge Park",      40.7000, -73.9967, "tourist", 1.0),

    # ---------------- Corporate / financial ----------------
    ("NYSE / Wall St",            40.7069, -74.0113, "corporate", 1.8),
    ("Hudson Yards",              40.7536, -74.0019, "corporate", 1.8),
    ("Rockefeller Center",        40.7587, -73.9787, "corporate", 1.5),
    ("Bryant Park (corporate)",   40.7536, -73.9832, "corporate", 1.3),
    ("Bloomberg Tower",           40.7587, -73.9686, "corporate", 1.3),
    ("One Vanderbilt",            40.7546, -73.9778, "corporate", 1.3),
]


# Default search radius around each zone centroid when scoring proximity.
# 0.4 mi is roughly a 6-block walk in Manhattan — close enough that the
# POI's foot traffic actually originates pickups in that zone, but wide
# enough to span small zones in dense areas.
DEFAULT_RADIUS_MI = 0.4


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in miles between two lat/lng points."""
    R_MI = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return R_MI * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def score_zone_from_pois(
    centroid_lat: float,
    centroid_lng: float,
    radius_mi: float = DEFAULT_RADIUS_MI,
) -> Dict[str, Any]:
    """
    Returns the proximity score + per-category breakdown for ONE zone.

    Score formula: sum over nearby POIs of `weight / (1 + 2*dist_mi/radius)`.
    The denominator gives:
      - distance 0     → divisor 1   (full weight)
      - distance radius → divisor 3   (one-third weight at the perimeter)
      - distance > radius → POI excluded entirely
    """
    if not (isinstance(centroid_lat, (int, float)) and isinstance(centroid_lng, (int, float))):
        return {"score": 0.0, "nearby_pois": [], "category_counts": {}}

    nearby: List[Dict[str, Any]] = []
    category_counts: Dict[str, int] = {}
    total = 0.0
    for name, lat, lng, category, weight in NYC_LONG_TRIP_POIS:
        d = haversine_miles(centroid_lat, centroid_lng, lat, lng)
        if d > radius_mi:
            continue
        decayed = weight / (1.0 + 2.0 * d / radius_mi)
        total += decayed
        nearby.append({
            "name": name,
            "category": category,
            "distance_mi": round(d, 3),
            "weight_applied": round(decayed, 3),
        })
        category_counts[category] = category_counts.get(category, 0) + 1

    nearby.sort(key=lambda r: r["distance_mi"])
    return {
        "score": round(total, 4),
        "nearby_pois": nearby,
        "category_counts": category_counts,
    }


def _tier_from_score(score: float, all_scores: List[float]) -> str:
    """Bucket the score into tiers based on the population distribution."""
    if score <= 0.0:
        return "none"
    sorted_nonzero = sorted([s for s in all_scores if s > 0.0])
    if not sorted_nonzero:
        return "none"
    idx = max(0, min(len(sorted_nonzero) - 1, int(0.50 * len(sorted_nonzero))))
    median = sorted_nonzero[idx]
    idx80 = max(0, min(len(sorted_nonzero) - 1, int(0.80 * len(sorted_nonzero))))
    p80 = sorted_nonzero[idx80]
    if score >= p80:
        return "high"
    if score >= median:
        return "medium"
    return "low"


def build_zone_long_trip_generator_scores(
    zones_geojson_path: Path,
    db_exec: Callable[..., Any],
    db_query_all: Callable[..., Any],
    radius_mi: float = DEFAULT_RADIUS_MI,
) -> Dict[str, Any]:
    """
    One-time computation.

    Walks every TLC zone, scores each by POI proximity, UPSERTs into
    `zone_long_trip_generator_scores`. Returns a summary dict.

    Idempotent — running it again with the same POI list produces the
    same rows (the only thing that changes is generated_at_unix).
    """
    from driver_guidance_engine import load_zone_centroid_lookup  # local import to avoid cycles

    centroids = load_zone_centroid_lookup(Path(zones_geojson_path))

    results: List[Tuple[int, Dict[str, Any]]] = []
    for zone_id, info in centroids.items():
        lat = info.get("centroid_lat")
        lng = info.get("centroid_lng")
        if lat is None or lng is None:
            results.append((int(zone_id), {"score": 0.0, "nearby_pois": [], "category_counts": {}}))
            continue
        results.append((int(zone_id), score_zone_from_pois(float(lat), float(lng), radius_mi)))

    all_scores = [r[1]["score"] for r in results]
    now_unix = _now_unix()

    # UPSERT each row. The query uses ? placeholders to match the
    # existing _db_exec adapter in main.py (works for both SQLite and
    # Postgres backends).
    for zone_id, scored in results:
        tier = _tier_from_score(scored["score"], all_scores)
        nearby_json = json.dumps(scored["nearby_pois"])
        cats_json = json.dumps(scored["category_counts"])
        db_exec(
            """
            INSERT INTO zone_long_trip_generator_scores
                (location_id, score, tier, nearby_pois_json, category_counts_json, radius_mi, generated_at_unix)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(location_id) DO UPDATE SET
                score = excluded.score,
                tier = excluded.tier,
                nearby_pois_json = excluded.nearby_pois_json,
                category_counts_json = excluded.category_counts_json,
                radius_mi = excluded.radius_mi,
                generated_at_unix = excluded.generated_at_unix
            """,
            (int(zone_id), float(scored["score"]), tier, nearby_json, cats_json, float(radius_mi), int(now_unix)),
        )

    total_with_score = sum(1 for r in results if r[1]["score"] > 0.0)
    return {
        "zones_total": len(results),
        "zones_with_score": total_with_score,
        "poi_count": len(NYC_LONG_TRIP_POIS),
        "radius_mi": radius_mi,
        "generated_at_unix": now_unix,
    }


def _now_unix() -> int:
    import time
    return int(time.time())
