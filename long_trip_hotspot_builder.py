"""
NYC long-trip hotspot CLUSTERS — point pins on the map.

Different from zone-level overlays: this module produces a small list of
specific lat/lng points where hospitals + transit hubs + major hotels +
convention centers cluster. The frontend places one icon per cluster
centroid, with a tooltip listing the contributing landmarks.

Build once via:
  POST /admin/long_trip_hotspots/rebuild  (admin auth)

Read once via:
  GET /long_trip_hotspots  (user auth)

Refresh whenever you edit the POI list below.
"""

from __future__ import annotations

import json
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

# (name, latitude, longitude, category, weight)
# Categories ranked by long-trip-generation intensity. The icon shown on
# the map will use the dominant category in each cluster.
NYC_LONG_TRIP_POIS: List[Tuple[str, float, float, str, float]] = [
    # Airports
    ("JFK Airport (central)",     40.6413, -73.7781, "airport", 5.0),
    ("LaGuardia Airport",         40.7769, -73.8740, "airport", 5.0),
    ("Newark Liberty Airport",    40.6895, -74.1745, "airport", 5.0),

    # Hospitals
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
    ("NewYork-Presbyterian (LM)", 40.7102, -74.0033, "hospital", 3.0),
    ("NewYork-Presbyterian Queens", 40.7656, -73.8268, "hospital", 3.0),
    ("Brooklyn Hospital Center",  40.6913, -73.9783, "hospital", 3.0),
    ("Maimonides Medical Center", 40.6363, -73.9931, "hospital", 3.0),
    ("Coney Island Hospital",     40.5832, -73.9534, "hospital", 3.0),
    ("Elmhurst Hospital",         40.7448, -73.8829, "hospital", 3.0),
    ("Queens Hospital Center",    40.7172, -73.7873, "hospital", 3.0),
    ("Cohen Children's Medical",  40.7626, -73.7212, "hospital", 3.0),
    ("Montefiore Medical (Bronx)", 40.8810, -73.8779, "hospital", 3.0),
    ("Lincoln Hospital (Bronx)",  40.8175, -73.9251, "hospital", 3.0),
    ("Staten Island Univ Hospital", 40.5832, -74.0884, "hospital", 3.0),

    # Transit hubs
    ("Penn Station",              40.7506, -73.9935, "transit_hub", 2.5),
    ("Grand Central Terminal",    40.7527, -73.9772, "transit_hub", 2.5),
    ("Port Authority Bus Terminal", 40.7570, -73.9893, "transit_hub", 2.5),
    ("Atlantic Terminal (Brooklyn)", 40.6841, -73.9772, "transit_hub", 2.5),
    ("Jamaica Station (LIRR/AirTrain)", 40.7028, -73.8087, "transit_hub", 2.5),
    ("Hunters Point LIRR",        40.7421, -73.9396, "transit_hub", 2.0),
    ("Newark Penn Station",       40.7345, -74.1645, "transit_hub", 2.5),

    # Hotels
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

    # Convention / performance / stadium
    ("Javits Center",             40.7577, -74.0024, "convention", 2.2),
    ("Madison Square Garden",     40.7505, -73.9934, "performance", 1.8),
    ("Barclays Center",           40.6826, -73.9754, "stadium", 1.8),
    ("Lincoln Center",            40.7725, -73.9835, "performance", 1.5),
    ("Carnegie Hall",             40.7651, -73.9799, "performance", 1.5),
    ("Brooklyn Academy of Music", 40.6864, -73.9783, "performance", 1.5),
    ("Apollo Theater",            40.8102, -73.9505, "performance", 1.3),
    ("Citi Field",                40.7571, -73.8458, "stadium", 1.8),
    ("Yankee Stadium",            40.8296, -73.9262, "stadium", 1.8),
    ("USTA Billie Jean King",     40.7500, -73.8458, "stadium", 1.8),

    # Tourist / cultural
    ("Times Square",              40.7580, -73.9855, "tourist", 1.2),
    ("Metropolitan Museum of Art", 40.7794, -73.9632, "tourist", 1.2),
    ("MoMA",                      40.7614, -73.9776, "tourist", 1.2),
    ("Museum of Natural History", 40.7813, -73.9740, "tourist", 1.2),
    ("One World Trade Center",    40.7127, -74.0134, "tourist", 1.5),
    ("Brooklyn Bridge Park",      40.7000, -73.9967, "tourist", 1.0),

    # Corporate / financial
    ("NYSE / Wall St",            40.7069, -74.0113, "corporate", 1.8),
    ("Hudson Yards",              40.7536, -74.0019, "corporate", 1.8),
    ("Rockefeller Center",        40.7587, -73.9787, "corporate", 1.5),
    ("Bryant Park (corporate)",   40.7536, -73.9832, "corporate", 1.3),
    ("Bloomberg Tower",           40.7587, -73.9686, "corporate", 1.3),
    ("One Vanderbilt",            40.7546, -73.9778, "corporate", 1.3),
]


# Clustering radius: two POIs land in the same cluster only if they're
# within this distance of EACH OTHER (complete-link, not single-link —
# single-link chains POIs across long Manhattan stretches and produces
# one giant blob covering Penn → Grand Central → Times Sq). 0.30 mi is
# about a 5-6 minute walk and lets genuine concentrations (UES hospital
# row, Midtown East hotel cluster, Times Sq hotel cluster, Lincoln
# Center area) coalesce while still keeping Penn-area separate from
# Grand Central / Times Sq.
CLUSTER_RADIUS_MI = 0.30

# Minimum POIs in a cluster for it to count as a hotspot. The whole
# point is "a SPOT where 3+ important buildings are nearby" — single
# isolated landmarks and lone pairs aren't significant enough to mark
# on the map.
MIN_MEMBERS_PER_HOTSPOT = 3

# Category priority for the cluster-icon label. Higher index in this
# list wins when a cluster has multiple categories. Drivers care most
# about airports → hospitals → transit → hotels, so airports win the
# label if any airport POI is in the cluster.
_CATEGORY_PRIORITY: List[str] = [
    "tourist", "corporate", "performance", "stadium", "convention",
    "hotel_luxury", "transit_hub", "hospital", "airport",
]


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R_MI = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return R_MI * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _cluster_pois(
    pois: List[Tuple[str, float, float, str, float]],
    radius_mi: float = CLUSTER_RADIUS_MI,
) -> List[List[int]]:
    """
    Complete-link agglomerative clustering. Returns groups of POI indices.

    A POI joins an existing cluster only if it's within radius_mi of
    EVERY current member (max-distance constraint). This prevents the
    "chain" failure mode of single-link where Penn → Times Sq → Grand
    Central all merge through intermediate hotels.

    O(N^2) on the POI list — fine for ~70 POIs.
    """
    n = len(pois)
    # Each cluster is a list of POI indices. Start with no clusters.
    clusters: List[List[int]] = []

    # Visit POIs in order. For each, find the best existing cluster
    # it can join (all members within radius), or start a new cluster.
    for i in range(n):
        _, lat_i, lng_i, _, _ = pois[i]
        best_cluster_idx = -1
        best_max_dist = float("inf")
        for cidx, members in enumerate(clusters):
            # Compute the max distance from poi[i] to any current member.
            max_d = 0.0
            ok = True
            for m in members:
                _, lat_m, lng_m, _, _ = pois[m]
                d = haversine_miles(lat_i, lng_i, lat_m, lng_m)
                if d > radius_mi:
                    ok = False
                    break
                if d > max_d:
                    max_d = d
            if ok and max_d < best_max_dist:
                best_max_dist = max_d
                best_cluster_idx = cidx
        if best_cluster_idx >= 0:
            clusters[best_cluster_idx].append(i)
        else:
            clusters.append([i])

    return clusters


def _dominant_category(member_indices: List[int]) -> str:
    """Highest-priority category present in the cluster."""
    cats_in_cluster = {NYC_LONG_TRIP_POIS[i][3] for i in member_indices}
    for c in reversed(_CATEGORY_PRIORITY):
        if c in cats_in_cluster:
            return c
    return "tourist"


def _cluster_label(member_indices: List[int]) -> str:
    """Short human-readable label for the cluster pin."""
    members = [NYC_LONG_TRIP_POIS[i] for i in member_indices]
    if len(members) == 1:
        return members[0][0]
    # Find the highest-weight member as the anchor name.
    members.sort(key=lambda m: -m[4])
    anchor = members[0][0]
    extra = len(members) - 1
    return f"{anchor} +{extra}" if extra > 0 else anchor


def build_long_trip_hotspots() -> List[Dict[str, Any]]:
    """
    Returns the cluster pins as a list of dicts:
      {
        "id": int,
        "lat": float, "lng": float,    # weighted centroid
        "label": str,
        "dominant_category": str,
        "member_count": int,
        "total_weight": float,
        "members": [{"name": str, "category": str, "weight": float}, ...],
      }
    """
    groups = _cluster_pois(NYC_LONG_TRIP_POIS, CLUSTER_RADIUS_MI)
    hotspots: List[Dict[str, Any]] = []
    next_id = 1
    for indices in groups:
        # A hotspot requires MIN_MEMBERS_PER_HOTSPOT (3 by default)
        # POIs clustered together. Singletons and isolated pairs are
        # dropped — they're not significant enough to merit an icon.
        if len(indices) < MIN_MEMBERS_PER_HOTSPOT:
            continue
        # Weighted centroid: sum(lat * weight) / sum(weight), same for lng.
        total_w = 0.0
        lat_w = 0.0
        lng_w = 0.0
        members: List[Dict[str, Any]] = []
        for i in indices:
            name, lat, lng, cat, w = NYC_LONG_TRIP_POIS[i]
            total_w += w
            lat_w += lat * w
            lng_w += lng * w
            members.append({"name": name, "category": cat, "weight": w})
        if total_w <= 0:
            continue
        members.sort(key=lambda m: -m["weight"])
        hotspots.append({
            "id": next_id,
            "lat": round(lat_w / total_w, 6),
            "lng": round(lng_w / total_w, 6),
            "label": _cluster_label(indices),
            "dominant_category": _dominant_category(indices),
            "member_count": len(indices),
            "total_weight": round(total_w, 3),
            "members": members,
        })
        next_id += 1
    # Sort by total_weight descending — drivers see the strongest
    # generators first if there's any list view.
    hotspots.sort(key=lambda h: -h["total_weight"])
    return hotspots


def write_long_trip_hotspots(
    db_exec: Callable[..., Any],
) -> Dict[str, Any]:
    """
    Build once, UPSERT into long_trip_hotspots table. Returns a summary.
    """
    import time
    hotspots = build_long_trip_hotspots()
    now_unix = int(time.time())

    # Clear the table first — cluster IDs aren't stable across POI list
    # edits (adding one POI can merge/split clusters), so a full replace
    # is safer than UPSERT-by-id.
    db_exec("DELETE FROM long_trip_hotspots")
    for h in hotspots:
        db_exec(
            """
            INSERT INTO long_trip_hotspots
                (id, lat, lng, label, dominant_category, member_count,
                 total_weight, members_json, generated_at_unix)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(h["id"]), float(h["lat"]), float(h["lng"]),
                str(h["label"]), str(h["dominant_category"]),
                int(h["member_count"]), float(h["total_weight"]),
                json.dumps(h["members"]), int(now_unix),
            ),
        )

    return {
        "hotspots_count": len(hotspots),
        "poi_count": len(NYC_LONG_TRIP_POIS),
        "cluster_radius_mi": CLUSTER_RADIUS_MI,
        "generated_at_unix": now_unix,
    }
