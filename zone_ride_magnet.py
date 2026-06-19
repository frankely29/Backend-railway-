"""Per-zone OSM ride-magnet anchoring.

Curated strategic clusters cover ~36 high-value zones and live pickup density
covers zones with enough recorded pickups -- but a driver can stand in any of
260+ zones, and the rest fall back to vague "this area is working" wording.

A residential/secondary zone still has a *structural* ride magnet: the local
subway/rail hub, the mall, the hospital, the campus. Those are exactly where
rides originate, and they are knowable from OpenStreetMap. This module picks the
single best ride magnet inside a zone from an Overpass result set (pure ranking,
no network here -- the caller does the HTTP and caches the answer per zone) and
phrases it honestly for the guidance directive.
"""

import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

# Significance of each magnet class as a rideshare origin in a residential zone.
# Real rail/subway hubs win: that's where riders without cars start trips. Bare
# public_transport=station nodes (often a bus stop or a university shuttle desk
# mislabeled in OSM) are a weak signal, so they sit below malls/hospitals and
# only win when nothing better is in the zone.
_KIND_BASE_SCORE = {
    "rail": 1.00,
    "mall": 0.86,
    "hospital": 0.80,
    "university": 0.74,
    "transit_minor": 0.62,
    "attraction": 0.50,
}

# Honest descriptor per class -- never implies pickup data we don't have.
_KIND_DESCRIPTOR = {
    "rail": "the local transit hub",
    "mall": "a steady ride magnet",
    "hospital": "a steady ride source",
    "university": "a campus ride magnet",
    "transit_minor": "the local transit stop",
    "attraction": "a visitor draw",
}

# Names that are not real ride magnets even when OSM tags them as a station:
# university shuttle desks, parking, depots, lone "office" nodes, etc.
_NAME_DENYLIST = (
    "ram van", "shuttle", "parking", "depot", "bus depot", "bus stop",
    "park and ride", "park & ride", "layover",
)

_RADIUS_DEFAULT_M = 750


def build_ride_magnet_overpass_query(lat: float, lng: float, radius_m: int = _RADIUS_DEFAULT_M) -> str:
    """Overpass QL for ride-generating features near a point."""
    r = int(radius_m)
    a = f"(around:{r},{float(lat)},{float(lng)})"
    parts = [
        f'node["railway"="station"]{a};',
        f'node["station"="subway"]{a};',
        f'node["public_transport"="station"]{a};',
        f'node["railway"="halt"]{a};',
        f'way["shop"="mall"]{a};',
        f'node["shop"="mall"]{a};',
        f'way["shop"="department_store"]{a};',
        f'way["amenity"="hospital"]{a};',
        f'node["amenity"="hospital"]{a};',
        f'way["amenity"="university"]{a};',
        f'way["tourism"="attraction"]{a};',
    ]
    return "[out:json][timeout:25];(" + "".join(parts) + ");out center tags;"


def _haversine_mi(la1: float, lo1: float, la2: float, lo2: float) -> float:
    R = 3958.8
    p1, p2 = math.radians(la1), math.radians(la2)
    dp = math.radians(la2 - la1)
    dl = math.radians(lo2 - lo1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _classify(tags: Mapping[str, Any]) -> Optional[str]:
    # Real rail/subway is the strong signal; bare public_transport=station is weak.
    if tags.get("railway") in ("station", "halt") or tags.get("station") == "subway":
        return "rail"
    if tags.get("shop") in ("mall", "department_store"):
        return "mall"
    if tags.get("amenity") == "hospital":
        return "hospital"
    if tags.get("amenity") == "university":
        return "university"
    if tags.get("public_transport") == "station":
        return "transit_minor"
    if tags.get("tourism") == "attraction":
        return "attraction"
    return None


def _element_lat_lng(el: Mapping[str, Any]):
    lat, lng = el.get("lat"), el.get("lon")
    if lat is None:
        ctr = el.get("center") or {}
        lat, lng = ctr.get("lat"), ctr.get("lon")
    if lat is None or lng is None:
        return None
    try:
        return float(lat), float(lng)
    except Exception:
        return None


def select_ride_magnet(
    elements: Sequence[Mapping[str, Any]],
    *,
    center_lat: float,
    center_lng: float,
    zone_geom: Any = None,
    radius_mi: float = 0.7,
) -> Optional[Dict[str, Any]]:
    """Pick the strongest ride magnet for a zone from an Overpass result set.

    Scores each named, classified feature by class significance * proximity to
    the zone's representative point, with a strong bonus for sitting inside the
    zone polygon. Returns the winner (label/lat/lng/kind/descriptor) or None.
    """
    point_cls = None
    if zone_geom is not None:
        try:
            from shapely.geometry import Point as point_cls  # type: ignore
        except Exception:
            point_cls = None

    best: Optional[Dict[str, Any]] = None
    best_score = 0.0
    seen_names: Dict[str, float] = {}

    for el in elements or []:
        tags = el.get("tags") or {}
        name = str(tags.get("name") or "").strip()
        if not name:
            continue
        low = name.lower()
        if any(bad in low for bad in _NAME_DENYLIST):
            continue
        kind = _classify(tags)
        if kind is None:
            continue
        ll = _element_lat_lng(el)
        if ll is None:
            continue
        elat, elng = ll
        dist = _haversine_mi(center_lat, center_lng, elat, elng)
        if dist > max(0.15, float(radius_mi)) * 1.6:
            continue
        inside = True
        if zone_geom is not None and point_cls is not None:
            try:
                inside = bool(zone_geom.covers(point_cls(elng, elat)))
            except Exception:
                inside = True
        base = _KIND_BASE_SCORE.get(kind, 0.5)
        proximity = 1.0 / (1.0 + dist * 1.5)
        score = base * proximity * (1.0 if inside else 0.4)
        # Collapse duplicate-named features (e.g. multi-entrance stations).
        prev = seen_names.get(name)
        if prev is not None and prev >= score:
            continue
        seen_names[name] = score
        if score > best_score:
            best_score = score
            best = {
                "label": name,
                "lat": round(elat, 6),
                "lng": round(elng, 6),
                "kind": kind,
                "descriptor": _KIND_DESCRIPTOR.get(kind, "a steady ride magnet"),
                "score": round(score, 4),
                "inside_zone": inside,
            }
    return best
