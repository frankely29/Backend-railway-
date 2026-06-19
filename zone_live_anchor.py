"""Live pickup micro-anchor selection for the guidance directive.

Only ~36 zones carry a curated strategic-POI cluster, but a driver can be
standing in any of 260+ zones. When the recommended zone has no curated
cluster, the directive used to fall back to a vague "this area is working".

This module derives a SPECIFIC spot for those zones from the live pickup
density in ``pickup_logs`` -- the very same machinery that paints the
per-zone pickup hotspots on the map -- so the text and the map agree on
where the busiest corner is. A short, driver-readable label is formatted
from a reverse-geocode result (see ``format_anchor_label``); the network
call itself lives in the caller so this module stays pure and testable.
"""

from typing import Any, Dict, Mapping, Optional, Sequence

from pickup_hotspot_intelligence import (
    build_zone_historical_anchor_components,
    build_zone_historical_anchor_points,
    _TO_4326,
)

# Below this much weighted pickup support the density is too thin to honestly
# point a driver at a corner -- caller keeps the generic zone-level wording.
LIVE_ANCHOR_MIN_WEIGHTED_SUPPORT = 2.5
LIVE_ANCHOR_MIN_POINTS = 4

# Address parts that are streets rather than landmarks. A landmark name
# ("Court Square", "Queens Center") is a better cue than the street it sits on.
_ROADLIKE_TYPES = {
    "road", "residential", "tertiary", "secondary", "primary", "trunk",
    "motorway", "footway", "pedestrian", "service", "unclassified",
    "living_street", "cycleway", "path", "track",
}


def select_zone_live_anchor(
    *,
    zone_id: int,
    zone_geom: Any,
    pickup_rows: Sequence[Mapping[str, Any]],
    frame_time: int,
    min_weighted_support: float = LIVE_ANCHOR_MIN_WEIGHTED_SUPPORT,
    min_points: int = LIVE_ANCHOR_MIN_POINTS,
) -> Optional[Dict[str, Any]]:
    """Return the busiest live pickup micro-anchor in a zone, or None.

    None means the zone's pickup density is too thin (or absent) to name a
    spot, in which case the caller should keep zone-level wording.
    """
    if zone_geom is None or not pickup_rows:
        return None
    points = build_zone_historical_anchor_points(
        pickup_rows=pickup_rows, frame_time=int(frame_time)
    )
    if not points:
        return None
    components = build_zone_historical_anchor_components(
        zone_id=int(zone_id), zone_geom=zone_geom, weighted_points=points
    )
    if not components:
        return None
    top = components[0]  # already sorted strongest-first by the builder
    support = float(top.get("weighted_point_count") or 0.0)
    point_count = int(top.get("point_count") or 0)
    if support < float(min_weighted_support) or point_count < int(min_points):
        return None
    try:
        lng, lat = _TO_4326.transform(float(top["centroid_x"]), float(top["centroid_y"]))
    except Exception:
        return None
    return {
        "zone_id": int(zone_id),
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "weighted_support": round(support, 3),
        "point_count": point_count,
        "component_score": round(float(top.get("component_score") or 0.0), 3),
    }


def format_anchor_label(geo: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Short, driver-readable place label from a Nominatim reverse payload.

    Prefers a recognizable landmark name, else the street (with neighbourhood
    for context), else the neighbourhood. Returns None when nothing usable is
    present so the caller can fall back to generic wording.
    """
    if not geo or not isinstance(geo, Mapping):
        return None
    addr = geo.get("address") or {}
    if not isinstance(addr, Mapping):
        addr = {}
    road = str(
        addr.get("road") or addr.get("pedestrian") or addr.get("footway") or ""
    ).strip()
    nbhd = str(
        addr.get("neighbourhood") or addr.get("suburb") or addr.get("quarter") or ""
    ).strip()
    name = str(geo.get("name") or "").strip()
    addr_type = str(geo.get("addresstype") or geo.get("type") or "").strip().lower()

    def with_area(label: str) -> str:
        if nbhd and nbhd.lower() not in label.lower():
            return f"{label} ({nbhd})"
        return label

    # A named landmark (station, mall, park) is the most recognizable cue.
    if name and addr_type not in _ROADLIKE_TYPES and name.lower() != road.lower():
        return with_area(name)
    if road:
        return with_area(road)
    if name:
        return name
    if nbhd:
        return nbhd
    return None
