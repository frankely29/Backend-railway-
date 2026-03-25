from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

MILES_PER_DEGREE_LAT = 69.0


def _ring_area_sq_miles(ring: List[List[float]]) -> Optional[float]:
    if len(ring) < 4:
        return None

    lons = [float(pt[0]) for pt in ring]
    lats = [float(pt[1]) for pt in ring]
    lat0 = sum(lats) / len(lats)
    miles_per_degree_lon = MILES_PER_DEGREE_LAT * math.cos(math.radians(lat0))
    if miles_per_degree_lon <= 0:
        return None

    xy = [(lon * miles_per_degree_lon, lat * MILES_PER_DEGREE_LAT) for lon, lat in zip(lons, lats)]

    area2 = 0.0
    for i in range(len(xy) - 1):
        x1, y1 = xy[i]
        x2, y2 = xy[i + 1]
        area2 += (x1 * y2) - (x2 * y1)

    return abs(area2) * 0.5


def _polygon_area_sq_miles(coords: Any) -> Optional[float]:
    if not isinstance(coords, list) or not coords:
        return None

    outer = _ring_area_sq_miles(coords[0])
    if outer is None:
        return None

    holes = 0.0
    for hole in coords[1:]:
        hole_area = _ring_area_sq_miles(hole)
        if hole_area is None:
            continue
        holes += hole_area

    area = outer - holes
    return area if area > 0 else None


def _geometry_area_sq_miles(geometry: Dict[str, Any]) -> Optional[float]:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")

    if gtype == "Polygon":
        return _polygon_area_sq_miles(coords)

    if gtype == "MultiPolygon":
        if not isinstance(coords, list):
            return None
        total = 0.0
        has_any = False
        for polygon in coords:
            poly_area = _polygon_area_sq_miles(polygon)
            if poly_area is None:
                continue
            has_any = True
            total += poly_area
        return total if has_any and total > 0 else None

    return None


def compute_zone_area_sq_miles_from_geojson(zones_geojson_path: str | Path) -> Dict[int, Optional[float]]:
    path = Path(zones_geojson_path)
    payload = json.loads(path.read_text(encoding="utf-8"))

    out: Dict[int, Optional[float]] = {}
    for feature in payload.get("features", []):
        props = feature.get("properties") or {}
        raw_id = props.get("LocationID")
        if raw_id is None:
            continue
        try:
            zone_id = int(raw_id)
        except Exception:
            continue

        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue

        try:
            area = _geometry_area_sq_miles(geometry)
            out[zone_id] = area
        except Exception:
            out[zone_id] = None

    return out


def build_zone_geometry_metrics_rows(zones_geojson_path: str | Path) -> List[Dict[str, Optional[float]]]:
    zone_area = compute_zone_area_sq_miles_from_geojson(zones_geojson_path)
    rows: List[Dict[str, Optional[float]]] = []
    for zone_id in sorted(zone_area):
        rows.append(
            {
                "PULocationID": int(zone_id),
                "zone_area_sq_miles": None if zone_area[zone_id] is None else float(zone_area[zone_id]),
            }
        )
    return rows
