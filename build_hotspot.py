from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json
import math
import os
import logging
import shutil
import tempfile
import zipfile
import duckdb

from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_geometry_metrics import build_zone_geometry_metrics_rows
from zone_mode_profiles import ZONE_MODE_PROFILES, validate_zone_mode_profiles_for_live_engine
from artifact_freshness import build_expected_artifact_signature

logger = logging.getLogger(__name__)

def ensure_zones_geojson(data_dir: Path, force: bool = False) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    geojson_path = data_dir / "taxi_zones.geojson"
    if geojson_path.exists() and geojson_path.stat().st_size > 0 and not force:
        return geojson_path
    zip_path = data_dir / "taxi_zones.zip"
    if zip_path.exists() and zip_path.is_file():
        try:
            with zipfile.ZipFile(zip_path, "r") as archive:
                if "taxi_zones.geojson" in archive.namelist():
                    with archive.open("taxi_zones.geojson", "r") as src, geojson_path.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
                    if geojson_path.exists() and geojson_path.stat().st_size > 0:
                        return geojson_path
        except Exception:
            pass
    raise RuntimeError("Missing /data/taxi_zones.geojson. Upload it via POST /upload_zones_geojson.")


def bucket_and_color_from_rating(rating: int) -> tuple[str, str]:
    """
    STRICT bucket order requested:
      Green  = Highest
      Purple = High
      Blue   = Medium
      Sky    = Normal
      Yellow = Below Normal
      Red    = Very Low / Avoid
    """
    r = int(rating)

    if r >= 87:
        return "green", "#00b050"
    if r >= 73:
        return "purple", "#8000ff"
    if r >= 60:
        return "indigo", "#4b3cff"
    if r >= 48:
        return "blue", "#0066ff"
    if r >= 40:
        return "sky", "#66ccff"
    if r >= 33:
        return "yellow", "#ffd400"
    if r >= 25:
        return "orange", "#ff8c00"
    return "red", "#e60000"


BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS = {
    41, 42, 74, 75, 116, 127, 128, 151, 152, 166, 243, 244
}
AIRPORT_ZONE_IDS = {1, 132, 138}
V3_PROFILE_CONFIG = {
    "citywide_v3": {
        "score": "earnings_shadow_score_citywide_v3_anchor_shadow",
        "confidence": "earnings_shadow_confidence_citywide_v3",
    },
    "manhattan_v3": {
        "score": "earnings_shadow_score_raw_manhattan_v3",
        "confidence": "earnings_shadow_confidence_manhattan_v3",
    },
    "bronx_wash_heights_v3": {
        "score": "earnings_shadow_score_raw_bronx_wash_heights_v3",
        "confidence": "earnings_shadow_confidence_bronx_wash_heights_v3",
    },
    "queens_v3": {
        "score": "earnings_shadow_score_raw_queens_v3",
        "confidence": "earnings_shadow_confidence_queens_v3",
    },
    "brooklyn_v3": {
        "score": "earnings_shadow_score_raw_brooklyn_v3",
        "confidence": "earnings_shadow_confidence_brooklyn_v3",
    },
    "staten_island_v3": {
        "score": "earnings_shadow_score_raw_staten_island_v3",
        "confidence": "earnings_shadow_confidence_staten_island_v3",
    },
}


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _normalized_borough_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalized_zone_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def is_airport_zone(location_id: Any, zone_name: Any, borough_name: Any) -> bool:
    try:
        if int(location_id) in AIRPORT_ZONE_IDS:
            return True
    except Exception:
        pass
    normalized_text = " ".join(
        part for part in (
            _normalized_zone_text(zone_name),
            _normalized_borough_name(borough_name),
        ) if part
    )
    return any(token in normalized_text for token in ("airport", "jfk", "la guardia", "laguardia", "newark", "ewr"))


def _is_airport_props(props: Dict[str, Any]) -> bool:
    return is_airport_zone(
        props.get("LocationID", 0),
        props.get("zone_name"),
        props.get("borough"),
    )


def _set_airport_excluded_profile_state(props: Dict[str, Any], profile_name: str) -> None:
    visible_bucket, visible_color = bucket_and_color_from_rating(1)
    props[f"earnings_shadow_visible_rank_{profile_name}"] = 0.0
    props[f"earnings_shadow_visible_base_score_{profile_name}"] = 0.0
    props[f"earnings_shadow_visible_score_{profile_name}"] = 0.0
    props[f"earnings_shadow_rating_{profile_name}"] = 1
    props[f"earnings_shadow_bucket_{profile_name}"] = visible_bucket
    props[f"earnings_shadow_color_{profile_name}"] = visible_color


def _apply_airport_exclusion_state(props: Dict[str, Any]) -> None:
    if not _is_airport_props(props):
        return
    props["airport_excluded"] = True
    props["citywide_visual_anchor_discount_factor_shadow"] = 1.0
    props["earnings_shadow_citywide_anchor_input_v3"] = 0.0
    props["earnings_shadow_citywide_anchor_base_v3"] = 0.0
    props["earnings_shadow_citywide_anchor_display_v3"] = 0.0
    props["earnings_shadow_citywide_anchor_norm_v3"] = 0.0
    for profile_name in V3_PROFILE_CONFIG:
        _set_airport_excluded_profile_state(props, profile_name)


def _is_airport_zone(zone_name: Any, location_id: int, borough_name: Any) -> bool:
    return is_airport_zone(location_id, zone_name, borough_name)


def _iter_geometry_points(geometry: Any):
    if not isinstance(geometry, dict):
        return
    coords = geometry.get("coordinates")
    gtype = str(geometry.get("type") or "")
    if not coords:
        return
    if gtype == "Point":
        if len(coords) >= 2:
            yield float(coords[0]), float(coords[1])
        return

    def _walk(node: Any):
        if isinstance(node, (list, tuple)):
            if len(node) >= 2 and isinstance(node[0], (int, float)) and isinstance(node[1], (int, float)):
                yield float(node[0]), float(node[1])
            else:
                for child in node:
                    yield from _walk(child)

    yield from _walk(coords)


def _geometry_centroid_latitude(geometry: Any) -> float | None:
    points = list(_iter_geometry_points(geometry))
    if not points:
        return None
    return sum(lat for _, lat in points) / len(points)


def _is_core_manhattan_for_citywide_discount(props: Dict[str, Any], geometry: Any) -> bool:
    borough = _normalized_borough_name(props.get("borough"))
    if "manhattan" not in borough:
        return False
    location_id = int(props.get("LocationID") or 0)
    if location_id in BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS:
        return False
    centroid_lat = _geometry_centroid_latitude(geometry)
    return centroid_lat is not None and centroid_lat <= 40.795


def _relaxed_display_curve(x: float) -> float:
    x = _clamp01(x)
    return _clamp01(0.62 * x + 0.38 * (x ** 0.5))


def _eligible_for_profile(profile_name: str, props: Dict[str, Any], geometry: Any) -> bool:
    location_id = int(props.get("LocationID", 0))
    borough = _normalized_borough_name(props.get("borough"))
    zone_name = props.get("zone_name")
    centroid_lat = _geometry_centroid_latitude(geometry)
    if _is_airport_zone(zone_name, location_id, borough):
        return False

    if profile_name == "citywide_v3":
        return props.get("earnings_shadow_score_citywide_v3") is not None
    if profile_name == "staten_island_v3":
        return "staten" in borough
    if profile_name == "brooklyn_v3":
        return "brooklyn" in borough
    if profile_name == "queens_v3":
        return "queens" in borough
    if profile_name == "bronx_wash_heights_v3":
        return ("bronx" in borough) or (location_id in BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS)
    if profile_name == "manhattan_v3":
        if "manhattan" not in borough:
            return False
        if location_id in BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS:
            return False
        return centroid_lat is not None and centroid_lat <= 40.795
    return False


def _recalibrate_visible_v3_fields(features: List[Dict[str, Any]]) -> None:
    for feature in features:
        props = feature.get("properties") or {}
        if _is_airport_props(props):
            _apply_airport_exclusion_state(props)
        else:
            props["airport_excluded"] = False

    citywide_rank_field = V3_PROFILE_CONFIG["citywide_v3"]["score"]
    citywide_conf_field = V3_PROFILE_CONFIG["citywide_v3"]["confidence"]
    for feature in features:
        props = feature.get("properties") or {}
        if not _is_airport_props(props):
            props["earnings_shadow_visible_rank_citywide_v3"] = None
            props["earnings_shadow_visible_base_score_citywide_v3"] = None
            props["earnings_shadow_visible_score_citywide_v3"] = None
            props["citywide_visual_anchor_discount_factor_shadow"] = 1.0

    citywide_rated = [
        feature for feature in features
        if _eligible_for_profile("citywide_v3", feature.get("properties") or {}, feature.get("geometry"))
        and (feature.get("properties") or {}).get("earnings_shadow_score_citywide_v3") is not None
    ]
    citywide_anchor_by_location: Dict[int, float] = {}
    ranked_citywide = sorted(
        citywide_rated,
        key=lambda f: (
            _clamp01(float((f.get("properties") or {}).get(citywide_rank_field) or 0.0)),
            float((f.get("properties") or {}).get(citywide_conf_field) or 0.0),
            int((f.get("properties") or {}).get("LocationID") or 0),
        ),
    )
    n_city = len(ranked_citywide)
    for idx, feature in enumerate(ranked_citywide):
        props = feature.get("properties") or {}
        location_id = int(props.get("LocationID") or 0)
        citywide_local_rank = 0.0 if n_city <= 1 else (idx / (n_city - 1))
        citywide_anchor_discount_factor = 1.0
        citywide_anchor_input = _clamp01(float(props.get(citywide_rank_field) or 0.0))
        citywide_conf = _clamp01(float(props.get("earnings_shadow_confidence_citywide_v3") or 0.0))
        citywide_base_norm = _clamp01(
            0.40 * citywide_local_rank +
            0.46 * citywide_anchor_input +
            0.14 * citywide_conf
        )
        citywide_display_norm = _relaxed_display_curve(citywide_base_norm)
        visible_rating = int(round(1 + 99 * citywide_display_norm))
        visible_bucket, visible_color = bucket_and_color_from_rating(visible_rating)
        props["earnings_shadow_visible_rank_citywide_v3"] = float(citywide_local_rank)
        props["earnings_shadow_visible_base_score_citywide_v3"] = float(citywide_base_norm)
        props["earnings_shadow_visible_score_citywide_v3"] = float(citywide_display_norm)
        props["citywide_visual_anchor_discount_factor_shadow"] = float(citywide_anchor_discount_factor)
        props["earnings_shadow_citywide_anchor_input_v3"] = float(citywide_anchor_input)
        props["earnings_shadow_citywide_anchor_base_v3"] = float(citywide_base_norm)
        props["earnings_shadow_citywide_anchor_display_v3"] = float(citywide_display_norm)
        props["earnings_shadow_citywide_anchor_norm_v3"] = float(citywide_base_norm)
        props["earnings_shadow_rating_citywide_v3"] = visible_rating
        props["earnings_shadow_bucket_citywide_v3"] = visible_bucket
        props["earnings_shadow_color_citywide_v3"] = visible_color
        citywide_anchor_by_location[location_id] = float(citywide_base_norm)

    for profile_name, profile_fields in V3_PROFILE_CONFIG.items():
        if profile_name == "citywide_v3":
            continue
        score_field = profile_fields["score"]
        confidence_field = profile_fields["confidence"]
        for feature in features:
            props = feature.get("properties") or {}
            if not _is_airport_props(props):
                props[f"earnings_shadow_visible_rank_{profile_name}"] = None
                props[f"earnings_shadow_visible_base_score_{profile_name}"] = None
                props[f"earnings_shadow_visible_score_{profile_name}"] = None

        rated_features = [
            feature for feature in features
            if _eligible_for_profile(profile_name, feature.get("properties") or {}, feature.get("geometry"))
            and (feature.get("properties") or {}).get(score_field) is not None
        ]
        ranked = sorted(
            rated_features,
            key=lambda f: (
                float((f.get("properties") or {}).get(score_field) or 0.0),
                float((f.get("properties") or {}).get(confidence_field) or 0.0),
                int((f.get("properties") or {}).get("LocationID") or 0),
            ),
        )
        n = len(ranked)
        for idx, feature in enumerate(ranked):
            props = feature.get("properties") or {}
            location_id = int(props.get("LocationID") or 0)
            citywide_anchor_norm = _clamp01(citywide_anchor_by_location.get(location_id, 0.0))
            profile_local_rank = 0.0 if n <= 1 else (idx / (n - 1))
            profile_raw_score = _clamp01(float(props.get(score_field) or 0.0))
            profile_conf = _clamp01(float(props.get(confidence_field) or 0.0))
            if profile_name == "manhattan_v3":
                base_visible_norm = _clamp01(
                    0.18 * citywide_anchor_norm +
                    0.18 * profile_local_rank +
                    0.50 * profile_raw_score +
                    0.14 * profile_conf
                )
            elif profile_name in {"bronx_wash_heights_v3", "queens_v3", "brooklyn_v3"}:
                base_visible_norm = _clamp01(
                    0.22 * citywide_anchor_norm +
                    0.20 * profile_local_rank +
                    0.46 * profile_raw_score +
                    0.12 * profile_conf
                )
            elif profile_name == "staten_island_v3":
                base_visible_norm = _clamp01(
                    0.10 * citywide_anchor_norm +
                    0.30 * profile_local_rank +
                    0.44 * profile_raw_score +
                    0.16 * profile_conf
                )
            else:
                base_visible_norm = _clamp01(
                    0.22 * citywide_anchor_norm +
                    0.20 * profile_local_rank +
                    0.46 * profile_raw_score +
                    0.12 * profile_conf
                )
            visible_display_norm = _relaxed_display_curve(base_visible_norm)
            visible_rating = int(round(1 + 99 * visible_display_norm))
            visible_bucket, visible_color = bucket_and_color_from_rating(visible_rating)

            props[f"earnings_shadow_visible_rank_{profile_name}"] = float(profile_local_rank)
            props[f"earnings_shadow_visible_base_score_{profile_name}"] = float(base_visible_norm)
            props[f"earnings_shadow_visible_score_{profile_name}"] = float(visible_display_norm)
            props[f"earnings_shadow_rating_{profile_name}"] = visible_rating
            props[f"earnings_shadow_bucket_{profile_name}"] = visible_bucket
            props[f"earnings_shadow_color_{profile_name}"] = visible_color


def _as_finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _resolve_popup_metrics(
    *,
    raw_shadow_props: Dict[str, Any],
    visible_pickups: int,
    geometry_area_sq_miles: float | None,
) -> Dict[str, float] | None:
    pickups_now = _as_finite_number(raw_shadow_props.get("pickups_now_shadow"))
    if pickups_now is None or pickups_now < 0:
        pickups_now = float(max(0, int(visible_pickups)))

    pickups_next = _as_finite_number(raw_shadow_props.get("next_pickups_shadow"))
    if pickups_next is None or pickups_next < 0:
        pickups_next = pickups_now

    zone_area = _as_finite_number(raw_shadow_props.get("zone_area_sq_miles_shadow"))
    if zone_area is None or zone_area <= 0:
        zone_area = _as_finite_number(geometry_area_sq_miles)
    if zone_area is None or zone_area <= 0:
        return None

    density_now = _as_finite_number(raw_shadow_props.get("pickups_per_sq_mile_now_shadow"))
    if density_now is None or density_now < 0:
        density_now = pickups_now / zone_area

    density_next = _as_finite_number(raw_shadow_props.get("pickups_per_sq_mile_next_shadow"))
    if density_next is None or density_next < 0:
        density_next = pickups_next / zone_area

    return {
        "pickups_now_shadow": float(pickups_now),
        "next_pickups_shadow": float(pickups_next),
        "zone_area_sq_miles_shadow": float(zone_area),
        "pickups_per_sq_mile_now_shadow": float(density_now),
        "pickups_per_sq_mile_next_shadow": float(density_next),
    }


def _validate_popup_metric_consistency(
    features: List[Dict[str, Any]],
    frame_time: str,
    diagnostics_by_location_id: Dict[int, Dict[str, Any]] | None = None,
) -> None:
    popup_metric_fields = [
        "pickups_now_shadow",
        "next_pickups_shadow",
        "zone_area_sq_miles_shadow",
        "pickups_per_sq_mile_now_shadow",
        "pickups_per_sq_mile_next_shadow",
    ]
    tolerance = 2.0
    failures: list[str] = []
    failure_diagnostics: list[Dict[str, Any]] = []

    def _append_failure(location_id: Any, zone_name: Any, borough: Any, reason: str) -> None:
        location_id_int = None
        try:
            location_id_int = int(location_id) if location_id is not None else None
        except Exception:
            location_id_int = None
        diagnostics = (diagnostics_by_location_id or {}).get(location_id_int) if location_id_int is not None else None
        failure_payload = {
            "LocationID": location_id,
            "zone_name": zone_name,
            "borough": borough,
            "frame_time": frame_time,
            "reason": reason,
            "shadow_sql_row_exists": None if not diagnostics else diagnostics.get("shadow_sql_row_exists"),
            "geometry_area_row_exists": None if not diagnostics else diagnostics.get("geometry_area_row_exists"),
            "pickups_now_shadow": None if not diagnostics else diagnostics.get("pickups_now_shadow"),
            "next_pickups_shadow": None if not diagnostics else diagnostics.get("next_pickups_shadow"),
            "zone_area_sq_miles_shadow": None if not diagnostics else diagnostics.get("zone_area_sq_miles_shadow"),
            "pickups_per_sq_mile_now_shadow": None if not diagnostics else diagnostics.get("pickups_per_sq_mile_now_shadow"),
            "pickups_per_sq_mile_next_shadow": None if not diagnostics else diagnostics.get("pickups_per_sq_mile_next_shadow"),
        }
        failure_diagnostics.append(failure_payload)
        failures.append(json.dumps(failure_payload, sort_keys=True))

    for feature in features:
        props = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(props, dict) or _is_airport_props(props):
            continue

        location_id = props.get("LocationID")
        zone_name = props.get("zone_name")
        borough = props.get("borough")
        parsed_values: Dict[str, float] = {}
        invalid_fields: list[str] = []
        for field_name in popup_metric_fields:
            number = _as_finite_number(props.get(field_name))
            if number is None:
                invalid_fields.append(field_name)
                continue
            parsed_values[field_name] = number
        if invalid_fields:
            _append_failure(location_id, zone_name, borough, f"invalid_fields={','.join(invalid_fields)}")

        pickups_now = parsed_values.get("pickups_now_shadow")
        pickups_next = parsed_values.get("next_pickups_shadow")
        zone_area = parsed_values.get("zone_area_sq_miles_shadow")
        density_now = parsed_values.get("pickups_per_sq_mile_now_shadow")
        density_next = parsed_values.get("pickups_per_sq_mile_next_shadow")

        if pickups_now is not None and pickups_now < 0:
            _append_failure(location_id, zone_name, borough, "pickups_now_shadow must be >= 0")
        if pickups_next is not None and pickups_next < 0:
            _append_failure(location_id, zone_name, borough, "next_pickups_shadow must be >= 0")
        if zone_area is not None and zone_area <= 0:
            _append_failure(location_id, zone_name, borough, "zone_area_sq_miles_shadow must be > 0")
        if density_now is not None and density_now < 0:
            _append_failure(location_id, zone_name, borough, "pickups_per_sq_mile_now_shadow must be >= 0")
        if density_next is not None and density_next < 0:
            _append_failure(location_id, zone_name, borough, "pickups_per_sq_mile_next_shadow must be >= 0")

        if zone_area is not None and pickups_now is not None and density_now is not None:
            expected_now = zone_area * density_now
            if abs(pickups_now - expected_now) > tolerance:
                _append_failure(
                    location_id,
                    zone_name,
                    borough,
                    f"pickups_now_shadow mismatch: got={pickups_now:.6f} expected={expected_now:.6f}",
                )
        if zone_area is not None and pickups_next is not None and density_next is not None:
            expected_next = zone_area * density_next
            if abs(pickups_next - expected_next) > tolerance:
                _append_failure(
                    location_id,
                    zone_name,
                    borough,
                    f"next_pickups_shadow mismatch: got={pickups_next:.6f} expected={expected_next:.6f}",
                )

    if failures:
        for failure in failure_diagnostics[:25]:
            logger.error("popup_metric_consistency_failure %s", json.dumps(failure, sort_keys=True))
        sample = "; ".join(failures[:8])
        raise RuntimeError(f"Popup metric consistency validation failed ({len(failures)}): {sample}")


def _validate_rating_bucket_color_consistency(features: List[Dict[str, Any]], frame_time: str) -> None:
    shadow_rating_families = [
        "citywide_v2",
        "citywide_v3",
        "manhattan_v2",
        "manhattan_v3",
        "bronx_wash_heights_v2",
        "bronx_wash_heights_v3",
        "queens_v2",
        "queens_v3",
        "brooklyn_v2",
        "brooklyn_v3",
        "staten_island_v2",
        "staten_island_v3",
    ]
    failures: list[str] = []

    def _parse_rating(props: Dict[str, Any], field_name: str) -> int | None:
        value = props.get(field_name)
        if value is None:
            return None
        try:
            rating = int(value)
        except Exception:
            return None
        return rating

    for feature in features:
        props = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(props, dict):
            continue
        location_id = props.get("LocationID")
        zone_name = props.get("zone_name")

        visible_rating = _parse_rating(props, "rating")
        if visible_rating is not None:
            expected_bucket, expected_color = bucket_and_color_from_rating(visible_rating)
            emitted_bucket = props.get("bucket")
            style = props.get("style") if isinstance(props.get("style"), dict) else {}
            emitted_color = style.get("fillColor")
            if emitted_bucket != expected_bucket or emitted_color != expected_color:
                failures.append(
                    f"LocationID={location_id} zone_name={zone_name!r} frame_time={frame_time} family=legacy_visible "
                    f"rating={visible_rating} emitted_bucket={emitted_bucket!r} expected_bucket={expected_bucket!r} "
                    f"emitted_color={emitted_color!r} expected_color={expected_color!r}"
                )

        for family in shadow_rating_families:
            rating_field = f"earnings_shadow_rating_{family}"
            bucket_field = f"earnings_shadow_bucket_{family}"
            color_field = f"earnings_shadow_color_{family}"
            rating_value = _parse_rating(props, rating_field)
            if rating_value is None:
                continue
            expected_bucket, expected_color = bucket_and_color_from_rating(rating_value)
            emitted_bucket = props.get(bucket_field)
            emitted_color = props.get(color_field)
            if emitted_bucket != expected_bucket or emitted_color != expected_color:
                failures.append(
                    f"LocationID={location_id} zone_name={zone_name!r} frame_time={frame_time} family={family} "
                    f"rating={rating_value} emitted_bucket={emitted_bucket!r} expected_bucket={expected_bucket!r} "
                    f"emitted_color={emitted_color!r} expected_color={expected_color!r}"
                )

    if failures:
        sample = "; ".join(failures[:8])
        raise RuntimeError(
            f"Rating/bucket/color consistency validation failed ({len(failures)}): {sample}"
        )


def build_hotspots_frames(
    parquet_files: List[Path],
    zones_geojson_path: Path,
    out_dir: Path,
    bin_minutes: int = 20,
    min_trips_per_window: int = 25,
) -> Dict[str, Any]:
    """
    Writes:
      /data/frames/timeline.json
      /data/frames/frame_000000.json ... etc

    Each frame contains:
      - time
      - polygons FeatureCollection
      - each feature has:
          LocationID, zone_name, borough, rating, bucket, pickups, avg_driver_pay, style(fillColor)

    FACTS + REALISM GUARANTEE:
      - Per-window normalization is percentile-rank based (NOT min/max), so airports cannot flatten the city.
      - Baseline per-zone normalization is ALSO percentile-rank based (NOT global min/max),
        so airports cannot compress baseline scores either.
    """
    validate_zone_mode_profiles_for_live_engine()
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_root = Path(os.environ.get("ARTIFACT_BUILD_TMP_DIR", "/tmp/tlc_artifact_build"))
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_run_dir_ctx = tempfile.TemporaryDirectory(prefix="build_", dir=str(temp_root))
    temp_run_dir = Path(temp_run_dir_ctx.name)
    stage_dir = temp_run_dir / "frames.__building__"
    duckdb_tmp_dir = temp_run_dir / "duckdb_tmp"
    stage_dir.mkdir(parents=True, exist_ok=True)
    duckdb_tmp_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Load zone geometry + names
    # ----------------------------
    zones = json.loads(zones_geojson_path.read_text(encoding="utf-8"))

    geom_by_id: Dict[int, Any] = {}
    name_by_id: Dict[int, str] = {}
    borough_by_id: Dict[int, str] = {}

    for f in zones.get("features", []):
        props = f.get("properties") or {}
        zid = props.get("LocationID")
        if zid is None:
            continue
        try:
            zid_int = int(zid)
        except Exception:
            continue

        geom = f.get("geometry")
        if geom:
            geom_by_id[zid_int] = geom

        zone_name = props.get("zone") or props.get("Zone") or props.get("name") or props.get("Name") or ""
        borough = props.get("borough") or props.get("Borough") or props.get("boro") or props.get("Boro") or ""

        name_by_id[zid_int] = str(zone_name) if zone_name is not None else ""
        borough_by_id[zid_int] = str(borough) if borough is not None else ""

    if not geom_by_id:
        raise RuntimeError("taxi_zones.geojson missing usable properties.LocationID geometry.")

    # ----------------------------
    # DuckDB (spill to volume)
    # ----------------------------
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(f"PRAGMA temp_directory='{duckdb_tmp_dir.as_posix()}'")

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)
    schema_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_sql}])").fetchall()
    available_columns = {str(row[0]) for row in schema_rows}

    zone_geometry_rows = build_zone_geometry_metrics_rows(zones_geojson_path)
    zone_geometry_by_id: Dict[int, Dict[str, float | None]] = {
        int(row["PULocationID"]): {
            "zone_area_sq_miles": None if row["zone_area_sq_miles"] is None else float(row["zone_area_sq_miles"]),
            "centroid_latitude": None if row.get("centroid_latitude") is None else float(row["centroid_latitude"]),
        }
        for row in zone_geometry_rows
    }
    con.execute("CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)")
    if zone_geometry_rows:
        con.executemany(
            "INSERT INTO zone_geometry_metrics (PULocationID, zone_area_sq_miles, centroid_latitude) VALUES (?, ?, ?)",
            [
                (
                    int(row["PULocationID"]),
                    None if row["zone_area_sq_miles"] is None else float(row["zone_area_sq_miles"]),
                    None if row.get("centroid_latitude") is None else float(row["centroid_latitude"]),
                )
                for row in zone_geometry_rows
            ],
        )
    con.execute("CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)")
    con.executemany(
        "INSERT INTO zone_metadata (PULocationID, zone_name, borough_name, airport_excluded) VALUES (?, ?, ?, ?)",
        [
            (
                int(zid),
                str(name_by_id.get(zid, "") or ""),
                str(borough_by_id.get(zid, "") or ""),
                bool(is_airport_zone(zid, name_by_id.get(zid, ""), borough_by_id.get(zid, ""))),
            )
            for zid in sorted(name_by_id.keys())
        ],
    )

    # ----------------------------
    # SQL build
    #
    # - "busy" drives score (pickups) more than pay, as you requested.
    # - Per-window normalization uses percentile rank (no max/min) so airports don't dominate.
    # - Baseline per-zone normalization ALSO uses percentile rank (no global min/max),
    #   so airports cannot compress base_score either.
    # - Confidence scales down low-sample windows (still data-driven).
    # ----------------------------
    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL
        AND pickup_datetime IS NOT NULL
        AND CAST(PULocationID AS INTEGER) NOT IN (1, 132, 138)
        AND CAST(PULocationID AS INTEGER) IN (
          SELECT PULocationID FROM zone_metadata WHERE airport_excluded = FALSE
        )
    ),
    t AS (
      SELECT
        PULocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay
      FROM base
    ),
    binned AS (
      SELECT
        PULocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
        CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        driver_pay
      FROM t
    ),
    agg AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS pickups,
        AVG(driver_pay) AS avg_driver_pay
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {int(min_trips_per_window)}
    ),

    -- ----------------------------
    -- Per-window percentile-rank normalization (robust to airport outliers)
    -- ----------------------------
    win AS (
      SELECT
        *,
        LN(1 + pickups) AS log_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY LN(1 + pickups)) AS rn_pickups,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY avg_driver_pay) AS rn_pay,
        COUNT(*) OVER (PARTITION BY dow_m, bin_start_min) AS n_in_window
      FROM agg
    ),
    win_scored AS (
      SELECT
        PULocationID, dow_m, bin_start_min, pickups, avg_driver_pay,
        CASE
          WHEN n_in_window <= 1 THEN 0.0
          ELSE (rn_pickups - 1) * 1.0 / (n_in_window - 1)
        END AS vol_n,
        CASE
          WHEN n_in_window <= 1 THEN 0.0
          ELSE (rn_pay - 1) * 1.0 / (n_in_window - 1)
        END AS pay_n
      FROM win
    ),

    -- ----------------------------
    -- Baseline per-zone (historical typical level)
    -- IMPORTANT CHANGE: baseline normalization uses percentile ranks (NOT min/max)
    -- so airports cannot compress baseline for all other zones.
    -- ----------------------------
    zone_base AS (
      SELECT
        PULocationID,
        LN(1 + AVG(pickups)) AS base_log_pickups,
        AVG(avg_driver_pay) AS base_pay
      FROM agg
      GROUP BY 1
    ),
    zone_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY base_log_pickups) AS rn_base_pickups,
        ROW_NUMBER() OVER (ORDER BY base_pay) AS rn_base_pay,
        COUNT(*) OVER () AS n_zones
      FROM zone_base
    ),
    zone_norm AS (
      SELECT
        PULocationID,
        CASE
          WHEN n_zones <= 1 THEN 0.0
          ELSE (rn_base_pickups - 1) * 1.0 / (n_zones - 1)
        END AS base_vol_n,
        CASE
          WHEN n_zones <= 1 THEN 0.0
          ELSE (rn_base_pay - 1) * 1.0 / (n_zones - 1)
        END AS base_pay_n
      FROM zone_ranked
    ),

    final AS (
      SELECT
        w.PULocationID,
        w.dow_m,
        w.bin_start_min,
        w.pickups,
        w.avg_driver_pay,

        -- Your rule: mostly busy, some driver pay
        (0.85*w.vol_n + 0.15*w.pay_n) AS moment_score,
        (0.85*z.base_vol_n + 0.15*z.base_pay_n) AS base_score,

        -- confidence: more pickups -> more trust in moment_score
        LEAST(1.0, w.pickups / 50.0) AS conf
      FROM win_scored w
      JOIN zone_norm z USING (PULocationID)
    )

    SELECT
      PULocationID,
      dow_m,
      bin_start_min,
      pickups,
      avg_driver_pay,
      CAST(
        ROUND(
          1 + 99 * LEAST(
            GREATEST(
              ((0.70*moment_score + 0.30*base_score) * (0.50 + 0.50*conf)),
              0.0
            ),
            1.0
          )
        ) AS INTEGER
      ) AS rating
    FROM final
    ORDER BY dow_m, bin_start_min, PULocationID;
    """

    shadow_sql = build_zone_earnings_shadow_sql(
        parquet_list,
        bin_minutes=int(bin_minutes),
        min_trips_per_window=int(min_trips_per_window),
        profile=ZONE_MODE_PROFILES["citywide_v2"],
        citywide_v3_profile=ZONE_MODE_PROFILES["citywide_v3"],
        manhattan_profile=ZONE_MODE_PROFILES["manhattan_v2"],
        bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v2"],
        queens_profile=ZONE_MODE_PROFILES["queens_v2"],
        brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v2"],
        staten_island_profile=ZONE_MODE_PROFILES["staten_island_v2"],
        manhattan_v3_profile=ZONE_MODE_PROFILES["manhattan_v3"],
        bronx_wash_heights_v3_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v3"],
        queens_v3_profile=ZONE_MODE_PROFILES["queens_v3"],
        brooklyn_v3_profile=ZONE_MODE_PROFILES["brooklyn_v3"],
        staten_island_v3_profile=ZONE_MODE_PROFILES["staten_island_v3"],
        available_columns=available_columns,
    )

    shadow_rows = con.execute(shadow_sql).fetchall()
    shadow_by_key: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for row in shadow_rows:
        (
            pu_id,
            s_dow_m,
            s_bin_start_min,
            pickups_now,
            pickups_next,
            median_driver_pay,
            median_pay_per_min,
            median_pay_per_mile,
            median_request_to_pickup_min,
            short_trip_share,
            shared_ride_share,
            zone_area_sq_miles,
            pickups_per_sq_mile_now,
            pickups_per_sq_mile_next,
            long_trip_share_20plus,
            balanced_trip_share,
            same_zone_dropoff_share,
            downstream_next_value_raw,
            demand_now_n,
            demand_next_n,
            pay_n,
            pay_per_min_n,
            pay_per_mile_n,
            pickup_friction_penalty_n,
            short_trip_penalty_n,
            shared_ride_penalty_n,
            downstream_value_n,
            demand_density_now_n,
            demand_density_next_n,
            demand_support_n_shadow,
            density_support_n_shadow,
            effective_demand_density_now_n_shadow,
            effective_demand_density_next_n_shadow,
            busy_now_base_n_shadow,
            busy_next_base_n_shadow,
            long_trip_share_20plus_n,
            balanced_trip_share_n_shadow,
            balanced_trip_share_shadow,
            same_zone_retention_penalty_n,
            churn_pressure_n_shadow,
            manhattan_core_saturation_proxy_n_shadow,
            manhattan_core_saturation_penalty_n_shadow,
            market_saturation_pressure_n_shadow,
            market_saturation_penalty_n_shadow,
            citywide_manhattan_saturation_discount_factor_shadow,
            earnings_shadow_positive_citywide_v3,
            earnings_shadow_negative_citywide_v3,
            earnings_shadow_score_raw_citywide_v3,
            earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow,
            earnings_shadow_score_citywide_v3_anchor_shadow,
            earnings_shadow_score_raw_manhattan_v3,
            earnings_shadow_score_raw_bronx_wash_heights_v3,
            earnings_shadow_score_raw_queens_v3,
            earnings_shadow_score_raw_brooklyn_v3,
            earnings_shadow_score_raw_staten_island_v3,
            earnings_shadow_busy_size_positive_citywide_v3,
            earnings_shadow_pay_quality_positive_citywide_v3,
            earnings_shadow_trip_mix_positive_citywide_v3,
            earnings_shadow_continuation_positive_citywide_v3,
            earnings_shadow_short_trip_penalty_citywide_v3,
            earnings_shadow_retention_penalty_citywide_v3,
            earnings_shadow_friction_penalty_citywide_v3,
            earnings_shadow_saturation_penalty_citywide_v3,
            earnings_shadow_busy_size_positive_manhattan_v3,
            earnings_shadow_pay_quality_positive_manhattan_v3,
            earnings_shadow_trip_mix_positive_manhattan_v3,
            earnings_shadow_continuation_positive_manhattan_v3,
            earnings_shadow_short_trip_penalty_manhattan_v3,
            earnings_shadow_retention_penalty_manhattan_v3,
            earnings_shadow_friction_penalty_manhattan_v3,
            earnings_shadow_saturation_penalty_manhattan_v3,
            earnings_shadow_busy_size_positive_bronx_wash_heights_v3,
            earnings_shadow_pay_quality_positive_bronx_wash_heights_v3,
            earnings_shadow_trip_mix_positive_bronx_wash_heights_v3,
            earnings_shadow_continuation_positive_bronx_wash_heights_v3,
            earnings_shadow_short_trip_penalty_bronx_wash_heights_v3,
            earnings_shadow_retention_penalty_bronx_wash_heights_v3,
            earnings_shadow_friction_penalty_bronx_wash_heights_v3,
            earnings_shadow_saturation_penalty_bronx_wash_heights_v3,
            earnings_shadow_busy_size_positive_queens_v3,
            earnings_shadow_pay_quality_positive_queens_v3,
            earnings_shadow_trip_mix_positive_queens_v3,
            earnings_shadow_continuation_positive_queens_v3,
            earnings_shadow_short_trip_penalty_queens_v3,
            earnings_shadow_retention_penalty_queens_v3,
            earnings_shadow_friction_penalty_queens_v3,
            earnings_shadow_saturation_penalty_queens_v3,
            earnings_shadow_busy_size_positive_brooklyn_v3,
            earnings_shadow_pay_quality_positive_brooklyn_v3,
            earnings_shadow_trip_mix_positive_brooklyn_v3,
            earnings_shadow_continuation_positive_brooklyn_v3,
            earnings_shadow_short_trip_penalty_brooklyn_v3,
            earnings_shadow_retention_penalty_brooklyn_v3,
            earnings_shadow_friction_penalty_brooklyn_v3,
            earnings_shadow_saturation_penalty_brooklyn_v3,
            earnings_shadow_busy_size_positive_staten_island_v3,
            earnings_shadow_pay_quality_positive_staten_island_v3,
            earnings_shadow_trip_mix_positive_staten_island_v3,
            earnings_shadow_continuation_positive_staten_island_v3,
            earnings_shadow_short_trip_penalty_staten_island_v3,
            earnings_shadow_retention_penalty_staten_island_v3,
            earnings_shadow_friction_penalty_staten_island_v3,
            earnings_shadow_saturation_penalty_staten_island_v3,
            earnings_shadow_score_citywide_v3,
            earnings_shadow_confidence_citywide_v3,
            citywide_v3_confidence_profile_shadow,
            manhattan_v3_confidence_profile_shadow,
            bronx_wash_heights_v3_confidence_profile_shadow,
            queens_v3_confidence_profile_shadow,
            brooklyn_v3_confidence_profile_shadow,
            staten_island_v3_confidence_profile_shadow,
            earnings_shadow_rating_citywide_v3,
            earnings_shadow_bucket_citywide_v3,
            earnings_shadow_color_citywide_v3,
            earnings_shadow_score_citywide_v2,
            earnings_shadow_confidence_citywide_v2,
            earnings_shadow_rating_citywide_v2,
            earnings_shadow_bucket_citywide_v2,
            earnings_shadow_color_citywide_v2,
            earnings_shadow_score_manhattan_v2,
            earnings_shadow_confidence_manhattan_v2,
            earnings_shadow_rating_manhattan_v2,
            earnings_shadow_bucket_manhattan_v2,
            earnings_shadow_color_manhattan_v2,
            earnings_shadow_score_bronx_wash_heights_v2,
            earnings_shadow_confidence_bronx_wash_heights_v2,
            earnings_shadow_rating_bronx_wash_heights_v2,
            earnings_shadow_bucket_bronx_wash_heights_v2,
            earnings_shadow_color_bronx_wash_heights_v2,
            earnings_shadow_score_queens_v2,
            earnings_shadow_confidence_queens_v2,
            earnings_shadow_rating_queens_v2,
            earnings_shadow_bucket_queens_v2,
            earnings_shadow_color_queens_v2,
            earnings_shadow_score_brooklyn_v2,
            earnings_shadow_confidence_brooklyn_v2,
            earnings_shadow_rating_brooklyn_v2,
            earnings_shadow_bucket_brooklyn_v2,
            earnings_shadow_color_brooklyn_v2,
            earnings_shadow_score_staten_island_v2,
            earnings_shadow_confidence_staten_island_v2,
            earnings_shadow_rating_staten_island_v2,
            earnings_shadow_bucket_staten_island_v2,
            earnings_shadow_color_staten_island_v2,
            earnings_shadow_score_manhattan_v3,
            earnings_shadow_confidence_manhattan_v3,
            earnings_shadow_rating_manhattan_v3,
            earnings_shadow_bucket_manhattan_v3,
            earnings_shadow_color_manhattan_v3,
            earnings_shadow_score_bronx_wash_heights_v3,
            earnings_shadow_confidence_bronx_wash_heights_v3,
            earnings_shadow_rating_bronx_wash_heights_v3,
            earnings_shadow_bucket_bronx_wash_heights_v3,
            earnings_shadow_color_bronx_wash_heights_v3,
            earnings_shadow_score_queens_v3,
            earnings_shadow_confidence_queens_v3,
            earnings_shadow_rating_queens_v3,
            earnings_shadow_bucket_queens_v3,
            earnings_shadow_color_queens_v3,
            earnings_shadow_score_brooklyn_v3,
            earnings_shadow_confidence_brooklyn_v3,
            earnings_shadow_rating_brooklyn_v3,
            earnings_shadow_bucket_brooklyn_v3,
            earnings_shadow_color_brooklyn_v3,
            earnings_shadow_score_staten_island_v3,
            earnings_shadow_confidence_staten_island_v3,
            earnings_shadow_rating_staten_island_v3,
            earnings_shadow_bucket_staten_island_v3,
            earnings_shadow_color_staten_island_v3,
        ) = row
        if bool(
            is_airport_zone(
                pu_id,
                name_by_id.get(int(pu_id), ""),
                borough_by_id.get(int(pu_id), ""),
            )
        ):
            continue
        shadow_by_key[(int(pu_id), int(s_dow_m), int(s_bin_start_min))] = {
            "pickups_now_shadow": None if pickups_now is None else int(pickups_now),
            "next_pickups_shadow": None if pickups_next is None else int(pickups_next),
            "median_driver_pay_shadow": None if median_driver_pay is None else float(median_driver_pay),
            "median_pay_per_min_shadow": None if median_pay_per_min is None else float(median_pay_per_min),
            "median_pay_per_mile_shadow": None if median_pay_per_mile is None else float(median_pay_per_mile),
            "median_request_to_pickup_min_shadow": None if median_request_to_pickup_min is None else float(median_request_to_pickup_min),
            "short_trip_share_shadow": None if short_trip_share is None else float(short_trip_share),
            "shared_ride_share_shadow": None if shared_ride_share is None else float(shared_ride_share),
            "zone_area_sq_miles_shadow": None if zone_area_sq_miles is None else float(zone_area_sq_miles),
            "pickups_per_sq_mile_now_shadow": None if pickups_per_sq_mile_now is None else float(pickups_per_sq_mile_now),
            "pickups_per_sq_mile_next_shadow": None if pickups_per_sq_mile_next is None else float(pickups_per_sq_mile_next),
            "long_trip_share_20plus_shadow": None if long_trip_share_20plus is None else float(long_trip_share_20plus),
            "balanced_trip_share_shadow": None if balanced_trip_share is None else float(balanced_trip_share),
            "same_zone_dropoff_share_shadow": None if same_zone_dropoff_share is None else float(same_zone_dropoff_share),
            "downstream_value_shadow": None if downstream_next_value_raw is None else float(downstream_next_value_raw),
            "demand_now_n_shadow": None if demand_now_n is None else float(demand_now_n),
            "demand_next_n_shadow": None if demand_next_n is None else float(demand_next_n),
            "pay_n_shadow": None if pay_n is None else float(pay_n),
            "pay_per_min_n_shadow": None if pay_per_min_n is None else float(pay_per_min_n),
            "pay_per_mile_n_shadow": None if pay_per_mile_n is None else float(pay_per_mile_n),
            "pickup_friction_penalty_n_shadow": None if pickup_friction_penalty_n is None else float(pickup_friction_penalty_n),
            "short_trip_penalty_n_shadow": None if short_trip_penalty_n is None else float(short_trip_penalty_n),
            "shared_ride_penalty_n_shadow": None if shared_ride_penalty_n is None else float(shared_ride_penalty_n),
            "downstream_value_n_shadow": None if downstream_value_n is None else float(downstream_value_n),
            "demand_density_now_n_shadow": None if demand_density_now_n is None else float(demand_density_now_n),
            "demand_density_next_n_shadow": None if demand_density_next_n is None else float(demand_density_next_n),
            "demand_support_n_shadow": None if demand_support_n_shadow is None else float(demand_support_n_shadow),
            "density_support_n_shadow": None if density_support_n_shadow is None else float(density_support_n_shadow),
            "effective_demand_density_now_n_shadow": None if effective_demand_density_now_n_shadow is None else float(effective_demand_density_now_n_shadow),
            "effective_demand_density_next_n_shadow": None if effective_demand_density_next_n_shadow is None else float(effective_demand_density_next_n_shadow),
            "busy_now_base_n_shadow": None if busy_now_base_n_shadow is None else float(busy_now_base_n_shadow),
            "busy_next_base_n_shadow": None if busy_next_base_n_shadow is None else float(busy_next_base_n_shadow),
            "long_trip_share_20plus_n_shadow": None if long_trip_share_20plus_n is None else float(long_trip_share_20plus_n),
            "balanced_trip_share_n_shadow": None if balanced_trip_share_n_shadow is None else float(balanced_trip_share_n_shadow),
            "same_zone_retention_penalty_n_shadow": None if same_zone_retention_penalty_n is None else float(same_zone_retention_penalty_n),
            "churn_pressure_n_shadow": None if churn_pressure_n_shadow is None else float(churn_pressure_n_shadow),
            "manhattan_core_saturation_proxy_n_shadow": None if manhattan_core_saturation_proxy_n_shadow is None else float(manhattan_core_saturation_proxy_n_shadow),
            "manhattan_core_saturation_penalty_n_shadow": None if manhattan_core_saturation_penalty_n_shadow is None else float(manhattan_core_saturation_penalty_n_shadow),
            "market_saturation_pressure_n_shadow": None if market_saturation_pressure_n_shadow is None else float(market_saturation_pressure_n_shadow),
            "market_saturation_penalty_n_shadow": None if market_saturation_penalty_n_shadow is None else float(market_saturation_penalty_n_shadow),
            "citywide_manhattan_saturation_discount_factor_shadow": None if citywide_manhattan_saturation_discount_factor_shadow is None else float(citywide_manhattan_saturation_discount_factor_shadow),
            "earnings_shadow_positive_citywide_v3": None if earnings_shadow_positive_citywide_v3 is None else float(earnings_shadow_positive_citywide_v3),
            "earnings_shadow_negative_citywide_v3": None if earnings_shadow_negative_citywide_v3 is None else float(earnings_shadow_negative_citywide_v3),
            "earnings_shadow_score_raw_citywide_v3": None if earnings_shadow_score_raw_citywide_v3 is None else float(earnings_shadow_score_raw_citywide_v3),
            "earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow": None if earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow is None else float(earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow),
            "earnings_shadow_score_citywide_v3_anchor_shadow": None if earnings_shadow_score_citywide_v3_anchor_shadow is None else float(earnings_shadow_score_citywide_v3_anchor_shadow),
            "earnings_shadow_score_raw_manhattan_v3": None if earnings_shadow_score_raw_manhattan_v3 is None else float(earnings_shadow_score_raw_manhattan_v3),
            "earnings_shadow_score_raw_bronx_wash_heights_v3": None if earnings_shadow_score_raw_bronx_wash_heights_v3 is None else float(earnings_shadow_score_raw_bronx_wash_heights_v3),
            "earnings_shadow_score_raw_queens_v3": None if earnings_shadow_score_raw_queens_v3 is None else float(earnings_shadow_score_raw_queens_v3),
            "earnings_shadow_score_raw_brooklyn_v3": None if earnings_shadow_score_raw_brooklyn_v3 is None else float(earnings_shadow_score_raw_brooklyn_v3),
            "earnings_shadow_score_raw_staten_island_v3": None if earnings_shadow_score_raw_staten_island_v3 is None else float(earnings_shadow_score_raw_staten_island_v3),
            "earnings_shadow_busy_size_positive_citywide_v3": None if earnings_shadow_busy_size_positive_citywide_v3 is None else float(earnings_shadow_busy_size_positive_citywide_v3),
            "earnings_shadow_pay_quality_positive_citywide_v3": None if earnings_shadow_pay_quality_positive_citywide_v3 is None else float(earnings_shadow_pay_quality_positive_citywide_v3),
            "earnings_shadow_trip_mix_positive_citywide_v3": None if earnings_shadow_trip_mix_positive_citywide_v3 is None else float(earnings_shadow_trip_mix_positive_citywide_v3),
            "earnings_shadow_continuation_positive_citywide_v3": None if earnings_shadow_continuation_positive_citywide_v3 is None else float(earnings_shadow_continuation_positive_citywide_v3),
            "earnings_shadow_short_trip_penalty_citywide_v3": None if earnings_shadow_short_trip_penalty_citywide_v3 is None else float(earnings_shadow_short_trip_penalty_citywide_v3),
            "earnings_shadow_retention_penalty_citywide_v3": None if earnings_shadow_retention_penalty_citywide_v3 is None else float(earnings_shadow_retention_penalty_citywide_v3),
            "earnings_shadow_friction_penalty_citywide_v3": None if earnings_shadow_friction_penalty_citywide_v3 is None else float(earnings_shadow_friction_penalty_citywide_v3),
            "earnings_shadow_saturation_penalty_citywide_v3": None if earnings_shadow_saturation_penalty_citywide_v3 is None else float(earnings_shadow_saturation_penalty_citywide_v3),
            "earnings_shadow_busy_size_positive_manhattan_v3": None if earnings_shadow_busy_size_positive_manhattan_v3 is None else float(earnings_shadow_busy_size_positive_manhattan_v3),
            "earnings_shadow_pay_quality_positive_manhattan_v3": None if earnings_shadow_pay_quality_positive_manhattan_v3 is None else float(earnings_shadow_pay_quality_positive_manhattan_v3),
            "earnings_shadow_trip_mix_positive_manhattan_v3": None if earnings_shadow_trip_mix_positive_manhattan_v3 is None else float(earnings_shadow_trip_mix_positive_manhattan_v3),
            "earnings_shadow_continuation_positive_manhattan_v3": None if earnings_shadow_continuation_positive_manhattan_v3 is None else float(earnings_shadow_continuation_positive_manhattan_v3),
            "earnings_shadow_short_trip_penalty_manhattan_v3": None if earnings_shadow_short_trip_penalty_manhattan_v3 is None else float(earnings_shadow_short_trip_penalty_manhattan_v3),
            "earnings_shadow_retention_penalty_manhattan_v3": None if earnings_shadow_retention_penalty_manhattan_v3 is None else float(earnings_shadow_retention_penalty_manhattan_v3),
            "earnings_shadow_friction_penalty_manhattan_v3": None if earnings_shadow_friction_penalty_manhattan_v3 is None else float(earnings_shadow_friction_penalty_manhattan_v3),
            "earnings_shadow_saturation_penalty_manhattan_v3": None if earnings_shadow_saturation_penalty_manhattan_v3 is None else float(earnings_shadow_saturation_penalty_manhattan_v3),
            "earnings_shadow_busy_size_positive_bronx_wash_heights_v3": None if earnings_shadow_busy_size_positive_bronx_wash_heights_v3 is None else float(earnings_shadow_busy_size_positive_bronx_wash_heights_v3),
            "earnings_shadow_pay_quality_positive_bronx_wash_heights_v3": None if earnings_shadow_pay_quality_positive_bronx_wash_heights_v3 is None else float(earnings_shadow_pay_quality_positive_bronx_wash_heights_v3),
            "earnings_shadow_trip_mix_positive_bronx_wash_heights_v3": None if earnings_shadow_trip_mix_positive_bronx_wash_heights_v3 is None else float(earnings_shadow_trip_mix_positive_bronx_wash_heights_v3),
            "earnings_shadow_continuation_positive_bronx_wash_heights_v3": None if earnings_shadow_continuation_positive_bronx_wash_heights_v3 is None else float(earnings_shadow_continuation_positive_bronx_wash_heights_v3),
            "earnings_shadow_short_trip_penalty_bronx_wash_heights_v3": None if earnings_shadow_short_trip_penalty_bronx_wash_heights_v3 is None else float(earnings_shadow_short_trip_penalty_bronx_wash_heights_v3),
            "earnings_shadow_retention_penalty_bronx_wash_heights_v3": None if earnings_shadow_retention_penalty_bronx_wash_heights_v3 is None else float(earnings_shadow_retention_penalty_bronx_wash_heights_v3),
            "earnings_shadow_friction_penalty_bronx_wash_heights_v3": None if earnings_shadow_friction_penalty_bronx_wash_heights_v3 is None else float(earnings_shadow_friction_penalty_bronx_wash_heights_v3),
            "earnings_shadow_saturation_penalty_bronx_wash_heights_v3": None if earnings_shadow_saturation_penalty_bronx_wash_heights_v3 is None else float(earnings_shadow_saturation_penalty_bronx_wash_heights_v3),
            "earnings_shadow_busy_size_positive_queens_v3": None if earnings_shadow_busy_size_positive_queens_v3 is None else float(earnings_shadow_busy_size_positive_queens_v3),
            "earnings_shadow_pay_quality_positive_queens_v3": None if earnings_shadow_pay_quality_positive_queens_v3 is None else float(earnings_shadow_pay_quality_positive_queens_v3),
            "earnings_shadow_trip_mix_positive_queens_v3": None if earnings_shadow_trip_mix_positive_queens_v3 is None else float(earnings_shadow_trip_mix_positive_queens_v3),
            "earnings_shadow_continuation_positive_queens_v3": None if earnings_shadow_continuation_positive_queens_v3 is None else float(earnings_shadow_continuation_positive_queens_v3),
            "earnings_shadow_short_trip_penalty_queens_v3": None if earnings_shadow_short_trip_penalty_queens_v3 is None else float(earnings_shadow_short_trip_penalty_queens_v3),
            "earnings_shadow_retention_penalty_queens_v3": None if earnings_shadow_retention_penalty_queens_v3 is None else float(earnings_shadow_retention_penalty_queens_v3),
            "earnings_shadow_friction_penalty_queens_v3": None if earnings_shadow_friction_penalty_queens_v3 is None else float(earnings_shadow_friction_penalty_queens_v3),
            "earnings_shadow_saturation_penalty_queens_v3": None if earnings_shadow_saturation_penalty_queens_v3 is None else float(earnings_shadow_saturation_penalty_queens_v3),
            "earnings_shadow_busy_size_positive_brooklyn_v3": None if earnings_shadow_busy_size_positive_brooklyn_v3 is None else float(earnings_shadow_busy_size_positive_brooklyn_v3),
            "earnings_shadow_pay_quality_positive_brooklyn_v3": None if earnings_shadow_pay_quality_positive_brooklyn_v3 is None else float(earnings_shadow_pay_quality_positive_brooklyn_v3),
            "earnings_shadow_trip_mix_positive_brooklyn_v3": None if earnings_shadow_trip_mix_positive_brooklyn_v3 is None else float(earnings_shadow_trip_mix_positive_brooklyn_v3),
            "earnings_shadow_continuation_positive_brooklyn_v3": None if earnings_shadow_continuation_positive_brooklyn_v3 is None else float(earnings_shadow_continuation_positive_brooklyn_v3),
            "earnings_shadow_short_trip_penalty_brooklyn_v3": None if earnings_shadow_short_trip_penalty_brooklyn_v3 is None else float(earnings_shadow_short_trip_penalty_brooklyn_v3),
            "earnings_shadow_retention_penalty_brooklyn_v3": None if earnings_shadow_retention_penalty_brooklyn_v3 is None else float(earnings_shadow_retention_penalty_brooklyn_v3),
            "earnings_shadow_friction_penalty_brooklyn_v3": None if earnings_shadow_friction_penalty_brooklyn_v3 is None else float(earnings_shadow_friction_penalty_brooklyn_v3),
            "earnings_shadow_saturation_penalty_brooklyn_v3": None if earnings_shadow_saturation_penalty_brooklyn_v3 is None else float(earnings_shadow_saturation_penalty_brooklyn_v3),
            "earnings_shadow_busy_size_positive_staten_island_v3": None if earnings_shadow_busy_size_positive_staten_island_v3 is None else float(earnings_shadow_busy_size_positive_staten_island_v3),
            "earnings_shadow_pay_quality_positive_staten_island_v3": None if earnings_shadow_pay_quality_positive_staten_island_v3 is None else float(earnings_shadow_pay_quality_positive_staten_island_v3),
            "earnings_shadow_trip_mix_positive_staten_island_v3": None if earnings_shadow_trip_mix_positive_staten_island_v3 is None else float(earnings_shadow_trip_mix_positive_staten_island_v3),
            "earnings_shadow_continuation_positive_staten_island_v3": None if earnings_shadow_continuation_positive_staten_island_v3 is None else float(earnings_shadow_continuation_positive_staten_island_v3),
            "earnings_shadow_short_trip_penalty_staten_island_v3": None if earnings_shadow_short_trip_penalty_staten_island_v3 is None else float(earnings_shadow_short_trip_penalty_staten_island_v3),
            "earnings_shadow_retention_penalty_staten_island_v3": None if earnings_shadow_retention_penalty_staten_island_v3 is None else float(earnings_shadow_retention_penalty_staten_island_v3),
            "earnings_shadow_friction_penalty_staten_island_v3": None if earnings_shadow_friction_penalty_staten_island_v3 is None else float(earnings_shadow_friction_penalty_staten_island_v3),
            "earnings_shadow_saturation_penalty_staten_island_v3": None if earnings_shadow_saturation_penalty_staten_island_v3 is None else float(earnings_shadow_saturation_penalty_staten_island_v3),
            "earnings_shadow_score_citywide_v3": None if earnings_shadow_score_citywide_v3 is None else float(earnings_shadow_score_citywide_v3),
            "earnings_shadow_confidence_citywide_v3": None if earnings_shadow_confidence_citywide_v3 is None else float(earnings_shadow_confidence_citywide_v3),
            "earnings_shadow_rating_citywide_v3": None if earnings_shadow_rating_citywide_v3 is None else int(earnings_shadow_rating_citywide_v3),
            "earnings_shadow_bucket_citywide_v3": earnings_shadow_bucket_citywide_v3,
            "earnings_shadow_color_citywide_v3": earnings_shadow_color_citywide_v3,
            "earnings_shadow_score_citywide_v2": None if earnings_shadow_score_citywide_v2 is None else float(earnings_shadow_score_citywide_v2),
            "earnings_shadow_confidence_citywide_v2": None if earnings_shadow_confidence_citywide_v2 is None else float(earnings_shadow_confidence_citywide_v2),
            "earnings_shadow_rating_citywide_v2": None if earnings_shadow_rating_citywide_v2 is None else int(earnings_shadow_rating_citywide_v2),
            "earnings_shadow_bucket_citywide_v2": earnings_shadow_bucket_citywide_v2,
            "earnings_shadow_color_citywide_v2": earnings_shadow_color_citywide_v2,
            "earnings_shadow_score_manhattan_v2": None if earnings_shadow_score_manhattan_v2 is None else float(earnings_shadow_score_manhattan_v2),
            "earnings_shadow_confidence_manhattan_v2": None if earnings_shadow_confidence_manhattan_v2 is None else float(earnings_shadow_confidence_manhattan_v2),
            "earnings_shadow_rating_manhattan_v2": None if earnings_shadow_rating_manhattan_v2 is None else int(earnings_shadow_rating_manhattan_v2),
            "earnings_shadow_bucket_manhattan_v2": earnings_shadow_bucket_manhattan_v2,
            "earnings_shadow_color_manhattan_v2": earnings_shadow_color_manhattan_v2,
            "earnings_shadow_score_bronx_wash_heights_v2": None if earnings_shadow_score_bronx_wash_heights_v2 is None else float(earnings_shadow_score_bronx_wash_heights_v2),
            "earnings_shadow_confidence_bronx_wash_heights_v2": None if earnings_shadow_confidence_bronx_wash_heights_v2 is None else float(earnings_shadow_confidence_bronx_wash_heights_v2),
            "earnings_shadow_rating_bronx_wash_heights_v2": None if earnings_shadow_rating_bronx_wash_heights_v2 is None else int(earnings_shadow_rating_bronx_wash_heights_v2),
            "earnings_shadow_bucket_bronx_wash_heights_v2": earnings_shadow_bucket_bronx_wash_heights_v2,
            "earnings_shadow_color_bronx_wash_heights_v2": earnings_shadow_color_bronx_wash_heights_v2,
            "earnings_shadow_score_queens_v2": None if earnings_shadow_score_queens_v2 is None else float(earnings_shadow_score_queens_v2),
            "earnings_shadow_confidence_queens_v2": None if earnings_shadow_confidence_queens_v2 is None else float(earnings_shadow_confidence_queens_v2),
            "earnings_shadow_rating_queens_v2": None if earnings_shadow_rating_queens_v2 is None else int(earnings_shadow_rating_queens_v2),
            "earnings_shadow_bucket_queens_v2": earnings_shadow_bucket_queens_v2,
            "earnings_shadow_color_queens_v2": earnings_shadow_color_queens_v2,
            "earnings_shadow_score_brooklyn_v2": None if earnings_shadow_score_brooklyn_v2 is None else float(earnings_shadow_score_brooklyn_v2),
            "earnings_shadow_confidence_brooklyn_v2": None if earnings_shadow_confidence_brooklyn_v2 is None else float(earnings_shadow_confidence_brooklyn_v2),
            "earnings_shadow_rating_brooklyn_v2": None if earnings_shadow_rating_brooklyn_v2 is None else int(earnings_shadow_rating_brooklyn_v2),
            "earnings_shadow_bucket_brooklyn_v2": earnings_shadow_bucket_brooklyn_v2,
            "earnings_shadow_color_brooklyn_v2": earnings_shadow_color_brooklyn_v2,
            "earnings_shadow_score_staten_island_v2": None if earnings_shadow_score_staten_island_v2 is None else float(earnings_shadow_score_staten_island_v2),
            "earnings_shadow_confidence_staten_island_v2": None if earnings_shadow_confidence_staten_island_v2 is None else float(earnings_shadow_confidence_staten_island_v2),
            "earnings_shadow_rating_staten_island_v2": None if earnings_shadow_rating_staten_island_v2 is None else int(earnings_shadow_rating_staten_island_v2),
            "earnings_shadow_bucket_staten_island_v2": earnings_shadow_bucket_staten_island_v2,
            "earnings_shadow_color_staten_island_v2": earnings_shadow_color_staten_island_v2,
            "earnings_shadow_score_manhattan_v3": None if earnings_shadow_score_manhattan_v3 is None else float(earnings_shadow_score_manhattan_v3),
            "earnings_shadow_confidence_manhattan_v3": None if earnings_shadow_confidence_manhattan_v3 is None else float(earnings_shadow_confidence_manhattan_v3),
            "earnings_shadow_rating_manhattan_v3": None if earnings_shadow_rating_manhattan_v3 is None else int(earnings_shadow_rating_manhattan_v3),
            "earnings_shadow_bucket_manhattan_v3": earnings_shadow_bucket_manhattan_v3,
            "earnings_shadow_color_manhattan_v3": earnings_shadow_color_manhattan_v3,
            "earnings_shadow_score_bronx_wash_heights_v3": None if earnings_shadow_score_bronx_wash_heights_v3 is None else float(earnings_shadow_score_bronx_wash_heights_v3),
            "earnings_shadow_confidence_bronx_wash_heights_v3": None if earnings_shadow_confidence_bronx_wash_heights_v3 is None else float(earnings_shadow_confidence_bronx_wash_heights_v3),
            "earnings_shadow_rating_bronx_wash_heights_v3": None if earnings_shadow_rating_bronx_wash_heights_v3 is None else int(earnings_shadow_rating_bronx_wash_heights_v3),
            "earnings_shadow_bucket_bronx_wash_heights_v3": earnings_shadow_bucket_bronx_wash_heights_v3,
            "earnings_shadow_color_bronx_wash_heights_v3": earnings_shadow_color_bronx_wash_heights_v3,
            "earnings_shadow_score_queens_v3": None if earnings_shadow_score_queens_v3 is None else float(earnings_shadow_score_queens_v3),
            "earnings_shadow_confidence_queens_v3": None if earnings_shadow_confidence_queens_v3 is None else float(earnings_shadow_confidence_queens_v3),
            "earnings_shadow_rating_queens_v3": None if earnings_shadow_rating_queens_v3 is None else int(earnings_shadow_rating_queens_v3),
            "earnings_shadow_bucket_queens_v3": earnings_shadow_bucket_queens_v3,
            "earnings_shadow_color_queens_v3": earnings_shadow_color_queens_v3,
            "earnings_shadow_score_brooklyn_v3": None if earnings_shadow_score_brooklyn_v3 is None else float(earnings_shadow_score_brooklyn_v3),
            "earnings_shadow_confidence_brooklyn_v3": None if earnings_shadow_confidence_brooklyn_v3 is None else float(earnings_shadow_confidence_brooklyn_v3),
            "earnings_shadow_rating_brooklyn_v3": None if earnings_shadow_rating_brooklyn_v3 is None else int(earnings_shadow_rating_brooklyn_v3),
            "earnings_shadow_bucket_brooklyn_v3": earnings_shadow_bucket_brooklyn_v3,
            "earnings_shadow_color_brooklyn_v3": earnings_shadow_color_brooklyn_v3,
            "earnings_shadow_score_staten_island_v3": None if earnings_shadow_score_staten_island_v3 is None else float(earnings_shadow_score_staten_island_v3),
            "earnings_shadow_confidence_staten_island_v3": None if earnings_shadow_confidence_staten_island_v3 is None else float(earnings_shadow_confidence_staten_island_v3),
            "earnings_shadow_rating_staten_island_v3": None if earnings_shadow_rating_staten_island_v3 is None else int(earnings_shadow_rating_staten_island_v3),
            "earnings_shadow_bucket_staten_island_v3": earnings_shadow_bucket_staten_island_v3,
            "earnings_shadow_color_staten_island_v3": earnings_shadow_color_staten_island_v3,
            "citywide_v3_confidence_profile_shadow": None if citywide_v3_confidence_profile_shadow is None else float(citywide_v3_confidence_profile_shadow),
            "manhattan_v3_confidence_profile_shadow": None if manhattan_v3_confidence_profile_shadow is None else float(manhattan_v3_confidence_profile_shadow),
            "bronx_wash_heights_v3_confidence_profile_shadow": None if bronx_wash_heights_v3_confidence_profile_shadow is None else float(bronx_wash_heights_v3_confidence_profile_shadow),
            "queens_v3_confidence_profile_shadow": None if queens_v3_confidence_profile_shadow is None else float(queens_v3_confidence_profile_shadow),
            "brooklyn_v3_confidence_profile_shadow": None if brooklyn_v3_confidence_profile_shadow is None else float(brooklyn_v3_confidence_profile_shadow),
            "staten_island_v3_confidence_profile_shadow": None if staten_island_v3_confidence_profile_shadow is None else float(staten_island_v3_confidence_profile_shadow),
        }

    cur = con.execute(sql)

    # timeline labels (Mon-based week anchor)
    week_start = datetime(2025, 1, 6, 0, 0, 0)  # Monday anchor
    timeline: List[str] = []
    frame_count = 0

    current_key: Tuple[int, int] | None = None
    current_features: List[Dict[str, Any]] = []
    current_time_iso: str | None = None
    current_popup_metric_diagnostics_by_location_id: Dict[int, Dict[str, Any]] = {}

    def flush_frame():
        nonlocal frame_count, current_features, current_time_iso, current_popup_metric_diagnostics_by_location_id
        if current_time_iso is None:
            return

        _recalibrate_visible_v3_fields(current_features)
        _validate_popup_metric_consistency(
            current_features,
            current_time_iso,
            diagnostics_by_location_id=current_popup_metric_diagnostics_by_location_id,
        )
        _validate_rating_bucket_color_consistency(current_features, current_time_iso)
        timeline.append(current_time_iso)
        frame_path = stage_dir / f"frame_{frame_count:06d}.json"
        payload = {
            "time": current_time_iso,
            "polygons": {"type": "FeatureCollection", "features": current_features},
        }
        frame_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

        frame_count += 1
        current_features = []
        current_time_iso = None
        current_popup_metric_diagnostics_by_location_id = {}

    total_rows = 0
    any_rows = False

    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        any_rows = True

        for (zid, dow_m, bin_start_min, pickups, avg_pay, rating) in batch:
            total_rows += 1
            key = (int(dow_m), int(bin_start_min))

            if current_key is None:
                current_key = key
            if key != current_key:
                flush_frame()
                current_key = key

            hour = int(bin_start_min // 60)
            minute = int(bin_start_min % 60)
            ts = week_start + timedelta(days=int(dow_m), hours=hour, minutes=minute)
            current_time_iso = ts.strftime("%Y-%m-%dT%H:%M:%S")

            zid_i = int(zid)
            geom = geom_by_id.get(zid_i)
            if not geom:
                continue

            r = int(rating)
            bucket, fill = bucket_and_color_from_rating(r)
            shadow_props = shadow_by_key.get((zid_i, int(dow_m), int(bin_start_min)), {})
            geometry_area_sq_miles = None
            if zid_i in zone_geometry_by_id:
                geometry_area_sq_miles = zone_geometry_by_id[zid_i].get("zone_area_sq_miles")
            popup_metrics = _resolve_popup_metrics(
                raw_shadow_props=shadow_props,
                visible_pickups=int(pickups),
                geometry_area_sq_miles=geometry_area_sq_miles,
            )
            if not bool(is_airport_zone(zid_i, name_by_id.get(zid_i, ""), borough_by_id.get(zid_i, ""))):
                current_popup_metric_diagnostics_by_location_id[zid_i] = {
                    "geometry_area_row_exists": bool(
                        zid_i in zone_geometry_by_id
                        and _as_finite_number(zone_geometry_by_id[zid_i].get("zone_area_sq_miles")) is not None
                        and float(zone_geometry_by_id[zid_i]["zone_area_sq_miles"]) > 0
                    ),
                    "shadow_sql_row_exists": bool(shadow_props),
                    "pickups_now_shadow": shadow_props.get("pickups_now_shadow"),
                    "next_pickups_shadow": shadow_props.get("next_pickups_shadow"),
                    "zone_area_sq_miles_shadow": shadow_props.get("zone_area_sq_miles_shadow"),
                    "pickups_per_sq_mile_now_shadow": shadow_props.get("pickups_per_sq_mile_now_shadow"),
                    "pickups_per_sq_mile_next_shadow": shadow_props.get("pickups_per_sq_mile_next_shadow"),
                }
            if (not bool(is_airport_zone(zid_i, name_by_id.get(zid_i, ""), borough_by_id.get(zid_i, "")))) and popup_metrics is None:
                popup_diagnostics = {
                    "LocationID": zid_i,
                    "zone_name": name_by_id.get(zid_i, ""),
                    "borough": borough_by_id.get(zid_i, ""),
                    "frame_time": current_time_iso,
                    "shadow_sql_row_exists": bool(shadow_props),
                    "geometry_area_row_exists": bool(
                        zid_i in zone_geometry_by_id
                        and _as_finite_number((zone_geometry_by_id.get(zid_i) or {}).get("zone_area_sq_miles")) is not None
                        and float((zone_geometry_by_id.get(zid_i) or {}).get("zone_area_sq_miles")) > 0
                    ),
                    "raw_popup_metric_inputs": {
                        "pickups_now_shadow": shadow_props.get("pickups_now_shadow"),
                        "next_pickups_shadow": shadow_props.get("next_pickups_shadow"),
                        "zone_area_sq_miles_shadow": shadow_props.get("zone_area_sq_miles_shadow"),
                        "pickups_per_sq_mile_now_shadow": shadow_props.get("pickups_per_sq_mile_now_shadow"),
                        "pickups_per_sq_mile_next_shadow": shadow_props.get("pickups_per_sq_mile_next_shadow"),
                        "geometry_area_sq_miles": geometry_area_sq_miles,
                        "visible_pickups": int(pickups),
                    },
                    "fallback_resolution_attempted": True,
                }
                logger.error("non_airport_popup_metric_resolution_failed %s", json.dumps(popup_diagnostics, sort_keys=True))
                raise RuntimeError(
                    "Popup metric resolution failed for non-airport zone "
                    f"LocationID={zid_i} zone_name={name_by_id.get(zid_i, '')!r} frame_time={current_time_iso}"
                )

            current_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "LocationID": zid_i,
                    "zone_name": name_by_id.get(zid_i, ""),
                    "borough": borough_by_id.get(zid_i, ""),
                    "airport_excluded": bool(is_airport_zone(zid_i, name_by_id.get(zid_i, ""), borough_by_id.get(zid_i, ""))),
                    "rating": r,
                    "bucket": bucket,
                    "pickups": int(pickups),
                    "avg_driver_pay": None if avg_pay is None else float(avg_pay),
                    "avg_tips": None,
                    "style": {
                        "color": fill,
                        "opacity": 0,
                        "weight": 0,
                        "fillColor": fill,
                        "fillOpacity": 0.82
                    },
                    "next_pickups_shadow": popup_metrics.get("next_pickups_shadow") if popup_metrics else shadow_props.get("next_pickups_shadow"),
                    "pickups_now_shadow": popup_metrics.get("pickups_now_shadow") if popup_metrics else shadow_props.get("pickups_now_shadow"),
                    "median_driver_pay_shadow": shadow_props.get("median_driver_pay_shadow"),
                    "median_pay_per_min_shadow": shadow_props.get("median_pay_per_min_shadow"),
                    "median_pay_per_mile_shadow": shadow_props.get("median_pay_per_mile_shadow"),
                    "median_request_to_pickup_min_shadow": shadow_props.get("median_request_to_pickup_min_shadow"),
                    "short_trip_share_shadow": shadow_props.get("short_trip_share_shadow"),
                    "shared_ride_share_shadow": shadow_props.get("shared_ride_share_shadow"),
                    "zone_area_sq_miles_shadow": popup_metrics.get("zone_area_sq_miles_shadow") if popup_metrics else shadow_props.get("zone_area_sq_miles_shadow"),
                    "pickups_per_sq_mile_now_shadow": popup_metrics.get("pickups_per_sq_mile_now_shadow") if popup_metrics else shadow_props.get("pickups_per_sq_mile_now_shadow"),
                    "pickups_per_sq_mile_next_shadow": popup_metrics.get("pickups_per_sq_mile_next_shadow") if popup_metrics else shadow_props.get("pickups_per_sq_mile_next_shadow"),
                    "long_trip_share_20plus_shadow": shadow_props.get("long_trip_share_20plus_shadow"),
                    "balanced_trip_share_shadow": shadow_props.get("balanced_trip_share_shadow"),
                    "same_zone_dropoff_share_shadow": shadow_props.get("same_zone_dropoff_share_shadow"),
                    "downstream_value_shadow": shadow_props.get("downstream_value_shadow"),
                    "demand_now_n_shadow": shadow_props.get("demand_now_n_shadow"),
                    "demand_next_n_shadow": shadow_props.get("demand_next_n_shadow"),
                    "pay_n_shadow": shadow_props.get("pay_n_shadow"),
                    "pay_per_min_n_shadow": shadow_props.get("pay_per_min_n_shadow"),
                    "pay_per_mile_n_shadow": shadow_props.get("pay_per_mile_n_shadow"),
                    "pickup_friction_penalty_n_shadow": shadow_props.get("pickup_friction_penalty_n_shadow"),
                    "short_trip_penalty_n_shadow": shadow_props.get("short_trip_penalty_n_shadow"),
                    "shared_ride_penalty_n_shadow": shadow_props.get("shared_ride_penalty_n_shadow"),
                    "downstream_value_n_shadow": shadow_props.get("downstream_value_n_shadow"),
                    "demand_density_now_n_shadow": shadow_props.get("demand_density_now_n_shadow"),
                    "demand_density_next_n_shadow": shadow_props.get("demand_density_next_n_shadow"),
                    "demand_support_n_shadow": shadow_props.get("demand_support_n_shadow"),
                    "density_support_n_shadow": shadow_props.get("density_support_n_shadow"),
                    "effective_demand_density_now_n_shadow": shadow_props.get("effective_demand_density_now_n_shadow"),
                    "effective_demand_density_next_n_shadow": shadow_props.get("effective_demand_density_next_n_shadow"),
                    "busy_now_base_n_shadow": shadow_props.get("busy_now_base_n_shadow"),
                    "busy_next_base_n_shadow": shadow_props.get("busy_next_base_n_shadow"),
                    "long_trip_share_20plus_n_shadow": shadow_props.get("long_trip_share_20plus_n_shadow"),
                    "balanced_trip_share_n_shadow": shadow_props.get("balanced_trip_share_n_shadow"),
                    "same_zone_retention_penalty_n_shadow": shadow_props.get("same_zone_retention_penalty_n_shadow"),
                    "churn_pressure_n_shadow": shadow_props.get("churn_pressure_n_shadow"),
                    "manhattan_core_saturation_proxy_n_shadow": shadow_props.get("manhattan_core_saturation_proxy_n_shadow"),
                    "manhattan_core_saturation_penalty_n_shadow": shadow_props.get("manhattan_core_saturation_penalty_n_shadow"),
                    "market_saturation_pressure_n_shadow": shadow_props.get("market_saturation_pressure_n_shadow"),
                    "market_saturation_penalty_n_shadow": shadow_props.get("market_saturation_penalty_n_shadow"),
                    "citywide_manhattan_saturation_discount_factor_shadow": shadow_props.get("citywide_manhattan_saturation_discount_factor_shadow"),
                    "earnings_shadow_positive_citywide_v3": shadow_props.get("earnings_shadow_positive_citywide_v3"),
                    "earnings_shadow_negative_citywide_v3": shadow_props.get("earnings_shadow_negative_citywide_v3"),
                    "earnings_shadow_score_raw_citywide_v3": shadow_props.get("earnings_shadow_score_raw_citywide_v3"),
                    "earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow": shadow_props.get("earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow"),
                    "earnings_shadow_score_citywide_v3_anchor_shadow": shadow_props.get("earnings_shadow_score_citywide_v3_anchor_shadow"),
                    "earnings_shadow_score_raw_manhattan_v3": shadow_props.get("earnings_shadow_score_raw_manhattan_v3"),
                    "earnings_shadow_score_raw_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_score_raw_bronx_wash_heights_v3"),
                    "earnings_shadow_score_raw_queens_v3": shadow_props.get("earnings_shadow_score_raw_queens_v3"),
                    "earnings_shadow_score_raw_brooklyn_v3": shadow_props.get("earnings_shadow_score_raw_brooklyn_v3"),
                    "earnings_shadow_score_raw_staten_island_v3": shadow_props.get("earnings_shadow_score_raw_staten_island_v3"),
                    "earnings_shadow_busy_size_positive_citywide_v3": shadow_props.get("earnings_shadow_busy_size_positive_citywide_v3"),
                    "earnings_shadow_pay_quality_positive_citywide_v3": shadow_props.get("earnings_shadow_pay_quality_positive_citywide_v3"),
                    "earnings_shadow_trip_mix_positive_citywide_v3": shadow_props.get("earnings_shadow_trip_mix_positive_citywide_v3"),
                    "earnings_shadow_continuation_positive_citywide_v3": shadow_props.get("earnings_shadow_continuation_positive_citywide_v3"),
                    "earnings_shadow_short_trip_penalty_citywide_v3": shadow_props.get("earnings_shadow_short_trip_penalty_citywide_v3"),
                    "earnings_shadow_retention_penalty_citywide_v3": shadow_props.get("earnings_shadow_retention_penalty_citywide_v3"),
                    "earnings_shadow_friction_penalty_citywide_v3": shadow_props.get("earnings_shadow_friction_penalty_citywide_v3"),
                    "earnings_shadow_saturation_penalty_citywide_v3": shadow_props.get("earnings_shadow_saturation_penalty_citywide_v3"),
                    "earnings_shadow_busy_size_positive_manhattan_v3": shadow_props.get("earnings_shadow_busy_size_positive_manhattan_v3"),
                    "earnings_shadow_pay_quality_positive_manhattan_v3": shadow_props.get("earnings_shadow_pay_quality_positive_manhattan_v3"),
                    "earnings_shadow_trip_mix_positive_manhattan_v3": shadow_props.get("earnings_shadow_trip_mix_positive_manhattan_v3"),
                    "earnings_shadow_continuation_positive_manhattan_v3": shadow_props.get("earnings_shadow_continuation_positive_manhattan_v3"),
                    "earnings_shadow_short_trip_penalty_manhattan_v3": shadow_props.get("earnings_shadow_short_trip_penalty_manhattan_v3"),
                    "earnings_shadow_retention_penalty_manhattan_v3": shadow_props.get("earnings_shadow_retention_penalty_manhattan_v3"),
                    "earnings_shadow_friction_penalty_manhattan_v3": shadow_props.get("earnings_shadow_friction_penalty_manhattan_v3"),
                    "earnings_shadow_saturation_penalty_manhattan_v3": shadow_props.get("earnings_shadow_saturation_penalty_manhattan_v3"),
                    "earnings_shadow_busy_size_positive_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_busy_size_positive_bronx_wash_heights_v3"),
                    "earnings_shadow_pay_quality_positive_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_pay_quality_positive_bronx_wash_heights_v3"),
                    "earnings_shadow_trip_mix_positive_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_trip_mix_positive_bronx_wash_heights_v3"),
                    "earnings_shadow_continuation_positive_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_continuation_positive_bronx_wash_heights_v3"),
                    "earnings_shadow_short_trip_penalty_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_short_trip_penalty_bronx_wash_heights_v3"),
                    "earnings_shadow_retention_penalty_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_retention_penalty_bronx_wash_heights_v3"),
                    "earnings_shadow_friction_penalty_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_friction_penalty_bronx_wash_heights_v3"),
                    "earnings_shadow_saturation_penalty_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_saturation_penalty_bronx_wash_heights_v3"),
                    "earnings_shadow_busy_size_positive_queens_v3": shadow_props.get("earnings_shadow_busy_size_positive_queens_v3"),
                    "earnings_shadow_pay_quality_positive_queens_v3": shadow_props.get("earnings_shadow_pay_quality_positive_queens_v3"),
                    "earnings_shadow_trip_mix_positive_queens_v3": shadow_props.get("earnings_shadow_trip_mix_positive_queens_v3"),
                    "earnings_shadow_continuation_positive_queens_v3": shadow_props.get("earnings_shadow_continuation_positive_queens_v3"),
                    "earnings_shadow_short_trip_penalty_queens_v3": shadow_props.get("earnings_shadow_short_trip_penalty_queens_v3"),
                    "earnings_shadow_retention_penalty_queens_v3": shadow_props.get("earnings_shadow_retention_penalty_queens_v3"),
                    "earnings_shadow_friction_penalty_queens_v3": shadow_props.get("earnings_shadow_friction_penalty_queens_v3"),
                    "earnings_shadow_saturation_penalty_queens_v3": shadow_props.get("earnings_shadow_saturation_penalty_queens_v3"),
                    "earnings_shadow_busy_size_positive_brooklyn_v3": shadow_props.get("earnings_shadow_busy_size_positive_brooklyn_v3"),
                    "earnings_shadow_pay_quality_positive_brooklyn_v3": shadow_props.get("earnings_shadow_pay_quality_positive_brooklyn_v3"),
                    "earnings_shadow_trip_mix_positive_brooklyn_v3": shadow_props.get("earnings_shadow_trip_mix_positive_brooklyn_v3"),
                    "earnings_shadow_continuation_positive_brooklyn_v3": shadow_props.get("earnings_shadow_continuation_positive_brooklyn_v3"),
                    "earnings_shadow_short_trip_penalty_brooklyn_v3": shadow_props.get("earnings_shadow_short_trip_penalty_brooklyn_v3"),
                    "earnings_shadow_retention_penalty_brooklyn_v3": shadow_props.get("earnings_shadow_retention_penalty_brooklyn_v3"),
                    "earnings_shadow_friction_penalty_brooklyn_v3": shadow_props.get("earnings_shadow_friction_penalty_brooklyn_v3"),
                    "earnings_shadow_saturation_penalty_brooklyn_v3": shadow_props.get("earnings_shadow_saturation_penalty_brooklyn_v3"),
                    "earnings_shadow_busy_size_positive_staten_island_v3": shadow_props.get("earnings_shadow_busy_size_positive_staten_island_v3"),
                    "earnings_shadow_pay_quality_positive_staten_island_v3": shadow_props.get("earnings_shadow_pay_quality_positive_staten_island_v3"),
                    "earnings_shadow_trip_mix_positive_staten_island_v3": shadow_props.get("earnings_shadow_trip_mix_positive_staten_island_v3"),
                    "earnings_shadow_continuation_positive_staten_island_v3": shadow_props.get("earnings_shadow_continuation_positive_staten_island_v3"),
                    "earnings_shadow_short_trip_penalty_staten_island_v3": shadow_props.get("earnings_shadow_short_trip_penalty_staten_island_v3"),
                    "earnings_shadow_retention_penalty_staten_island_v3": shadow_props.get("earnings_shadow_retention_penalty_staten_island_v3"),
                    "earnings_shadow_friction_penalty_staten_island_v3": shadow_props.get("earnings_shadow_friction_penalty_staten_island_v3"),
                    "earnings_shadow_saturation_penalty_staten_island_v3": shadow_props.get("earnings_shadow_saturation_penalty_staten_island_v3"),
                    "earnings_shadow_score_citywide_v3": shadow_props.get("earnings_shadow_score_citywide_v3"),
                    "earnings_shadow_confidence_citywide_v3": shadow_props.get("earnings_shadow_confidence_citywide_v3"),
                    "earnings_shadow_citywide_anchor_input_v3": shadow_props.get("earnings_shadow_citywide_anchor_input_v3"),
                    "earnings_shadow_citywide_anchor_base_v3": shadow_props.get("earnings_shadow_citywide_anchor_base_v3"),
                    "earnings_shadow_citywide_anchor_display_v3": shadow_props.get("earnings_shadow_citywide_anchor_display_v3"),
                    "earnings_shadow_citywide_anchor_norm_v3": shadow_props.get("earnings_shadow_citywide_anchor_norm_v3"),
                    "earnings_shadow_rating_citywide_v3": shadow_props.get("earnings_shadow_rating_citywide_v3"),
                    "earnings_shadow_bucket_citywide_v3": shadow_props.get("earnings_shadow_bucket_citywide_v3"),
                    "earnings_shadow_color_citywide_v3": shadow_props.get("earnings_shadow_color_citywide_v3"),
                    "earnings_shadow_score_citywide_v2": shadow_props.get("earnings_shadow_score_citywide_v2"),
                    "earnings_shadow_confidence_citywide_v2": shadow_props.get("earnings_shadow_confidence_citywide_v2"),
                    "earnings_shadow_rating_citywide_v2": shadow_props.get("earnings_shadow_rating_citywide_v2"),
                    "earnings_shadow_bucket_citywide_v2": shadow_props.get("earnings_shadow_bucket_citywide_v2"),
                    "earnings_shadow_color_citywide_v2": shadow_props.get("earnings_shadow_color_citywide_v2"),
                    "earnings_shadow_score_manhattan_v2": shadow_props.get("earnings_shadow_score_manhattan_v2"),
                    "earnings_shadow_confidence_manhattan_v2": shadow_props.get("earnings_shadow_confidence_manhattan_v2"),
                    "earnings_shadow_rating_manhattan_v2": shadow_props.get("earnings_shadow_rating_manhattan_v2"),
                    "earnings_shadow_bucket_manhattan_v2": shadow_props.get("earnings_shadow_bucket_manhattan_v2"),
                    "earnings_shadow_color_manhattan_v2": shadow_props.get("earnings_shadow_color_manhattan_v2"),
                    "earnings_shadow_score_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_score_bronx_wash_heights_v2"),
                    "earnings_shadow_confidence_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_confidence_bronx_wash_heights_v2"),
                    "earnings_shadow_rating_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_rating_bronx_wash_heights_v2"),
                    "earnings_shadow_bucket_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_bucket_bronx_wash_heights_v2"),
                    "earnings_shadow_color_bronx_wash_heights_v2": shadow_props.get("earnings_shadow_color_bronx_wash_heights_v2"),
                    "earnings_shadow_score_queens_v2": shadow_props.get("earnings_shadow_score_queens_v2"),
                    "earnings_shadow_confidence_queens_v2": shadow_props.get("earnings_shadow_confidence_queens_v2"),
                    "earnings_shadow_rating_queens_v2": shadow_props.get("earnings_shadow_rating_queens_v2"),
                    "earnings_shadow_bucket_queens_v2": shadow_props.get("earnings_shadow_bucket_queens_v2"),
                    "earnings_shadow_color_queens_v2": shadow_props.get("earnings_shadow_color_queens_v2"),
                    "earnings_shadow_score_brooklyn_v2": shadow_props.get("earnings_shadow_score_brooklyn_v2"),
                    "earnings_shadow_confidence_brooklyn_v2": shadow_props.get("earnings_shadow_confidence_brooklyn_v2"),
                    "earnings_shadow_rating_brooklyn_v2": shadow_props.get("earnings_shadow_rating_brooklyn_v2"),
                    "earnings_shadow_bucket_brooklyn_v2": shadow_props.get("earnings_shadow_bucket_brooklyn_v2"),
                    "earnings_shadow_color_brooklyn_v2": shadow_props.get("earnings_shadow_color_brooklyn_v2"),
                    "earnings_shadow_score_staten_island_v2": shadow_props.get("earnings_shadow_score_staten_island_v2"),
                    "earnings_shadow_confidence_staten_island_v2": shadow_props.get("earnings_shadow_confidence_staten_island_v2"),
                    "earnings_shadow_rating_staten_island_v2": shadow_props.get("earnings_shadow_rating_staten_island_v2"),
                    "earnings_shadow_bucket_staten_island_v2": shadow_props.get("earnings_shadow_bucket_staten_island_v2"),
                    "earnings_shadow_color_staten_island_v2": shadow_props.get("earnings_shadow_color_staten_island_v2"),
                    "earnings_shadow_score_manhattan_v3": shadow_props.get("earnings_shadow_score_manhattan_v3"),
                    "earnings_shadow_confidence_manhattan_v3": shadow_props.get("earnings_shadow_confidence_manhattan_v3"),
                    "earnings_shadow_rating_manhattan_v3": shadow_props.get("earnings_shadow_rating_manhattan_v3"),
                    "earnings_shadow_bucket_manhattan_v3": shadow_props.get("earnings_shadow_bucket_manhattan_v3"),
                    "earnings_shadow_color_manhattan_v3": shadow_props.get("earnings_shadow_color_manhattan_v3"),
                    "earnings_shadow_score_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_score_bronx_wash_heights_v3"),
                    "earnings_shadow_confidence_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_confidence_bronx_wash_heights_v3"),
                    "earnings_shadow_rating_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_rating_bronx_wash_heights_v3"),
                    "earnings_shadow_bucket_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_bucket_bronx_wash_heights_v3"),
                    "earnings_shadow_color_bronx_wash_heights_v3": shadow_props.get("earnings_shadow_color_bronx_wash_heights_v3"),
                    "earnings_shadow_score_queens_v3": shadow_props.get("earnings_shadow_score_queens_v3"),
                    "earnings_shadow_confidence_queens_v3": shadow_props.get("earnings_shadow_confidence_queens_v3"),
                    "earnings_shadow_rating_queens_v3": shadow_props.get("earnings_shadow_rating_queens_v3"),
                    "earnings_shadow_bucket_queens_v3": shadow_props.get("earnings_shadow_bucket_queens_v3"),
                    "earnings_shadow_color_queens_v3": shadow_props.get("earnings_shadow_color_queens_v3"),
                    "earnings_shadow_score_brooklyn_v3": shadow_props.get("earnings_shadow_score_brooklyn_v3"),
                    "earnings_shadow_confidence_brooklyn_v3": shadow_props.get("earnings_shadow_confidence_brooklyn_v3"),
                    "earnings_shadow_rating_brooklyn_v3": shadow_props.get("earnings_shadow_rating_brooklyn_v3"),
                    "earnings_shadow_bucket_brooklyn_v3": shadow_props.get("earnings_shadow_bucket_brooklyn_v3"),
                    "earnings_shadow_color_brooklyn_v3": shadow_props.get("earnings_shadow_color_brooklyn_v3"),
                    "earnings_shadow_score_staten_island_v3": shadow_props.get("earnings_shadow_score_staten_island_v3"),
                    "earnings_shadow_confidence_staten_island_v3": shadow_props.get("earnings_shadow_confidence_staten_island_v3"),
                    "citywide_v3_confidence_profile_shadow": shadow_props.get("citywide_v3_confidence_profile_shadow"),
                    "manhattan_v3_confidence_profile_shadow": shadow_props.get("manhattan_v3_confidence_profile_shadow"),
                    "bronx_wash_heights_v3_confidence_profile_shadow": shadow_props.get("bronx_wash_heights_v3_confidence_profile_shadow"),
                    "queens_v3_confidence_profile_shadow": shadow_props.get("queens_v3_confidence_profile_shadow"),
                    "brooklyn_v3_confidence_profile_shadow": shadow_props.get("brooklyn_v3_confidence_profile_shadow"),
                    "staten_island_v3_confidence_profile_shadow": shadow_props.get("staten_island_v3_confidence_profile_shadow"),
                    "earnings_shadow_rating_staten_island_v3": shadow_props.get("earnings_shadow_rating_staten_island_v3"),
                    "earnings_shadow_bucket_staten_island_v3": shadow_props.get("earnings_shadow_bucket_staten_island_v3"),
                    "earnings_shadow_color_staten_island_v3": shadow_props.get("earnings_shadow_color_staten_island_v3"),
                }
            })

    if not any_rows:
        raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

    flush_frame()

    (stage_dir / "timeline.json").write_text(
        json.dumps({"timeline": timeline, "count": len(timeline)}, separators=(",", ":")),
        encoding="utf-8"
    )
    live_shadow_profiles = [
        "citywide_v2",
        "citywide_v3",
        "manhattan_v2",
        "manhattan_v3",
        "bronx_wash_heights_v2",
        "bronx_wash_heights_v3",
        "queens_v2",
        "queens_v3",
        "brooklyn_v2",
        "brooklyn_v3",
        "staten_island_v2",
        "staten_island_v3",
    ]
    visible_profiles_live = [
        "citywide_v3",
        "manhattan_v3",
        "bronx_wash_heights_v3",
        "queens_v3",
        "brooklyn_v3",
        "staten_island_v3",
    ]

    expected_freshness = build_expected_artifact_signature(
        repo_root=Path(__file__).resolve().parent,
        data_dir=zones_geojson_path.parent,
        frames_dir=stage_dir,
        bin_minutes=int(bin_minutes),
        min_trips_per_window=int(min_trips_per_window),
    )

    manifest_payload = {
                "engine_version": "team-joseo-score-v2-final-live",
                "engine_release": "team-joseo-score-v2-final-live",
                "source": "HVFHV",
                "bin_minutes": int(bin_minutes),
                # Backward-compatibility field kept for external legacy consumers.
                "active_shadow_profile": "citywide_v2",
                "default_citywide_profile": "citywide_v3",
                "all_profiles_live": True,
                "active_shadow_profiles": live_shadow_profiles,
                "visible_profiles_live": visible_profiles_live,
                "candidate_shadow_profiles": [],
                "comparison_profiles": [
                    "citywide_v2",
                    "manhattan_v2",
                    "bronx_wash_heights_v2",
                    "queens_v2",
                    "brooklyn_v2",
                    "staten_island_v2",
                ],
                "base_color_truth": "tlc_hvfhv_earnings_opportunity",
                "community_caution_truth": "team_joseo_presence_only",
                "presence_logic_changed": False,
                "notes": [
                    "Base colors reflect Team Joseo earnings opportunity derived from HVFHV/Taxi Zone data.",
                    "Community crowding caution is separate and based only on Team Joseo live presence.",
                    "No real-time presence timing was changed by the score rollout.",
                    "Phase 1 adds zone-size density and long-trip/trap metrics in shadow form only.",
                    "Phase 2 adds a citywide_v3 shadow candidate that blends raw demand, demand density, long-trip share, pay quality, downstream value, and trap penalties.",
                    "Phase 3 promotes citywide_v3 to the live visible citywide score while borough profiles remain on v2.",
                    "Phase 4 adds borough_v3 shadow candidates that blend density, long-trip quality, pay quality, downstream value, and local trap penalties without changing visible borough scores yet.",
                    "Phase 5 promotes manhattan_v3 to the live visible Manhattan score while other borough visible profiles remain unchanged.",
                    "Phase 6 promotes bronx_wash_heights_v3 to the live visible Bronx/Wash Heights score while Queens, Brooklyn, and Staten Island remain on v2 visible profiles.",
                    "Phase 7 promotes queens_v3 to the live visible Queens score while Brooklyn and Staten Island remain on v2 visible profiles.",
                    "Phase 8 promotes brooklyn_v3 to the live visible Brooklyn score while Staten Island remains on the v2 visible profile.",
                    "Phase 9 promotes staten_island_v3 to the live visible Staten Island score and completes the visible v3 rollout across citywide and all borough modes.",
                ],
                "shadow_fields": [
                    "pickups_now_shadow",
                    "next_pickups_shadow",
                    "median_driver_pay_shadow",
                    "median_pay_per_min_shadow",
                    "median_pay_per_mile_shadow",
                    "median_request_to_pickup_min_shadow",
                    "short_trip_share_shadow",
                    "shared_ride_share_shadow",
                    "zone_area_sq_miles_shadow",
                    "pickups_per_sq_mile_now_shadow",
                    "pickups_per_sq_mile_next_shadow",
                    "long_trip_share_20plus_shadow",
                    "balanced_trip_share_shadow",
                    "same_zone_dropoff_share_shadow",
                    "downstream_value_shadow",
                    "demand_now_n_shadow",
                    "demand_next_n_shadow",
                    "pay_n_shadow",
                    "pay_per_min_n_shadow",
                    "pay_per_mile_n_shadow",
                    "pickup_friction_penalty_n_shadow",
                    "short_trip_penalty_n_shadow",
                    "shared_ride_penalty_n_shadow",
                    "downstream_value_n_shadow",
                    "demand_density_now_n_shadow",
                    "demand_density_next_n_shadow",
                    "demand_support_n_shadow",
                    "density_support_n_shadow",
                    "effective_demand_density_now_n_shadow",
                    "effective_demand_density_next_n_shadow",
                    "busy_now_base_n_shadow",
                    "busy_next_base_n_shadow",
                    "long_trip_share_20plus_n_shadow",
                    "balanced_trip_share_n_shadow",
                    "same_zone_retention_penalty_n_shadow",
                    "churn_pressure_n_shadow",
                    "manhattan_core_saturation_proxy_n_shadow",
                    "manhattan_core_saturation_penalty_n_shadow",
                    "market_saturation_pressure_n_shadow",
                    "market_saturation_penalty_n_shadow",
                    "citywide_manhattan_saturation_discount_factor_shadow",
                    "citywide_visual_anchor_discount_factor_shadow",
                    "earnings_shadow_positive_citywide_v3",
                    "earnings_shadow_negative_citywide_v3",
                    "earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow",
                    "earnings_shadow_score_citywide_v3_anchor_shadow",
                    "earnings_shadow_busy_size_positive_citywide_v3",
                    "earnings_shadow_pay_quality_positive_citywide_v3",
                    "earnings_shadow_trip_mix_positive_citywide_v3",
                    "earnings_shadow_continuation_positive_citywide_v3",
                    "earnings_shadow_short_trip_penalty_citywide_v3",
                    "earnings_shadow_retention_penalty_citywide_v3",
                    "earnings_shadow_friction_penalty_citywide_v3",
                    "earnings_shadow_saturation_penalty_citywide_v3",
                    "earnings_shadow_busy_size_positive_manhattan_v3",
                    "earnings_shadow_pay_quality_positive_manhattan_v3",
                    "earnings_shadow_trip_mix_positive_manhattan_v3",
                    "earnings_shadow_continuation_positive_manhattan_v3",
                    "earnings_shadow_short_trip_penalty_manhattan_v3",
                    "earnings_shadow_retention_penalty_manhattan_v3",
                    "earnings_shadow_friction_penalty_manhattan_v3",
                    "earnings_shadow_saturation_penalty_manhattan_v3",
                    "earnings_shadow_busy_size_positive_bronx_wash_heights_v3",
                    "earnings_shadow_pay_quality_positive_bronx_wash_heights_v3",
                    "earnings_shadow_trip_mix_positive_bronx_wash_heights_v3",
                    "earnings_shadow_continuation_positive_bronx_wash_heights_v3",
                    "earnings_shadow_short_trip_penalty_bronx_wash_heights_v3",
                    "earnings_shadow_retention_penalty_bronx_wash_heights_v3",
                    "earnings_shadow_friction_penalty_bronx_wash_heights_v3",
                    "earnings_shadow_saturation_penalty_bronx_wash_heights_v3",
                    "earnings_shadow_busy_size_positive_queens_v3",
                    "earnings_shadow_pay_quality_positive_queens_v3",
                    "earnings_shadow_trip_mix_positive_queens_v3",
                    "earnings_shadow_continuation_positive_queens_v3",
                    "earnings_shadow_short_trip_penalty_queens_v3",
                    "earnings_shadow_retention_penalty_queens_v3",
                    "earnings_shadow_friction_penalty_queens_v3",
                    "earnings_shadow_saturation_penalty_queens_v3",
                    "earnings_shadow_busy_size_positive_brooklyn_v3",
                    "earnings_shadow_pay_quality_positive_brooklyn_v3",
                    "earnings_shadow_trip_mix_positive_brooklyn_v3",
                    "earnings_shadow_continuation_positive_brooklyn_v3",
                    "earnings_shadow_short_trip_penalty_brooklyn_v3",
                    "earnings_shadow_retention_penalty_brooklyn_v3",
                    "earnings_shadow_friction_penalty_brooklyn_v3",
                    "earnings_shadow_saturation_penalty_brooklyn_v3",
                    "earnings_shadow_busy_size_positive_staten_island_v3",
                    "earnings_shadow_pay_quality_positive_staten_island_v3",
                    "earnings_shadow_trip_mix_positive_staten_island_v3",
                    "earnings_shadow_continuation_positive_staten_island_v3",
                    "earnings_shadow_short_trip_penalty_staten_island_v3",
                    "earnings_shadow_retention_penalty_staten_island_v3",
                    "earnings_shadow_friction_penalty_staten_island_v3",
                    "earnings_shadow_saturation_penalty_staten_island_v3",
                    "earnings_shadow_score_citywide_v3",
                    "earnings_shadow_confidence_citywide_v3",
                    "earnings_shadow_citywide_anchor_input_v3",
                    "earnings_shadow_citywide_anchor_base_v3",
                    "earnings_shadow_citywide_anchor_display_v3",
                    "earnings_shadow_citywide_anchor_norm_v3",
                    "earnings_shadow_visible_base_score_citywide_v3",
                    "earnings_shadow_visible_base_score_manhattan_v3",
                    "earnings_shadow_visible_base_score_bronx_wash_heights_v3",
                    "earnings_shadow_visible_base_score_queens_v3",
                    "earnings_shadow_visible_base_score_brooklyn_v3",
                    "earnings_shadow_visible_base_score_staten_island_v3",
                    "citywide_v3_confidence_profile_shadow",
                    "manhattan_v3_confidence_profile_shadow",
                    "bronx_wash_heights_v3_confidence_profile_shadow",
                    "queens_v3_confidence_profile_shadow",
                    "brooklyn_v3_confidence_profile_shadow",
                    "staten_island_v3_confidence_profile_shadow",
                    "earnings_shadow_rating_citywide_v3",
                    "earnings_shadow_bucket_citywide_v3",
                    "earnings_shadow_color_citywide_v3",
                    "earnings_shadow_score_citywide_v2",
                    "earnings_shadow_confidence_citywide_v2",
                    "earnings_shadow_rating_citywide_v2",
                    "earnings_shadow_bucket_citywide_v2",
                    "earnings_shadow_color_citywide_v2",
                    "earnings_shadow_score_manhattan_v2",
                    "earnings_shadow_confidence_manhattan_v2",
                    "earnings_shadow_rating_manhattan_v2",
                    "earnings_shadow_bucket_manhattan_v2",
                    "earnings_shadow_color_manhattan_v2",
                    "earnings_shadow_score_bronx_wash_heights_v2",
                    "earnings_shadow_confidence_bronx_wash_heights_v2",
                    "earnings_shadow_rating_bronx_wash_heights_v2",
                    "earnings_shadow_bucket_bronx_wash_heights_v2",
                    "earnings_shadow_color_bronx_wash_heights_v2",
                    "earnings_shadow_score_queens_v2",
                    "earnings_shadow_confidence_queens_v2",
                    "earnings_shadow_rating_queens_v2",
                    "earnings_shadow_bucket_queens_v2",
                    "earnings_shadow_color_queens_v2",
                    "earnings_shadow_score_brooklyn_v2",
                    "earnings_shadow_confidence_brooklyn_v2",
                    "earnings_shadow_rating_brooklyn_v2",
                    "earnings_shadow_bucket_brooklyn_v2",
                    "earnings_shadow_color_brooklyn_v2",
                    "earnings_shadow_score_staten_island_v2",
                    "earnings_shadow_confidence_staten_island_v2",
                    "earnings_shadow_rating_staten_island_v2",
                    "earnings_shadow_bucket_staten_island_v2",
                    "earnings_shadow_color_staten_island_v2",
                    "earnings_shadow_score_manhattan_v3",
                    "earnings_shadow_confidence_manhattan_v3",
                    "earnings_shadow_rating_manhattan_v3",
                    "earnings_shadow_bucket_manhattan_v3",
                    "earnings_shadow_color_manhattan_v3",
                    "earnings_shadow_score_bronx_wash_heights_v3",
                    "earnings_shadow_confidence_bronx_wash_heights_v3",
                    "earnings_shadow_rating_bronx_wash_heights_v3",
                    "earnings_shadow_bucket_bronx_wash_heights_v3",
                    "earnings_shadow_color_bronx_wash_heights_v3",
                    "earnings_shadow_score_queens_v3",
                    "earnings_shadow_confidence_queens_v3",
                    "earnings_shadow_rating_queens_v3",
                    "earnings_shadow_bucket_queens_v3",
                    "earnings_shadow_color_queens_v3",
                    "earnings_shadow_score_brooklyn_v3",
                    "earnings_shadow_confidence_brooklyn_v3",
                    "earnings_shadow_rating_brooklyn_v3",
                    "earnings_shadow_bucket_brooklyn_v3",
                    "earnings_shadow_color_brooklyn_v3",
                    "earnings_shadow_score_staten_island_v3",
                    "earnings_shadow_confidence_staten_island_v3",
                    "earnings_shadow_rating_staten_island_v3",
                    "earnings_shadow_bucket_staten_island_v3",
                    "earnings_shadow_color_staten_island_v3",
                ],
                "artifact_schema_version": expected_freshness.get("artifact_schema_version"),
                "code_dependency_hash": expected_freshness.get("code_dependency_hash"),
                "source_data_hash": expected_freshness.get("source_data_hash"),
                "artifact_signature": expected_freshness.get("artifact_signature"),
                "dependency_files": expected_freshness.get("code_dependencies", {}).get("dependency_files", []),
                "parquet_inventory": expected_freshness.get("source_inventory", {}).get("parquet_files", []),
                "zones_geojson_signature": expected_freshness.get("source_inventory", {}).get("zones_geojson"),
            }

    (stage_dir / "scoring_shadow_manifest.json").write_text(
        json.dumps(manifest_payload, separators=(",", ":")),
        encoding="utf-8",
    )
    staged_timeline = stage_dir / "timeline.json"
    staged_manifest = stage_dir / "scoring_shadow_manifest.json"
    staged_frames = sorted(stage_dir.glob("frame_*.json"))
    if not staged_timeline.exists() or not staged_manifest.exists() or not staged_frames:
        raise RuntimeError("Staged artifact build did not produce required files.")

    for generated in out_dir.glob("frame_*.json"):
        try:
            generated.unlink()
        except Exception:
            pass
    for generated_name in ("timeline.json", "scoring_shadow_manifest.json"):
        generated_path = out_dir / generated_name
        try:
            if generated_path.exists() and generated_path.is_file():
                generated_path.unlink()
        except Exception:
            pass
    for built_file in stage_dir.iterdir():
        if built_file.is_file():
            shutil.move(str(built_file), str(out_dir / built_file.name))
    try:
        con.close()
    except Exception:
        pass
    temp_run_dir_ctx.cleanup()

    return {"ok": True, "count": len(timeline), "frames_dir": str(out_dir), "rows": total_rows}
