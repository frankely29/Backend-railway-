from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime
import json
import math
import statistics
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
from artifact_db_store import save_generated_artifact

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
CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED = (
    str(os.environ.get("CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED", "0"))
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)

CITYWIDE_TRAP_CANDIDATE_SCORE_FIELD = "earnings_shadow_score_citywide_v3_trap_candidate"
CITYWIDE_TRAP_CANDIDATE_CONF_FIELD = "earnings_shadow_confidence_citywide_v3_trap_candidate"
CITYWIDE_BASELINE_SCORE_FIELD = "earnings_shadow_score_citywide_v3_anchor_shadow"
CITYWIDE_BASELINE_CONF_FIELD = "earnings_shadow_confidence_citywide_v3"
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

TRAP_CANDIDATE_REVIEW_PROFILE_CONFIG: Dict[str, Dict[str, str]] = {
    "citywide_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_citywide_v3",
        "live_rating_field": "earnings_shadow_rating_citywide_v3",
        "live_bucket_field": "earnings_shadow_bucket_citywide_v3",
        "candidate_score_field": "earnings_shadow_score_citywide_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_citywide_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_citywide_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_citywide_v3_trap_candidate",
    },
    "manhattan_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_manhattan_v3",
        "live_rating_field": "earnings_shadow_rating_manhattan_v3",
        "live_bucket_field": "earnings_shadow_bucket_manhattan_v3",
        "candidate_score_field": "earnings_shadow_score_manhattan_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_manhattan_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_manhattan_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_manhattan_v3_trap_candidate",
    },
    "bronx_wash_heights_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_bronx_wash_heights_v3",
        "live_rating_field": "earnings_shadow_rating_bronx_wash_heights_v3",
        "live_bucket_field": "earnings_shadow_bucket_bronx_wash_heights_v3",
        "candidate_score_field": "earnings_shadow_score_bronx_wash_heights_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_bronx_wash_heights_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_bronx_wash_heights_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_bronx_wash_heights_v3_trap_candidate",
    },
    "queens_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_queens_v3",
        "live_rating_field": "earnings_shadow_rating_queens_v3",
        "live_bucket_field": "earnings_shadow_bucket_queens_v3",
        "candidate_score_field": "earnings_shadow_score_queens_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_queens_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_queens_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_queens_v3_trap_candidate",
    },
    "brooklyn_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_brooklyn_v3",
        "live_rating_field": "earnings_shadow_rating_brooklyn_v3",
        "live_bucket_field": "earnings_shadow_bucket_brooklyn_v3",
        "candidate_score_field": "earnings_shadow_score_brooklyn_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_brooklyn_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_brooklyn_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_brooklyn_v3_trap_candidate",
    },
    "staten_island_v3_trap_candidate": {
        "live_score_field": "earnings_shadow_score_staten_island_v3",
        "live_rating_field": "earnings_shadow_rating_staten_island_v3",
        "live_bucket_field": "earnings_shadow_bucket_staten_island_v3",
        "candidate_score_field": "earnings_shadow_score_staten_island_v3_trap_candidate",
        "candidate_rating_field": "earnings_shadow_rating_staten_island_v3_trap_candidate",
        "candidate_bucket_field": "earnings_shadow_bucket_staten_island_v3_trap_candidate",
        "delta_field": "earnings_shadow_delta_staten_island_v3_trap_candidate",
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




def _citywide_visible_source_fields(features: List[Dict[str, Any]]) -> Tuple[str, str, str]:
    if not CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED:
        return (
            CITYWIDE_BASELINE_SCORE_FIELD,
            CITYWIDE_BASELINE_CONF_FIELD,
            "citywide_v3",
        )

    candidate_ready = any(
        isinstance((feature.get("properties") or {}), dict)
        and (feature.get("properties") or {}).get(CITYWIDE_TRAP_CANDIDATE_SCORE_FIELD) is not None
        for feature in features
    )
    if candidate_ready:
        return (
            CITYWIDE_TRAP_CANDIDATE_SCORE_FIELD,
            CITYWIDE_TRAP_CANDIDATE_CONF_FIELD,
            "citywide_v3_trap_candidate",
        )

    return (
        CITYWIDE_BASELINE_SCORE_FIELD,
        CITYWIDE_BASELINE_CONF_FIELD,
        "citywide_v3_fallback_from_missing_candidate",
    )

def _recalibrate_visible_v3_fields(features: List[Dict[str, Any]]) -> None:
    for feature in features:
        props = feature.get("properties") or {}
        if _is_airport_props(props):
            _apply_airport_exclusion_state(props)
        else:
            props["airport_excluded"] = False

    citywide_rank_field, citywide_conf_field, citywide_visible_source_name = _citywide_visible_source_fields(features)
    for feature in features:
        props = feature.get("properties") or {}
        if not _is_airport_props(props):
            props["earnings_shadow_visible_rank_citywide_v3"] = None
            props["earnings_shadow_visible_base_score_citywide_v3"] = None
            props["earnings_shadow_visible_score_citywide_v3"] = None
            props["citywide_visual_anchor_discount_factor_shadow"] = 1.0
            props["citywide_visible_source_shadow"] = citywide_visible_source_name
            props["citywide_trap_candidate_live_promotion_enabled"] = bool(CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED)
            props["citywide_trap_candidate_live_promotion_fallback_shadow"] = (
                citywide_visible_source_name == "citywide_v3_fallback_from_missing_candidate"
            )

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
        citywide_conf = _clamp01(float(props.get(citywide_conf_field) or 0.0))
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


def _safe_float(value: Any) -> float | None:
    parsed = _as_finite_number(value)
    return None if parsed is None else float(parsed)


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except Exception:
        return None


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _bucket_change_direction(live_bucket: Any, candidate_bucket: Any) -> str:
    bucket_order = {
        "red": 0,
        "orange": 1,
        "yellow": 2,
        "sky": 3,
        "blue": 4,
        "indigo": 5,
        "purple": 6,
        "green": 7,
    }
    live = str(live_bucket or "").strip().lower()
    candidate = str(candidate_bucket or "").strip().lower()
    if not live or not candidate:
        return "unknown"
    live_rank = bucket_order.get(live)
    candidate_rank = bucket_order.get(candidate)
    if live_rank is None or candidate_rank is None:
        return "unknown"
    if candidate_rank > live_rank:
        return "better"
    if candidate_rank < live_rank:
        return "worse"
    return "same"


def _build_profile_candidate_review(frame_time: str, profile_name: str, features: list[dict]) -> dict:
    profile_lookup = {
        "citywide_v3_trap_candidate": "citywide_v3",
        "manhattan_v3_trap_candidate": "manhattan_v3",
        "bronx_wash_heights_v3_trap_candidate": "bronx_wash_heights_v3",
        "queens_v3_trap_candidate": "queens_v3",
        "brooklyn_v3_trap_candidate": "brooklyn_v3",
        "staten_island_v3_trap_candidate": "staten_island_v3",
    }
    eligibility_profile = profile_lookup.get(profile_name)
    if eligibility_profile is None:
        return {"frame_time": frame_time, "profile_name": profile_name, "eligible_zone_count": 0}
    cfg = TRAP_CANDIDATE_REVIEW_PROFILE_CONFIG[profile_name]
    row_items: list[dict] = []
    deltas: list[float] = []
    promoted_count = 0
    demoted_count = 0
    near_unchanged_count = 0
    changed_count = 0

    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties")
        if not isinstance(props, dict) or _is_airport_props(props):
            continue
        geometry = feature.get("geometry")
        if not _eligible_for_profile(eligibility_profile, props, geometry):
            continue
        delta = _safe_float(props.get(cfg["delta_field"]))
        if delta is None:
            delta = 0.0
        if abs(delta) > 0.000001:
            changed_count += 1
        if delta > 0.03:
            promoted_count += 1
        elif delta < -0.03:
            demoted_count += 1
        else:
            near_unchanged_count += 1
        deltas.append(delta)
        row_items.append({
            "LocationID": _safe_int(props.get("LocationID")),
            "zone_name": _safe_text(props.get("zone_name")),
            "borough": _safe_text(props.get("borough")),
            "live_score": _safe_float(props.get(cfg["live_score_field"])),
            "live_rating": _safe_int(props.get(cfg["live_rating_field"])),
            "live_bucket": _safe_text(props.get(cfg["live_bucket_field"])),
            "candidate_score": _safe_float(props.get(cfg["candidate_score_field"])),
            "candidate_rating": _safe_int(props.get(cfg["candidate_rating_field"])),
            "candidate_bucket": _safe_text(props.get(cfg["candidate_bucket_field"])),
            "bucket_change_direction": _bucket_change_direction(
                props.get(cfg["live_bucket_field"]),
                props.get(cfg["candidate_bucket_field"]),
            ),
            "delta": float(delta),
            "return_risk_shadow": _safe_float(props.get("return_risk_shadow")),
            "escape_quality_shadow": _safe_float(props.get("escape_quality_shadow")),
            "airport_exit_share_shadow": _safe_float(props.get("airport_exit_share_shadow")),
            "out_of_scored_network_exit_share_shadow": _safe_float(props.get("out_of_scored_network_exit_share_shadow")),
            "short_external_exit_share_8mi_40min_shadow": _safe_float(props.get("short_external_exit_share_8mi_40min_shadow")),
            "good_long_external_exit_share_shadow": _safe_float(props.get("good_long_external_exit_share_shadow")),
        })

    sorted_deltas = sorted(deltas)
    top_demotions = sorted(row_items, key=lambda item: item["delta"])[:20]
    top_promotions = sorted(row_items, key=lambda item: item["delta"], reverse=True)[:20]
    return {
        "frame_time": frame_time,
        "profile_name": profile_name,
        "eligible_zone_count": len(row_items),
        "changed_count": changed_count,
        "promoted_count": promoted_count,
        "demoted_count": demoted_count,
        "near_unchanged_count": near_unchanged_count,
        "average_delta": (sum(deltas) / len(deltas)) if deltas else 0.0,
        "median_delta": statistics.median(sorted_deltas) if sorted_deltas else 0.0,
        "min_delta": sorted_deltas[0] if sorted_deltas else 0.0,
        "max_delta": sorted_deltas[-1] if sorted_deltas else 0.0,
        "top_demotions": top_demotions,
        "top_promotions": top_promotions,
    }


def _resolve_popup_metrics(
    *,
    raw_shadow_props: Dict[str, Any],
    visible_pickups: int,
    geometry_area_sq_miles: float | None,
) -> Dict[str, float]:
    pickups_now = _as_finite_number(raw_shadow_props.get("pickups_now_shadow"))
    if pickups_now is None or pickups_now < 0:
        pickups_now = float(max(0, int(visible_pickups)))

    pickups_next = _as_finite_number(raw_shadow_props.get("next_pickups_shadow"))
    if pickups_next is None or pickups_next < 0:
        pickups_next = pickups_now

    zone_area = _as_finite_number(raw_shadow_props.get("zone_area_sq_miles_shadow"))
    if zone_area is None or zone_area <= 0:
        zone_area = _as_finite_number(geometry_area_sq_miles)

    density_now = _as_finite_number(raw_shadow_props.get("pickups_per_sq_mile_now_shadow"))
    if (density_now is None or density_now < 0) and zone_area is not None and zone_area > 0:
        density_now = pickups_now / zone_area

    density_next = _as_finite_number(raw_shadow_props.get("pickups_per_sq_mile_next_shadow"))
    if (density_next is None or density_next < 0) and zone_area is not None and zone_area > 0:
        density_next = pickups_next / zone_area

    if zone_area is None or zone_area <= 0:
        if density_now is not None and density_now > 0:
            zone_area = max(pickups_now / density_now, 0.01)
        elif density_next is not None and density_next > 0:
            zone_area = max(pickups_next / density_next, 0.01)
        else:
            zone_area = 1.0

    if density_now is None or density_now < 0:
        density_now = max(pickups_now / max(zone_area, 0.01), 0.0)
    if density_next is None or density_next < 0:
        density_next = max(pickups_next / max(zone_area, 0.01), 0.0)

    resolved = {
        "pickups_now_shadow": float(pickups_now),
        "next_pickups_shadow": float(pickups_next),
        "zone_area_sq_miles_shadow": float(zone_area),
        "pickups_per_sq_mile_now_shadow": float(density_now),
        "pickups_per_sq_mile_next_shadow": float(density_next),
    }
    for metric_name, metric_value in resolved.items():
        if not math.isfinite(metric_value):
            raise ValueError(f"popup metric {metric_name} resolved to non-finite value")
    return resolved


def _popup_failure_diagnostics_payload(
    *,
    location_id: int,
    zone_name: str,
    borough: str,
    frame_time: str | None,
    shadow_props: Dict[str, Any],
    zone_geometry_by_id: Dict[int, Dict[str, Any]],
    geometry_area_sq_miles: float | None,
    visible_pickups: int,
    fallback_resolution_attempted: bool,
) -> Dict[str, Any]:
    return {
        "LocationID": location_id,
        "zone_name": zone_name,
        "borough": borough,
        "frame_time": frame_time,
        "shadow_sql_row_exists": bool(shadow_props),
        "geometry_area_row_exists": bool(
            location_id in zone_geometry_by_id
            and _as_finite_number((zone_geometry_by_id.get(location_id) or {}).get("zone_area_sq_miles")) is not None
            and float((zone_geometry_by_id.get(location_id) or {}).get("zone_area_sq_miles")) > 0
        ),
        "raw_popup_metric_inputs": {
            "pickups_now_shadow": shadow_props.get("pickups_now_shadow"),
            "next_pickups_shadow": shadow_props.get("next_pickups_shadow"),
            "zone_area_sq_miles_shadow": shadow_props.get("zone_area_sq_miles_shadow"),
            "pickups_per_sq_mile_now_shadow": shadow_props.get("pickups_per_sq_mile_now_shadow"),
            "pickups_per_sq_mile_next_shadow": shadow_props.get("pickups_per_sq_mile_next_shadow"),
            "geometry_area_sq_miles": geometry_area_sq_miles,
            "visible_pickups": int(visible_pickups),
        },
        "fallback_resolution_attempted": bool(fallback_resolution_attempted),
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
        "citywide_v3_trap_candidate",
        "manhattan_v2",
        "manhattan_v3",
        "manhattan_v3_trap_candidate",
        "bronx_wash_heights_v2",
        "bronx_wash_heights_v3",
        "bronx_wash_heights_v3_trap_candidate",
        "queens_v2",
        "queens_v3",
        "queens_v3_trap_candidate",
        "brooklyn_v2",
        "brooklyn_v3",
        "brooklyn_v3_trap_candidate",
        "staten_island_v2",
        "staten_island_v3",
        "staten_island_v3_trap_candidate",
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
    exact_history_dir: Path | None = None,
    exact_history_stage_dir: Path | None = None,
    exact_history_backup_dir: Path | None = None,
    timeline_output_path: Path | None = None,
    cleanup_out_dir_frames: bool = True,
) -> Dict[str, Any]:
    """
    Writes:
      /data/frames/timeline.json
      /data/exact_history/exact_shadow.duckdb

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
    duckdb_tmp_dir = temp_run_dir / "duckdb_tmp"
    duckdb_tmp_dir.mkdir(parents=True, exist_ok=True)
    resolved_exact_history_dir = Path(exact_history_dir) if exact_history_dir is not None else (out_dir.parent / "exact_history")
    resolved_exact_history_stage_dir = (
        Path(exact_history_stage_dir)
        if exact_history_stage_dir is not None
        else (out_dir.parent / "exact_history.__building__")
    )
    resolved_exact_history_backup_dir = (
        Path(exact_history_backup_dir)
        if exact_history_backup_dir is not None
        else (out_dir.parent / "exact_history.__backup__")
    )
    stage_dir = resolved_exact_history_stage_dir
    if resolved_exact_history_stage_dir.exists():
        shutil.rmtree(resolved_exact_history_stage_dir, ignore_errors=True)
    resolved_exact_history_stage_dir.mkdir(parents=True, exist_ok=True)
    exact_history_db_path = resolved_exact_history_dir / "exact_shadow.duckdb"
    staged_exact_history_db_path = resolved_exact_history_stage_dir / "exact_shadow.duckdb"
    resolved_timeline_output_path = Path(timeline_output_path) if timeline_output_path is not None else (out_dir / "timeline.json")

    con = None
    build_result: Dict[str, Any] | None = None
    try:
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

        parquet_list = [str(p) for p in parquet_files]
        sorted_parquet_files = sorted(parquet_list)

        zone_geometry_rows = build_zone_geometry_metrics_rows(zones_geojson_path)
        zone_geometry_by_id: Dict[int, Dict[str, float | None]] = {
            int(row["PULocationID"]): {
                "zone_area_sq_miles": None if row["zone_area_sq_miles"] is None else float(row["zone_area_sq_miles"]),
                "centroid_latitude": None if row.get("centroid_latitude") is None else float(row["centroid_latitude"]),
            }
            for row in zone_geometry_rows
        }
        zone_metadata_rows = [
            (
                int(zid),
                str(name_by_id.get(zid, "") or ""),
                str(borough_by_id.get(zid, "") or ""),
                bool(is_airport_zone(zid, name_by_id.get(zid, ""), borough_by_id.get(zid, ""))),
            )
            for zid in sorted(name_by_id.keys())
        ]

        def _open_incremental_connection() -> duckdb.DuckDBPyConnection:
            connection = duckdb.connect(database=str(staged_exact_history_db_path))
            connection.execute("PRAGMA enable_progress_bar=false")
            connection.execute("PRAGMA threads=1")
            connection.execute(f"PRAGMA temp_directory='{duckdb_tmp_dir.as_posix()}'")
            memory_limit = str(os.environ.get("DUCKDB_MEMORY_LIMIT", "512MB")).strip() or "512MB"
            connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
            connection.execute(
                "CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)"
            )
            if zone_geometry_rows:
                connection.executemany(
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
            connection.execute(
                "CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)"
            )
            connection.executemany(
                "INSERT INTO zone_metadata (PULocationID, zone_name, borough_name, airport_excluded) VALUES (?, ?, ?, ?)",
                zone_metadata_rows,
            )
            return connection

        rows_total_in_shadow = 0
        for parquet_index, parquet_path in enumerate(sorted_parquet_files):
            logger.info("exact_history_append_start filename=%s", Path(parquet_path).name)
            if con is not None:
                con.close()
            con = _open_incremental_connection()
            parquet_single_sql = "'" + parquet_path.replace("'", "''") + "'"
            schema_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{parquet_single_sql}])").fetchall()
            available_columns = {str(row[0]) for row in schema_rows}
            shadow_sql = build_zone_earnings_shadow_sql(
                [parquet_path],
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
            if parquet_index == 0:
                con.execute(
                    f"""
                    CREATE OR REPLACE TABLE exact_shadow_rows AS
                    {shadow_sql}
                    """
                )
            else:
                con.execute(
                    f"""
                    INSERT INTO exact_shadow_rows
                    {shadow_sql}
                    """
                )
            rows_total_in_shadow = int(
                con.execute("SELECT COUNT(*) FROM exact_shadow_rows").fetchone()[0] or 0
            )
            con.execute("CHECKPOINT")
            logger.info(
                "exact_history_append_done filename=%s rows_total=%d",
                Path(parquet_path).name,
                rows_total_in_shadow,
            )

        if con is None:
            raise RuntimeError("No parquet files available for exact-history append.")

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_exact_shadow_rows_ts
            ON exact_shadow_rows(exact_bin_local_ts)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_exact_shadow_rows_ts_zone
            ON exact_shadow_rows(exact_bin_local_ts, PULocationID)
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE exact_frame_features (
                exact_bin_local_ts VARCHAR,
                PULocationID INTEGER,
                feature_properties_json VARCHAR
            )
            """
        )
        shadow_cursor = con.execute(
            """
            SELECT *
            FROM exact_shadow_rows
            ORDER BY exact_bin_local_ts, PULocationID
            """
        )
        shadow_columns = [str(desc[0]) for desc in (shadow_cursor.description or [])]

        def _build_shadow_props_from_row(row_map: Dict[str, Any]) -> Dict[str, Any]:
            shadow_props = dict(row_map)
            shadow_props["exact_bin_local_ts"] = None if row_map.get("exact_bin_local_ts") is None else str(row_map.get("exact_bin_local_ts"))
            shadow_props["exact_bin_date_local"] = None if row_map.get("exact_bin_date_local") is None else str(row_map.get("exact_bin_date_local"))
            shadow_props["exact_bin_unix_utc"] = None if row_map.get("exact_bin_unix_utc") is None else int(row_map.get("exact_bin_unix_utc"))
            shadow_props["pickups_now_shadow"] = None if row_map.get("pickups_now") is None else int(row_map.get("pickups_now"))
            shadow_props["next_pickups_shadow"] = None if row_map.get("pickups_next") is None else int(row_map.get("pickups_next"))
            shadow_props["median_driver_pay_shadow"] = None if row_map.get("median_driver_pay") is None else float(row_map.get("median_driver_pay"))
            shadow_props["median_pay_per_min_shadow"] = None if row_map.get("median_pay_per_min") is None else float(row_map.get("median_pay_per_min"))
            shadow_props["median_pay_per_mile_shadow"] = None if row_map.get("median_pay_per_mile") is None else float(row_map.get("median_pay_per_mile"))
            shadow_props["median_request_to_pickup_min_shadow"] = None if row_map.get("median_request_to_pickup_min") is None else float(row_map.get("median_request_to_pickup_min"))
            shadow_props["short_trip_share_shadow"] = None if row_map.get("short_trip_share_3mi_12min") is None else float(row_map.get("short_trip_share_3mi_12min"))
            shadow_props["shared_ride_share_shadow"] = None if row_map.get("shared_ride_share") is None else float(row_map.get("shared_ride_share"))
            shadow_props["zone_area_sq_miles_shadow"] = None if row_map.get("zone_area_sq_miles") is None else float(row_map.get("zone_area_sq_miles"))
            shadow_props["pickups_per_sq_mile_now_shadow"] = None if row_map.get("pickups_per_sq_mile_now") is None else float(row_map.get("pickups_per_sq_mile_now"))
            shadow_props["pickups_per_sq_mile_next_shadow"] = None if row_map.get("pickups_per_sq_mile_next") is None else float(row_map.get("pickups_per_sq_mile_next"))
            shadow_props["long_trip_share_20plus_shadow"] = None if row_map.get("long_trip_share_20plus") is None else float(row_map.get("long_trip_share_20plus"))
            shadow_props["balanced_trip_share_shadow"] = None if row_map.get("balanced_trip_share") is None else float(row_map.get("balanced_trip_share"))
            shadow_props["same_zone_dropoff_share_shadow"] = None if row_map.get("same_zone_dropoff_share") is None else float(row_map.get("same_zone_dropoff_share"))
            shadow_props["downstream_value_shadow"] = None if row_map.get("downstream_next_value_raw") is None else float(row_map.get("downstream_next_value_raw"))
            shadow_props["demand_now_n_shadow"] = None if row_map.get("demand_now_n") is None else float(row_map.get("demand_now_n"))
            shadow_props["demand_next_n_shadow"] = None if row_map.get("demand_next_n") is None else float(row_map.get("demand_next_n"))
            shadow_props["pay_n_shadow"] = None if row_map.get("pay_n") is None else float(row_map.get("pay_n"))
            shadow_props["pay_per_min_n_shadow"] = None if row_map.get("pay_per_min_n") is None else float(row_map.get("pay_per_min_n"))
            shadow_props["pay_per_mile_n_shadow"] = None if row_map.get("pay_per_mile_n") is None else float(row_map.get("pay_per_mile_n"))
            shadow_props["pickup_friction_penalty_n_shadow"] = None if row_map.get("pickup_friction_penalty_n") is None else float(row_map.get("pickup_friction_penalty_n"))
            shadow_props["short_trip_penalty_n_shadow"] = None if row_map.get("short_trip_penalty_n") is None else float(row_map.get("short_trip_penalty_n"))
            shadow_props["shared_ride_penalty_n_shadow"] = None if row_map.get("shared_ride_penalty_n") is None else float(row_map.get("shared_ride_penalty_n"))
            shadow_props["downstream_value_n_shadow"] = None if row_map.get("downstream_value_n") is None else float(row_map.get("downstream_value_n"))
            return shadow_props

        timeline_entries: List[Dict[str, Any]] = []
        frame_count = 0
        candidate_review_by_profile: Dict[str, Dict[str, Any]] = {
            profile_name: {
                "frame_count": 0,
                "eligible_zone_observations": 0,
                "promoted_observations": 0,
                "demoted_observations": 0,
                "sum_delta": 0.0,
                "delta_observation_count": 0,
                "min_delta_seen": None,
                "max_delta_seen": None,
                "recurring_demotions_by_zone": {},
                "recurring_promotions_by_zone": {},
            }
            for profile_name in TRAP_CANDIDATE_REVIEW_PROFILE_CONFIG
        }

        current_key: str | None = None
        current_features: List[Dict[str, Any]] = []
        current_time_iso: str | None = None
        current_date_local: str | None = None
        current_weekday_name_local: str | None = None
        current_time_label_local: str | None = None
        current_popup_metric_diagnostics_by_location_id: Dict[int, Dict[str, Any]] = {}

        def flush_frame():
            nonlocal frame_count, current_features, current_time_iso, current_date_local, current_weekday_name_local, current_time_label_local, current_popup_metric_diagnostics_by_location_id
            if current_time_iso is None:
                return

            _recalibrate_visible_v3_fields(current_features)
            per_frame_candidate_review: Dict[str, Any] = {}
            for profile_name in TRAP_CANDIDATE_REVIEW_PROFILE_CONFIG:
                profile_review = _build_profile_candidate_review(current_time_iso, profile_name, current_features)
                per_frame_candidate_review[profile_name] = profile_review
                accumulator = candidate_review_by_profile[profile_name]
                accumulator["frame_count"] += 1
                eligible_zone_count = int(profile_review.get("eligible_zone_count") or 0)
                accumulator["eligible_zone_observations"] += eligible_zone_count
                accumulator["promoted_observations"] += int(profile_review.get("promoted_count") or 0)
                accumulator["demoted_observations"] += int(profile_review.get("demoted_count") or 0)
                accumulator["sum_delta"] += float(profile_review.get("average_delta") or 0.0) * eligible_zone_count
                accumulator["delta_observation_count"] += eligible_zone_count
                frame_min = _safe_float(profile_review.get("min_delta"))
                frame_max = _safe_float(profile_review.get("max_delta"))
                min_seen = accumulator["min_delta_seen"]
                max_seen = accumulator["max_delta_seen"]
                if frame_min is not None:
                    accumulator["min_delta_seen"] = frame_min if min_seen is None else min(min_seen, frame_min)
                if frame_max is not None:
                    accumulator["max_delta_seen"] = frame_max if max_seen is None else max(max_seen, frame_max)

                for item in profile_review.get("top_demotions", []):
                    delta = _safe_float(item.get("delta"))
                    if delta is None or delta >= -0.03:
                        continue
                    location_id = _safe_int(item.get("LocationID"))
                    if location_id is None:
                        continue
                    recurring = accumulator["recurring_demotions_by_zone"].setdefault(
                        location_id,
                        {
                            "LocationID": location_id,
                            "zone_name": _safe_text(item.get("zone_name")),
                            "borough": _safe_text(item.get("borough")),
                            "appearances": 0,
                            "cumulative_delta": 0.0,
                            "worst_delta_seen": delta,
                            "best_delta_seen": delta,
                        },
                    )
                    recurring["appearances"] += 1
                    recurring["cumulative_delta"] += delta
                    recurring["worst_delta_seen"] = min(float(recurring["worst_delta_seen"]), delta)
                    recurring["best_delta_seen"] = max(float(recurring["best_delta_seen"]), delta)

                for item in profile_review.get("top_promotions", []):
                    delta = _safe_float(item.get("delta"))
                    if delta is None or delta <= 0.03:
                        continue
                    location_id = _safe_int(item.get("LocationID"))
                    if location_id is None:
                        continue
                    recurring = accumulator["recurring_promotions_by_zone"].setdefault(
                        location_id,
                        {
                            "LocationID": location_id,
                            "zone_name": _safe_text(item.get("zone_name")),
                            "borough": _safe_text(item.get("borough")),
                            "appearances": 0,
                            "cumulative_delta": 0.0,
                            "worst_delta_seen": delta,
                            "best_delta_seen": delta,
                        },
                    )
                    recurring["appearances"] += 1
                    recurring["cumulative_delta"] += delta
                    recurring["worst_delta_seen"] = min(float(recurring["worst_delta_seen"]), delta)
                    recurring["best_delta_seen"] = max(float(recurring["best_delta_seen"]), delta)

            _validate_popup_metric_consistency(
                current_features,
                current_time_iso,
                diagnostics_by_location_id=current_popup_metric_diagnostics_by_location_id,
            )
            _validate_rating_bucket_color_consistency(current_features, current_time_iso)
            timeline_entries.append(
                {
                    "frame_time": current_time_iso,
                    "frame_date": current_date_local,
                    "frame_weekday_name": current_weekday_name_local,
                    "frame_time_label": current_time_label_local,
                    "bin_minutes": int(bin_minutes),
                }
            )
            feature_rows = []
            for feature in current_features:
                props = feature.get("properties") if isinstance(feature, dict) else None
                if not isinstance(props, dict):
                    continue
                location_id = props.get("LocationID")
                try:
                    location_id_int = int(location_id)
                except Exception:
                    continue
                feature_rows.append(
                    (
                        str(current_time_iso),
                        location_id_int,
                        json.dumps(props, separators=(",", ":")),
                    )
                )
            if feature_rows:
                con.executemany(
                    """
                    INSERT INTO exact_frame_features (
                        exact_bin_local_ts,
                        PULocationID,
                        feature_properties_json
                    )
                    VALUES (?, ?, ?)
                    """,
                    feature_rows,
                )

            frame_count += 1
            if frame_count % 250 == 0:
                logger.info(
                    "stream_exact_history_progress rows_processed=%d frames_built=%d current_exact_bin_local_ts=%s",
                    total_rows,
                    frame_count,
                    current_time_iso,
                )
            current_features = []
            current_time_iso = None
            current_date_local = None
            current_weekday_name_local = None
            current_time_label_local = None
            current_popup_metric_diagnostics_by_location_id = {}

        total_rows = 0
        any_rows = False

        while True:
            batch = shadow_cursor.fetchmany(250)
            if not batch:
                break
            any_rows = True

            for row in batch:
                row_map = {shadow_columns[idx]: row[idx] for idx in range(len(shadow_columns))}
                zid = row_map.get("PULocationID")
                exact_bin_local_ts = row_map.get("exact_bin_local_ts")
                exact_bin_date_local = row_map.get("exact_bin_date_local")
                exact_weekday_name_local = row_map.get("exact_weekday_name_local")
                exact_bin_time_label_local = row_map.get("exact_bin_time_label_local")
                pickups = row_map.get("pickups_now")
                avg_pay = row_map.get("median_driver_pay")
                rating = row_map.get("earnings_shadow_rating_citywide_v3")
                total_rows += 1
                key = str(exact_bin_local_ts)

                if current_key is None:
                    current_key = key
                if key != current_key:
                    flush_frame()
                    current_key = key

                current_time_iso = str(exact_bin_local_ts)
                current_date_local = None if exact_bin_date_local is None else str(exact_bin_date_local)
                current_weekday_name_local = None if exact_weekday_name_local is None else str(exact_weekday_name_local)
                current_time_label_local = None if exact_bin_time_label_local is None else str(exact_bin_time_label_local)

                zid_i = int(zid)
                geom = geom_by_id.get(zid_i)
                if not geom:
                    continue

                r = int(rating)
                bucket, fill = bucket_and_color_from_rating(r)
                shadow_props = _build_shadow_props_from_row(row_map)
                geometry_area_sq_miles = None
                if zid_i in zone_geometry_by_id:
                    geometry_area_sq_miles = zone_geometry_by_id[zid_i].get("zone_area_sq_miles")
                fallback_resolution_attempted = False
                try:
                    fallback_resolution_attempted = True
                    popup_metrics = _resolve_popup_metrics(
                        raw_shadow_props=shadow_props,
                        visible_pickups=int(pickups),
                        geometry_area_sq_miles=geometry_area_sq_miles,
                    )
                except Exception as exc:
                    if not bool(is_airport_zone(zid_i, name_by_id.get(zid_i, ""), borough_by_id.get(zid_i, ""))):
                        popup_diagnostics = _popup_failure_diagnostics_payload(
                            location_id=zid_i,
                            zone_name=name_by_id.get(zid_i, ""),
                            borough=borough_by_id.get(zid_i, ""),
                            frame_time=current_time_iso,
                            shadow_props=shadow_props,
                            zone_geometry_by_id=zone_geometry_by_id,
                            geometry_area_sq_miles=geometry_area_sq_miles,
                            visible_pickups=int(pickups),
                            fallback_resolution_attempted=fallback_resolution_attempted,
                        )
                        logger.error(
                            "non_airport_popup_metric_resolution_failed %s",
                            json.dumps(popup_diagnostics, sort_keys=True),
                        )
                        raise RuntimeError(
                            "Popup metric resolution failed for non-airport zone "
                            f"LocationID={zid_i} zone_name={name_by_id.get(zid_i, '')!r} frame_time={current_time_iso}"
                        ) from exc
                    raise
                if not bool(is_airport_zone(zid_i, name_by_id.get(zid_i, ""), borough_by_id.get(zid_i, ""))):
                    current_popup_metric_diagnostics_by_location_id[zid_i] = {
                        "fallback_resolution_attempted": fallback_resolution_attempted,
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
                    popup_diagnostics = _popup_failure_diagnostics_payload(
                        location_id=zid_i,
                        zone_name=name_by_id.get(zid_i, ""),
                        borough=borough_by_id.get(zid_i, ""),
                        frame_time=current_time_iso,
                        shadow_props=shadow_props,
                        zone_geometry_by_id=zone_geometry_by_id,
                        geometry_area_sq_miles=geometry_area_sq_miles,
                        visible_pickups=int(pickups),
                        fallback_resolution_attempted=fallback_resolution_attempted,
                    )
                    logger.error("non_airport_popup_metric_resolution_failed %s", json.dumps(popup_diagnostics, sort_keys=True))
                    raise RuntimeError(
                        "Popup metric resolution failed for non-airport zone "
                        f"LocationID={zid_i} zone_name={name_by_id.get(zid_i, '')!r} frame_time={current_time_iso}"
                    )
                if total_rows % 100000 == 0:
                    logger.info(
                        "stream_exact_history_progress rows_processed=%d frames_built=%d current_exact_bin_local_ts=%s",
                        total_rows,
                        frame_count,
                        current_time_iso,
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
                        "airport_exit_share_shadow": shadow_props.get("airport_exit_share_shadow"),
                        "out_of_scored_network_exit_share_shadow": shadow_props.get("out_of_scored_network_exit_share_shadow"),
                        "short_external_exit_share_6mi_30min_shadow": shadow_props.get("short_external_exit_share_6mi_30min_shadow"),
                        "short_external_exit_share_8mi_40min_shadow": shadow_props.get("short_external_exit_share_8mi_40min_shadow"),
                        "good_long_external_exit_share_shadow": shadow_props.get("good_long_external_exit_share_shadow"),
                        "airport_exit_share_n_shadow": shadow_props.get("airport_exit_share_n_shadow"),
                        "out_of_scored_network_exit_share_n_shadow": shadow_props.get("out_of_scored_network_exit_share_n_shadow"),
                        "short_external_exit_share_6mi_30min_n_shadow": shadow_props.get("short_external_exit_share_6mi_30min_n_shadow"),
                        "short_external_exit_share_8mi_40min_n_shadow": shadow_props.get("short_external_exit_share_8mi_40min_n_shadow"),
                        "good_long_external_exit_share_n_shadow": shadow_props.get("good_long_external_exit_share_n_shadow"),
                        "return_risk_shadow": shadow_props.get("return_risk_shadow"),
                        "escape_quality_shadow": shadow_props.get("escape_quality_shadow"),
                        "safe_return_risk": shadow_props.get("safe_return_risk"),
                        "safe_escape_quality": shadow_props.get("safe_escape_quality"),
                        "safe_airport_exit": shadow_props.get("safe_airport_exit"),
                        "safe_external_exit": shadow_props.get("safe_external_exit"),
                        "safe_short_external": shadow_props.get("safe_short_external"),
                        "safe_good_long_external": shadow_props.get("safe_good_long_external"),
                        "safe_downstream": shadow_props.get("safe_downstream"),
                        "citywide_trap_adjustment_factor": shadow_props.get("citywide_trap_adjustment_factor"),
                        "queens_trap_adjustment_factor": shadow_props.get("queens_trap_adjustment_factor"),
                        "bronx_wash_heights_trap_adjustment_factor": shadow_props.get("bronx_wash_heights_trap_adjustment_factor"),
                        "brooklyn_trap_adjustment_factor": shadow_props.get("brooklyn_trap_adjustment_factor"),
                        "staten_island_trap_adjustment_factor": shadow_props.get("staten_island_trap_adjustment_factor"),
                        "manhattan_trap_adjustment_factor": shadow_props.get("manhattan_trap_adjustment_factor"),
                        "earnings_shadow_score_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_score_citywide_v3_trap_candidate"),
                        "earnings_shadow_score_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_score_manhattan_v3_trap_candidate"),
                        "earnings_shadow_score_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_score_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_score_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_score_queens_v3_trap_candidate"),
                        "earnings_shadow_score_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_score_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_score_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_score_staten_island_v3_trap_candidate"),
                        "earnings_shadow_confidence_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_citywide_v3_trap_candidate"),
                        "earnings_shadow_confidence_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_manhattan_v3_trap_candidate"),
                        "earnings_shadow_confidence_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_confidence_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_queens_v3_trap_candidate"),
                        "earnings_shadow_confidence_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_confidence_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_confidence_staten_island_v3_trap_candidate"),
                        "earnings_shadow_rating_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_citywide_v3_trap_candidate"),
                        "earnings_shadow_bucket_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_citywide_v3_trap_candidate"),
                        "earnings_shadow_color_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_color_citywide_v3_trap_candidate"),
                        "earnings_shadow_rating_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_manhattan_v3_trap_candidate"),
                        "earnings_shadow_bucket_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_manhattan_v3_trap_candidate"),
                        "earnings_shadow_color_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_color_manhattan_v3_trap_candidate"),
                        "earnings_shadow_rating_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_bucket_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_color_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_color_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_rating_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_queens_v3_trap_candidate"),
                        "earnings_shadow_bucket_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_queens_v3_trap_candidate"),
                        "earnings_shadow_color_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_color_queens_v3_trap_candidate"),
                        "earnings_shadow_rating_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_bucket_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_color_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_color_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_rating_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_rating_staten_island_v3_trap_candidate"),
                        "earnings_shadow_bucket_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_bucket_staten_island_v3_trap_candidate"),
                        "earnings_shadow_color_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_color_staten_island_v3_trap_candidate"),
                        "earnings_shadow_delta_citywide_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_citywide_v3_trap_candidate"),
                        "earnings_shadow_delta_manhattan_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_manhattan_v3_trap_candidate"),
                        "earnings_shadow_delta_bronx_wash_heights_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_bronx_wash_heights_v3_trap_candidate"),
                        "earnings_shadow_delta_queens_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_queens_v3_trap_candidate"),
                        "earnings_shadow_delta_brooklyn_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_brooklyn_v3_trap_candidate"),
                        "earnings_shadow_delta_staten_island_v3_trap_candidate": shadow_props.get("earnings_shadow_delta_staten_island_v3_trap_candidate"),
                    }
                })

        if not any_rows:
            raise RuntimeError("No data after filtering. Lower min_trips_per_window.")

        flush_frame()

        timeline_rows = con.execute(
            """
            SELECT DISTINCT
                exact_bin_local_ts,
                exact_bin_date_local,
                exact_weekday_name_local,
                exact_bin_time_label_local
            FROM exact_shadow_rows
            ORDER BY exact_bin_local_ts
            """
        ).fetchall()
        timeline_entries = [
            {
                "frame_time": str(row[0]),
                "frame_date": None if row[1] is None else str(row[1]),
                "frame_weekday_name": None if row[2] is None else str(row[2]),
                "frame_time_label": None if row[3] is None else str(row[3]),
                "bin_minutes": int(bin_minutes),
            }
            for row in timeline_rows
        ]
        timeline = [str(entry.get("frame_time")) for entry in timeline_entries]
        timeline_payload = {
            "timeline": timeline,
            "entries": timeline_entries,
            "count": len(timeline),
            "bin_minutes": int(bin_minutes),
            "timeline_mode": "exact_historical",
            "frame_time_model": "exact_local_20min",
            "synthetic_week_enabled": False,
        }
        (stage_dir / "timeline.json").write_text(
            json.dumps(timeline_payload, separators=(",", ":")),
            encoding="utf-8"
        )
        logger.info("exact_history_build_timeline_done count=%d", len(timeline_entries))
        # Keep timeline.json on volume for compatibility while mirroring metadata copy in DB.
        save_generated_artifact("timeline", timeline_payload, compress=False)

        trap_candidate_review_payload = {
            "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "engine_release": "team-joseo-score-v2-final-live",
            "bin_minutes": int(bin_minutes),
            "profile_reviews": {},
        }
        for profile_name, accumulator in candidate_review_by_profile.items():
            demotions = list(accumulator["recurring_demotions_by_zone"].values())
            promotions = list(accumulator["recurring_promotions_by_zone"].values())
            for collection in (demotions, promotions):
                for item in collection:
                    appearances = max(1, int(item.get("appearances") or 0))
                    item["average_delta"] = float(item.get("cumulative_delta") or 0.0) / appearances
            recurring_top_demotions = sorted(
                demotions,
                key=lambda item: (
                    float(item.get("average_delta") or 0.0),
                    float(item.get("cumulative_delta") or 0.0),
                ),
            )[:30]
            recurring_top_promotions = sorted(
                promotions,
                key=lambda item: (
                    float(item.get("average_delta") or 0.0),
                    float(item.get("cumulative_delta") or 0.0),
                ),
                reverse=True,
            )[:30]
            delta_observation_count = int(accumulator["delta_observation_count"] or 0)
            trap_candidate_review_payload["profile_reviews"][profile_name] = {
                "frame_count": int(accumulator["frame_count"] or 0),
                "eligible_zone_observations": int(accumulator["eligible_zone_observations"] or 0),
                "promoted_observations": int(accumulator["promoted_observations"] or 0),
                "demoted_observations": int(accumulator["demoted_observations"] or 0),
                "average_delta_overall": (
                    float(accumulator["sum_delta"]) / delta_observation_count
                    if delta_observation_count > 0 else 0.0
                ),
                "min_delta_seen": accumulator["min_delta_seen"],
                "max_delta_seen": accumulator["max_delta_seen"],
                "recurring_top_demotions": recurring_top_demotions,
                "recurring_top_promotions": recurring_top_promotions,
            }
        save_generated_artifact("trap_candidate_review", trap_candidate_review_payload, compress=False)

        legacy_assistant_outlook_path = out_dir / "assistant_outlook.json"
        stage_assistant_outlook_path = stage_dir / "assistant_outlook.json"
        try:
            legacy_assistant_outlook_path.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            stage_assistant_outlook_path.unlink(missing_ok=True)
        except Exception:
            pass
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
                    "citywide_trap_candidate_live_promotion_enabled": bool(CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED),
                    "citywide_visible_source_live": (
                        "citywide_v3_trap_candidate"
                        if CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED
                        else "citywide_v3"
                    ),
                    "citywide_visible_score_field_live": (
                        CITYWIDE_TRAP_CANDIDATE_SCORE_FIELD
                        if CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED
                        else CITYWIDE_BASELINE_SCORE_FIELD
                    ),
                    "candidate_shadow_profiles": [
                        "citywide_v3_trap_candidate",
                        "manhattan_v3_trap_candidate",
                        "bronx_wash_heights_v3_trap_candidate",
                        "queens_v3_trap_candidate",
                        "brooklyn_v3_trap_candidate",
                        "staten_island_v3_trap_candidate",
                    ],
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
                        "Phase 1 adds shadow-only trap-exit diagnostics built from existing historical trip data. Visible score output is unchanged.",
                        "Phase 2 adds trap-adjusted candidate score families in shadow form only. Live visible score output is unchanged.",
                        "Trap-adjusted candidates combine existing live v3 scores with Phase 1 return-risk and escape-quality diagnostics for review.",
                        "Phase 2 adds a citywide_v3 shadow candidate that blends raw demand, demand density, long-trip share, pay quality, downstream value, and trap penalties.",
                        "Phase 3 promotes citywide_v3 to the live visible citywide score while borough profiles remain on v2.",
                        "Phase 4 adds borough_v3 shadow candidates that blend density, long-trip quality, pay quality, downstream value, and local trap penalties without changing visible borough scores yet.",
                        "Phase 5 promotes manhattan_v3 to the live visible Manhattan score while other borough visible profiles remain unchanged.",
                        "Phase 6 promotes bronx_wash_heights_v3 to the live visible Bronx/Wash Heights score while Queens, Brooklyn, and Staten Island remain on v2 visible profiles.",
                        "Phase 7 promotes queens_v3 to the live visible Queens score while Brooklyn and Staten Island remain on v2 visible profiles.",
                        "Phase 8 promotes brooklyn_v3 to the live visible Brooklyn score while Staten Island remains on the v2 visible profile.",
                        "Phase 9 promotes staten_island_v3 to the live visible Staten Island score and completes the visible v3 rollout across citywide and all borough modes.",
                        "Phase 3 adds a trap-candidate review artifact summarizing candidate vs live deltas by profile.",
                        "No live promotion occurs in Phase 3; this phase is proof-only.",
                        "Citywide trap-candidate live promotion is gated by CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED and does not affect borough visible profiles.",
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
                        "airport_exit_share_shadow",
                        "out_of_scored_network_exit_share_shadow",
                        "short_external_exit_share_6mi_30min_shadow",
                        "short_external_exit_share_8mi_40min_shadow",
                        "good_long_external_exit_share_shadow",
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
                        "airport_exit_share_n_shadow",
                        "out_of_scored_network_exit_share_n_shadow",
                        "short_external_exit_share_6mi_30min_n_shadow",
                        "short_external_exit_share_8mi_40min_n_shadow",
                        "good_long_external_exit_share_n_shadow",
                        "return_risk_shadow",
                        "escape_quality_shadow",
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
                        "safe_return_risk",
                        "safe_escape_quality",
                        "safe_airport_exit",
                        "safe_external_exit",
                        "safe_short_external",
                        "safe_good_long_external",
                        "safe_downstream",
                        "citywide_trap_adjustment_factor",
                        "queens_trap_adjustment_factor",
                        "bronx_wash_heights_trap_adjustment_factor",
                        "brooklyn_trap_adjustment_factor",
                        "staten_island_trap_adjustment_factor",
                        "manhattan_trap_adjustment_factor",
                        "earnings_shadow_score_citywide_v3_trap_candidate",
                        "earnings_shadow_score_manhattan_v3_trap_candidate",
                        "earnings_shadow_score_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_score_queens_v3_trap_candidate",
                        "earnings_shadow_score_brooklyn_v3_trap_candidate",
                        "earnings_shadow_score_staten_island_v3_trap_candidate",
                        "earnings_shadow_confidence_citywide_v3_trap_candidate",
                        "earnings_shadow_confidence_manhattan_v3_trap_candidate",
                        "earnings_shadow_confidence_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_confidence_queens_v3_trap_candidate",
                        "earnings_shadow_confidence_brooklyn_v3_trap_candidate",
                        "earnings_shadow_confidence_staten_island_v3_trap_candidate",
                        "earnings_shadow_rating_citywide_v3_trap_candidate",
                        "earnings_shadow_bucket_citywide_v3_trap_candidate",
                        "earnings_shadow_color_citywide_v3_trap_candidate",
                        "earnings_shadow_rating_manhattan_v3_trap_candidate",
                        "earnings_shadow_bucket_manhattan_v3_trap_candidate",
                        "earnings_shadow_color_manhattan_v3_trap_candidate",
                        "earnings_shadow_rating_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_bucket_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_color_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_rating_queens_v3_trap_candidate",
                        "earnings_shadow_bucket_queens_v3_trap_candidate",
                        "earnings_shadow_color_queens_v3_trap_candidate",
                        "earnings_shadow_rating_brooklyn_v3_trap_candidate",
                        "earnings_shadow_bucket_brooklyn_v3_trap_candidate",
                        "earnings_shadow_color_brooklyn_v3_trap_candidate",
                        "earnings_shadow_rating_staten_island_v3_trap_candidate",
                        "earnings_shadow_bucket_staten_island_v3_trap_candidate",
                        "earnings_shadow_color_staten_island_v3_trap_candidate",
                        "earnings_shadow_delta_citywide_v3_trap_candidate",
                        "earnings_shadow_delta_manhattan_v3_trap_candidate",
                        "earnings_shadow_delta_bronx_wash_heights_v3_trap_candidate",
                        "earnings_shadow_delta_queens_v3_trap_candidate",
                        "earnings_shadow_delta_brooklyn_v3_trap_candidate",
                        "earnings_shadow_delta_staten_island_v3_trap_candidate",
                    ],
                    "artifact_schema_version": expected_freshness.get("artifact_schema_version"),
                    "code_dependency_hash": expected_freshness.get("code_dependency_hash"),
                    "source_data_hash": expected_freshness.get("source_data_hash"),
                    "artifact_signature": expected_freshness.get("artifact_signature"),
                    "dependency_files": expected_freshness.get("code_dependencies", {}).get("dependency_files", []),
                    "parquet_inventory": expected_freshness.get("source_inventory", {}).get("parquet_files", []),
                    "zones_geojson_signature": expected_freshness.get("source_inventory", {}).get("zones_geojson"),
                }

        # scoring_shadow_manifest is DB-first; do not keep a file copy on volume.
        if CITYWIDE_TRAP_CANDIDATE_LIVE_PROMOTION_ENABLED:
            manifest_payload.setdefault("notes", []).append(
                "Limited citywide live promotion enabled: citywide visible score now uses citywide_v3_trap_candidate while borough visible profiles remain unchanged."
            )

        save_generated_artifact("scoring_shadow_manifest", manifest_payload, compress=False)
        if con is not None:
            con.close()
            con = None
        staged_timeline = stage_dir / "timeline.json"
        exact_store_ready = staged_exact_history_db_path.exists() and staged_exact_history_db_path.stat().st_size > 0
        if not staged_timeline.exists() or not exact_store_ready:
            raise RuntimeError("Staged artifact build did not produce required files.")
        legacy_manifest_path = out_dir / "scoring_shadow_manifest.json"
        try:
            legacy_manifest_path.unlink(missing_ok=True)
        except Exception:
            pass

        if cleanup_out_dir_frames:
            for generated in out_dir.glob("frame_*.json"):
                try:
                    generated.unlink()
                except Exception:
                    pass
        try:
            if resolved_timeline_output_path.exists() and resolved_timeline_output_path.is_file():
                resolved_timeline_output_path.unlink()
        except Exception:
            pass
        if resolved_exact_history_backup_dir.exists():
            shutil.rmtree(resolved_exact_history_backup_dir, ignore_errors=True)
        try:
            if resolved_exact_history_dir.exists():
                resolved_exact_history_dir.rename(resolved_exact_history_backup_dir)
            resolved_exact_history_stage_dir.rename(resolved_exact_history_dir)
            if resolved_exact_history_backup_dir.exists():
                shutil.rmtree(resolved_exact_history_backup_dir, ignore_errors=True)
        except Exception:
            if resolved_exact_history_dir.exists():
                shutil.rmtree(resolved_exact_history_dir, ignore_errors=True)
            if resolved_exact_history_backup_dir.exists():
                resolved_exact_history_backup_dir.rename(resolved_exact_history_dir)
            raise

        resolved_timeline_output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged_timeline), str(resolved_timeline_output_path))
        logger.info("exact_history_publish_done")

        build_result = {
            "ok": True,
            "count": len(timeline),
            "first_frame_datetime": timeline[0] if timeline else None,
            "last_frame_datetime": timeline[-1] if timeline else None,
            "frames_dir": str(out_dir),
            "exact_history_store": str(exact_history_db_path),
            "rows": total_rows,
            "assistant_outlook": {
                "mode": "on_demand_frame_bucket",
                "path": None,
            },
        }
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        # Ensure temp build dirs are always cleaned on success/failure.
        temp_run_dir_ctx.cleanup()

    if build_result is None:
        raise RuntimeError("Artifact build did not complete.")
    return build_result
