from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
import json

# Assistant outlook contract:
# - Read only generated timeline/frame artifacts as future truth.
# - Team Joseo scoring truth remains in zone_earnings_engine.py.
# - Frontend assistant maps these horizon points into trap/busy/slow/hold windows.
# - Backend packaging here does not own assistant thresholds.

HORIZON_BINS_DEFAULT = 6
TRACK_KEYS = (
    "citywide_v3_shadow",
    "citywide_shadow",
    "manhattan_v3_shadow",
    "manhattan_shadow",
    "bronx_wash_heights_v3_shadow",
    "bronx_wash_heights_shadow",
    "queens_v3_shadow",
    "queens_shadow",
    "brooklyn_v3_shadow",
    "brooklyn_shadow",
    "staten_island_v3_shadow",
    "staten_island_shadow",
)
TRACK_SOURCES = {
    "citywide_v3_shadow": ("citywide_v3",),
    "citywide_shadow": ("citywide_v2", "citywide"),
    "manhattan_v3_shadow": ("manhattan_v3",),
    "manhattan_shadow": ("manhattan_v2", "manhattan"),
    "bronx_wash_heights_v3_shadow": ("bronx_wash_heights_v3",),
    "bronx_wash_heights_shadow": ("bronx_wash_heights_v2", "bronx_wash_heights"),
    "queens_v3_shadow": ("queens_v3",),
    "queens_shadow": ("queens_v2", "queens"),
    "brooklyn_v3_shadow": ("brooklyn_v3",),
    "brooklyn_shadow": ("brooklyn_v2", "brooklyn"),
    "staten_island_v3_shadow": ("staten_island_v3",),
    "staten_island_shadow": ("staten_island_v2", "staten_island"),
}


def _to_location_id_str(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return str(int(raw))
    except Exception:
        return raw


def _first_present(props: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in props and props.get(key) is not None:
            return props.get(key)
    return None


def _extract_track_entry(props: Dict[str, Any], source_name: str) -> Dict[str, Any] | None:
    rating = props.get(f"earnings_shadow_rating_{source_name}")
    bucket = props.get(f"earnings_shadow_bucket_{source_name}")
    if rating is None and bucket is None:
        return None
    return {"rating": rating, "bucket": bucket}


def extract_assistant_feature_payload(feature: Dict[str, Any]) -> Dict[str, Any] | None:
    props = (feature or {}).get("properties") or {}
    location_id = _to_location_id_str(props.get("LocationID"))
    if not location_id:
        return None

    tracks: Dict[str, Any] = {}
    for track_key in TRACK_KEYS:
        for source in TRACK_SOURCES.get(track_key, ()):  # tolerate old/new naming
            entry = _extract_track_entry(props, source)
            if entry is not None:
                tracks[track_key] = entry
                break

    raw = {
        "busy_now_base_n_shadow": props.get("busy_now_base_n_shadow"),
        "busy_next_base_n_shadow": props.get("busy_next_base_n_shadow"),
        "short_trip_penalty_n_shadow": _first_present(
            props,
            (
                "short_trip_penalty_n_shadow",
                "short_trip_penalty_n",
                "short_trip_share_shadow",
                "short_trip_share",
            ),
        ),
        "long_trip_share_20plus": _first_present(
            props,
            (
                "long_trip_share_20plus",
                "long_trip_share_20plus_shadow",
            ),
        ),
        "balanced_trip_share_shadow": _first_present(
            props,
            (
                "balanced_trip_share_shadow",
                "balanced_trip_share",
            ),
        ),
        "churn_pressure_n_shadow": props.get("churn_pressure_n_shadow"),
        "market_saturation_penalty_n_shadow": props.get("market_saturation_penalty_n_shadow"),
        "manhattan_core_saturation_penalty_n_shadow": props.get("manhattan_core_saturation_penalty_n_shadow"),
        "downstream_next_value_raw": _first_present(
            props,
            (
                "downstream_next_value_raw",
                "downstream_value_shadow",
            ),
        ),
    }
    balanced_trip_share = _first_present(
        raw,
        (
            "balanced_trip_share_shadow",
            "balanced_trip_share",
        ),
    )

    return {
        "location_id": location_id,
        "zone_name": props.get("zone_name"),
        "borough": props.get("borough"),
        "tracks": tracks,
        "raw": raw,
        "busy_now_base": raw.get("busy_now_base_n_shadow"),
        "busy_next_base": raw.get("busy_next_base_n_shadow"),
        "short_trip_penalty": raw.get("short_trip_penalty_n_shadow"),
        "long_trip_share_20plus": raw.get("long_trip_share_20plus"),
        "balanced_trip_share": balanced_trip_share,
        "churn_pressure": raw.get("churn_pressure_n_shadow"),
        "market_saturation_penalty": raw.get("market_saturation_penalty_n_shadow"),
        "manhattan_core_saturation_penalty": raw.get("manhattan_core_saturation_penalty_n_shadow"),
        "continuation_raw": raw.get("downstream_next_value_raw"),
    }


def _load_frame_features(frames_dir: Path, frame_idx: int) -> List[Dict[str, Any]]:
    frame_path = Path(frames_dir) / f"frame_{int(frame_idx):06d}.json"
    if not frame_path.exists():
        return []
    doc = json.loads(frame_path.read_text(encoding="utf-8"))
    polygons = doc.get("polygons") or {}
    features = polygons.get("features") or []
    return features if isinstance(features, list) else []


def build_zone_outlook_for_frame(
    frame_idx: int,
    timeline: List[str],
    frames_dir: Path,
    *,
    horizon_bins: int = HORIZON_BINS_DEFAULT,
) -> Dict[str, Dict[str, Any]]:
    if not timeline:
        return {}

    total_frames = len(timeline)
    zone_lookup: Dict[str, Dict[str, Any]] = {}
    max_bins = max(1, int(horizon_bins))

    start_idx = max(0, int(frame_idx))
    end_exclusive = min(total_frames, start_idx + max_bins)

    for future_idx in range(start_idx, end_exclusive):
        future_time = str(timeline[future_idx])
        for feature in _load_frame_features(frames_dir, future_idx):
            payload = extract_assistant_feature_payload(feature)
            if payload is None:
                continue
            location_id = payload["location_id"]
            zone_payload = zone_lookup.get(location_id)
            if zone_payload is None:
                zone_payload = {
                    "location_id": location_id,
                    "zone_name": payload.get("zone_name"),
                    "borough": payload.get("borough"),
                    "points": [],
                }
                zone_lookup[location_id] = zone_payload

            zone_payload["points"].append(
                {
                    "frame_time": future_time,
                    "tracks": payload.get("tracks") or {},
                    "raw": payload.get("raw") or {},
                    "busy_now_base": payload.get("busy_now_base"),
                    "busy_next_base": payload.get("busy_next_base"),
                    "short_trip_penalty": payload.get("short_trip_penalty"),
                    "long_trip_share_20plus": payload.get("long_trip_share_20plus"),
                    "balanced_trip_share": payload.get("balanced_trip_share"),
                    "churn_pressure": payload.get("churn_pressure"),
                    "market_saturation_penalty": payload.get("market_saturation_penalty"),
                    "manhattan_core_saturation_penalty": payload.get("manhattan_core_saturation_penalty"),
                    "continuation_raw": payload.get("continuation_raw"),
                }
            )

    return zone_lookup


def build_assistant_outlook_index(
    timeline_payload: Dict[str, Any],
    frames_dir: Path,
    *,
    bin_minutes: int = 20,
    horizon_bins: int = HORIZON_BINS_DEFAULT,
) -> Dict[str, Any]:
    timeline = [str(item) for item in ((timeline_payload or {}).get("timeline") or []) if str(item).strip()]
    timeline_index: Dict[str, Dict[str, Any]] = {}

    for idx, frame_time in enumerate(timeline):
        timeline_index[str(frame_time)] = build_zone_outlook_for_frame(
            idx,
            timeline,
            Path(frames_dir),
            horizon_bins=horizon_bins,
        )

    return {
        "version": 1,
        "bin_minutes": int(bin_minutes),
        "horizon_bins": int(horizon_bins),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timeline": timeline,
        "timeline_index": timeline_index,
    }


def get_assistant_outlook_payload(
    index: Dict[str, Any],
    frame_time: str,
    location_ids: Iterable[Any],
) -> Dict[str, Any]:
    frame_key = str(frame_time or "").strip()
    timeline_index = (index or {}).get("timeline_index") or {}
    frame_bucket = timeline_index.get(frame_key)
    if frame_bucket is None:
        raise KeyError(f"frame_time not found: {frame_key}")

    requested: List[str] = []
    seen: set[str] = set()
    for raw_id in location_ids or []:
        normalized = _to_location_id_str(raw_id)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requested.append(normalized)

    zones = [frame_bucket[zone_id] for zone_id in requested if zone_id in frame_bucket]
    zones_by_location_id = {zone.get("location_id"): zone for zone in zones if zone.get("location_id")}
    return {
        "frame_time": frame_key,
        "bin_minutes": int((index or {}).get("bin_minutes") or 20),
        "horizon_bins": int((index or {}).get("horizon_bins") or HORIZON_BINS_DEFAULT),
        "requested_count": len(requested),
        "returned_count": len(zones),
        "zones": zones,
        "zones_by_location_id": zones_by_location_id,
        "by_location_id": zones_by_location_id,
    }
