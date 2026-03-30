from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List
import json

# Source-of-truth contract:
# - This engine reads only already-generated timeline/frame artifacts.
# - Scoring truth remains in zone_earnings_engine.py and downstream frame builders.
# - Frontend assistant interpretation stays in the assistant frontend module.
# - Backend outlook packaging here does not own trap/busy/slow thresholds.

HORIZON_BINS_DEFAULT = 6
TRACK_FAMILIES = (
    "citywide_v3",
    "citywide_v2",
    "manhattan_v3",
    "manhattan_v2",
    "bronx_wash_heights_v3",
    "bronx_wash_heights_v2",
    "queens_v3",
    "queens_v2",
    "brooklyn_v3",
    "brooklyn_v2",
    "staten_island_v3",
    "staten_island_v2",
)
TRACK_KEY_ALIASES = {
    "citywide_v3": "citywide_v3_shadow",
    "citywide_v2": "citywide_shadow",
    "manhattan_v3": "manhattan_v3_shadow",
    "manhattan_v2": "manhattan_shadow",
    "bronx_wash_heights_v3": "bronx_wash_heights_v3_shadow",
    "bronx_wash_heights_v2": "bronx_wash_heights_shadow",
    "queens_v3": "queens_v3_shadow",
    "queens_v2": "queens_shadow",
    "brooklyn_v3": "brooklyn_v3_shadow",
    "brooklyn_v2": "brooklyn_shadow",
    "staten_island_v3": "staten_island_v3_shadow",
    "staten_island_v2": "staten_island_shadow",
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


def _track_entry_from_props(props: Dict[str, Any], family: str) -> Dict[str, Any] | None:
    rating = props.get(f"earnings_shadow_rating_{family}")
    bucket = props.get(f"earnings_shadow_bucket_{family}")
    if rating is None and bucket is None:
        return None
    return {"rating": rating, "bucket": bucket}


def _raw_payload_from_props(props: Dict[str, Any]) -> Dict[str, Any]:
    balanced_trip = props.get("balanced_trip_share_shadow")
    if balanced_trip is None:
        balanced_trip = props.get("balanced_trip_share")

    short_trip_penalty = props.get("short_trip_penalty_n_shadow")
    if short_trip_penalty is None:
        short_trip_penalty = props.get("short_trip_penalty_n")

    long_trip_share = props.get("long_trip_share_20plus_shadow")
    if long_trip_share is None:
        long_trip_share = props.get("long_trip_share_20plus")

    downstream_next = props.get("downstream_next_value_raw")
    if downstream_next is None:
        downstream_next = props.get("downstream_value_shadow")

    return {
        "busy_now_base_n_shadow": props.get("busy_now_base_n_shadow"),
        "busy_next_base_n_shadow": props.get("busy_next_base_n_shadow"),
        "short_trip_penalty_n_shadow": short_trip_penalty,
        "long_trip_share_20plus": long_trip_share,
        "balanced_trip_share_shadow": balanced_trip,
        "churn_pressure_n_shadow": props.get("churn_pressure_n_shadow"),
        "market_saturation_penalty_n_shadow": props.get("market_saturation_penalty_n_shadow"),
        "manhattan_core_saturation_penalty_n_shadow": props.get("manhattan_core_saturation_penalty_n_shadow"),
        "downstream_next_value_raw": downstream_next,
    }


def build_assistant_outlook_index(
    timeline: List[str],
    frames_by_time: Dict[str, Dict[str, Any]],
    *,
    bin_minutes: int = 20,
    horizon_bins: int = HORIZON_BINS_DEFAULT,
) -> Dict[str, Any]:
    timeline_in_order = [str(t) for t in (timeline or []) if str(t).strip()]
    total_frames = len(timeline_in_order)
    index: Dict[str, Dict[str, Any]] = {}

    for frame_idx, frame_time in enumerate(timeline_in_order):
        frame_outlook: Dict[str, Any] = {}
        for offset in range(max(1, int(horizon_bins))):
            if total_frames <= 0:
                break
            future_time = timeline_in_order[(frame_idx + offset) % total_frames]
            future_frame = frames_by_time.get(future_time) or {}
            polygons = future_frame.get("polygons") or {}
            features = polygons.get("features") or []
            for feature in features:
                props = feature.get("properties") or {}
                location_id = _to_location_id_str(props.get("LocationID"))
                if not location_id:
                    continue
                zone_payload = frame_outlook.get(location_id)
                if zone_payload is None:
                    zone_payload = {
                        "location_id": location_id,
                        "zone_name": props.get("zone_name"),
                        "borough": props.get("borough"),
                        "points": [],
                    }
                    frame_outlook[location_id] = zone_payload

                tracks: Dict[str, Any] = {}
                for family in TRACK_FAMILIES:
                    track_entry = _track_entry_from_props(props, family)
                    if track_entry is not None:
                        tracks[TRACK_KEY_ALIASES[family]] = track_entry

                zone_payload["points"].append(
                    {
                        "frame_time": future_time,
                        "tracks": tracks,
                        "raw": _raw_payload_from_props(props),
                    }
                )
        index[frame_time] = frame_outlook

    return {
        "bin_minutes": int(bin_minutes),
        "horizon_bins": int(horizon_bins),
        "timeline": timeline_in_order,
        "outlook_index": index,
    }


def get_assistant_outlook_payload(
    assistant_outlook_artifact: Dict[str, Any],
    frame_time: str,
    location_ids: Iterable[Any],
) -> Dict[str, Any]:
    index = (assistant_outlook_artifact or {}).get("outlook_index") or {}
    frame_key = str(frame_time or "").strip()
    frame_bucket = index.get(frame_key)
    if frame_bucket is None:
        raise KeyError(f"frame_time not found: {frame_key}")

    requested_zone_ids: List[str] = []
    seen: set[str] = set()
    for raw_location_id in location_ids or []:
        normalized = _to_location_id_str(raw_location_id)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requested_zone_ids.append(normalized)

    zones = []
    for location_id in requested_zone_ids:
        zone_payload = frame_bucket.get(location_id)
        if zone_payload is not None:
            zones.append(zone_payload)

    return {
        "frame_time": frame_key,
        "bin_minutes": int((assistant_outlook_artifact or {}).get("bin_minutes") or 20),
        "horizon_bins": int((assistant_outlook_artifact or {}).get("horizon_bins") or HORIZON_BINS_DEFAULT),
        "zones": zones,
    }


def load_timeline_and_frames_from_artifacts(frames_dir: Path) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
    timeline_path = Path(frames_dir) / "timeline.json"
    timeline_doc = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline = [str(value) for value in (timeline_doc.get("timeline") or [])]

    frames_by_time: Dict[str, Dict[str, Any]] = {}
    for idx, frame_time in enumerate(timeline):
        frame_path = Path(frames_dir) / f"frame_{idx:06d}.json"
        if not frame_path.exists():
            continue
        frame_doc = json.loads(frame_path.read_text(encoding="utf-8"))
        frames_by_time[frame_time] = frame_doc

    return timeline, frames_by_time
