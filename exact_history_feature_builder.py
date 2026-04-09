from __future__ import annotations

from typing import Any, Dict
import math


AIRPORT_ZONE_IDS = {1, 132, 138}


def bucket_and_color_from_rating(rating: int) -> tuple[str, str]:
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


def _as_finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


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
    pay_n_value = row_map.get("pay_n_safe", row_map.get("pay_n"))
    pay_per_min_n_value = row_map.get("pay_per_min_n_safe", row_map.get("pay_per_min_n"))
    pay_per_mile_n_value = row_map.get("pay_per_mile_n_safe", row_map.get("pay_per_mile_n"))
    shadow_props["pay_n_shadow"] = None if pay_n_value is None else float(pay_n_value)
    shadow_props["pay_per_min_n_shadow"] = None if pay_per_min_n_value is None else float(pay_per_min_n_value)
    shadow_props["pay_per_mile_n_shadow"] = None if pay_per_mile_n_value is None else float(pay_per_mile_n_value)
    shadow_props["pickup_friction_penalty_n_shadow"] = None if row_map.get("pickup_friction_penalty_n") is None else float(row_map.get("pickup_friction_penalty_n"))
    shadow_props["short_trip_penalty_n_shadow"] = None if row_map.get("short_trip_penalty_n") is None else float(row_map.get("short_trip_penalty_n"))
    shadow_props["shared_ride_penalty_n_shadow"] = None if row_map.get("shared_ride_penalty_n") is None else float(row_map.get("shared_ride_penalty_n"))
    shadow_props["downstream_value_n_shadow"] = None if row_map.get("downstream_value_n") is None else float(row_map.get("downstream_value_n"))
    shadow_props["window_trip_count_shadow"] = None if row_map.get("window_trip_count_shadow") is None else int(row_map.get("window_trip_count_shadow"))
    shadow_props["sample_support_strength_shadow"] = None if row_map.get("sample_support_strength_shadow") is None else float(row_map.get("sample_support_strength_shadow"))
    return shadow_props


def build_feature_properties_from_shadow_row(
    *,
    row_map: Dict[str, Any],
    zone_name: str,
    borough: str,
    geometry_area_sq_miles: float | None,
) -> Dict[str, Any]:
    location_id = int(row_map.get("PULocationID"))
    pickups = int(row_map.get("pickups_now") or 0)
    rating = int(row_map.get("earnings_shadow_rating_citywide_v3") or 1)
    bucket, fill = bucket_and_color_from_rating(rating)
    shadow_props = _build_shadow_props_from_row(row_map)
    popup_metrics = _resolve_popup_metrics(
        raw_shadow_props=shadow_props,
        visible_pickups=pickups,
        geometry_area_sq_miles=geometry_area_sq_miles,
    )
    props: Dict[str, Any] = {
        "LocationID": location_id,
        "zone_name": zone_name or "",
        "borough": borough or "",
        "airport_excluded": bool(is_airport_zone(location_id, zone_name, borough)),
        "rating": rating,
        "bucket": bucket,
        "pickups": pickups,
        "avg_driver_pay": None if row_map.get("median_driver_pay") is None else float(row_map.get("median_driver_pay")),
        "avg_tips": None,
        "style": {
            "color": fill,
            "opacity": 0,
            "weight": 0,
            "fillColor": fill,
            "fillOpacity": 0.82,
        },
    }
    props.update(shadow_props)
    props["next_pickups_shadow"] = popup_metrics.get("next_pickups_shadow")
    props["pickups_now_shadow"] = popup_metrics.get("pickups_now_shadow")
    props["zone_area_sq_miles_shadow"] = popup_metrics.get("zone_area_sq_miles_shadow")
    props["pickups_per_sq_mile_now_shadow"] = popup_metrics.get("pickups_per_sq_mile_now_shadow")
    props["pickups_per_sq_mile_next_shadow"] = popup_metrics.get("pickups_per_sq_mile_next_shadow")
    return props
