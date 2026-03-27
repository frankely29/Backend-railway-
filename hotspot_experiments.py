from __future__ import annotations

from typing import Iterable, Optional

from hotspot_models import MicroHotspotScoreResult, ZoneScoreResult


MAX_ZONE_BIN_LOGS = 32
MAX_MICRO_BIN_LOGS = 3


def _bool_db_value(flag: bool):
    return bool(flag)


def log_zone_bins(db_exec, *, bin_time: int, rows: Iterable[ZoneScoreResult]) -> None:
    ranked = sorted(list(rows), key=lambda r: r.final_score, reverse=True)[:MAX_ZONE_BIN_LOGS]
    for r in ranked:
        db_exec(
            """
            INSERT INTO hotspot_experiment_bins(
                bin_time, zone_id, final_score, confidence,
                historical_component, live_component, same_timeslot_component,
                density_penalty, weighted_trip_count, unique_driver_count, recommended
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(bin_time), int(r.zone_id), float(r.final_score), float(r.confidence),
                float(r.historical_component), float(r.live_component), float(r.same_timeslot_component),
                float(r.density_penalty), float(r.weighted_trip_count), int(r.unique_driver_count),
                _bool_db_value(bool(r.recommended)),
            ),
        )


def log_micro_bins(db_exec, *, bin_time: int, rows: Iterable[MicroHotspotScoreResult]) -> None:
    ranked = sorted(list(rows), key=lambda r: r.final_score, reverse=True)[:MAX_MICRO_BIN_LOGS]
    for r in ranked:
        db_exec(
            """
            INSERT INTO micro_hotspot_experiment_bins(
                bin_time, zone_id, cluster_id, final_score, confidence,
                weighted_trip_count, unique_driver_count, crowding_penalty, recommended
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                int(bin_time), int(r.zone_id), str(r.cluster_id), float(r.final_score), float(r.confidence),
                float(r.weighted_trip_count), int(r.unique_driver_count), float(r.crowding_penalty), _bool_db_value(bool(r.recommended)),
            ),
        )


def log_recommendation_outcome(
    db_exec,
    *,
    recommended_at: int,
    zone_id: int,
    score: float,
    confidence: float,
    user_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    converted_to_trip: Optional[bool] = None,
    minutes_to_trip: Optional[float] = None,
) -> None:
    db_exec(
        """
        INSERT INTO recommendation_outcomes(
            user_id, recommended_at, zone_id, cluster_id, score, confidence, converted_to_trip, minutes_to_trip
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            int(user_id) if user_id is not None else None,
            int(recommended_at),
            int(zone_id),
            cluster_id,
            float(score),
            float(confidence),
            None if converted_to_trip is None else _bool_db_value(bool(converted_to_trip)),
            minutes_to_trip,
        ),
    )


def prune_experiment_tables(db_exec, *, now_ts: int) -> None:
    cutoff = int(now_ts) - (14 * 24 * 3600)
    db_exec("DELETE FROM hotspot_experiment_bins WHERE bin_time < ?", (cutoff,))
    db_exec("DELETE FROM micro_hotspot_experiment_bins WHERE bin_time < ?", (cutoff,))
    db_exec("DELETE FROM recommendation_outcomes WHERE recommended_at < ?", (cutoff,))
