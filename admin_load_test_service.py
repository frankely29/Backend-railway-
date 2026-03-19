from __future__ import annotations

import math
import random
import resource
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

from admin_load_test_models import AdminLoadTestStartRequest
from admin_test_service import build_admin_response

SUPPORTED_PRESETS = [100, 300, 500, 1000, 1500, 2000]
SUPPORTED_MODES = ["map_core", "map_plus_chat", "custom"]
SUPPORTED_DURATIONS = [30, 45, 60, 90]
SNAPSHOT_INTERVAL_SEC = 5.0
RUN_SLICE_SEC = 0.25
MAX_TOP_ERRORS = 8
MAX_SLOW_SAMPLES = 10
MAX_SNAPSHOTS = 12
MAX_WARNINGS = 8
MAX_REASON_LINES = 12
ACTIVITY_MIX = (
    ("idle", 0.60, (7.0, 14.0), 0.00018),
    ("normal", 0.25, (3.0, 7.0), 0.00035),
    ("high", 0.10, (1.5, 4.0), 0.00065),
    ("heavy", 0.05, (0.6, 2.0), 0.00120),
)
CLUSTERS = [
    {"name": "midtown", "center": (40.7549, -73.9840), "bbox": (40.7350, -74.0100, 40.7750, -73.9550)},
    {"name": "lower_manhattan", "center": (40.7115, -74.0105), "bbox": (40.7000, -74.0250, 40.7250, -73.9900)},
    {"name": "brooklyn_downtown", "center": (40.6928, -73.9857), "bbox": (40.6750, -74.0100, 40.7100, -73.9650)},
    {"name": "lic_astoria", "center": (40.7440, -73.9370), "bbox": (40.7260, -73.9580, 40.7630, -73.9100)},
    {"name": "jfk_airport", "center": (40.6445, -73.7827), "bbox": (40.6280, -73.8150, 40.6650, -73.7550)},
    {"name": "uptown_bronx_edge", "center": (40.8255, -73.9380), "bbox": (40.8040, -73.9650, 40.8450, -73.9100)},
]
MODE_DEFAULTS = {
    "map_core": {
        "include_presence_writes": True,
        "include_presence_viewport_reads": True,
        "include_presence_summary_reads": True,
        "include_presence_delta_reads": True,
        "include_pickup_overlay_reads": False,
        "include_leaderboard_reads": False,
        "include_chat_lite": False,
    },
    "map_plus_chat": {
        "include_presence_writes": True,
        "include_presence_viewport_reads": True,
        "include_presence_summary_reads": True,
        "include_presence_delta_reads": True,
        "include_pickup_overlay_reads": False,
        "include_leaderboard_reads": False,
        "include_chat_lite": True,
    },
    "custom": {},
}
THRESHOLDS: Dict[int, Dict[str, float]] = {
    100: {
        "overall_error_rate": 1.0,
        "presence_write_p95_ms": 250.0,
        "viewport_read_p95_ms": 400.0,
        "summary_read_p95_ms": 180.0,
        "delta_read_p95_ms": 250.0,
        "rss_growth_mb": 80.0,
        "chat_send_p95_ms": 500.0,
        "chat_read_p95_ms": 600.0,
    },
    300: {
        "overall_error_rate": 1.5,
        "presence_write_p95_ms": 350.0,
        "viewport_read_p95_ms": 550.0,
        "summary_read_p95_ms": 220.0,
        "delta_read_p95_ms": 320.0,
        "rss_growth_mb": 120.0,
        "chat_send_p95_ms": 500.0,
        "chat_read_p95_ms": 600.0,
    },
    500: {
        "overall_error_rate": 2.0,
        "presence_write_p95_ms": 500.0,
        "viewport_read_p95_ms": 750.0,
        "summary_read_p95_ms": 280.0,
        "delta_read_p95_ms": 400.0,
        "rss_growth_mb": 180.0,
        "chat_send_p95_ms": 900.0,
        "chat_read_p95_ms": 1000.0,
    },
    1000: {
        "overall_error_rate": 3.0,
        "presence_write_p95_ms": 800.0,
        "viewport_read_p95_ms": 1200.0,
        "summary_read_p95_ms": 380.0,
        "delta_read_p95_ms": 550.0,
        "rss_growth_mb": 260.0,
        "chat_send_p95_ms": 900.0,
        "chat_read_p95_ms": 1000.0,
    },
    1500: {
        "overall_error_rate": 3.5,
        "presence_write_p95_ms": 1000.0,
        "viewport_read_p95_ms": 1500.0,
        "summary_read_p95_ms": 450.0,
        "delta_read_p95_ms": 700.0,
        "rss_growth_mb": 340.0,
        "chat_send_p95_ms": 1000.0,
        "chat_read_p95_ms": 1100.0,
    },
    2000: {
        "overall_error_rate": 4.0,
        "presence_write_p95_ms": 1200.0,
        "viewport_read_p95_ms": 1800.0,
        "summary_read_p95_ms": 520.0,
        "delta_read_p95_ms": 850.0,
        "rss_growth_mb": 420.0,
        "chat_send_p95_ms": 1100.0,
        "chat_read_p95_ms": 1200.0,
    },
}


@dataclass
class SyntheticDriver:
    driver_id: int
    lat: float
    lng: float
    heading: float
    activity_class: str
    cluster_name: str
    viewport_cluster: str
    next_update_at: float
    score: float = 0.0
    last_seq: int = 0


@dataclass
class OperationStats:
    latencies_ms: List[float] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: Counter = field(default_factory=Counter)
    slow_samples: List[Dict[str, Any]] = field(default_factory=list)

    def record(
        self,
        latency_ms: float,
        *,
        success: bool,
        error_message: Optional[str] = None,
        sample: Optional[Dict[str, Any]] = None,
    ) -> None:
        bounded_latency = round(max(0.0, float(latency_ms)), 3)
        self.latencies_ms.append(bounded_latency)
        self.total_count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
            if error_message:
                self.errors[error_message] += 1
        if sample and (len(self.slow_samples) < MAX_SLOW_SAMPLES or bounded_latency > self.slow_samples[-1]["latency_ms"]):
            self.slow_samples.append({"latency_ms": bounded_latency, **sample})
            self.slow_samples.sort(key=lambda item: item["latency_ms"], reverse=True)
            del self.slow_samples[MAX_SLOW_SAMPLES:]

    def to_metrics(self) -> Dict[str, Any]:
        if not self.latencies_ms:
            return {
                "total_count": self.total_count,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "error_rate": round((self.error_count / self.total_count) * 100.0, 3) if self.total_count else 0.0,
                "min_latency_ms": None,
                "p50_latency_ms": None,
                "p95_latency_ms": None,
                "p99_latency_ms": None,
                "max_latency_ms": None,
            }
        sorted_latencies = sorted(self.latencies_ms)
        return {
            "total_count": self.total_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "error_rate": round((self.error_count / self.total_count) * 100.0, 3) if self.total_count else 0.0,
            "min_latency_ms": sorted_latencies[0],
            "p50_latency_ms": _percentile(sorted_latencies, 50),
            "p95_latency_ms": _percentile(sorted_latencies, 95),
            "p99_latency_ms": _percentile(sorted_latencies, 99),
            "max_latency_ms": sorted_latencies[-1],
        }


@dataclass
class RunContext:
    run_id: str
    config: Dict[str, Any]
    started_at_unix: float
    started_at: str
    stop_event: threading.Event = field(default_factory=threading.Event)
    status: str = "running"
    summary: str = "Synthetic admin load test is running"
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    checks: List[Dict[str, Any]] = field(default_factory=list)
    snapshots: List[Dict[str, Any]] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, OperationStats] = field(default_factory=dict)
    top_errors: Counter = field(default_factory=Counter)
    operation_counts: Counter = field(default_factory=Counter)
    partial_debug: Dict[str, Any] = field(default_factory=dict)
    ended_at: Optional[str] = None
    ended_at_unix: Optional[float] = None

    def operation(self, key: str) -> OperationStats:
        if key not in self.metrics:
            self.metrics[key] = OperationStats()
        return self.metrics[key]


class SyntheticLoadTestManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_run: Optional[RunContext] = None
        self._active_thread: Optional[threading.Thread] = None
        self._last_result: Optional[Dict[str, Any]] = None

    def capabilities(self) -> Dict[str, Any]:
        with self._lock:
            active = self._active_run is not None and self._active_run.status == "running"
            current_run = None if self._active_run is None else {
                "run_id": self._active_run.run_id,
                "status": self._active_run.status,
                "started_at": self._active_run.started_at,
                "config": dict(self._active_run.config),
                "summary": self._active_run.summary,
            }
            return {
                "active": active,
                "supported_presets": list(SUPPORTED_PRESETS),
                "supported_modes": list(SUPPORTED_MODES),
                "default_durations": list(SUPPORTED_DURATIONS),
                "default_thresholds": THRESHOLDS,
                "current_run": current_run,
                "last_run": self._compact_result(self._last_result),
            }

    def start(self, payload: AdminLoadTestStartRequest) -> Tuple[bool, Dict[str, Any]]:
        config = self._normalized_config(payload)
        with self._lock:
            if self._active_run is not None and self._active_run.status == "running":
                conflict = {
                    "message": "A synthetic admin load test run is already active. Stop it or wait for completion before starting another run.",
                    "active_run_id": self._active_run.run_id,
                    "started_at": self._active_run.started_at,
                    "status": self._active_run.status,
                    "selected_config": dict(self._active_run.config),
                }
                return False, conflict
            run_id = f"admin-load-{int(time.time() * 1000)}"
            context = RunContext(
                run_id=run_id,
                config=config,
                started_at_unix=time.time(),
                started_at=_iso_now(),
            )
            self._active_run = context
            self._active_thread = threading.Thread(target=self._run, args=(context,), name=run_id, daemon=True)
            self._active_thread.start()
        return True, {
            "run_id": run_id,
            "status": "running",
            "selected_config": config,
            "summary": f"Started synthetic admin load test for {config['preset']} drivers in {config['mode']} mode.",
        }

    def status(self) -> Dict[str, Any]:
        with self._lock:
            if self._active_run is None:
                if self._last_result is not None:
                    return {
                        "status": self._last_result.get("status", "idle"),
                        "progress_percent": float(self._last_result.get("progress_percent") or 100.0),
                        "started_at": self._last_result.get("started_at"),
                        "ended_at": self._last_result.get("ended_at"),
                        "elapsed_sec": float(self._last_result.get("elapsed_sec") or 0.0),
                        "remaining_estimate_sec": 0.0,
                        "active_run_id": None,
                        "selected_config": self._last_result.get("selected_config") or {},
                        "current_metrics": self._last_result.get("current_metrics") or {},
                        "current_reasons": self._last_result.get("current_reasons") or [],
                        "warnings": self._last_result.get("warnings") or [],
                        "errors": self._last_result.get("errors") or [],
                        "checks": self._last_result.get("checks") or [],
                        "last_result": self._last_result,
                    }
                return {
                    "status": "idle",
                    "progress_percent": 0.0,
                    "started_at": None,
                    "ended_at": None,
                    "elapsed_sec": 0.0,
                    "remaining_estimate_sec": 0.0,
                    "active_run_id": None,
                    "selected_config": {},
                    "current_metrics": {},
                    "current_reasons": [],
                    "warnings": [],
                    "errors": [],
                    "checks": [],
                    "last_result": None,
                }
            return self._status_payload(self._active_run, include_last=True)

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            if self._active_run is None or self._active_run.status != "running":
                return {
                    "status": "idle",
                    "message": "No synthetic admin load test is currently running.",
                    "last_result": self._last_result,
                }
            self._active_run.stop_event.set()
            return {
                "status": "running",
                "message": f"Stop requested for run {self._active_run.run_id}. The runner will finish the current work slice and preserve partial metrics.",
                "run_id": self._active_run.run_id,
            }

    def last(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._last_result

    def reset_for_tests(self) -> None:
        thread = None
        with self._lock:
            if self._active_run is not None and self._active_run.status == "running":
                self._active_run.stop_event.set()
                thread = self._active_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._lock:
            self._active_run = None
            self._active_thread = None
            self._last_result = None

    def _run(self, context: RunContext) -> None:
        seed = int(context.config["seed"])
        rng = random.Random(seed)
        duration_sec = int(context.config["duration_sec"])
        store = _SyntheticWorld(context.config, rng)
        start_monotonic = time.monotonic()
        last_snapshot_at = start_monotonic
        rss_before_mb = _rss_mb()
        cpu_before_sec = time.process_time()
        summary_prefix = f"Synthetic admin load test for preset {context.config['preset']} in {context.config['mode']} mode"
        try:
            while True:
                now_monotonic = time.monotonic()
                elapsed_sec = now_monotonic - start_monotonic
                if context.stop_event.is_set():
                    context.status = "stopped"
                    context.reasons.append("Run stopped by admin request before the configured duration completed.")
                    break
                if elapsed_sec >= duration_sec:
                    break
                slice_started = time.perf_counter()
                self._run_slice(context, store, rng, elapsed_sec)
                if now_monotonic - last_snapshot_at >= SNAPSHOT_INTERVAL_SEC:
                    snapshot = self._build_snapshot(context, elapsed_sec)
                    context.snapshots.append(snapshot)
                    context.snapshots = context.snapshots[-MAX_SNAPSHOTS:]
                    last_snapshot_at = now_monotonic
                slice_elapsed = time.perf_counter() - slice_started
                sleep_for = max(0.0, RUN_SLICE_SEC - slice_elapsed)
                if sleep_for > 0:
                    time.sleep(min(sleep_for, RUN_SLICE_SEC))
            if context.status == "running":
                context.status = "pass"
            ended_at_unix = time.time()
            ended_at = _iso_now()
            context.ended_at_unix = ended_at_unix
            context.ended_at = ended_at
            result = self._finalize_result(
                context,
                rss_before_mb=rss_before_mb,
                cpu_before_sec=cpu_before_sec,
                summary_prefix=summary_prefix,
            )
        except Exception as exc:
            context.status = "error"
            context.errors.append(str(exc))
            context.reasons.append(f"Run crashed with an internal error: {exc}")
            context.ended_at_unix = time.time()
            context.ended_at = _iso_now()
            result = self._finalize_result(
                context,
                rss_before_mb=rss_before_mb,
                cpu_before_sec=cpu_before_sec,
                summary_prefix=summary_prefix,
                fatal_error=str(exc),
            )
        with self._lock:
            self._last_result = result
            if self._active_run is context:
                self._active_run = None
                self._active_thread = None

    def _run_slice(self, context: RunContext, store: "_SyntheticWorld", rng: random.Random, elapsed_sec: float) -> None:
        config = context.config
        slice_target_time = store.started_monotonic + elapsed_sec
        if config["include_presence_writes"]:
            for driver in store.drivers_due(slice_target_time):
                latency_ms, success, error_message, sample = store.presence_write(driver, rng)
                context.operation("presence_write").record(latency_ms, success=success, error_message=error_message, sample=sample)
                context.operation_counts["presence_write"] += 1
                if error_message:
                    context.top_errors[error_message] += 1
        viewport_reads = store.per_slice_reads("viewport") if config["include_presence_viewport_reads"] else 0
        for _ in range(viewport_reads):
            latency_ms, success, error_message, sample = store.viewport_read(rng)
            context.operation("viewport_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
            context.operation_counts["viewport_read"] += 1
            if error_message:
                context.top_errors[error_message] += 1
        summary_reads = store.per_slice_reads("summary") if config["include_presence_summary_reads"] else 0
        for _ in range(summary_reads):
            latency_ms, success, error_message, sample = store.summary_read()
            context.operation("summary_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
            context.operation_counts["summary_read"] += 1
            if error_message:
                context.top_errors[error_message] += 1
        delta_reads = store.per_slice_reads("delta") if config["include_presence_delta_reads"] else 0
        for _ in range(delta_reads):
            latency_ms, success, error_message, sample = store.delta_read(rng)
            context.operation("delta_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
            context.operation_counts["delta_read"] += 1
            if error_message:
                context.top_errors[error_message] += 1
        pickup_reads = store.per_slice_reads("pickup_overlay") if config["include_pickup_overlay_reads"] else 0
        for _ in range(pickup_reads):
            latency_ms, success, error_message, sample = store.pickup_overlay_read(rng)
            context.operation("pickup_overlay_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
            context.operation_counts["pickup_overlay_read"] += 1
            if error_message:
                context.top_errors[error_message] += 1
        leaderboard_reads = store.per_slice_reads("leaderboard") if config["include_leaderboard_reads"] else 0
        for _ in range(leaderboard_reads):
            latency_ms, success, error_message, sample = store.leaderboard_read()
            context.operation("leaderboard_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
            context.operation_counts["leaderboard_read"] += 1
            if error_message:
                context.top_errors[error_message] += 1
        if config["include_chat_lite"]:
            chat_send_ops = store.per_slice_reads("chat_send")
            chat_read_ops = store.per_slice_reads("chat_read")
            for _ in range(chat_send_ops):
                latency_ms, success, error_message, sample = store.chat_send(rng)
                context.operation("chat_send").record(latency_ms, success=success, error_message=error_message, sample=sample)
                context.operation_counts["chat_send"] += 1
                if error_message:
                    context.top_errors[error_message] += 1
            for _ in range(chat_read_ops):
                latency_ms, success, error_message, sample = store.chat_read(rng)
                context.operation("chat_read").record(latency_ms, success=success, error_message=error_message, sample=sample)
                context.operation_counts["chat_read"] += 1
                if error_message:
                    context.top_errors[error_message] += 1

    def _build_snapshot(self, context: RunContext, elapsed_sec: float) -> Dict[str, Any]:
        duration_sec = max(1, int(context.config["duration_sec"]))
        progress = min(99.0, round((elapsed_sec / duration_sec) * 100.0, 2))
        return {
            "recorded_at": _iso_now(),
            "elapsed_sec": round(elapsed_sec, 3),
            "progress_percent": progress,
            "operation_counts": dict(context.operation_counts),
            "metrics": {key: stats.to_metrics() for key, stats in context.metrics.items()},
            "top_errors": self._top_errors(context.top_errors),
        }

    def _finalize_result(
        self,
        context: RunContext,
        *,
        rss_before_mb: float,
        cpu_before_sec: float,
        summary_prefix: str,
        fatal_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        rss_after_mb = _rss_mb()
        cpu_after_sec = time.process_time()
        elapsed_sec = max(0.0, (context.ended_at_unix or time.time()) - context.started_at_unix)
        metrics = {key: stats.to_metrics() for key, stats in context.metrics.items()}
        checks, reasons = _evaluate_checks(context.config, metrics, rss_growth_mb=max(0.0, rss_after_mb - rss_before_mb))
        top_errors = self._top_errors(context.top_errors)
        if context.status == "stopped":
            checks.insert(0, {
                "key": "run_status",
                "status": "warn",
                "measured": None,
                "threshold": None,
                "reason": "Run was stopped before completion, so threshold evaluation is informational only.",
            })
        if fatal_error:
            checks.insert(0, {
                "key": "fatal_error",
                "status": "fail",
                "measured": None,
                "threshold": None,
                "reason": f"The load runner crashed: {fatal_error}",
            })
        if context.status not in {"stopped", "error"} and any(check["status"] == "fail" for check in checks):
            context.status = "fail"
        if context.status == "pass" and not reasons:
            reasons = ["PASS because all enabled checks stayed within thresholds and no enabled operation breached the configured error budget."]
        elif context.status == "stopped" and not context.reasons:
            context.reasons = ["Run stopped by admin request and partial metrics were preserved."]
        elif context.status == "error" and not context.reasons:
            context.reasons = ["Run ended with an internal error before threshold evaluation could complete."]
        context.checks = checks
        combined_reasons = list(dict.fromkeys((context.reasons or []) + reasons))[:MAX_REASON_LINES]
        summary = _result_summary(context.status, combined_reasons, summary_prefix)
        debug = {
            "summary": summary,
            "status": context.status,
            "scenario": {
                "preset": context.config["preset"],
                "driver_count": context.config["preset"],
                "mode": context.config["mode"],
                "duration_sec": context.config["duration_sec"],
            },
            "config": context.config,
            "checks": checks,
            "metrics": metrics,
            "snapshots": context.snapshots[-MAX_SNAPSHOTS:],
            "top_errors": top_errors,
            "notes": [note for note in [context.config.get("notes")] if note],
            "slowest_operations": {
                key: stats.slow_samples[:MAX_SLOW_SAMPLES] for key, stats in context.metrics.items() if stats.slow_samples
            },
            "resource_usage": {
                "rss_before_mb": round(rss_before_mb, 3),
                "rss_after_mb": round(rss_after_mb, 3),
                "rss_growth_mb": round(max(0.0, rss_after_mb - rss_before_mb), 3),
                "cpu_before_sec": round(cpu_before_sec, 3),
                "cpu_after_sec": round(cpu_after_sec, 3),
                "cpu_time_delta_sec": round(max(0.0, cpu_after_sec - cpu_before_sec), 3),
            },
            "operation_mix_counts": dict(context.operation_counts),
        }
        result = {
            "run_id": context.run_id,
            "status": context.status,
            "summary": summary,
            "started_at": context.started_at,
            "ended_at": context.ended_at,
            "elapsed_sec": round(elapsed_sec, 3),
            "progress_percent": 100.0 if context.status in {"pass", "fail", "stopped", "error"} else 0.0,
            "remaining_estimate_sec": 0.0,
            "selected_config": context.config,
            "current_metrics": metrics,
            "current_reasons": combined_reasons,
            "warnings": context.warnings[:MAX_WARNINGS],
            "errors": context.errors[:MAX_WARNINGS],
            "checks": checks,
            "debug": debug,
        }
        return result

    def _normalized_config(self, payload: AdminLoadTestStartRequest) -> Dict[str, Any]:
        raw = payload.model_dump()
        mode_defaults = MODE_DEFAULTS.get(payload.mode, {})
        config = {**mode_defaults, **raw}
        config["driver_count"] = config["preset"]
        config["duration_seconds"] = config["duration_sec"]
        if config.get("seed") is None:
            config["seed"] = int(time.time())
        if config.get("viewport_count") is None:
            config["viewport_count"] = max(2, min(16, math.ceil(int(config["preset"]) / 80)))
        return config

    def _status_payload(self, context: RunContext, *, include_last: bool) -> Dict[str, Any]:
        elapsed_sec = max(0.0, time.time() - context.started_at_unix)
        duration_sec = max(1, int(context.config["duration_sec"]))
        progress = min(99.0 if context.status == "running" else 100.0, round((elapsed_sec / duration_sec) * 100.0, 2))
        reasons = list(context.reasons)[:MAX_REASON_LINES]
        if context.status == "running" and not reasons:
            reasons = ["Synthetic admin load test is running and collecting partial metrics."]
        return {
            "status": context.status,
            "progress_percent": progress,
            "started_at": context.started_at,
            "ended_at": context.ended_at,
            "elapsed_sec": round(elapsed_sec, 3),
            "remaining_estimate_sec": round(max(0.0, duration_sec - elapsed_sec), 3) if context.status == "running" else 0.0,
            "active_run_id": context.run_id,
            "selected_config": dict(context.config),
            "current_metrics": {key: stats.to_metrics() for key, stats in context.metrics.items()},
            "current_reasons": reasons,
            "warnings": context.warnings[:MAX_WARNINGS],
            "errors": context.errors[:MAX_WARNINGS],
            "checks": context.checks,
            "last_result": self._last_result if include_last else None,
        }

    @staticmethod
    def _top_errors(errors: Counter) -> List[Dict[str, Any]]:
        return [{"message": message, "count": count} for message, count in errors.most_common(MAX_TOP_ERRORS)]

    @staticmethod
    def _compact_result(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not result:
            return None
        return {
            "run_id": result.get("run_id"),
            "status": result.get("status"),
            "summary": result.get("summary"),
            "started_at": result.get("started_at"),
            "ended_at": result.get("ended_at"),
            "elapsed_sec": result.get("elapsed_sec"),
        }


class _SyntheticWorld:
    def __init__(self, config: Dict[str, Any], rng: random.Random) -> None:
        self.config = config
        self.rng = rng
        self.started_monotonic = time.monotonic()
        self.drivers = self._build_drivers(int(config["preset"]), rng)
        self.sequence = 0
        self.recent_changes: Deque[Dict[str, Any]] = deque(maxlen=max(4000, int(config["preset"]) * 12))
        self.chat_messages: Deque[Dict[str, Any]] = deque(maxlen=300)
        self.delta_cursor = 0
        viewport_limit = max(1, min(len(CLUSTERS), int(config["viewport_count"])))
        self.viewport_boxes = [cluster["bbox"] for cluster in CLUSTERS[:viewport_limit]]
        if not self.viewport_boxes:
            self.viewport_boxes = [cluster["bbox"] for cluster in CLUSTERS]
        self.read_rates = {
            "viewport": max(1, math.ceil(int(config["preset"]) / 45)),
            "summary": max(1, math.ceil(int(config["preset"]) / 150)),
            "delta": max(1, math.ceil(int(config["preset"]) / 70)),
            "pickup_overlay": max(1, math.ceil(int(config["preset"]) / 220)),
            "leaderboard": max(1, math.ceil(int(config["preset"]) / 260)),
            "chat_send": max(1, math.ceil(int(config["preset"]) / 300)),
            "chat_read": max(1, math.ceil(int(config["preset"]) / 220)),
        }
        self.read_carry = {key: 0.0 for key in self.read_rates}

    def _build_drivers(self, count: int, rng: random.Random) -> List[SyntheticDriver]:
        drivers: List[SyntheticDriver] = []
        class_cutoffs = []
        running = 0.0
        for name, weight, interval_range, movement_factor in ACTIVITY_MIX:
            running += weight
            class_cutoffs.append((running, name, interval_range, movement_factor))
        now = self.started_monotonic
        for driver_id in range(1, count + 1):
            cluster = CLUSTERS[(driver_id - 1) % len(CLUSTERS)]
            lat, lng = _jitter_point(cluster["center"], rng, lat_scale=0.012, lng_scale=0.015)
            roll = rng.random()
            selected = class_cutoffs[-1]
            for cutoff in class_cutoffs:
                if roll <= cutoff[0]:
                    selected = cutoff
                    break
            _, activity_class, interval_range, _movement_factor = selected
            next_update_at = now + rng.uniform(interval_range[0], interval_range[1])
            drivers.append(
                SyntheticDriver(
                    driver_id=driver_id,
                    lat=lat,
                    lng=lng,
                    heading=round(rng.uniform(0.0, 359.0), 2),
                    activity_class=activity_class,
                    cluster_name=cluster["name"],
                    viewport_cluster=cluster["name"],
                    next_update_at=next_update_at,
                    score=round(rng.uniform(0.0, 10.0), 3),
                )
            )
        return drivers

    def drivers_due(self, target_monotonic: float) -> List[SyntheticDriver]:
        due: List[SyntheticDriver] = []
        for driver in self.drivers:
            if driver.next_update_at <= target_monotonic:
                due.append(driver)
        return due

    def per_slice_reads(self, key: str) -> int:
        per_second = self.read_rates[key]
        carry = self.read_carry[key] + (per_second * RUN_SLICE_SEC)
        whole = int(carry)
        self.read_carry[key] = carry - whole
        return whole

    def presence_write(self, driver: SyntheticDriver, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        class_config = next(item for item in ACTIVITY_MIX if item[0] == driver.activity_class)
        interval_range = class_config[2]
        movement_factor = class_config[3]
        lat, lng = _jitter_point((driver.lat, driver.lng), rng, lat_scale=movement_factor * 12, lng_scale=movement_factor * 15)
        driver.lat = _clamp(lat, 40.58, 40.92)
        driver.lng = _clamp(lng, -74.10, -73.70)
        driver.heading = round((driver.heading + rng.uniform(-35.0, 35.0)) % 360.0, 2)
        driver.score = round(max(0.0, driver.score + rng.uniform(-0.3, 1.1)), 3)
        driver.next_update_at = time.monotonic() + rng.uniform(interval_range[0], interval_range[1])
        self.sequence += 1
        driver.last_seq = self.sequence
        self.recent_changes.append(
            {
                "seq": self.sequence,
                "driver_id": driver.driver_id,
                "cluster": driver.viewport_cluster,
                "lat": round(driver.lat, 5),
                "lng": round(driver.lng, 5),
                "heading": driver.heading,
            }
        )
        visible_count = 0
        for candidate in self.drivers[: min(len(self.drivers), 20)]:
            if abs(candidate.lat - driver.lat) < 0.02 and abs(candidate.lng - driver.lng) < 0.02:
                visible_count += 1
        latency_ms = 8.0 + min(900.0, 0.03 * len(self.drivers) + (visible_count * 0.4) + rng.uniform(0.0, 20.0))
        return latency_ms, True, None, {
            "driver_id": driver.driver_id,
            "cluster": driver.cluster_name,
            "nearby_visible": visible_count,
        }

    def viewport_read(self, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        bbox = self.viewport_boxes[rng.randrange(0, len(self.viewport_boxes))]
        min_lat, min_lng, max_lat, max_lng = bbox
        items = []
        for driver in self.drivers:
            if min_lat <= driver.lat <= max_lat and min_lng <= driver.lng <= max_lng:
                items.append(
                    {
                        "user_id": driver.driver_id,
                        "lat": round(driver.lat, 5),
                        "lng": round(driver.lng, 5),
                        "heading": round(driver.heading, 2),
                        "activity_class": driver.activity_class,
                    }
                )
        visible_count = len(items)
        lite_payload = items[: min(visible_count, 200)]
        _ = {"count": visible_count, "items": lite_payload, "visible_count": visible_count}
        latency_ms = 14.0 + min(1400.0, 0.06 * len(self.drivers) + 0.85 * visible_count + rng.uniform(0.0, 35.0))
        return latency_ms, True, None, {
            "bbox": [round(value, 5) for value in bbox],
            "visible_count": visible_count,
            "serialized_items": len(lite_payload),
        }

    def summary_read(self) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        cluster_counts = Counter(driver.cluster_name for driver in self.drivers)
        online_count = len(self.drivers)
        visible_count = sum(cluster_counts.values())
        _ = {
            "online_count": online_count,
            "visible_count": visible_count,
            "cluster_breakdown": dict(cluster_counts.most_common(4)),
        }
        latency_ms = 6.0 + min(700.0, 0.015 * len(self.drivers) + len(cluster_counts) * 0.8 + self.rng.uniform(0.0, 12.0))
        return latency_ms, True, None, {
            "online_count": online_count,
            "cluster_count": len(cluster_counts),
        }

    def delta_read(self, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        bbox = self.viewport_boxes[rng.randrange(0, len(self.viewport_boxes))]
        min_lat, min_lng, max_lat, max_lng = bbox
        cursor = self.delta_cursor
        delta_items = []
        for change in self.recent_changes:
            if change["seq"] <= cursor:
                continue
            if min_lat <= change["lat"] <= max_lat and min_lng <= change["lng"] <= max_lng:
                delta_items.append(change)
        if self.recent_changes:
            self.delta_cursor = self.recent_changes[-1]["seq"]
        _ = {
            "cursor": self.delta_cursor,
            "items": delta_items[: min(len(delta_items), 120)],
            "removed": [],
        }
        latency_ms = 9.0 + min(900.0, 0.025 * len(self.recent_changes) + 0.9 * len(delta_items) + rng.uniform(0.0, 18.0))
        return latency_ms, True, None, {
            "from_cursor": cursor,
            "to_cursor": self.delta_cursor,
            "delta_count": len(delta_items),
        }

    def pickup_overlay_read(self, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        hotspots = []
        for cluster in CLUSTERS:
            points = [driver for driver in self.drivers if driver.cluster_name == cluster["name"]]
            centroid_lat = round(sum(item.lat for item in points) / max(1, len(points)), 5)
            centroid_lng = round(sum(item.lng for item in points) / max(1, len(points)), 5)
            hotspots.append({"cluster": cluster["name"], "centroid": [centroid_lat, centroid_lng], "count": len(points)})
        _ = {"features": hotspots[:5]}
        latency_ms = 18.0 + min(1200.0, 0.08 * len(self.drivers) + len(hotspots) * 2.5 + rng.uniform(0.0, 24.0))
        return latency_ms, True, None, {"hotspot_count": len(hotspots)}

    def leaderboard_read(self) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        top_drivers = sorted(self.drivers, key=lambda driver: driver.score, reverse=True)[:20]
        _ = [{"driver_id": driver.driver_id, "score": driver.score} for driver in top_drivers]
        latency_ms = 12.0 + min(1200.0, 0.055 * len(self.drivers) + self.rng.uniform(0.0, 22.0))
        return latency_ms, True, None, {"top_count": len(top_drivers)}

    def chat_send(self, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        driver = self.drivers[rng.randrange(0, len(self.drivers))]
        message = {
            "message_id": len(self.chat_messages) + 1,
            "driver_id": driver.driver_id,
            "text": f"synthetic ping {driver.driver_id}",
            "created_at": _iso_now(),
        }
        self.chat_messages.append(message)
        latency_ms = 16.0 + min(1000.0, 0.03 * len(self.chat_messages) + 0.04 * len(self.drivers) + rng.uniform(0.0, 28.0))
        return latency_ms, True, None, {"driver_id": driver.driver_id, "queue_depth": len(self.chat_messages)}

    def chat_read(self, rng: random.Random) -> Tuple[float, bool, Optional[str], Dict[str, Any]]:
        recent = list(self.chat_messages)[-20:]
        _ = {"items": recent}
        latency_ms = 12.0 + min(1000.0, 0.02 * len(recent) + 0.02 * len(self.drivers) + rng.uniform(0.0, 20.0))
        return latency_ms, True, None, {"message_count": len(recent)}


_manager = SyntheticLoadTestManager()


def get_load_test_capabilities() -> Dict[str, Any]:
    capabilities = _manager.capabilities()
    return build_admin_response(
        True,
        "load-capabilities",
        "Synthetic admin load test capabilities are available.",
        capabilities,
    )


def start_load_test(payload: AdminLoadTestStartRequest) -> Tuple[Dict[str, Any], int]:
    started, details = _manager.start(payload)
    if not started:
        return (
            build_admin_response(False, "load-start", "Synthetic admin load test is already running.", details),
            409,
        )
    return (
        build_admin_response(True, "load-start", details["summary"], details),
        200,
    )


def get_load_test_status() -> Dict[str, Any]:
    status = _manager.status()
    ok = status["status"] in {"idle", "running", "pass", "stopped"}
    summary = {
        "idle": "Synthetic admin load test is idle.",
        "running": "Synthetic admin load test is running.",
        "pass": "Synthetic admin load test passed.",
        "fail": "Synthetic admin load test failed.",
        "stopped": "Synthetic admin load test was stopped.",
        "error": "Synthetic admin load test ended with an internal error.",
    }.get(status["status"], "Synthetic admin load test status is unavailable.")
    return build_admin_response(ok, "load-status", summary, status)


def stop_load_test() -> Dict[str, Any]:
    details = _manager.stop()
    ok = details.get("status") in {"idle", "running"}
    summary = details.get("message") or "Synthetic admin load test stop request processed."
    return build_admin_response(ok, "load-stop", summary, details)


def get_last_load_test_result() -> Dict[str, Any]:
    result = _manager.last()
    if result is None:
        return build_admin_response(True, "load-last", "No synthetic admin load test result is available yet.", {"last_result": None})
    return build_admin_response(True, "load-last", "Last synthetic admin load test result is available.", {"last_result": result})


def get_load_test_manager() -> SyntheticLoadTestManager:
    return _manager


def _evaluate_checks(config: Dict[str, Any], metrics: Dict[str, Dict[str, Any]], *, rss_growth_mb: float) -> Tuple[List[Dict[str, Any]], List[str]]:
    preset_thresholds = THRESHOLDS[int(config["preset"])]
    checks: List[Dict[str, Any]] = []
    reasons: List[str] = []
    total_ops = sum(item.get("total_count", 0) for item in metrics.values())
    total_errors = sum(item.get("error_count", 0) for item in metrics.values())
    overall_error_rate = (total_errors / total_ops) * 100.0 if total_ops else 0.0
    checks.append(_threshold_check(
        key="overall_error_rate",
        measured=overall_error_rate,
        threshold=preset_thresholds["overall_error_rate"],
        label="overall error rate",
        unit="%",
        failure_reason=f"FAIL because overall error rate {overall_error_rate:.2f}% exceeded threshold {preset_thresholds['overall_error_rate']:.2f}%.",
        pass_reason=f"Overall error rate {overall_error_rate:.2f}% stayed within threshold {preset_thresholds['overall_error_rate']:.2f}%.",
    ))
    checks.append(_threshold_check(
        key="rss_growth_mb",
        measured=rss_growth_mb,
        threshold=preset_thresholds["rss_growth_mb"],
        label="RSS growth",
        unit="MB",
        failure_reason=f"FAIL because RSS growth {rss_growth_mb:.2f}MB exceeded threshold {preset_thresholds['rss_growth_mb']:.2f}MB.",
        pass_reason=f"RSS growth {rss_growth_mb:.2f}MB stayed within threshold {preset_thresholds['rss_growth_mb']:.2f}MB.",
    ))
    op_checks = [
        ("presence_write", "presence_write_p95_ms", config.get("include_presence_writes"), "Presence writes became too slow under the selected preset"),
        ("viewport_read", "viewport_read_p95_ms", config.get("include_presence_viewport_reads"), "Viewport reads became too slow under the selected preset"),
        ("summary_read", "summary_read_p95_ms", config.get("include_presence_summary_reads"), "Presence summary reads became too slow under the selected preset"),
        ("delta_read", "delta_read_p95_ms", config.get("include_presence_delta_reads"), "Presence delta reads became too slow under the selected preset"),
        ("chat_send", "chat_send_p95_ms", config.get("include_chat_lite"), "Chat send operations became too slow under chat-lite load"),
        ("chat_read", "chat_read_p95_ms", config.get("include_chat_lite"), "Chat read operations became too slow under chat-lite load"),
    ]
    for metric_key, threshold_key, enabled, reason_text in op_checks:
        if not enabled:
            checks.append({
                "key": threshold_key,
                "status": "skipped",
                "measured": None,
                "threshold": None,
                "reason": f"Skipped because {metric_key} is disabled for this scenario.",
            })
            continue
        p95 = metrics.get(metric_key, {}).get("p95_latency_ms")
        if p95 is None:
            checks.append({
                "key": threshold_key,
                "status": "fail",
                "measured": None,
                "threshold": f"<= {preset_thresholds[threshold_key]}",
                "reason": f"FAIL because no {metric_key} samples were recorded for an enabled operation.",
            })
            continue
        status = "pass" if float(p95) <= float(preset_thresholds[threshold_key]) else "fail"
        checks.append({
            "key": threshold_key,
            "status": status,
            "measured": round(float(p95), 3),
            "threshold": f"<= {preset_thresholds[threshold_key]}",
            "reason": (
                f"PASS because {metric_key.replace('_', ' ')} p95 {float(p95):.2f}ms stayed within threshold {preset_thresholds[threshold_key]:.2f}ms."
                if status == "pass"
                else f"FAIL because {metric_key.replace('_', ' ')} p95 {float(p95):.2f}ms exceeded threshold {preset_thresholds[threshold_key]:.2f}ms. {reason_text}."
            ),
        })
    reasons = [check["reason"] for check in checks if check["status"] == "fail"]
    if not reasons:
        reasons = ["PASS because all enabled hard checks stayed within thresholds and no operation exceeded the configured error budget."]
    return checks, reasons[:MAX_REASON_LINES]


def _threshold_check(
    *,
    key: str,
    measured: float,
    threshold: float,
    label: str,
    unit: str,
    failure_reason: str,
    pass_reason: str,
) -> Dict[str, Any]:
    return {
        "key": key,
        "status": "pass" if measured <= threshold else "fail",
        "measured": round(measured, 3),
        "threshold": f"<= {threshold}",
        "reason": pass_reason if measured <= threshold else failure_reason,
    }


def _result_summary(status: str, reasons: List[str], summary_prefix: str) -> str:
    if status == "pass":
        return f"PASS — {summary_prefix} passed. {reasons[0] if reasons else ''}".strip()
    if status == "fail":
        return f"FAIL — {summary_prefix} failed. {reasons[0] if reasons else ''}".strip()
    if status == "stopped":
        return f"STOPPED — {summary_prefix} stopped early. {reasons[0] if reasons else ''}".strip()
    if status == "error":
        return f"ERROR — {summary_prefix} crashed. {reasons[0] if reasons else ''}".strip()
    return f"{summary_prefix}."


def _percentile(sorted_values: List[float], percentile: int) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return round(sorted_values[0], 3)
    rank = (len(sorted_values) - 1) * (percentile / 100.0)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(sorted_values[int(rank)], 3)
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    interpolated = lower_value + (upper_value - lower_value) * (rank - lower)
    return round(interpolated, 3)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return float(usage.ru_maxrss) / 1024.0


def _jitter_point(center: Tuple[float, float], rng: random.Random, *, lat_scale: float, lng_scale: float) -> Tuple[float, float]:
    lat, lng = center
    return lat + rng.uniform(-lat_scale, lat_scale), lng + rng.uniform(-lng_scale, lng_scale)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
