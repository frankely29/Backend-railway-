from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


LoadTestPreset = Literal[100, 300, 500, 1000]
LoadTestDuration = Literal[30, 45, 60, 90]
LoadTestMode = Literal["map_core", "map_plus_chat", "custom"]
LoadTestStatus = Literal["idle", "running", "pass", "fail", "stopped", "error"]


class AdminLoadTestStartRequest(BaseModel):
    preset: LoadTestPreset
    duration_sec: LoadTestDuration = 30
    mode: LoadTestMode = "map_core"
    include_presence_writes: bool = True
    include_presence_viewport_reads: bool = True
    include_presence_summary_reads: bool = True
    include_presence_delta_reads: bool = True
    include_pickup_overlay_reads: bool = False
    include_leaderboard_reads: bool = False
    include_chat_lite: bool = False
    seed: Optional[int] = None
    viewport_count: Optional[int] = Field(default=None, ge=1, le=24)
    notes: Optional[str] = Field(default=None, max_length=400)


class LoadTestCheck(BaseModel):
    key: str
    status: Literal["pass", "fail", "warn", "skipped"]
    measured: Optional[float] = None
    threshold: Optional[str] = None
    reason: str


class LoadTestOperationMetrics(BaseModel):
    total_count: int = 0
    success_count: int = 0
    error_count: int = 0
    error_rate: float = 0.0
    min_latency_ms: Optional[float] = None
    p50_latency_ms: Optional[float] = None
    p95_latency_ms: Optional[float] = None
    p99_latency_ms: Optional[float] = None
    max_latency_ms: Optional[float] = None


class LoadTestSnapshot(BaseModel):
    recorded_at: str
    elapsed_sec: float
    progress_percent: float
    operation_counts: Dict[str, int]
    metrics: Dict[str, LoadTestOperationMetrics]
    top_errors: List[Dict[str, Any]]


class LoadTestStatusDetails(BaseModel):
    status: LoadTestStatus
    progress_percent: float
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    elapsed_sec: float = 0.0
    remaining_estimate_sec: float = 0.0
    active_run_id: Optional[str] = None
    selected_config: Dict[str, Any] = Field(default_factory=dict)
    current_metrics: Dict[str, LoadTestOperationMetrics] = Field(default_factory=dict)
    current_reasons: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    checks: List[LoadTestCheck] = Field(default_factory=list)
    last_result: Optional[Dict[str, Any]] = None


class LoadTestCapabilities(BaseModel):
    active: bool
    supported_presets: List[int]
    supported_modes: List[str]
    default_durations: List[int]
    default_thresholds: Dict[str, Any]
    current_run: Optional[Dict[str, Any]] = None
    last_run: Optional[Dict[str, Any]] = None
