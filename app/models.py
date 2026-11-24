"""Pydantic models used by the aggregation engine and the HTTP API."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    BLOCKED = "blocked"
    FAILED = "failed"


class ScanStatus(StrEnum):
    PENDING = "pending"
    DONE = "done"
    UNREADABLE = "unreadable"
    FAILED = "failed"


class PartitionStatus(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"
    BLOCKED = "blocked"


class FaultMode(StrEnum):
    """Simulation actions applied by a run request (demo/test only)."""

    DELETE_PART = "delete_part"
    TRUNCATE_PART = "truncate_part"
    SOURCE_UNAVAILABLE = "source_unavailable"


PART_FAULT_MODES = {FaultMode.DELETE_PART, FaultMode.TRUNCATE_PART}


class FaultInjection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shard_id: str = Field(min_length=1, description="Source shard that owns the temporary file")
    mode: FaultMode
    domain: str | None = Field(
        default=None,
        description="Required for delete_part/truncate_part; partition whose temporary file is damaged",
    )
    truncate_bytes: int | None = Field(
        default=None,
        ge=0,
        description="For truncate_part. Defaults to half the file, which normally makes the JSON invalid.",
    )

    @model_validator(mode="after")
    def _check_target(self) -> "FaultInjection":
        if self.mode in PART_FAULT_MODES and not self.domain:
            raise ValueError(f"domain is required for fault mode {self.mode}")
        if self.mode == FaultMode.SOURCE_UNAVAILABLE and self.domain:
            raise ValueError("domain must not be set for fault mode source_unavailable")
        return self


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shard_ids: list[str] = Field(
        min_length=1,
        description="Fixed, ordered set of source shard file names under LOG_DATA_DIR",
        examples=[["gw-01.log", "gw-02.log", "gw-03.log"]],
    )
    job_id: str | None = Field(
        default=None,
        min_length=3,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$",
        description="Optional caller-supplied id. Must be unique.",
    )

    @model_validator(mode="after")
    def _unique_shards(self) -> "JobCreateRequest":
        if len(set(self.shard_ids)) != len(self.shard_ids):
            raise ValueError("shard_ids must be unique; every source may be counted only once")
        return self


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fault_injection: list[FaultInjection] = Field(
        default_factory=list,
        description="Demo/test only: damage temporary files or make source shards unavailable",
    )
    rescan_shards: list[str] = Field(
        default_factory=list,
        description=(
            "Force a fresh read of these source shards (for rotated logs). "
            "Their parts are rebuilt before merging; counted once per run."
        ),
    )

    @model_validator(mode="after")
    def _unique(self) -> "RunRequest":
        if len(set(self.rescan_shards)) != len(self.rescan_shards):
            raise ValueError("rescan_shards must be unique")
        return self


class FaultRecord(BaseModel):
    shard_id: str
    mode: FaultMode
    domain: str | None = None
    status: str
    detail: str | None = None


class SourceRead(BaseModel):
    shard_id: str
    kind: str = Field(description="scan, retry_scan or repair")
    success: bool
    error_type: str | None = None
    error: str | None = None


class RunReport(BaseModel):
    started_at: str = Field(default_factory=utcnow)
    finished_at: str | None = None
    reread_shards: list[str] = Field(
        default_factory=list,
        description="Source shards re-read during this run (repairs and retry scans)",
    )
    reread_shard_numbers: list[int] = Field(default_factory=list, description="1-based positions")
    repaired_shards: list[str] = Field(default_factory=list)
    failed_repairs: list[str] = Field(default_factory=list)
    source_read_counts: dict[str, int] = Field(
        default_factory=dict, description="Successful full reads per shard during this run"
    )
    source_reads: list[SourceRead] = Field(default_factory=list)
    faults: list[FaultRecord] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class Contribution(BaseModel):
    total_requests: int = 0
    status_5xx: int = 0


class ShardState(BaseModel):
    shard_id: str
    index: int = Field(description="1-based position in the job's fixed shard list")
    scan_status: ScanStatus = ScanStatus.PENDING
    source_sha256: str | None = None
    source_size: int | None = None
    read_attempts: int = 0
    successful_reads: int = 0
    repair_reads: int = 0
    malformed_lines: int = 0
    stale_parts: bool = Field(
        default=False,
        description=(
            "The most recent read of this source failed after a previous successful "
            "read, so the existing part files may not reflect a requested rescan"
        ),
    )
    last_error_type: str | None = None
    last_error: str | None = None


class PartRecord(BaseModel):
    shard_id: str
    domain: str
    path: str = Field(description="Path relative to the job directory")
    sha256: str
    size_bytes: int
    total_requests: int
    status_5xx: int
    malformed_lines: int = 0
    source_sha256: str | None = None
    created_at: str = Field(default_factory=utcnow)


class PartitionState(BaseModel):
    domain: str
    status: PartitionStatus = PartitionStatus.PENDING
    total_requests: int = 0
    status_5xx: int = 0
    contributions: dict[str, Contribution] = Field(default_factory=dict)
    blocked_shards: list[str] = Field(default_factory=list)
    error: str | None = None
    updated_at: str | None = None


class JobState(BaseModel):
    schema_version: int = 1
    job_id: str
    status: JobStatus = JobStatus.PENDING
    created_at: str = Field(default_factory=utcnow)
    updated_at: str = Field(default_factory=utcnow)
    shard_ids: list[str]
    shards: dict[str, ShardState]
    parts: dict[str, dict[str, PartRecord]] = Field(
        default_factory=dict, description="shard_id -> domain -> temporary part manifest"
    )
    partitions: dict[str, PartitionState] = Field(default_factory=dict)
    simulated_unavailable_sources: list[str] = Field(default_factory=list)
    reread_shards: list[str] = Field(
        default_factory=list, description="Cumulative re-read shards across runs"
    )
    last_run: RunReport | None = None
    last_error: str | None = None

    def reread_shard_numbers(self) -> list[int]:
        positions = {sid: i for i, sid in enumerate(self.shard_ids, start=1)}
        return [positions[sid] for sid in self.reread_shards if sid in positions]

    @property
    def shard_states(self) -> list[ShardState]:
        return [self.shards[sid] for sid in self.shard_ids]


class JobRunResponse(BaseModel):
    job: JobState
    run: RunReport


class JobSummary(BaseModel):
    job_id: str
    status: JobStatus
    created_at: str
    updated_at: str
    shard_ids: list[str]
    published_partitions: int
    blocked_partitions: int
    reread_shards: list[str]


class SourceShardInfo(BaseModel):
    shard_id: str
    size_bytes: int
    modified_at: str


class DomainResult(BaseModel):
    job_id: str
    domain: str
    total_requests: int
    status_5xx: int
    contributions: dict[str, Contribution]
    updated_at: str | None = None


class BlockedPartition(BaseModel):
    domain: str
    blocked_shards: list[str]
    error: str | None = None


class ResultsResponse(BaseModel):
    job_id: str
    job_status: JobStatus
    results: list[DomainResult]
    blocked: list[BlockedPartition]
    pending: list[str]
    unscanned_shards: list[str] = Field(default_factory=list)
    reread_shards: list[str] = Field(default_factory=list)
    reread_shard_numbers: list[int] = Field(default_factory=list)


class RestoreSourcesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    all: bool = Field(default=False, description="Clear every simulated source outage")
    shard_ids: list[str] = Field(default_factory=list)
    @model_validator(mode="after")
    def _choose(self) -> "RestoreSourcesRequest":
        if self.all and self.shard_ids:
            raise ValueError("use either all=true or shard_ids, not both")
        if not self.all and not self.shard_ids:
            raise ValueError("provide all=true or at least one shard id")
        return self


class RestoreSourcesResponse(BaseModel):
    job_id: str
    restored: list[str]
    still_unavailable: list[str]
    job_status: JobStatus


class MarkSourcesUnavailableRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shard_ids: list[str] = Field(min_length=1)


class ErrorResponse(BaseModel):
    detail: Any
