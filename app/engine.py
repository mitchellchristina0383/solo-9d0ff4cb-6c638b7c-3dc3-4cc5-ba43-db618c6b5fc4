"""Scan → partition → merge engine with source-scoped repair.

Exactly-once rule: a partition total is always rebuilt as a sum over the job's
fixed shard list, with at most one contribution per shard. Repair re-reads a
damaged shard once, rewrites that shard's part files and replaces (never adds)
its contributions. A partition with any unverifiable contribution is withheld.

Phases per run:
1. Scan   - unread source shards are read and their per-domain parts written.
2. Faults - demo/test faults are applied to temporary part files.
3. Verify - every part referenced by the manifest is hash/size/content checked.
4. Repair - for every shard owning a damaged part, re-read *only that source*
            once and rewrite its parts.
5. Merge  - sum verified per-shard parts into per-domain partitions.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import threading
import time

from .config import Settings
from .diversion import (
    DiversionInputError,
    build_trial,
    confirmable,
)
from .logparse import ReadResult, ShardReadError, read_shard
from .models import (
    BlockedPartition,
    Contribution,
    DiversionConfirmRequest,
    DiversionTrial,
    DiversionTrialRequest,
    DiversionTrialSummary,
    DomainResult,
    FaultInjection,
    FaultMode,
    FaultRecord,
    JobCreateRequest,
    JobRunResponse,
    JobState,
    JobStatus,
    JobSummary,
    PartitionState,
    PartitionStatus,
    PartRecord,
    RestoreSourcesResponse,
    ResultsResponse,
    RunReport,
    RunRequest,
    ScanStatus,
    ShardState,
    SourceRead,
    SourceShardInfo,
    utcnow,
)
from .normalize import normalize_domain
from .storage import (
    JobAlreadyExists,
    JobStore,
    StorageError,
    validate_job_id,
    validate_shard_id,
)


class EngineError(RuntimeError):
    pass


class JobBusy(EngineError):
    code = "job_busy"


class BadRequest(EngineError):
    pass


class TrialConflict(EngineError):
    """A trial cannot be confirmed because the job changed; recalculate."""

    def __init__(self, message: str, code: str = "results_changed") -> None:
        super().__init__(message)
        self.code = code


class TrialNotConfirmable(EngineError):
    """The trial itself can never be confirmed (incomplete or not fully divertible)."""

    def __init__(self, message: str, code: str = "trial_not_confirmable") -> None:
        super().__init__(message)
        self.code = code


_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


def _thread_lock(job_id: str) -> threading.Lock:
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(job_id, threading.Lock())


class Engine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = JobStore(settings.data_dir, settings.work_dir)

    # ------------------------------------------------------------------ jobs
    def create_job(self, request: JobCreateRequest) -> JobState:
        shard_ids = [validate_shard_id(sid) for sid in request.shard_ids]
        if len(set(shard_ids)) != len(shard_ids):
            raise BadRequest("shard_ids must be unique")
        missing = [sid for sid in shard_ids if not self.store.source_path(sid).is_file()]
        if missing:
            raise BadRequest(
                "these source shards do not exist under the data directory: " + ", ".join(missing)
            )
        job_id = validate_job_id(request.job_id) if request.job_id else self.store.new_job_id()
        job = JobState(
            job_id=job_id,
            shard_ids=shard_ids,
            shards={
                sid: ShardState(shard_id=sid, index=i)
                for i, sid in enumerate(shard_ids, start=1)
            },
        )
        try:
            self.store.create_job(job)
        except JobAlreadyExists as exc:
            raise BadRequest(f"job id already exists: {exc}") from exc
        return job

    def get_job(self, job_id: str) -> JobState:
        return self.store.load_job(job_id)

    def list_jobs(self) -> list[JobSummary]:
        summaries = []
        for job_id in self.store.list_job_ids():
            try:
                job = self.store.load_job(job_id)
            except StorageError:
                continue
            summaries.append(
                JobSummary(
                    job_id=job.job_id,
                    status=job.status,
                    created_at=job.created_at,
                    updated_at=job.updated_at,
                    shard_ids=job.shard_ids,
                    published_partitions=sum(
                        1 for p in job.partitions.values() if p.status == PartitionStatus.PUBLISHED
                    ),
                    blocked_partitions=sum(
                        1 for p in job.partitions.values() if p.status == PartitionStatus.BLOCKED
                    ),
                    reread_shards=list(job.reread_shards),
                )
            )
        return summaries

    def list_sources(self) -> list[SourceShardInfo]:
        data_dir = self.store.data_dir
        if not data_dir.is_dir():
            return []
        infos = []
        for entry in sorted(data_dir.iterdir(), key=lambda p: p.name):
            try:
                if not entry.is_file():
                    continue
                st = entry.stat()
            except OSError:
                continue
            infos.append(
                SourceShardInfo(
                    shard_id=entry.name,
                    size_bytes=st.st_size,
                    modified_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(st.st_mtime)),
                )
            )
        return infos

    # ------------------------------------------------------------ simulation
    def mark_unavailable(self, job_id: str, shard_ids: list[str]) -> list[str]:
        with self._job_lock(job_id):
            job = self.store.load_job(job_id)
            unknown = [sid for sid in shard_ids if sid not in job.shards]
            if unknown:
                raise BadRequest("unknown shard(s) for this job: " + ", ".join(unknown))
            for sid in shard_ids:
                if sid not in job.simulated_unavailable_sources:
                    job.simulated_unavailable_sources.append(sid)
            job.simulated_unavailable_sources.sort(key=job.shard_ids.index)
            self.store.save_job(job)
            return list(job.simulated_unavailable_sources)

    def restore_sources(
        self, job_id: str, shard_ids: list[str] | None
    ) -> RestoreSourcesResponse:
        with self._job_lock(job_id):
            job = self.store.load_job(job_id)
            if shard_ids is None:
                restored = list(job.simulated_unavailable_sources)
                job.simulated_unavailable_sources = []
            else:
                unknown = [sid for sid in shard_ids if sid not in job.shards]
                if unknown:
                    raise BadRequest("unknown shard(s) for this job: " + ", ".join(unknown))
                restored = [sid for sid in shard_ids if sid in job.simulated_unavailable_sources]
                job.simulated_unavailable_sources = [
                    sid for sid in job.simulated_unavailable_sources if sid not in set(shard_ids)
                ]
            self.store.save_job(job)
            return RestoreSourcesResponse(
                job_id=job.job_id,
                restored=restored,
                still_unavailable=list(job.simulated_unavailable_sources),
                job_status=job.status,
            )

    # ------------------------------------------------------------------- run
    def run_job(self, job_id: str, request: RunRequest | None = None) -> JobRunResponse:
        request = request or RunRequest()
        with self._job_lock(job_id):
            job = self.store.load_job(job_id)
            # A ``processing`` status found while we hold the exclusive lock is
            # always a leftover from an interrupted process (the OS released its
            # lock when it died), so resuming is safe and required by the retry
            # contract. Live concurrent runs are rejected by the lock itself.
            interrupted = job.status == JobStatus.PROCESSING
            job.status = JobStatus.PROCESSING
            job.last_error = None
            self.store.save_job(job)
            run = RunReport()
            if interrupted:
                run.warnings.append(
                    "previous run did not finish (interrupted); resuming from persisted progress"
                )
            try:
                self._execute(job, request, run)
            except Exception as exc:
                job.status = JobStatus.FAILED
                job.last_error = f"{type(exc).__name__}: {exc}"
                run.errors.append(job.last_error)
                run.finished_at = utcnow()
                job.last_run = run
                self.store.save_job(job)
                raise
            run.finished_at = utcnow()
            job.last_run = run
            job.updated_at = utcnow()
            self.store.save_job(job)
            return JobRunResponse(job=job, run=run)

    # -------------------------------------------------------------- internals
    def _execute(self, job: JobState, request: RunRequest, run: RunReport) -> None:
        self._validate_faults(job, request.fault_injection)

        # Persistent outage flags can also be set through the REST demo endpoints.
        for fault in request.fault_injection:
            if fault.mode == FaultMode.SOURCE_UNAVAILABLE:
                if fault.shard_id not in job.simulated_unavailable_sources:
                    job.simulated_unavailable_sources.append(fault.shard_id)
                run.faults.append(
                    FaultRecord(
                        shard_id=fault.shard_id,
                        mode=fault.mode,
                        status="applied",
                        detail="source read will fail until the outage is restored",
                    )
                )
        job.simulated_unavailable_sources.sort(key=job.shard_ids.index)

        # ---- Phase 1: scan shards that have no trusted snapshot yet ---------
        rescan = set(request.rescan_shards)
        unknown_rescan = sorted(rescan - set(job.shard_ids))
        if unknown_rescan:
            raise BadRequest(
                "rescan_shards contains shards that are not in this job: "
                + ", ".join(unknown_rescan)
            )
        for sid in job.shard_ids:
            state = job.shards[sid]
            if sid in rescan:
                self._scan_shard(job, sid, run, kind="rescan", forced=True)
                continue
            if state.scan_status == ScanStatus.DONE and self._shard_manifest_trusted(job, sid):
                continue
            resumed = state.read_attempts > 0 or job.last_run is not None
            self._scan_shard(job, sid, run, kind="retry_scan" if resumed else "scan")
        # Persist scan progress so a crash does not discard completed work.
        self.store.save_job(job)

        # ---- Phase 2: inject temporary-part damage ---------------------------
        for fault in request.fault_injection:
            if fault.mode in (FaultMode.DELETE_PART, FaultMode.TRUNCATE_PART):
                self._apply_part_fault(job, fault, run)
        self.store.save_job(job)

        # ---- Phase 3+4: verify every part, then repair by re-reading the
        # owning source shard at most once per run. ---------------------------
        damaged, unrepairable, errors = self._find_damaged_shards(job)
        # Every source this run has already tried to read: never read one twice.
        attempted = {read.shard_id for read in run.source_reads}
        for sid in damaged:
            if sid in attempted:
                state = job.shards[sid]
                state.scan_status = ScanStatus.FAILED
                message = (
                    f"shard {sid} could not be read earlier in this run; its damaged "
                    "part files wait for the next retry"
                )
                state.last_error = state.last_error or message
                state.last_error_type = state.last_error_type or "part_damaged"
                if sid not in run.failed_repairs:
                    run.failed_repairs.append(sid)
                if message not in run.errors:
                    run.errors.append(message)
                continue
            self._repair_shard(job, sid, run)
        _, post_unrepairable, post_errors = self._find_damaged_shards(job)
        for sid in job.shard_ids:
            if sid in post_unrepairable:
                state = job.shards[sid]
                state.scan_status = ScanStatus.FAILED
                reason = errors.get(sid) or post_errors.get(sid) or "part_damaged"
                message = f"part files for shard {sid} are damaged and cannot be repaired: {reason}"
                state.last_error = message
                state.last_error_type = "part_damaged"
                if message not in run.errors:
                    run.errors.append(message)
            elif job.shards[sid].successful_reads == 0:
                # No snapshot and no trustworthy parts: coverage is unknown.
                state = job.shards[sid]
                state.scan_status = ScanStatus.FAILED
                if not state.last_error:
                    state.last_error = "source was never read successfully"
                    state.last_error_type = state.last_error_type or "unread"

        # ---- Phase 5: merge verified parts per domain ------------------------
        self._merge(job, run)
        self._finalize_status(job)
        self.store.save_job(job)

    def _validate_faults(self, job: JobState, faults: list[FaultInjection]) -> None:
        for fault in faults:
            if fault.shard_id not in job.shards:
                raise BadRequest(f"fault targets a shard not in this job: {fault.shard_id}")
            if fault.mode in (FaultMode.DELETE_PART, FaultMode.TRUNCATE_PART):
                if normalize_domain(fault.domain or "") is None:
                    raise BadRequest(f"fault domain is not a valid host name: {fault.domain!r}")

    def _shard_manifest_trusted(self, job: JobState, shard_id: str) -> bool:
        state = job.shards[shard_id]
        return state.successful_reads > 0 and state.source_sha256 is not None

    def _scan_shard(
        self, job: JobState, shard_id: str, run: RunReport, *, kind: str, forced: bool = False
    ) -> bool:
        state = job.shards[shard_id]
        state.read_attempts += 1
        unavailable = shard_id in job.simulated_unavailable_sources
        source_read = SourceRead(shard_id=shard_id, kind=kind, success=False)
        try:
            result = read_shard(
                self.store.source_path(shard_id), shard_id, simulated_unavailable=unavailable
            )
        except ShardReadError as exc:
            state.scan_status = ScanStatus.FAILED
            state.last_error_type = exc.error_type
            state.last_error = str(exc)
            source_read.error_type = exc.error_type
            source_read.error = str(exc)
            run.source_reads.append(source_read)
            run.errors.append(f"{shard_id}: {exc}")
            if forced and state.successful_reads:
                # The requested fresh snapshot was not obtained; the parts on
                # disk are now known-stale and must not feed a published sum.
                state.stale_parts = True
            self.store.save_job(job)
            return False

        self._write_shard_parts(job, shard_id, result)
        state.scan_status = ScanStatus.DONE
        state.successful_reads += 1
        state.source_sha256 = result.sha256
        state.source_size = result.size_bytes
        state.malformed_lines = result.malformed_lines
        state.stale_parts = False
        state.last_error_type = None
        state.last_error = None
        source_read.success = True
        run.source_reads.append(source_read)
        run.source_read_counts[shard_id] = run.source_read_counts.get(shard_id, 0) + 1
        if kind in ("retry_scan", "rescan"):
            # The caller explicitly wants to know which sources were recomputed.
            if shard_id not in run.reread_shards:
                run.reread_shards.append(shard_id)
                run.reread_shards.sort(key=job.shard_ids.index)
                run.reread_shard_numbers = self._numbers(job, run.reread_shards)
            if shard_id not in job.reread_shards:
                job.reread_shards.append(shard_id)
                job.reread_shards.sort(key=job.shard_ids.index)
        if result.malformed_samples:
            run.warnings.extend(
                f"{shard_id}: malformed line {sample}" for sample in result.malformed_samples
            )
        self.store.save_job(job)
        return True

    def _write_shard_parts(self, job: JobState, shard_id: str, result: ReadResult) -> None:
        records: dict[str, PartRecord] = {}
        for domain in sorted(result.counters):
            counter = result.counters[domain]
            payload = {
                "schema_version": 1,
                "shard_id": shard_id,
                "domain": domain,
                "total_requests": counter.total_requests,
                "status_5xx": counter.status_5xx,
                "malformed_lines": 0,
                "source_sha256": result.sha256,
                "source_size": result.size_bytes,
            }
            path = self.store.part_path(job.job_id, shard_id, domain)
            record = PartRecord(
                shard_id=shard_id,
                domain=domain,
                path=str(path.relative_to(self.store.job_dir(job.job_id))),
                sha256="",
                size_bytes=0,
                total_requests=counter.total_requests,
                status_5xx=counter.status_5xx,
                source_sha256=result.sha256,
            )
            record = self.store.write_part(job.job_id, record, payload)
            records[domain] = record

        # Remove part files for domains that vanished from this shard.
        old = job.parts.get(shard_id, {})
        for domain, record in old.items():
            if domain not in records:
                (self.store.job_dir(job.job_id) / record.path).unlink(missing_ok=True)
        job.parts[shard_id] = records

    def _apply_part_fault(self, job: JobState, fault: FaultInjection, run: RunReport) -> None:
        domain = normalize_domain(fault.domain or "")
        assert domain is not None
        record = job.parts.get(fault.shard_id, {}).get(domain)
        if record is None:
            run.faults.append(
                FaultRecord(
                    shard_id=fault.shard_id,
                    mode=fault.mode,
                    domain=domain,
                    status="noop",
                    detail=(
                        f"shard {fault.shard_id} has no temporary part for {domain}; "
                        "nothing to damage"
                    ),
                )
            )
            return
        path = self.store.job_dir(job.job_id) / record.path
        detail = None
        try:
            if not path.is_file():
                detail = "part file is already absent"
                status = "noop"
            elif fault.mode == FaultMode.DELETE_PART:
                path.unlink()
                detail = f"deleted {record.path}"
                status = "applied"
            else:
                size = path.stat().st_size
                target = fault.truncate_bytes if fault.truncate_bytes is not None else size // 2
                with path.open("r+b") as fh:
                    fh.truncate(min(target, size))
                    fh.flush()
                    os.fsync(fh.fileno())
                detail = f"truncated {record.path} from {size} to {min(target, size)} bytes"
                status = "applied"
        except OSError as exc:
            detail = f"could not damage part file: {exc}"
            status = "failed"
        run.faults.append(
            FaultRecord(
                shard_id=fault.shard_id,
                mode=fault.mode,
                domain=domain,
                status=status,
                detail=detail,
            )
        )

    def _find_damaged_shards(
        self, job: JobState
    ) -> tuple[list[str], list[str], dict[str, str]]:
        """Return (source-level damaged, part-level damaged, first errors).

        Only shards that already had a successful read are candidates for repair.
        Shards that were never read are handled by the scan phase; retrying a
        repair read for them would read the same source twice in one run.
        """

        damaged: set[str] = set()
        unrepairable: set[str] = set()
        errors: dict[str, str] = {}
        for sid in job.shard_ids:
            state = job.shards[sid]
            for record in job.parts.get(sid, {}).values():
                _, error = self.store.read_part(job.job_id, record)
                if error is None:
                    continue
                damaged.add(sid)
                errors.setdefault(sid, error)
                if state.successful_reads == 0:
                    unrepairable.add(sid)
                break
        source_level = [s for s in job.shard_ids if s in damaged and s not in unrepairable]
        part_only = [s for s in job.shard_ids if s in unrepairable]
        return source_level, part_only, errors

    def _repair_shard(self, job: JobState, shard_id: str, run: RunReport) -> None:
        state = job.shards[shard_id]
        run.reread_shards.append(shard_id)
        run.reread_shard_numbers = self._numbers(job, run.reread_shards)
        if shard_id not in job.reread_shards:
            job.reread_shards.append(shard_id)
            job.reread_shards.sort(key=job.shard_ids.index)
        state.read_attempts += 1
        state.repair_reads += 1
        source_read = SourceRead(shard_id=shard_id, kind="repair", success=False)
        try:
            result = read_shard(
                self.store.source_path(shard_id),
                shard_id,
                simulated_unavailable=shard_id in job.simulated_unavailable_sources,
            )
        except ShardReadError as exc:
            state.scan_status = ScanStatus.FAILED
            state.last_error_type = exc.error_type
            state.last_error = str(exc)
            source_read.error_type = exc.error_type
            source_read.error = str(exc)
            run.source_reads.append(source_read)
            run.errors.append(f"repair of {shard_id} failed: {exc}")
            run.failed_repairs.append(shard_id)
            return
        self._write_shard_parts(job, shard_id, result)
        state.scan_status = ScanStatus.DONE
        state.successful_reads += 1
        state.source_sha256 = result.sha256
        state.source_size = result.size_bytes
        state.malformed_lines = result.malformed_lines
        state.stale_parts = False
        state.last_error_type = None
        state.last_error = None
        source_read.success = True
        run.source_reads.append(source_read)
        run.source_read_counts[shard_id] = run.source_read_counts.get(shard_id, 0) + 1
        if shard_id not in run.repaired_shards:
            run.repaired_shards.append(shard_id)
        if shard_id in run.failed_repairs:
            run.failed_repairs.remove(shard_id)
        self.store.save_job(job)

    @staticmethod
    def _numbers(job: JobState, shard_ids: list[str]) -> list[int]:
        positions = {sid: i for i, sid in enumerate(job.shard_ids, start=1)}
        return [positions[sid] for sid in shard_ids]

    def _merge(self, job: JobState, run: RunReport) -> None:
        verified, damaged, known, unknown = self._evaluate_parts(job)
        if damaged:
            run.warnings.extend(f"{sid}: {err}" for sid, err in damaged.items())

        domains: set[str] = set()
        for domain_set in known.values():
            domains.update(domain_set)

        for domain in sorted(domains):
            contributions: dict[str, Contribution] = {}
            blocked: list[str] = []
            for sid in job.shard_ids:
                record = verified.get(sid, {}).get(domain)
                if record is not None:
                    contributions[sid] = Contribution(
                        total_requests=record.total_requests, status_5xx=record.status_5xx
                    )
                elif sid in unknown:
                    # Never read successfully: its domain coverage is unknown, so
                    # this domain might be missing an entire source.
                    blocked.append(sid)
                elif domain in known[sid]:
                    # The shard did contribute here, but that part could not be
                    # verified or rewritten: publishing would be a partial sum.
                    blocked.append(sid)
                else:
                    # The shard's last successful scan did not contain this domain;
                    # its verified-zero coverage is still trustworthy.
                    continue

            partition = job.partitions.get(domain)
            if partition is None:
                partition = PartitionState(domain=domain)
                job.partitions[domain] = partition

            if blocked:
                partition.status = PartitionStatus.BLOCKED
                partition.total_requests = 0
                partition.status_5xx = 0
                partition.contributions = {}
                partition.blocked_shards = blocked
                partition.error = (
                    "withheld: source shard(s) could not be verified for this domain: "
                    + ", ".join(blocked)
                )
                partition.updated_at = utcnow()
                run.warnings.append(f"partition {domain} withheld; blocked shards: {blocked}")
                continue

            partition.status = PartitionStatus.PUBLISHED
            partition.total_requests = sum(c.total_requests for c in contributions.values())
            partition.status_5xx = sum(c.status_5xx for c in contributions.values())
            partition.contributions = contributions
            partition.blocked_shards = []
            partition.error = None
            partition.updated_at = utcnow()

        # A previously published domain that is no longer attested by any manifest
        # cannot be safely republished.
        for domain, partition in job.partitions.items():
            if partition.status == PartitionStatus.PUBLISHED and domain not in domains:
                partition.status = PartitionStatus.BLOCKED
                partition.total_requests = 0
                partition.status_5xx = 0
                partition.contributions = {}
                partition.blocked_shards = [
                    s for s in job.shard_ids if job.shards[s].scan_status != ScanStatus.DONE
                ] or list(job.shard_ids)
                partition.error = "withheld: no verified contribution remains for this domain"
                partition.updated_at = utcnow()

    def _evaluate_parts(
        self, job: JobState
    ) -> tuple[dict[str, dict[str, PartRecord]], dict[str, str], dict[str, set[str]], list[str]]:
        """Return verified parts, damage, known coverage and unknown-coverage shards.

        ``known[sid]`` is the set of domains that shard has ever contributed to and
        is therefore the coverage we can reason about. ``unknown`` contains shards
        that never completed a scan; every partition must be withheld while one
        exists because no coverage information is available for it.
        """

        verified: dict[str, dict[str, PartRecord]] = {}
        damaged: dict[str, str] = {}
        known: dict[str, set[str]] = {}
        unknown: list[str] = []
        for sid in job.shard_ids:
            records = job.parts.get(sid, {})
            known[sid] = set(records)
            if job.shards[sid].successful_reads == 0:
                unknown.append(sid)
                continue
            if job.shards[sid].stale_parts:
                # A forced rescan failed after a previous good read. Whatever is
                # on disk no longer reflects the requested snapshot, so the
                # shard's domains are withheld until a successful rescan.
                damaged[sid] = "part files are stale after a failed rescan"
                continue
            shard_ok: dict[str, PartRecord] = {}
            first_error: str | None = None
            for domain, record in records.items():
                _, error = self.store.read_part(job.job_id, record)
                if error is None:
                    shard_ok[domain] = record
                elif first_error is None:
                    first_error = error
            if first_error is not None:
                damaged[sid] = first_error
            else:
                verified[sid] = shard_ok
        return verified, damaged, known, unknown

    def _finalize_status(self, job: JobState) -> None:
        published = [p for p in job.partitions.values() if p.status == PartitionStatus.PUBLISHED]
        blocked = [p for p in job.partitions.values() if p.status == PartitionStatus.BLOCKED]
        failed_shards = [s for s in job.shard_ids if job.shards[s].scan_status != ScanStatus.DONE]
        if failed_shards:
            job.status = JobStatus.BLOCKED if not published else JobStatus.PARTIAL
        elif blocked:
            job.status = JobStatus.PARTIAL if published else JobStatus.BLOCKED
        else:
            job.status = JobStatus.COMPLETED

    # ------------------------------------------------------------- diversion
    def create_trial(self, job_id: str, request: DiversionTrialRequest) -> DiversionTrial:
        """Calculate a diversion trial from one published snapshot.

        Deliberately does **not** take the job's run lock: reading the atomic
        ``job.json`` gives a consistent snapshot, and an operator must be able to
        calculate a trial while the source job is being rescanned. The snapshot
        digest is what later guards confirmation.
        """

        job = self.store.load_job(job_id)
        trial_id = self.store.new_trial_id(job_id)
        try:
            trial = build_trial(job, trial_id, request)
        except DiversionInputError as exc:
            raise BadRequest(str(exc)) from exc
        self.store.save_trial(trial)
        return trial

    def get_trial(self, job_id: str, trial_id: str) -> DiversionTrial:
        # Ensures a missing job is reported as 404 rather than a stray trial file.
        self.store.load_job(job_id)
        return self.store.load_trial(job_id, trial_id)

    def list_trials(self, job_id: str) -> list[DiversionTrialSummary]:
        self.store.load_job(job_id)
        summaries = [
            DiversionTrialSummary(
                trial_id=trial.trial_id,
                created_at=trial.created_at,
                offline_shard_id=trial.offline_shard_id,
                job_status_at_creation=trial.job_status_at_creation,
                plan_status=trial.plan_status,
                fully_divertible=trial.fully_divertible,
                confirmed_at=trial.confirmed_at,
            )
            for trial in self.store.list_trials(job_id)
        ]
        summaries.sort(key=lambda item: (item.created_at, item.trial_id))
        return summaries

    def confirm_trial(
        self, job_id: str, trial_id: str, request: DiversionConfirmRequest | None = None
    ) -> DiversionTrial:
        """Confirm a fully divertible trial if its snapshot is still current.

        The job lock is taken so a run cannot interleave between the freshness
        check and the confirmation write.
        """

        request = request or DiversionConfirmRequest()
        with self._job_lock(job_id):
            trial = self.store.load_trial(job_id, trial_id)
            if trial.confirmed_at is not None:
                return trial
            job = self.store.load_job(job_id)
            allowed, error_code, reason = confirmable(trial, job)
            if not allowed:
                if error_code in ("summary_incomplete_now", "results_changed"):
                    raise TrialConflict(reason, code=error_code or "results_changed")
                raise TrialNotConfirmable(reason, code=error_code or "trial_not_confirmable")
            trial.confirmed_at = utcnow()
            trial.confirmation_note = request.note
            self.store.save_trial(trial)
            return trial

    # -------------------------------------------------------------- results
    def get_results(self, job_id: str) -> ResultsResponse:
        job = self.store.load_job(job_id)
        results: list[DomainResult] = []
        blocked: list[BlockedPartition] = []
        pending: list[str] = []
        for domain in sorted(job.partitions):
            partition = job.partitions[domain]
            if partition.status == PartitionStatus.PUBLISHED:
                results.append(
                    DomainResult(
                        job_id=job.job_id,
                        domain=domain,
                        total_requests=partition.total_requests,
                        status_5xx=partition.status_5xx,
                        contributions=partition.contributions,
                        updated_at=partition.updated_at,
                    )
                )
            elif partition.status == PartitionStatus.BLOCKED:
                blocked.append(
                    BlockedPartition(
                        domain=domain,
                        blocked_shards=list(partition.blocked_shards),
                        error=partition.error,
                    )
                )
            else:
                pending.append(domain)
        unscanned = [s for s in job.shard_ids if job.shards[s].scan_status != ScanStatus.DONE]
        return ResultsResponse(
            job_id=job.job_id,
            job_status=job.status,
            results=results,
            blocked=blocked,
            pending=pending,
            unscanned_shards=unscanned,
            reread_shards=list(job.reread_shards),
            reread_shard_numbers=job.reread_shard_numbers(),
        )

    # ----------------------------------------------------------------- locks
    @contextlib.contextmanager
    def _job_lock(self, job_id: str):
        validate_job_id(job_id)
        thread_lock = _thread_lock(job_id)
        if not thread_lock.acquire(blocking=False):
            raise JobBusy(f"job {job_id} is already being processed")
        lock_path = self.store.job_dir(job_id) / ".lock"
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock_path.open("a+b") as fh:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise JobBusy(f"job {job_id} is locked by another process") from exc
                try:
                    yield
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            thread_lock.release()
