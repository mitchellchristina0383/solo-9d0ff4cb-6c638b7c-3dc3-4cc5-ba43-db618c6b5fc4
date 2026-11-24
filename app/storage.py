"""Job directory layout and crash-safe persistence helpers.

Layout under ``WORK_DIR``::

    jobs/<job_id>/job.json                  authoritative job state (atomic replace)
    jobs/<job_id>/parts/<shard>/<h>.json    per-shard, per-domain temporary counts

The JSON state is rewritten via a temp file + fsync + ``os.replace`` so a killed
process can never observe a half-written manifest.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .models import DiversionTrial, JobState, PartRecord

SHARD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$")
TRIAL_ID_RE = re.compile(r"^trial-[0-9a-f]{12}$")


class StorageError(RuntimeError):
    pass


class JobNotFound(StorageError):
    pass


class TrialNotFound(StorageError):
    pass


class JobAlreadyExists(StorageError):
    pass


class InvalidName(StorageError):
    pass


def validate_job_id(job_id: str) -> str:
    if not JOB_ID_RE.fullmatch(job_id) or ".." in job_id:
        raise InvalidName(
            "job_id must be 3-64 chars of letters, digits, '_' or '-', starting alphanumeric"
        )
    return job_id


def validate_shard_id(shard_id: str) -> str:
    if not SHARD_ID_RE.fullmatch(shard_id) or ".." in shard_id:
        raise InvalidName(
            "shard id must be a plain file name (letters, digits, '.', '_', '-'), "
            "starting alphanumeric, at most 128 chars"
        )
    return shard_id


def validate_trial_id(trial_id: str) -> str:
    if not TRIAL_ID_RE.fullmatch(trial_id):
        raise InvalidName("trial id must look like trial-<12 lowercase hex chars>")
    return trial_id


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JobStore:
    def __init__(self, data_dir: Path, work_dir: Path) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.work_dir = Path(work_dir).expanduser().resolve()
        self.jobs_dir = self.work_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ paths
    def job_dir(self, job_id: str) -> Path:
        validate_job_id(job_id)
        return self.jobs_dir / job_id

    def state_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "job.json"

    def parts_dir(self, job_id: str, shard_id: str) -> Path:
        validate_shard_id(shard_id)
        return self.job_dir(job_id) / "parts" / shard_id

    def part_path(self, job_id: str, shard_id: str, domain: str) -> Path:
        digest = hashlib.sha256(domain.encode("utf-8")).hexdigest()
        return self.parts_dir(job_id, shard_id) / f"{digest}.json"

    def source_path(self, shard_id: str) -> Path:
        """Resolve a source shard strictly inside ``data_dir``."""

        validate_shard_id(shard_id)
        base = self.data_dir
        candidate = (base / shard_id).resolve()
        if not candidate.is_relative_to(base) or candidate == base:
            raise InvalidName(f"shard path escapes the data directory: {shard_id}")
        return candidate

    def source_sha256(self, shard_id: str) -> str:
        digest = hashlib.sha256()
        with self.source_path(shard_id).open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    # ------------------------------------------------------------------- jobs
    def new_job_id(self) -> str:
        for _ in range(10):
            candidate = f"job-{uuid.uuid4().hex[:12]}"
            if not self.state_path(candidate).exists():
                return candidate
        raise StorageError("could not allocate a unique job id")

    def list_job_ids(self) -> list[str]:
        if not self.jobs_dir.exists():
            return []
        ids = [p.name for p in self.jobs_dir.iterdir() if p.is_dir() and self.state_path(p.name).is_file()]
        return sorted(ids)

    def create_job(self, job: JobState) -> None:
        validate_job_id(job.job_id)
        job_dir = self.job_dir(job.job_id)
        if self.state_path(job.job_id).exists():
            raise JobAlreadyExists(job.job_id)
        job_dir.mkdir(parents=True, exist_ok=False)
        (job_dir / "parts").mkdir(parents=True, exist_ok=True)
        for sid in job.shard_ids:
            (job_dir / "parts" / sid).mkdir(parents=True, exist_ok=True)
        self.save_job(job)

    def load_job(self, job_id: str) -> JobState:
        path = self.state_path(job_id)
        if not path.is_file():
            raise JobNotFound(job_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            job = JobState.model_validate(raw)
        except Exception as exc:  # pragma: no cover - defensive
            raise StorageError(f"job state is unreadable for {job_id}: {exc}") from exc
        if [s for s in job.shard_ids if s not in job.shards]:
            raise StorageError(f"job state is inconsistent for {job_id}")
        return job

    def save_job(self, job: JobState) -> None:
        job.updated_at = utcnow()
        self._atomic_write(
            self.state_path(job.job_id),
            json.dumps(job.model_dump(mode="json"), ensure_ascii=False, indent=2).encode("utf-8"),
        )

    def delete_job(self, job_id: str) -> None:  # pragma: no cover - admin helper
        import shutil

        path = self.job_dir(job_id)
        if not path.is_dir():
            raise JobNotFound(job_id)
        shutil.rmtree(path)

    # ----------------------------------------------------------------- trials
    def trials_dir(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "trials"

    def trial_path(self, job_id: str, trial_id: str) -> Path:
        return self.trials_dir(job_id) / f"{validate_trial_id(trial_id)}.json"

    def new_trial_id(self, job_id: str) -> str:
        for _ in range(10):
            candidate = f"trial-{uuid.uuid4().hex[:12]}"
            if not self.trial_path(job_id, candidate).exists():
                return candidate
        raise StorageError("could not allocate a unique trial id")

    def save_trial(self, trial: DiversionTrial) -> None:
        self._atomic_write(
            self.trial_path(trial.job_id, trial.trial_id),
            json.dumps(trial.model_dump(mode="json"), ensure_ascii=False, indent=2).encode("utf-8"),
        )

    def load_trial(self, job_id: str, trial_id: str) -> DiversionTrial:
        path = self.trial_path(job_id, trial_id)
        if not path.is_file():
            raise TrialNotFound(f"trial {trial_id} does not exist for job {job_id}")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            trial = DiversionTrial.model_validate(raw)
        except Exception as exc:  # pragma: no cover - defensive
            raise StorageError(f"trial state is unreadable for {trial_id}: {exc}") from exc
        if trial.job_id != job_id or trial.trial_id != trial_id:
            raise StorageError(f"trial state is inconsistent for {trial_id}")
        return trial

    def list_trials(self, job_id: str) -> list[DiversionTrial]:
        directory = self.trials_dir(job_id)
        if not directory.is_dir():
            return []
        trials = []
        for path in sorted(directory.glob("trial-*.json")):
            try:
                trials.append(self.load_trial(job_id, path.stem))
            except (StorageError, InvalidName):
                continue
        return trials

    # ------------------------------------------------------------------ parts
    def write_part(self, job_id: str, record: PartRecord, payload: dict) -> PartRecord:
        path = self.job_dir(job_id) / record.path
        path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        self._atomic_write(path, data)
        record.size_bytes = len(data)
        record.sha256 = hashlib.sha256(data).hexdigest()
        return record

    def read_part(self, job_id: str, record: PartRecord) -> tuple[dict | None, str | None]:
        """Read and integrity-check one temporary part file."""

        path = self.job_dir(job_id) / record.path
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None, "part_missing"
        except OSError as exc:
            return None, f"part_unreadable: {exc}"
        if len(raw) != record.size_bytes:
            return None, f"part_size_mismatch: expected {record.size_bytes}, got {len(raw)}"
        actual = hashlib.sha256(raw).hexdigest()
        if actual != record.sha256:
            return None, "part_hash_mismatch"
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return None, f"part_json_invalid: {exc}"
        if not isinstance(payload, dict):
            return None, "part_json_invalid: not an object"
        problems = _validate_payload(payload, record)
        if problems:
            return None, f"part_content_invalid: {problems}"
        return payload, None

    # ------------------------------------------------------------------ fs io
    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            try:
                dir_fd = os.open(path.parent, os.O_DIRECTORY)
            except (AttributeError, OSError):  # pragma: no cover - non-POSIX
                return
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise


def _validate_payload(payload: dict, record: PartRecord) -> list[str]:
    problems: list[str] = []
    if payload.get("schema_version") != 1:
        problems.append("schema_version")
    if payload.get("shard_id") != record.shard_id:
        problems.append("shard_id")
    if payload.get("domain") != record.domain:
        problems.append("domain")
    totals = payload.get("total_requests")
    failures = payload.get("status_5xx")
    malformed = payload.get("malformed_lines", 0)
    for name, value in (("total_requests", totals), ("status_5xx", failures), ("malformed_lines", malformed)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            problems.append(name)
    if not problems:
        assert isinstance(totals, int) and isinstance(failures, int)
        if failures > totals:
            problems.append("status_5xx>total_requests")
        if totals != record.total_requests or failures != record.status_5xx:
            problems.append("manifest_totals_mismatch")
    if record.source_sha256 is not None and payload.get("source_sha256") != record.source_sha256:
        problems.append("source_sha256")
    return problems
