from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.storage import JobStore, validate_shard_id


def _job_store(tmp_path: Path) -> JobStore:
    data = tmp_path / "data"
    work = tmp_path / "work"
    data.mkdir()
    work.mkdir()
    return JobStore(data, work)


def test_state_write_is_atomic_and_reloadable(tmp_path: Path) -> None:
    from app.models import JobState, ShardState

    store = _job_store(tmp_path)
    job = JobState(
        job_id="atomic-1",
        shard_ids=["a.log"],
        shards={"a.log": ShardState(shard_id="a.log", index=1)},
    )
    store.create_job(job)
    reloaded = store.load_job("atomic-1")
    assert reloaded.shard_ids == ["a.log"]
    leftovers = list((store.job_dir("atomic-1")).glob("*.tmp"))
    assert leftovers == []


def test_part_read_detects_truncation_and_tampering(tmp_path: Path) -> None:
    from app.models import JobState, PartRecord, ShardState

    store = _job_store(tmp_path)
    job = JobState(
        job_id="parts-1",
        shard_ids=["a.log"],
        shards={"a.log": ShardState(shard_id="a.log", index=1)},
    )
    store.create_job(job)
    record = PartRecord(
        shard_id="a.log",
        domain="api.example.com",
        path="parts/a.log/x.json",
        sha256="",
        size_bytes=0,
        total_requests=3,
        status_5xx=1,
    )
    payload = {
        "schema_version": 1,
        "shard_id": "a.log",
        "domain": "api.example.com",
        "total_requests": 3,
        "status_5xx": 1,
        "malformed_lines": 0,
        "source_sha256": None,
    }
    record = store.write_part("parts-1", record, payload)
    loaded, error = store.read_part("parts-1", record)
    assert error is None and loaded == payload

    path = store.job_dir("parts-1") / record.path
    path.write_bytes(path.read_bytes()[: len(path.read_bytes()) // 2])
    _, error = store.read_part("parts-1", record)
    assert error is not None and "size_mismatch" in error


def test_part_read_detects_same_size_hash_tampering(tmp_path: Path) -> None:
    from app.models import JobState, PartRecord, ShardState

    store = _job_store(tmp_path)
    job = JobState(
        job_id="parts-2",
        shard_ids=["a.log"],
        shards={"a.log": ShardState(shard_id="a.log", index=1)},
    )
    store.create_job(job)
    record = PartRecord(
        shard_id="a.log",
        domain="api.example.com",
        path="parts/a.log/y.json",
        sha256="",
        size_bytes=0,
        total_requests=1,
        status_5xx=0,
    )
    payload = {
        "schema_version": 1,
        "shard_id": "a.log",
        "domain": "api.example.com",
        "total_requests": 1,
        "status_5xx": 0,
        "malformed_lines": 0,
    }
    record = store.write_part("parts-2", record, payload)
    path = store.job_dir("parts-2") / record.path
    data = bytearray(path.read_bytes())
    # Flip a digit in "total_requests": 1 -> 7, same byte length
    index = data.index(b'"total_requests": 1')
    data[index + len(b'"total_requests": ')] = ord("7")
    path.write_bytes(bytes(data))
    loaded, error = store.read_part("parts-2", record)
    assert loaded is None
    assert error == "part_hash_mismatch"


def test_part_payload_consistency_checks() -> None:
    from app.models import PartRecord
    from app.storage import _validate_payload

    record = PartRecord(
        shard_id="a.log",
        domain="api.example.com",
        path="x.json",
        sha256="",
        size_bytes=1,
        total_requests=2,
        status_5xx=0,
    )
    payload = {
        "schema_version": 1,
        "shard_id": "a.log",
        "domain": "api.example.com",
        "total_requests": 3,
        "status_5xx": 0,
        "malformed_lines": 0,
    }
    assert "manifest_totals_mismatch" in _validate_payload(payload, record)


@pytest.mark.parametrize(
    "shard_id",
    ["../etc/passwd", "a/b.log", "", ".hidden", "x" * 129, "bad name.log"],
)
def test_shard_ids_cannot_escape_or_be_weird(shard_id: str) -> None:
    with pytest.raises(Exception):
        validate_shard_id(shard_id)


def test_source_path_is_confined(tmp_path: Path) -> None:
    from app.storage import InvalidName

    store = _job_store(tmp_path)
    (tmp_path / "outside.log").write_text("x", encoding="utf-8")
    for bad in ("..%2Foutside.log", "a/b"):
        with pytest.raises(InvalidName):
            store.source_path(bad)
    good = store.source_path("inside.log")
    assert good.parent == store.data_dir


def test_symlink_escape_is_rejected(tmp_path: Path) -> None:
    from app.storage import InvalidName

    store = _job_store(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("nope", encoding="utf-8")
    link = store.data_dir / "escape.log"
    link.symlink_to(secret)
    with pytest.raises(InvalidName):
        store.source_path("escape.log")


def test_job_id_validation(tmp_path: Path) -> None:
    from app.storage import InvalidName, validate_job_id

    assert validate_job_id("job-123") == "job-123"
    for bad in ("..", "a", "has space", "slash/name", ""):
        with pytest.raises(InvalidName):
            validate_job_id(bad)


def test_unreadable_job_state_raises(tmp_path: Path) -> None:
    from app.storage import JobNotFound, StorageError

    store = _job_store(tmp_path)
    with pytest.raises(JobNotFound):
        store.load_job("missing-job")
    path = store.job_dir("broken-job")
    path.mkdir(parents=True)
    (path / "job.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(StorageError):
        store.load_job("broken-job")


def test_source_sha256_matches_file(tmp_path: Path) -> None:
    store = _job_store(tmp_path)
    content = b"api.example.com 200\n"
    (store.data_dir / "a.log").write_bytes(content)
    assert store.source_sha256("a.log") == hashlib.sha256(content).hexdigest()


def test_client_rejects_duplicate_and_missing_shards(client: TestClient) -> None:
    duplicate = client.post("/jobs", json={"shard_ids": ["gw-a.log", "gw-a.log"]})
    assert duplicate.status_code == 422
    missing = client.post("/jobs", json={"shard_ids": ["gw-a.log", "nope.log"]})
    assert missing.status_code == 400
    bad_id = client.post(
        "/jobs", json={"shard_ids": ["gw-a.log"], "job_id": "no spaces allowed"}
    )
    assert bad_id.status_code == 422
