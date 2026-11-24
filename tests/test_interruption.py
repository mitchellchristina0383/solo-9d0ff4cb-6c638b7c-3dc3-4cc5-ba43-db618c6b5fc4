"""Interruption / crash / retry behaviour.

These tests simulate a process dying mid-run (no Python-level cleanup) by killing
a real subprocess with SIGKILL and by rewriting the persisted state to the shape a
crash leaves behind. The invariant under test: a retry must resume or re-read, it
must never double-count and it must never publish a half aggregate.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from app.engine import Engine, JobBusy
from app.models import RunRequest


def _strip_timestamps(body: dict) -> dict:
    """Results carry per-run ``updated_at``; compare only the counts."""

    clone = json.loads(json.dumps(body))
    for item in clone.get("results", []):
        item.pop("updated_at", None)
    return clone


def _write_script(tmp_path: Path, body: str) -> Path:
    script = tmp_path / "runner.py"
    script.write_text(textwrap.dedent(body), encoding="utf-8")
    return script


def test_state_left_processing_is_resumed_not_refused(client, make_job, env) -> None:
    settings, _, work = env
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    expected = client.get(f"/jobs/{job_id}/results").json()

    # Simulate a process killed after persisting the "processing" marker: the OS
    # released its flock, but job.json still says processing.
    state_path = work / "jobs" / job_id / "job.json"
    state = json.loads(state_path.read_text())
    state["status"] = "processing"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    response = client.post(f"/jobs/{job_id}/run", json={})
    assert response.status_code == 200, response.text
    body = response.json()
    assert any("interrupted" in warning for warning in body["run"]["warnings"])
    assert body["job"]["status"] == "completed"
    after = _strip_timestamps(client.get(f"/jobs/{job_id}/results").json())
    assert after["results"] == _strip_timestamps(expected)["results"]


def test_partial_scan_progress_is_not_recounted(client, make_job, env) -> None:
    """A crash after shard 1 was persisted must not make a retry recount it."""

    settings, _, work = env
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    state_path = work / "jobs" / job_id / "job.json"
    original = json.loads(state_path.read_text())

    # Rewind gw-c to "never scanned" and drop its parts, as if the process died
    # before reaching it. gw-a/gw-b keep their trusted snapshots.
    crashed = json.loads(json.dumps(original))
    crashed["status"] = "processing"
    shard_c = crashed["shards"]["gw-c.log"]
    shard_c.update(
        scan_status="pending",
        source_sha256=None,
        source_size=None,
        successful_reads=0,
        read_attempts=0,
        malformed_lines=0,
    )
    crashed["parts"]["gw-c.log"] = {}
    state_path.write_text(json.dumps(crashed), encoding="utf-8")
    for part in (work / "jobs" / job_id / "parts" / "gw-c.log").glob("*.json"):
        part.unlink()

    response = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert response["run"]["source_read_counts"] == {"gw-c.log": 1}
    # The resumed shard is a re-read, and its 1-based number is reported.
    assert response["run"]["reread_shards"] == ["gw-c.log"]
    assert response["run"]["reread_shard_numbers"] == [3]
    results = {
        item["domain"]: (item["total_requests"], item["status_5xx"])
        for item in client.get(f"/jobs/{job_id}/results").json()["results"]
    }
    assert results == {
        "api.example.com": (6, 3),
        "www.example.com": (4, 2),
        "cdn.example.net": (3, 1),
    }


def test_sigkill_mid_run_then_retry(env, tmp_path: Path) -> None:
    """Kill a real uvicorn-like engine process while it is running, then retry."""

    settings, data, work = env
    (data / "big.log").write_text(
        "".join(
            f"domain-{i % 7}.example.com {200 if i % 5 else 502}\n" for i in range(400_000)
        ),
        encoding="utf-8",
    )
    job_id = "kill-me-1"
    engine = Engine(settings)
    from app.models import JobCreateRequest

    engine.create_job(JobCreateRequest(shard_ids=["big.log"], job_id=job_id))

    script = _write_script(
        tmp_path,
        f"""
        import sys, time
        sys.path.insert(0, {str(Path.cwd())!r})
        from app.config import Settings
        from app.engine import Engine
        from app.models import RunRequest
        engine = Engine(Settings(data_dir={str(settings.data_dir)!r},
                                 work_dir={str(settings.work_dir)!r}))
        engine.run_job({job_id!r}, RunRequest())
        """,
    )
    env_vars = dict(os.environ, PYTHONPATH=str(Path.cwd()))
    process = subprocess.Popen(
        [sys.executable, "-I", str(script)],
        env=env_vars,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(Path.cwd()),
    )
    # Wait until the job is visibly processing, then kill without cleanup.
    deadline = time.time() + 20
    saw_processing = False
    while time.time() < deadline:
        try:
            state = json.loads((work / "jobs" / job_id / "job.json").read_text())
        except (OSError, json.JSONDecodeError):
            time.sleep(0.02)
            continue
        if state.get("status") == "processing":
            saw_processing = True
            break
        if process.poll() is not None:
            break
        time.sleep(0.02)
    process.send_signal(signal.SIGKILL)
    process.wait(timeout=10)
    assert saw_processing, "process finished before it could be interrupted"

    state = json.loads((work / "jobs" / job_id / "job.json").read_text())
    assert state["status"] == "processing"

    # The flock the dead child held is gone with it; the retry must proceed.
    retry = engine.run_job(job_id, RunRequest())
    assert retry.job.status.value == "completed"
    # Resumed work may be zero or one read, but never more than one per retry.
    assert retry.run.source_read_counts.get("big.log", 0) in (0, 1)
    results = engine.get_results(job_id).results
    assert len(results) == 7
    assert sum(item.total_requests for item in results) == 400_000
    assert sum(item.status_5xx for item in results) == 400_000 // 5

    # A further retry must not change anything.
    again = engine.run_job(job_id, RunRequest())
    assert again.run.source_read_counts == {}
    assert sum(item.total_requests for item in engine.get_results(job_id).results) == 400_000


def test_flock_blocks_a_second_engine_process(env, make_job, client, tmp_path) -> None:
    """The cross-process flock, not just the in-process lock, must guard a job."""

    settings, _, work = env
    job_id = make_job(["gw-a.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    lock_path = work / "jobs" / job_id / ".lock"
    assert lock_path.is_file()
    holder = subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(f"""
            import fcntl, sys, time
            fh = open({str(lock_path)!r}, "a+b")
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            print("held", flush=True)
            time.sleep(30)
            """)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        engine = Engine(settings)
        started = time.time()
        with pytest.raises(JobBusy) as excinfo:
            engine.run_job(job_id, RunRequest())
        assert "locked by another process" in str(excinfo.value)
        assert time.time() - started < 5  # non-blocking lock
    finally:
        holder.kill()
        holder.wait(timeout=10)


def test_rescan_shard_rebuilds_and_counts_once(client, make_job, env) -> None:
    settings, data, _ = env
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    before = client.get(f"/jobs/{job_id}/results").json()

    # Log rotation appends a new line to gw-a.
    path = data / "gw-a.log"
    path.write_text(path.read_text() + "api.example.com 503\n", encoding="utf-8")

    plain = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert plain["run"]["source_read_counts"] == {}
    assert _strip_timestamps(client.get(f"/jobs/{job_id}/results").json()) == _strip_timestamps(before)

    rescanned = client.post(
        f"/jobs/{job_id}/run", json={"rescan_shards": ["gw-a.log"]}
    ).json()
    assert rescanned["run"]["source_read_counts"] == {"gw-a.log": 1}
    after = {
        item["domain"]: (item["total_requests"], item["status_5xx"])
        for item in client.get(f"/jobs/{job_id}/results").json()["results"]
    }
    assert after["api.example.com"] == (6, 3)

    # Rescanning again is idempotent (replaces the part, never appends).
    client.post(f"/jobs/{job_id}/run", json={"rescan_shards": ["gw-a.log"]})
    again = {
        item["domain"]: (item["total_requests"], item["status_5xx"])
        for item in client.get(f"/jobs/{job_id}/results").json()["results"]
    }
    assert again == after


def test_rescan_unknown_shard_is_rejected(client, make_job) -> None:
    job_id = make_job(["gw-a.log"])
    response = client.post(f"/jobs/{job_id}/run", json={"rescan_shards": ["gw-zz.log"]})
    assert response.status_code == 400
    # The job is not wedged by the rejected request.
    assert client.post(f"/jobs/{job_id}/run", json={}).status_code == 200


def test_rescan_failure_marks_parts_stale_not_silent(client, make_job, env) -> None:
    settings, data, _ = env
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    (data / "gw-a.log").unlink()

    body = client.post(
        f"/jobs/{job_id}/run",
        json={
            "rescan_shards": ["gw-a.log"],
            "fault_injection": [{"shard_id": "gw-a.log", "mode": "source_unavailable"}],
        },
    ).json()
    assert body["job"]["shards"]["gw-a.log"]["stale_parts"] is True
    # With no verified rescan, no partition may be published from stale parts.
    results = client.get(f"/jobs/{job_id}/results").json()
    assert results["results"] == []
    assert results["job_status"] in {"blocked", "partial"}
