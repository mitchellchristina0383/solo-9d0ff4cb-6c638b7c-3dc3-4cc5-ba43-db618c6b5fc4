from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def test_health_and_sources(client) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "ok" and body["data_dir_exists"] and body["work_dir_writable"]

    sources = {item["shard_id"] for item in client.get("/sources").json()}
    assert {"gw-a.log", "gw-b.log", "gw-c.log"} <= sources


def test_openapi_schema_builds(client) -> None:
    schema = client.get("/openapi.json").json()
    for path in (
        "/jobs",
        "/jobs/{job_id}",
        "/jobs/{job_id}/run",
        "/jobs/{job_id}/results",
        "/sources",
        "/health",
    ):
        assert path in schema["paths"], path


def test_create_job_returns_fixed_shard_order(client) -> None:
    response = client.post(
        "/jobs", json={"shard_ids": ["gw-c.log", "gw-a.log"], "job_id": "ordered-1"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["shard_ids"] == ["gw-c.log", "gw-a.log"]
    assert body["shards"]["gw-c.log"]["index"] == 1
    assert body["shards"]["gw-a.log"]["index"] == 2
    assert body["status"] == "pending"


def test_duplicate_job_id_is_rejected(client) -> None:
    payload = {"shard_ids": ["gw-a.log"], "job_id": "twice-1"}
    assert client.post("/jobs", json=payload).status_code == 201
    assert client.post("/jobs", json=payload).status_code == 400


def test_unknown_job_is_404(client) -> None:
    assert client.get("/jobs/no-such-job").status_code == 404
    assert client.post("/jobs/no-such-job/run", json={}).status_code == 404
    assert client.get("/jobs/no-such-job/results").status_code == 404


def test_fault_validation_errors(client, make_job) -> None:
    job_id = make_job(["gw-a.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    # domain is required for part damage
    bad = client.post(
        f"/jobs/{job_id}/run",
        json={"fault_injection": [{"shard_id": "gw-a.log", "mode": "delete_part"}]},
    )
    assert bad.status_code == 422
    # unknown shard
    bad = client.post(
        f"/jobs/{job_id}/run",
        json={"fault_injection": [{"shard_id": "gw-x.log", "mode": "source_unavailable"}]},
    )
    assert bad.status_code == 400
    # unknown domain: allowed, reported as a no-op instead of failing the run
    noop = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-a.log", "mode": "delete_part", "domain": "absent.example"}
            ]
        },
    ).json()
    assert noop["run"]["faults"][0]["status"] == "noop"


def test_restore_requires_a_target(client) -> None:
    assert client.post("/jobs/whatever/sources/restore", json={}).status_code == 422


def test_simulation_endpoints_can_be_disabled(env) -> None:
    settings, _, _ = env
    disabled = settings.__class__(
        data_dir=settings.data_dir, work_dir=settings.work_dir, allow_simulation=False
    )
    with TestClient(create_app(disabled)) as locked:
        locked.post("/jobs", json={"shard_ids": ["gw-a.log"], "job_id": "locked-1"})
        response = locked.post(
            "/jobs/locked-1/sources/unavailable", json={"shard_ids": ["gw-a.log"]}
        )
        assert response.status_code == 403
        # run-level fault injection remains available for tests/automation
        run = locked.post(
            "/jobs/locked-1/run",
            json={"fault_injection": [{"shard_id": "gw-a.log", "mode": "source_unavailable"}]},
        )
        assert run.status_code == 200


def test_job_lock_rejects_concurrent_run(client, make_job) -> None:
    from app.engine import JobBusy
    from app.models import RunRequest

    job_id = make_job(["gw-a.log"])
    engine = client.app.state.engine
    with engine._job_lock(job_id):
        with pytest.raises(JobBusy):
            engine.run_job(job_id, RunRequest())
    # Lock released again after the with-block.
    assert client.post(f"/jobs/{job_id}/run", json={}).status_code == 200


def test_jobs_listing(client, make_job) -> None:
    make_job(["gw-a.log"], job_id="list-1")
    make_job(["gw-b.log"], job_id="list-2")
    client.post("/jobs/list-1/run", json={})
    listing = {item["job_id"]: item for item in client.get("/jobs").json()}
    assert listing["list-1"]["status"] == "completed"
    assert listing["list-1"]["published_partitions"] >= 1
    assert listing["list-2"]["status"] == "pending"


def test_results_are_stable_and_sorted(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    first = client.get(f"/jobs/{job_id}/results").json()
    second = client.get(f"/jobs/{job_id}/results").json()
    assert first == second
    domains = [item["domain"] for item in first["results"]]
    assert domains == sorted(domains)


def test_truncation_to_zero_bytes_is_detected(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    body = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {
                    "shard_id": "gw-a.log",
                    "mode": "truncate_part",
                    "domain": "api.example.com",
                    "truncate_bytes": 0,
                }
            ]
        },
    ).json()
    assert body["run"]["repaired_shards"] == ["gw-a.log"]


def test_run_report_is_persisted_with_job(client, make_job) -> None:
    job_id = make_job(["gw-a.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    job = client.get(f"/jobs/{job_id}").json()
    assert job["last_run"]["finished_at"] is not None
    assert job["last_run"]["source_read_counts"] == {"gw-a.log": 1}
    assert job["shards"]["gw-a.log"]["successful_reads"] == 1


def test_work_dir_survives_state_reload_with_parts(client, make_job, env) -> None:
    settings, _, work = env
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    manifest = json.loads((work / "jobs" / job_id / "job.json").read_text())
    assert manifest["schema_version"] == 1
    assert set(manifest["parts"]) == {"gw-a.log", "gw-b.log"}
    for shard in ("gw-a.log", "gw-b.log"):
        for record in manifest["parts"][shard].values():
            assert (work / "jobs" / job_id / record["path"]).is_file()
