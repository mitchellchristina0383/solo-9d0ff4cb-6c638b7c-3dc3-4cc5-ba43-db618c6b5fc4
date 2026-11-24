from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tests.conftest import EXPECTED_TOTALS


def _results(client: TestClient, job_id: str) -> dict:
    response = client.get(f"/jobs/{job_id}/results")
    assert response.status_code == 200, response.text
    return response.json()


def _published(body: dict) -> dict[str, tuple[int, int]]:
    return {
        item["domain"]: (item["total_requests"], item["status_5xx"])
        for item in body["results"]
    }


def test_happy_path_counts_each_source_once(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    response = client.post(f"/jobs/{job_id}/run", json={})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["run"]["reread_shards"] == []
    assert body["run"]["source_read_counts"] == {"gw-a.log": 1, "gw-b.log": 1, "gw-c.log": 1}
    assert body["job"]["status"] == "completed"
    assert _published(_results(client, job_id)) == EXPECTED_TOTALS
    assert _results(client, job_id)["blocked"] == []


def test_rerun_without_damage_does_not_reread_sources(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    again = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert again["run"]["source_read_counts"] == {}
    assert again["run"]["reread_shards"] == []
    assert _published(_results(client, job_id)) == {
        "api.example.com": (5, 2),
        "www.example.com": (3, 2),
        "cdn.example.net": (2, 0),
    }


def test_malformed_lines_reported_but_not_counted(client, make_job) -> None:
    job_id = make_job(["gw-c.log"])
    body = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert any("malformed" in warning for warning in body["run"]["warnings"])
    assert body["job"]["shards"]["gw-c.log"]["malformed_lines"] == 1
    assert _published(_results(client, job_id)) == {
        "api.example.com": (1, 1),
        "www.example.com": (1, 0),
        "cdn.example.net": (1, 1),
    }


@pytest.mark.parametrize(
    ("mode", "domain", "shard"),
    [
        ("delete_part", "api.example.com", "gw-b.log"),
        ("truncate_part", "api.example.com", "gw-b.log"),
        ("delete_part", "www.example.com", "gw-a.log"),
        ("truncate_part", "cdn.example.net", "gw-c.log"),
    ],
)
def test_damaged_part_is_repaired_by_rereading_only_its_source(
    client, make_job, mode, domain, shard
) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    baseline = _published(_results(client, job_id))
    assert baseline == EXPECTED_TOTALS

    damaged_run = client.post(
        f"/jobs/{job_id}/run",
        json={"fault_injection": [{"shard_id": shard, "mode": mode, "domain": domain}]},
    ).json()
    fault = damaged_run["run"]["faults"][0]
    assert fault["status"] == "applied"
    # The damaged source is re-read in the same run that finds it, and only it.
    assert damaged_run["run"]["reread_shards"] == [shard]
    assert damaged_run["run"]["source_read_counts"] == {shard: 1}
    assert damaged_run["run"]["repaired_shards"] == [shard]
    assert _published(_results(client, job_id)) == baseline

    # A follow-up run finds everything intact and touches no source.
    follow_up = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert follow_up["run"]["reread_shards"] == []
    assert follow_up["run"]["source_read_counts"] == {}
    assert _published(_results(client, job_id)) == baseline


def test_fault_on_one_part_leaves_other_partitions_published(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    body = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-a.log", "mode": "truncate_part", "domain": "api.example.com"}
            ]
        },
    ).json()
    assert body["run"]["reread_shards"] == ["gw-a.log"]
    assert _published(_results(client, job_id)) == {
        "api.example.com": (5, 2),
        "www.example.com": (3, 2),
        "cdn.example.net": (2, 0),
    }


def test_damaged_part_with_unavailable_source_withholds_partition(
    client, make_job
) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    body = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-b.log", "mode": "delete_part", "domain": "api.example.com"},
                {"shard_id": "gw-b.log", "mode": "source_unavailable"},
            ]
        },
    ).json()
    run = body["run"]
    assert run["reread_shards"] == ["gw-b.log"]
    assert run["failed_repairs"] == ["gw-b.log"]
    # gw-b contributes to every domain, so nothing can be published yet.
    assert body["job"]["status"] == "blocked"

    results = _results(client, job_id)
    blocked_domains = {item["domain"] for item in results["blocked"]}
    # gw-b contributes to api, www and cdn; all must be withheld. No half sums.
    assert blocked_domains == {"api.example.com", "www.example.com", "cdn.example.net"}
    assert results["results"] == []
    for item in results["blocked"]:
        assert item["blocked_shards"] == ["gw-b.log"]

    # Restore and retry: the previously failed shard is scanned and totals match.
    restored = client.post(f"/jobs/{job_id}/sources/restore", json={"all": True})
    assert restored.status_code == 200
    retry = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert retry["run"]["source_read_counts"] == {"gw-b.log": 1}
    assert retry["job"]["status"] == "completed"
    assert _published(_results(client, job_id)) == EXPECTED_TOTALS


def test_unavailable_source_withholds_only_partitions_it_could_affect(
    client, make_job, env
) -> None:
    settings, data, _ = env
    (data / "gw-d.log").write_text("solo.example.com 200\nsolo.example.com 503\n", encoding="utf-8")
    job_id = make_job(["gw-a.log", "gw-d.log"])
    client.post(f"/jobs/{job_id}/run", json={})

    # Damage one of gw-a's parts and take gw-a offline. Repair must fail, but
    # gw-d's partitions (solo.example.com) are not affected and stay queryable.
    out = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-a.log", "mode": "delete_part", "domain": "api.example.com"},
                {"shard_id": "gw-a.log", "mode": "source_unavailable"},
            ]
        },
    ).json()
    assert out["job"]["status"] == "partial"
    assert out["run"]["failed_repairs"] == ["gw-a.log"]
    results = _results(client, job_id)
    published = {item["domain"]: (item["total_requests"], item["status_5xx"]) for item in results["results"]}
    assert published == {"solo.example.com": (2, 1)}
    assert {item["domain"] for item in results["blocked"]} == {
        "api.example.com",
        "www.example.com",
        "cdn.example.net",
    }
    assert results["unscanned_shards"] == ["gw-a.log"]

    client.post(f"/jobs/{job_id}/sources/restore", json={"all": True})
    retry = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert retry["run"]["source_read_counts"] == {"gw-a.log": 1}
    published = {item["domain"]: (item["total_requests"], item["status_5xx"]) for item in _results(client, job_id)["results"]}
    assert published == {
        "api.example.com": (3, 2),
        "www.example.com": (2, 1),
        "cdn.example.net": (1, 0),
        "solo.example.com": (2, 1),
    }


def test_source_unavailable_from_job_creation_is_retried_after_restore(
    client, make_job
) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/sources/unavailable", json={"shard_ids": ["gw-a.log"]})
    body = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert body["run"]["source_read_counts"] == {"gw-b.log": 1}
    assert body["job"]["status"] == "blocked"
    results = _results(client, job_id)
    assert results["results"] == []
    assert results["unscanned_shards"] == ["gw-a.log"]

    # Repeated retries while still broken must not publish anything, ever.
    for _ in range(3):
        again = client.post(f"/jobs/{job_id}/run", json={}).json()
        assert again["job"]["status"] == "blocked"
        assert _results(client, job_id)["results"] == []

    client.post(f"/jobs/{job_id}/sources/restore", json={"all": True})
    final = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert final["job"]["status"] == "completed"
    assert _published(_results(client, job_id)) == {
        "api.example.com": (5, 2),
        "www.example.com": (3, 2),
        "cdn.example.net": (2, 0),
    }


def test_repair_is_idempotent_across_repeated_runs(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    client.post(
        f"/jobs/{job_id}/run",
        json={"fault_injection": [{"shard_id": "gw-a.log", "mode": "delete_part", "domain": "www.example.com"}]},
    )
    snapshots = set()
    for _ in range(4):
        client.post(f"/jobs/{job_id}/run", json={})
        snapshots.add(json.dumps(_published(_results(client, job_id)), sort_keys=True))
    assert snapshots == {json.dumps(EXPECTED_TOTALS, sort_keys=True)}


def test_two_damaged_shards_are_both_reread(client, make_job) -> None:
    job_id = make_job(["gw-a.log", "gw-b.log", "gw-c.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    body = client.post(
        f"/jobs/{job_id}/run",
        json={
            "fault_injection": [
                {"shard_id": "gw-c.log", "mode": "truncate_part", "domain": "cdn.example.net"},
                {"shard_id": "gw-a.log", "mode": "delete_part", "domain": "api.example.com"},
            ]
        },
    ).json()
    assert body["run"]["reread_shards"] == ["gw-a.log", "gw-c.log"]
    assert body["run"]["reread_shard_numbers"] == [1, 3]
    assert body["run"]["source_read_counts"] == {"gw-a.log": 1, "gw-c.log": 1}
    assert _published(_results(client, job_id)) == EXPECTED_TOTALS


def test_external_part_deletion_is_detected_and_repaired(client, make_job, env) -> None:
    settings, _, work = env
    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    parts = sorted((work / "jobs" / job_id / "parts" / "gw-a.log").glob("*.json"))
    assert parts
    parts[0].unlink()  # simulate a temp file lost between runs
    body = client.post(f"/jobs/{job_id}/run", json={}).json()
    assert body["run"]["reread_shards"] == ["gw-a.log"]
    assert body["run"]["repaired_shards"] == ["gw-a.log"]
    assert _published(_results(client, job_id)) == {
        "api.example.com": (5, 2),
        "www.example.com": (3, 2),
        "cdn.example.net": (2, 0),
    }


def test_reread_numbers_follow_the_fixed_shard_order(client, make_job) -> None:
    job_id = make_job(["gw-c.log", "gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    body = client.post(
        f"/jobs/{job_id}/run",
        json={"fault_injection": [{"shard_id": "gw-a.log", "mode": "delete_part", "domain": "api.example.com"}]},
    ).json()
    assert body["run"]["reread_shards"] == ["gw-a.log"]
    assert body["run"]["reread_shard_numbers"] == [2]
    results = _results(client, job_id)
    assert results["reread_shard_numbers"] == [2]


def test_job_survives_new_client_constructed_on_same_work_dir(env, make_job, client) -> None:
    from app.main import create_app

    job_id = make_job(["gw-a.log", "gw-b.log"])
    client.post(f"/jobs/{job_id}/run", json={})
    settings, _, _ = env
    with TestClient(create_app(settings)) as second:
        assert _published(_results(second, job_id)) == {
            "api.example.com": (5, 2),
            "www.example.com": (3, 2),
            "cdn.example.net": (2, 0),
        }
