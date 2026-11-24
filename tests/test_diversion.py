"""Diversion trial API: capacity accounting, max-flow plan, freshness guard."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.flow import max_split_flow


def _make_job(client: TestClient, shard_ids: list[str], job_id: str = "divert-1") -> str:
    response = client.post("/jobs", json={"shard_ids": shard_ids, "job_id": job_id})
    assert response.status_code == 201, response.text
    return job_id


def _run(client: TestClient, job_id: str, payload: dict | None = None) -> dict:
    response = client.post(f"/jobs/{job_id}/run", json=payload or {})
    assert response.status_code == 200, response.text
    return response.json()


def _published(client: TestClient, job_id: str) -> dict[str, dict[str, int]]:
    body = client.get(f"/jobs/{job_id}/results").json()
    return {
        item["domain"]: {
            shard_id: contribution["total_requests"]
            for shard_id, contribution in item["contributions"].items()
        }
        for item in body["results"]
    }


def _trial(client: TestClient, job_id: str, payload: dict) -> dict:
    response = client.post(f"/jobs/{job_id}/diversion-trials", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def _targets_for(published: dict[str, dict[str, int]], offline: str, gateways: list[str]):
    return {
        domain: gateways
        for domain, contributions in published.items()
        if contributions.get(offline, 0) > 0
    }


# ------------------------------------------------------------------ unit flow
def test_max_split_flow_competes_for_shared_capacity() -> None:
    # a and b each want 10; only one gateway with 12 free -> total 12.
    result = max_split_flow(
        {"a.example": 10, "b.example": 10},
        {"a.example": ["gw-1"], "b.example": ["gw-1"]},
        {"gw-1": 12},
    )
    assert result.total == 12
    assert sum(result.assignments["a.example"].values()) + sum(
        result.assignments["b.example"].values()
    ) == 12


def test_max_split_flow_respects_per_domain_targets() -> None:
    result = max_split_flow(
        {"a.example": 5, "b.example": 5},
        {"a.example": ["gw-1"], "b.example": ["gw-2"]},
        {"gw-1": 3, "gw-2": 100},
    )
    assert result.total == 8
    assert result.assignments == {"a.example": {"gw-1": 3}, "b.example": {"gw-2": 5}}


def test_max_split_flow_splits_one_domain_across_gateways() -> None:
    result = max_split_flow(
        {"a.example": 10},
        {"a.example": ["gw-1", "gw-2"]},
        {"gw-1": 4, "gw-2": 6},
    )
    assert result.total == 10
    assert result.assignments["a.example"] == {"gw-1": 4, "gw-2": 6}


def test_max_split_flow_is_deterministic() -> None:
    kwargs = dict(
        request_by_domain={"b.example": 7, "a.example": 7},
        allowed_targets={"a.example": ["gw-1", "gw-2"], "b.example": ["gw-1", "gw-2"]},
        gateway_capacity={"gw-1": 8, "gw-2": 8},
    )
    first = max_split_flow(**kwargs)
    second = max_split_flow(**kwargs)
    assert first == second


# --------------------------------------------------------------- happy path
def test_full_diversion_plan_and_confirm(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"])
    _run(client, job_id)
    published = _published(client, job_id)
    # Offline gw-c contributes api 1, www 1, cdn 1 (conftest shard C).
    request = {
        "offline_shard_id": "gw-c.log",
        "gateway_request_limits": {"gw-a.log": 100, "gw-b.log": 100},
        "domain_allowed_targets": _targets_for(published, "gw-c.log", ["gw-a.log", "gw-b.log"]),
    }
    trial = _trial(client, job_id, request)
    assert trial["summary_complete"] is True
    assert trial["missing"] == []
    assert trial["plan_status"] == "full"
    assert trial["fully_divertible"] is True
    assert trial["total_requests_to_move"] == 3
    assert trial["total_migrated_requests"] == 3
    assert trial["total_unmigrated_requests"] == 0

    plans = {plan["domain"]: plan for plan in trial["domains"]}
    assert plans["api.example.com"]["requests_to_move"] == 1
    assert plans["api.example.com"]["migrated_requests"] == 1
    assert plans["www.example.com"]["requests_to_move"] == 1
    assert plans["cdn.example.net"]["requests_to_move"] == 1
    for plan in plans.values():
        assert plan["unmigrated_requests"] == 0
        assert sum(a["requests"] for a in plan["assignments"]) == plan["requests_to_move"]
        for assignment in plan["assignments"]:
            assert assignment["shard_id"] in {"gw-a.log", "gw-b.log"}

    capacity = {row["shard_id"]: row for row in trial["gateway_capacity"]}
    # gw-a serves api 3, www 2 and cdn 1 in the fixture (6 total).
    assert capacity["gw-a.log"]["existing_requests"] == 6
    assert capacity["gw-a.log"]["remaining_capacity"] == 94
    assert capacity["gw-a.log"]["final_requests"] <= 100

    confirmed = client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={})
    assert confirmed.status_code == 200, confirmed.text
    now = confirmed.json()
    assert now["confirmed_at"] is not None
    # Confirming twice is idempotent and keeps the original timestamp.
    again = client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={})
    assert again.status_code == 200
    assert again.json()["confirmed_at"] == now["confirmed_at"]


def test_limits_deduct_all_existing_requests_not_only_offline_domain(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"])
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-c.log",
            "gateway_request_limits": {"gw-a.log": 7, "gw-b.log": 5},
            "domain_allowed_targets": _targets_for(
                published, "gw-c.log", ["gw-a.log", "gw-b.log"]
            ),
        },
    )
    capacity = {row["shard_id"]: row for row in trial["gateway_capacity"]}
    # gw-a already serves 6 published requests; limit 7 leaves exactly 1.
    assert capacity["gw-a.log"]["existing_requests"] == 6
    assert capacity["gw-a.log"]["remaining_capacity"] == 1
    # gw-b already serves 4 published requests; limit 5 leaves 1.
    assert capacity["gw-b.log"]["existing_requests"] == 4
    assert capacity["gw-b.log"]["remaining_capacity"] == 1
    # Only 2 of gw-c's 3 requests fit into the shared remaining capacity.
    assert trial["total_migrated_requests"] == 2
    assert trial["total_unmigrated_requests"] == 1
    assert trial["plan_status"] == "partial"
    assert trial["fully_divertible"] is False


def test_over_limit_gateway_is_treated_as_zero_capacity(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"])
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-c.log",
            "gateway_request_limits": {"gw-a.log": 0, "gw-b.log": 100},
            "domain_allowed_targets": _targets_for(
                published, "gw-c.log", ["gw-a.log", "gw-b.log"]
            ),
        },
    )
    capacity = {row["shard_id"]: row for row in trial["gateway_capacity"]}
    assert capacity["gw-a.log"]["over_limit"] is True
    assert capacity["gw-a.log"]["remaining_capacity"] == 0
    assert any("over" in warning or "above its limit" in warning for warning in trial["warnings"])
    # All three requests move to gw-b.
    assert trial["total_migrated_requests"] == 3
    assert trial["domains"][0]["assignments"][0]["shard_id"] == "gw-b.log"


def test_domain_with_no_allowed_target_is_reported_unmigrated(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"])
    _run(client, job_id)
    published = _published(client, job_id)
    targets = _targets_for(published, "gw-c.log", ["gw-a.log", "gw-b.log"])
    targets["cdn.example.net"] = []
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-c.log",
            "gateway_request_limits": {"gw-a.log": 100, "gw-b.log": 100},
            "domain_allowed_targets": targets,
        },
    )
    cdn = next(plan for plan in trial["domains"] if plan["domain"] == "cdn.example.net")
    assert cdn["migrated_requests"] == 0
    assert cdn["unmigrated_requests"] == 1
    assert trial["total_unmigrated_requests"] == 1
    assert trial["plan_status"] == "partial"


def test_multiple_domains_compete_for_shared_gateway_capacity(client) -> None:
    """The brief's core case: several domains, one pool of remaining capacity."""

    data_settings = client.app.state.engine.settings
    (data_settings.data_dir / "gw-d.log").write_text(
        "\n".join(
            ["alpha.example.com 200"] * 5
            + ["beta.example.com 200"] * 5
            + ["gamma.example.com 200"] * 5
        )
        + "\n",
        encoding="utf-8",
    )
    (data_settings.data_dir / "gw-e.log").write_text("alpha.example.com 200\n", encoding="utf-8")
    job_id = _make_job(client, ["gw-d.log", "gw-e.log"], job_id="contend-1")
    _run(client, job_id)
    published = _published(client, job_id)
    assert published["alpha.example.com"] == {"gw-d.log": 5, "gw-e.log": 1}

    targets = _targets_for(published, "gw-d.log", ["gw-e.log"])
    # gw-e already serves alpha 1; limit 9 leaves 8. The three domains need 15.
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-d.log",
            "gateway_request_limits": {"gw-e.log": 9},
            "domain_allowed_targets": targets,
        },
    )
    assert trial["total_requests_to_move"] == 15
    assert trial["total_migrated_requests"] == 8
    assert trial["total_unmigrated_requests"] == 7
    # Every domain is split across the *same* single gateway without overlap.
    assert sum(plan["migrated_requests"] for plan in trial["domains"]) == 8
    assert sum(
        assignment["requests"]
        for plan in trial["domains"]
        for assignment in plan["assignments"]
    ) == 8
    capacity = trial["gateway_capacity"][0]
    assert capacity["existing_requests"] == 1
    assert capacity["remaining_capacity"] == 8
    assert capacity["assigned_requests"] == 8
    assert capacity["final_requests"] == 9


def test_multiple_domains_split_across_multiple_gateways(client) -> None:
    settings = client.app.state.engine.settings
    (settings.data_dir / "gw-x.log").write_text(
        "one.example 200\none.example 200\ntwo.example 200\ntwo.example 200\n", encoding="utf-8"
    )
    (settings.data_dir / "gw-y.log").write_text("one.example 200\n", encoding="utf-8")
    (settings.data_dir / "gw-z.log").write_text("two.example 200\n", encoding="utf-8")
    job_id = _make_job(client, ["gw-x.log", "gw-y.log", "gw-z.log"], job_id="split-1")
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-x.log",
            "gateway_request_limits": {"gw-y.log": 2, "gw-z.log": 2},
            "domain_allowed_targets": _targets_for(
                published, "gw-x.log", ["gw-y.log", "gw-z.log"]
            ),
        },
    )
    assert trial["total_requests_to_move"] == 4
    # gw-y: existing 1, limit 2 -> 1 free; gw-z: existing 1, limit 2 -> 1 free.
    assert trial["total_migrated_requests"] == 2
    capacity = {row["shard_id"]: row for row in trial["gateway_capacity"]}
    assert capacity["gw-y.log"]["remaining_capacity"] == 1
    assert capacity["gw-z.log"]["remaining_capacity"] == 1
    assert trial["total_unmigrated_requests"] == 2


# ------------------------------------------------------------ incomplete run
def test_incomplete_summary_returns_only_missing_items(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"])
    _run(client, job_id)
    # Take gw-b offline and damage a part so its partitions are withheld.
    out = _run(
        client,
        job_id,
        {
            "fault_injection": [
                {"shard_id": "gw-b.log", "mode": "delete_part", "domain": "api.example.com"},
                {"shard_id": "gw-b.log", "mode": "source_unavailable"},
            ]
        },
    )
    assert out["job"]["status"] == "blocked"

    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-c.log",
            "gateway_request_limits": {"gw-a.log": 100, "gw-b.log": 100},
            "domain_allowed_targets": {},
        },
    )
    assert trial["summary_complete"] is False
    assert trial["plan_status"] == "incomplete"
    assert trial["fully_divertible"] is None
    assert trial["domains"] == []
    assert trial["gateway_capacity"] == []
    assert trial["total_migrated_requests"] is None
    assert trial["missing"]
    kinds = {item["kind"] for item in trial["missing"]}
    # gw-b has a verified snapshot but its parts are damaged, so its partitions
    # are the missing items (each names the responsible shard).
    assert kinds == {"blocked_domain"}
    blocked = {item["domain"] for item in trial["missing"] if item["kind"] == "blocked_domain"}
    assert blocked == {"api.example.com", "www.example.com", "cdn.example.net"}
    for item in trial["missing"]:
        assert item["shard_ids"] == ["gw-b.log"]

    # An incomplete trial can never be confirmed, even after the job recovers.
    response = client.post(
        f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={}
    )
    assert response.status_code == 400
    assert response.json()["code"] == "trial_incomplete"


def test_trial_before_any_run_lists_no_published_results(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="never-run-1")
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": {},
        },
    )
    assert trial["summary_complete"] is False
    kinds = {item["kind"] for item in trial["missing"]}
    assert {"unscanned_shard", "no_published_results"} <= kinds
    assert trial["plan_status"] == "incomplete"


def test_trial_during_rescan_of_offline_job_is_allowed(client, env) -> None:
    """A trial is calculation-only: the original job can still be rescanned.

    We hold the job's run lock to emulate an in-flight scan, and verify that
    creating a trial still works (it never takes the run lock).
    """

    settings, data, _ = env
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="concurrent-1")
    _run(client, job_id)
    published = _published(client, job_id)
    engine = client.app.state.engine
    with engine._job_lock(job_id):
        response = client.post(
            f"/jobs/{job_id}/diversion-trials",
            json={
                "offline_shard_id": "gw-b.log",
                "gateway_request_limits": {"gw-a.log": 100},
                "domain_allowed_targets": _targets_for(published, "gw-b.log", ["gw-a.log"]),
            },
        )
    assert response.status_code == 201, response.text
    assert response.json()["summary_complete"] is True
    # Confirmation, on the other hand, is serialized behind the run lock.
    trial_id = response.json()["trial_id"]
    with engine._job_lock(job_id):
        blocked = client.post(f"/jobs/{job_id}/diversion-trials/{trial_id}/confirm", json={})
    assert blocked.status_code == 409
    # ...and a busy job is not misreported as a stale trial.
    assert blocked.json()["code"] == "job_busy"
    assert blocked.json()["recalculate"] is False


# ------------------------------------------------------- stale trial guard
def test_confirm_rejects_stale_trial_after_rescan(client, env) -> None:
    settings, data, _ = env
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="stale-1")
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 1000},
            "domain_allowed_targets": _targets_for(published, "gw-b.log", ["gw-a.log"]),
        },
    )
    assert trial["fully_divertible"] is True

    # Log rotation: gw-a (the target) changes, so at least one assignment is stale.
    path = data / "gw-a.log"
    path.write_text(path.read_text() + "api.example.com 200\n", encoding="utf-8")
    _run(client, job_id, {"rescan_shards": ["gw-a.log"]})

    response = client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={})
    assert response.status_code == 409, response.text
    body = response.json()
    assert body["code"] == "results_changed"
    assert body["recalculate"] is True
    assert "recalculate" in body["detail"]

    # A fresh trial on the new snapshot confirms.
    published = _published(client, job_id)
    fresh = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 1000},
            "domain_allowed_targets": _targets_for(published, "gw-b.log", ["gw-a.log"]),
        },
    )
    assert fresh["results_fingerprint"] != trial["results_fingerprint"]
    confirmed = client.post(f"/jobs/{job_id}/diversion-trials/{fresh['trial_id']}/confirm", json={})
    assert confirmed.status_code == 200, confirmed.text


def test_confirm_rejects_when_summary_becomes_incomplete(client, env) -> None:
    settings, data, _ = env
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="stale-2")
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 1000},
            "domain_allowed_targets": _targets_for(published, "gw-b.log", ["gw-a.log"]),
        },
    )
    _run(
        client,
        job_id,
        {
            "fault_injection": [
                {"shard_id": "gw-a.log", "mode": "delete_part", "domain": "api.example.com"},
                {"shard_id": "gw-a.log", "mode": "source_unavailable"},
            ]
        },
    )
    response = client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={})
    assert response.status_code == 409
    assert response.json()["code"] == "summary_incomplete_now"
    assert response.json()["recalculate"] is True


def test_confirm_rejects_partial_trial_even_if_job_is_complete(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log", "gw-c.log"], job_id="partial-1")
    _run(client, job_id)
    published = _published(client, job_id)
    assert published
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-c.log",
            "gateway_request_limits": {"gw-a.log": 100, "gw-b.log": 100},
            "domain_allowed_targets": {
                "api.example.com": ["gw-a.log"],
                "www.example.com": [],
                "cdn.example.net": [],
            },
        },
    )
    # www and cdn have no allowed target -> not fully divertible.
    assert trial["fully_divertible"] is False
    response = client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={})
    assert response.status_code == 400
    assert response.json()["code"] == "trial_not_fully_divertible"


# --------------------------------------------------------------- validation
@pytest.mark.parametrize(
    ("offline", "limits", "fragment"),
    [
        ("gw-x.log", {"gw-a.log": 10}, "not a shard"),
        ("gw-b.log", {"gw-a.log": 10, "gw-b.log": 10}, "not remaining"),
        ("gw-b.log", {"gw-a.log": 10, "gw-c.log": 10}, "not remaining"),
        ("gw-b.log", {"gw-a.log": -1}, "must be an integer >= 0"),
    ],
)
def test_invalid_trial_requests_are_rejected(client, offline, limits, fragment) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="invalid-1")
    _run(client, job_id)
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": offline,
            "gateway_request_limits": limits,
            "domain_allowed_targets": {},
        },
    )
    expected = 422 if fragment.startswith("must be") else 400
    assert response.status_code == expected, response.text
    detail = response.json()["detail"]
    if isinstance(detail, list):  # pydantic model validation error
        detail = detail[0]["msg"]
    assert fragment in detail


def test_allowed_targets_must_match_offline_domains(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="targets-1")
    _run(client, job_id)
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": {},
        },
    )
    assert response.status_code == 400
    assert "missing allowed targets" in response.json()["detail"]

    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": {"api.example.com": ["gw-a.log"]},
        },
    )
    assert response.status_code == 400
    assert "missing allowed targets" in response.json()["detail"]


def test_offline_gateway_cannot_be_its_own_target(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="targets-2")
    _run(client, job_id)
    published = _published(client, job_id)
    targets = _targets_for(published, "gw-b.log", ["gw-a.log"])
    targets["api.example.com"] = ["gw-b.log"]
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": targets,
        },
    )
    assert response.status_code == 400
    assert "offline gateway" in response.json()["detail"]


def test_offline_gateway_with_no_requests_is_a_full_empty_plan(client) -> None:
    settings = client.app.state.engine.settings
    # A successfully scanned shard that serves no domain counts zero everywhere.
    (settings.data_dir / "gw-idle.log").write_text("# no traffic\n", encoding="utf-8")
    job_id = _make_job(client, ["gw-a.log", "gw-idle.log"], job_id="empty-offline-1")
    _run(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-idle.log",
            "gateway_request_limits": {"gw-a.log": 6},
            "domain_allowed_targets": {},
        },
    )
    assert trial["summary_complete"] is True
    assert trial["fully_divertible"] is True
    assert trial["plan_status"] == "full"
    assert trial["total_requests_to_move"] == 0
    assert trial["domains"] == []
    assert any("no published requests" in warning for warning in trial["warnings"])
    assert (
        client.post(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}/confirm", json={}).status_code
        == 200
    )


def test_allowed_target_must_be_a_remaining_gateway(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="targets-3")
    _run(client, job_id)
    published = _published(client, job_id)
    targets = _targets_for(published, "gw-b.log", ["gw-a.log"])
    targets["api.example.com"] = ["gw-zz.log"]
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": targets,
        },
    )
    assert response.status_code == 400
    assert "not a remaining gateway" in response.json()["detail"]


def test_domain_keys_are_normalized_and_duplicates_rejected(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="targets-4")
    _run(client, job_id)
    published = _published(client, job_id)
    targets = _targets_for(published, "gw-b.log", ["gw-a.log"])
    upper = {domain.upper(): gateways for domain, gateways in targets.items()}
    duplicate = dict(targets)
    duplicate["API.Example.COM"] = ["gw-a.log"]  # same domain after normalization
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": duplicate,
        },
    )
    assert response.status_code == 400
    assert "more than once after normalization" in response.json()["detail"]

    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": upper,
        },
    )
    assert response.status_code == 201, response.text
    assert set(response.json()["domain_allowed_targets"]) == set(targets)


def test_duplicate_target_is_rejected(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="targets-5")
    _run(client, job_id)
    published = _published(client, job_id)
    targets = _targets_for(published, "gw-b.log", ["gw-a.log", "gw-a.log"])
    response = client.post(
        f"/jobs/{job_id}/diversion-trials",
        json={
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 10},
            "domain_allowed_targets": targets,
        },
    )
    assert response.status_code == 422


def test_unknown_job_and_unknown_trial_are_404(client) -> None:
    assert client.get("/jobs/no-such-job/diversion-trials").status_code == 404
    assert (
        client.post(
            "/jobs/no-such-job/diversion-trials",
            json={
                "offline_shard_id": "x.log",
                "gateway_request_limits": {},
                "domain_allowed_targets": {},
            },
        ).status_code
        == 404
    )
    job_id = _make_job(client, ["gw-a.log"], job_id="unknown-trial-1")
    assert (
        client.get(f"/jobs/{job_id}/diversion-trials/trial-0123456789ab").status_code == 404
    )
    assert (
        client.get(f"/jobs/{job_id}/diversion-trials/not-a-trial").status_code == 422
    )


def test_trial_is_persisted_and_listed(client) -> None:
    job_id = _make_job(client, ["gw-a.log", "gw-b.log"], job_id="persist-1")
    _run(client, job_id)
    published = _published(client, job_id)
    trial = _trial(
        client,
        job_id,
        {
            "offline_shard_id": "gw-b.log",
            "gateway_request_limits": {"gw-a.log": 50},
            "domain_allowed_targets": _targets_for(published, "gw-b.log", ["gw-a.log"]),
        },
    )
    listing = client.get(f"/jobs/{job_id}/diversion-trials").json()
    assert [item["trial_id"] for item in listing] == [trial["trial_id"]]
    assert listing[0]["plan_status"] == trial["plan_status"]
    fetched = client.get(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}").json()
    assert fetched == trial
    # It survives a new app instance on the same work dir.
    from app.main import create_app

    with TestClient(create_app(client.app.state.engine.settings)) as second:
        again = second.get(f"/jobs/{job_id}/diversion-trials/{trial['trial_id']}").json()
        assert again["trial_id"] == trial["trial_id"]


def test_openapi_exposes_diversion_endpoints(client) -> None:
    schema = client.get("/openapi.json").json()
    assert "/jobs/{job_id}/diversion-trials" in schema["paths"]
    assert "/jobs/{job_id}/diversion-trials/{trial_id}" in schema["paths"]
    assert "/jobs/{job_id}/diversion-trials/{trial_id}/confirm" in schema["paths"]
