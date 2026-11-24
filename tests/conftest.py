"""Shared fixtures: isolated data/work dirs and deterministic source shards."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


SHARD_A = """\
api.example.com 200
api.example.com 500
www.example.com 404
2026-11-22T10:00:00Z api.example.com 503
cdn.example.net,201
{"domain": "www.example.com", "status": 502}
"""

SHARD_B = """\
api.example.com 200
api.example.com 200
www.example.com 500
{"host": "cdn.example.net", "status_code": 200}
"""

SHARD_C = """\
api.example.com 500
www.example.com 301
cdn.example.net 503
this is not a log line
"""


@pytest.fixture()
def env(tmp_path: Path) -> tuple[Settings, Path, Path]:
    data = tmp_path / "data"
    work = tmp_path / "work"
    data.mkdir()
    work.mkdir()
    (data / "gw-a.log").write_text(SHARD_A, encoding="utf-8")
    (data / "gw-b.log").write_text(SHARD_B, encoding="utf-8")
    (data / "gw-c.log").write_text(SHARD_C, encoding="utf-8")
    settings = Settings(data_dir=data, work_dir=work, allow_simulation=True)
    return settings, data, work


@pytest.fixture()
def client(env: tuple[Settings, Path, Path]) -> TestClient:
    settings, _, _ = env
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture()
def make_job(client: TestClient):
    def _make(shard_ids: list[str], job_id: str = "test-job") -> str:
        response = client.post("/jobs", json={"shard_ids": shard_ids, "job_id": job_id})
        assert response.status_code == 201, response.text
        return job_id

    return _make


EXPECTED_TOTALS = {
    "api.example.com": (6, 3),
    "www.example.com": (4, 2),
    "cdn.example.net": (3, 1),
}
