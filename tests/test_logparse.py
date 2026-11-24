from __future__ import annotations

from pathlib import Path

import pytest

from app.logparse import ShardReadError, read_shard, parse_line
from app.normalize import normalize_domain


def test_domain_normalization() -> None:
    assert normalize_domain("API.Example.COM.") == "api.example.com"
    assert normalize_domain("  www.example.com  ") == "www.example.com"
    assert normalize_domain("") is None
    assert normalize_domain("bad host") is None


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("api.example.com 200", ("api.example.com", 200)),
        ("2026-11-22T10:00:00Z api.example.com 503", ("api.example.com", 503)),
        ("api.example.com:8443 404", ("api.example.com", 404)),
        ("https://api.example.com/v1 500", ("api.example.com", 500)),
        ("api.example.com,200,payload", ("api.example.com", 200)),
        ('{"domain": "api.example.com", "status": 502}', ("api.example.com", 502)),
        ('{"host": "api.example.com", "status_code": "504"}', ("api.example.com", 504)),
        (
            '203.0.113.9 - - [22/Nov/2026:10:00:00 +0000] "GET / HTTP/1.1" 500 12',
            ("203.0.113.9", 500),
        ),
        ("api.example.com 599", ("api.example.com", 599)),
    ],
)
def test_parse_line_accepts_common_shapes(line: str, expected: tuple[str, int]) -> None:
    assert parse_line(line) == expected


@pytest.mark.parametrize(
    "line",
    [
        "this is not a log line",
        "api.example.com 999",
        "api.example.com abc",
        '{"domain": "api.example.com", "status": "oops"}',
        '{"domain": "api.example.com"',
        "api.example.com 20x",
    ],
)
def test_parse_line_rejects_garbage(line: str) -> None:
    result = parse_line(line)
    assert isinstance(result, str), result


def test_blank_and_comment_lines_are_skipped(tmp_path: Path) -> None:
    path = tmp_path / "s.log"
    path.write_text("\n# comment\n   \napi.example.com 200\n", encoding="utf-8")
    result = read_shard(path, "s.log")
    assert result.malformed_lines == 0
    assert result.counters["api.example.com"].total_requests == 1


def test_5xx_boundaries_are_counted(tmp_path: Path) -> None:
    path = tmp_path / "s.log"
    path.write_text(
        "a.example 499\nb.example 500\nc.example 599\nd.example 600\n", encoding="utf-8"
    )
    result = read_shard(path, "s.log")
    assert result.counters["a.example"].status_5xx == 0
    assert result.counters["b.example"].status_5xx == 1
    assert result.counters["c.example"].status_5xx == 1
    assert "d.example" not in result.counters
    assert result.malformed_lines == 1


def test_invalid_utf8_is_malformed_not_fatal(tmp_path: Path) -> None:
    path = tmp_path / "s.log"
    path.write_bytes(b"api.example.com 200\n\xff\xfe broken\n")
    result = read_shard(path, "s.log")
    assert result.counters["api.example.com"].total_requests == 1
    assert result.malformed_lines == 1


def test_missing_file_raises_typed_error(tmp_path: Path) -> None:
    with pytest.raises(ShardReadError) as excinfo:
        read_shard(tmp_path / "nope.log", "nope.log")
    assert excinfo.value.error_type == "not_found"


def test_simulated_unavailable_raises_before_reading(tmp_path: Path) -> None:
    path = tmp_path / "s.log"
    path.write_text("api.example.com 200\n", encoding="utf-8")
    with pytest.raises(ShardReadError) as excinfo:
        read_shard(path, "s.log", simulated_unavailable=True)
    assert excinfo.value.error_type == "simulated_unavailable"


def test_line_count_and_sha_are_stable(tmp_path: Path) -> None:
    path = tmp_path / "s.log"
    path.write_text("api.example.com 200\nwww.example.com 500\n", encoding="utf-8")
    first = read_shard(path, "s.log")
    second = read_shard(path, "s.log")
    assert first.sha256 == second.sha256
    assert first.line_count == 2
    assert first.counters == second.counters
