"""Streaming parser for gateway access-log shards.

Accepted line shapes (documented in README):

1. Canonical gateway line: ``<domain> <status> [extra fields ...]``
2. Timestamped gateway line: ``<ISO-8601 timestamp> <domain> <status> [...]``
3. Combined Log Format: ``<host> <ident> <user> [date] "<request>" <status> <bytes> ...``
4. CSV with no spaces: ``<domain>,<status>[,extra ...]``
5. JSON object line with ``domain``/``host`` and ``status``/``status_code`` keys.

Blank lines and lines starting with ``#`` are ignored. Anything else that does
not yield a valid host token and a status in 100-599 is counted as malformed.
Malformed lines never contribute to totals; counts and samples are reported.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .normalize import normalize_domain

_ISO_TS = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
_COMBINED = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[[^\]]*\]\s+"[^"]*"\s+(?P<status>\d{3})(?:\s|$)'
)
_MAX_MALFORMED_SAMPLES = 5


class ShardReadError(RuntimeError):
    """A whole-shard read failed; no counter is trustworthy."""

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type


@dataclass
class DomainCount:
    total_requests: int = 0
    status_5xx: int = 0


@dataclass
class ReadResult:
    shard_id: str
    sha256: str
    size_bytes: int
    counters: dict[str, DomainCount]
    line_count: int = 0
    malformed_lines: int = 0
    malformed_samples: list[str] = field(default_factory=list)


def _extract_host(token: str) -> str | None:
    token = token.strip().strip('"\'')
    if "://" in token:
        try:
            host = urlsplit(token).hostname
        except ValueError:
            return None
        return normalize_domain(host or "")
    if token.startswith("[") and "]" in token:  # bracketed IPv6, optional port
        token = token[1 : token.index("]")]
        return normalize_domain(token)
    if token.count(":") == 1:  # host:port
        host, _, port = token.partition(":")
        if port.isdigit():
            token = host
    return normalize_domain(token)


def _parse_status(token: str) -> int | None:
    token = token.strip().strip('"')
    if not re.fullmatch(r"\d{3}", token):
        return None
    status = int(token)
    if not 100 <= status <= 599:
        return None
    return status


def _parse_json_line(line: str) -> tuple[str, int] | str:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        return f"invalid JSON: {exc.msg}"
    if not isinstance(obj, dict):
        return "JSON line is not an object"
    host = obj.get("domain", obj.get("host"))
    status = obj.get("status", obj.get("status_code"))
    if not isinstance(host, str):
        return "JSON line has no string domain/host"
    if isinstance(status, str) and status.strip().isdigit():
        status = int(status.strip())
    if not isinstance(status, int) or isinstance(status, bool):
        return "JSON line has no integer status"
    if not 100 <= status <= 599:
        return "JSON line status out of range"
    domain = _extract_host(host)
    if domain is None:
        return "JSON line domain is not a valid host"
    return domain, status


def parse_line(raw: str) -> tuple[str, int] | str:
    """Return ``(domain, status)`` or a human-readable malformed reason."""

    line = raw.strip().lstrip("﻿")
    if not line or line.startswith("#"):
        raise _SkipLine

    if line.startswith("{"):
        return _parse_json_line(line)

    combined = _COMBINED.match(line)
    if combined:
        domain = _extract_host(combined.group("host"))
        status = _parse_status(combined.group("status"))
        if domain is None:
            return "combined log host is not a valid domain"
        if status is None:
            return "combined log status out of range"
        return domain, status

    if "," in line and not any(ch.isspace() for ch in line):
        fields = line.split(",")
        if len(fields) < 2:
            return "CSV line needs at least domain,status"
        domain = _extract_host(fields[0])
        status = _parse_status(fields[1])
        if domain is None:
            return "invalid domain field"
        if status is None:
            return "invalid status field"
        return domain, status

    fields = line.split()
    if _ISO_TS.match(fields[0]):
        fields = fields[1:]
    if len(fields) < 2:
        return "expected at least <domain> <status>"
    domain = _extract_host(fields[0])
    status = _parse_status(fields[1])
    if domain is None:
        return "invalid domain field"
    if status is None:
        return "invalid status field"
    return domain, status


class _SkipLine(Exception):
    pass


def read_shard(
    path: Path,
    shard_id: str,
    *,
    simulated_unavailable: bool = False,
) -> ReadResult:
    """Read a shard once, hashing raw bytes and counting per domain.

    The file is read in a single pass. A stat-before/stat-after comparison
    detects a file that changed size or mtime mid-read so a torn snapshot is
    never published.
    """

    if simulated_unavailable:
        raise ShardReadError("simulated_unavailable", "source shard is marked unavailable")
    try:
        before = os.stat(path)
    except FileNotFoundError as exc:
        raise ShardReadError("not_found", f"source shard not found: {path.name}") from exc
    except OSError as exc:
        raise ShardReadError("stat_error", f"cannot stat source shard: {exc}") from exc
    if not os.path.isfile(path):
        raise ShardReadError("not_a_regular_file", f"source shard is not a regular file: {path.name}")

    digest = hashlib.sha256()
    counters: dict[str, DomainCount] = {}
    line_count = 0
    malformed = 0
    samples: list[str] = []
    try:
        with path.open("rb") as fh:
            for line_number, raw in enumerate(fh, start=1):
                digest.update(raw)
                line_count += 1
                try:
                    text = raw.decode("utf-8")
                    text = text.lstrip("﻿")
                    parsed = parse_line(text)
                except _SkipLine:
                    continue
                except UnicodeDecodeError:
                    parsed = "invalid UTF-8 bytes"
                if isinstance(parsed, str):
                    malformed += 1
                    if len(samples) < _MAX_MALFORMED_SAMPLES:
                        preview = raw[:160].decode("utf-8", errors="replace").strip()
                        preview = preview.replace("\t", " ")
                        samples.append(f"line {line_number}: {parsed}: {preview!r}")
                    continue
                domain, status = parsed
                count = counters.setdefault(domain, DomainCount())
                count.total_requests += 1
                if 500 <= status <= 599:
                    count.status_5xx += 1
    except FileNotFoundError as exc:  # disappeared between stat and open
        raise ShardReadError("not_found", f"source shard disappeared: {path.name}") from exc
    except PermissionError as exc:
        raise ShardReadError("permission_denied", f"source shard is not readable: {path.name}") from exc
    except OSError as exc:
        raise ShardReadError("io_error", f"failed reading source shard: {exc}") from exc

    try:
        after = os.stat(path)
    except OSError as exc:
        raise ShardReadError("stat_error", f"cannot re-stat source shard: {exc}") from exc
    if after.st_size != before.st_size or after.st_mtime_ns != before.st_mtime_ns:
        raise ShardReadError("source_changed_during_read", "source shard changed while being read")

    return ReadResult(
        shard_id=shard_id,
        sha256=digest.hexdigest(),
        size_bytes=after.st_size,
        counters=counters,
        line_count=line_count,
        malformed_lines=malformed,
        malformed_samples=samples,
    )
