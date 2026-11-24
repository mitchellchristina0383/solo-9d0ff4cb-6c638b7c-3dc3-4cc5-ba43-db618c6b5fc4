#!/usr/bin/env python3
"""Generate deterministic sample gateway shards for local demos and tests.

Run from anywhere: ``python3 scripts/make_samples.py [--out DIR]``.
The default destination is ``samples/data`` relative to the repo root.

Each shard mixes line formats, has overlapping domains across shards, and a few
deliberately malformed lines so the reports demonstrate that malformed input is
counted but never inflates totals.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DOMAINS = [
    "api.example.com",
    "www.example.com",
    "static.example.net",
    "shop.example.org",
    "cdn.shop.example.org",
]

# Domains that only some gateways serve. They demonstrate that a partition whose
# sources are all readable keeps being published while another partition is
# withheld because one of *its* sources is unavailable.
EXTRA_DOMAINS = {
    1: ["ledger.example.io"],
    3: ["ledger.example.io", "media.example.dev"],
}


def domains_for(shard_no: int) -> list[str]:
    return DOMAINS + EXTRA_DOMAINS.get(shard_no, [])

STATUS_WEIGHTS = [
    (200, 60),
    (201, 5),
    (204, 4),
    (301, 4),
    (304, 3),
    (400, 5),
    (401, 1),
    (403, 2),
    (404, 8),
    (429, 2),
    (500, 3),
    (502, 2),
    (503, 1),
    (504, 1),
]


def pick_status(n: int) -> int:
    bucket = n % 101
    running = 0
    for code, weight in STATUS_WEIGHTS:
        running += weight
        if bucket < running:
            return code
    return 200


def shard_lines(shard_no: int, count: int) -> list[str]:
    """Deterministic pseudo-random lines; shard 1..3 overlap on all core domains."""

    domains = domains_for(shard_no)
    lines: list[str] = []
    for i in range(count):
        seed = shard_no * 100_003 + i * 7 + 13
        # Round-robin guarantees every domain of this shard is present.
        domain = domains[i % len(domains)]
        status = pick_status(seed // 3)
        style = (seed + shard_no) % 6
        seconds = (seed * 31) % 60
        hour = (seed // 60) % 24
        if style == 0:
            lines.append(f"{domain} {status} 91.203.7.{seed % 250} GET /v1/items")
        elif style == 1:
            ts = f"2026-11-2{shard_no}T{hour:02d}:{seconds:02d}:11Z"
            lines.append(f"{ts} {domain} {status}")
        elif style == 2:
            lines.append(
                f'{domain} - - [22/Nov/2026:{hour:02d}:{seconds:02d}:00 +0000] '
                f'"GET /checkout HTTP/1.1" {status} 512 "-" "curl/8.5.0"'
            )
        elif style == 3:
            lines.append(f"{domain},{status}")
        elif style == 4:
            lines.append(
                json.dumps(
                    {"domain": domain, "status": status, "path": "/api/v2/search"},
                    ensure_ascii=False,
                )
            )
        else:
            lines.append(f"{domain} {status}")
    return lines


MALFORMED = [
    "this line has no status at all",
    "broken.example.com nine-hundred",
    '{"domain": "api.example.com", "status": "not-a-number"}',
    "shop.example.org,999",
    '{"domain": "api.example.com"',
    "api.example.com 20x - -",
]

IGNORED = ["# comment lines are ignored", ""]


def build(out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    spec = [
        ("gw-01.log", 1, 120, 2),
        ("gw-02.log", 2, 90, 2),
        ("gw-03.log", 3, 65, 2),
    ]
    written: list[Path] = []
    for name, shard_no, count, malformed_no in spec:
        lines = shard_lines(shard_no, count)
        for offset, bad in enumerate(MALFORMED[:malformed_no]):
            lines.insert((shard_no * 23 + offset * 11) % len(lines), bad)
        for offset, ignored in enumerate(IGNORED):
            lines.insert((shard_no * 13 + offset * 5) % len(lines), ignored)
        path = out_dir / name
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "samples" / "data",
        help="destination directory for sample shards",
    )
    args = parser.parse_args(argv)
    paths = build(args.out)
    for path in paths:
        print(f"wrote {path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
