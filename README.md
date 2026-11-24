# Gateway Log Aggregator

FastAPI service that sums gateway access-log shards **by domain** and reports
**total requests** and **5xx responses**. Each job owns a fixed set of source
shards. Shards are first scanned into per-domain temporary count files, then
merged per domain.

It is built for the operational cases in the brief:

* a temporary part file goes missing or gets truncated between partition and merge;
* repair re-reads **only** the source shards that own damaged parts and reports
  the re-read shard numbers;
* every source is counted **once** — repair replaces a shard's contribution, it
  never adds a second one;
* if the source shard is itself unreadable, its partitions are **withheld** (no
  half result is published), the run can be retried after recovery, and
  partitions whose sources are all healthy remain queryable throughout.

---

## 1. Quick start (Docker Compose)

```bash
python3 scripts/make_samples.py          # regenerate samples/data if needed
docker compose up --build -d
docker compose logs -f aggregator        # wait for "engine ready …"
```

Open **http://localhost:8000/docs** and use *Try it out*:

1. `GET /sources` → `gw-01.log`, `gw-02.log`, `gw-03.log`.
2. `POST /jobs`
   ```json
   {"shard_ids": ["gw-01.log", "gw-02.log", "gw-03.log"], "job_id": "demo-1"}
   ```
3. `POST /jobs/demo-1/run` with `{}` → per-domain totals.
4. `GET /jobs/demo-1/results` → published partitions.

To demo repair with lost/truncated temp files:

5. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "truncate_part", "domain": "api.example.com"}
   ]}
   ```
   The damaged part is detected and the same run re-reads only `gw-02.log`.
   The response contains
   `"reread_shards": ["gw-02.log"], "reread_shard_numbers": [2]`.

To demo an unrecoverable source (partition withheld, then retried):

6. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "delete_part", "domain": "api.example.com"},
     {"shard_id": "gw-02.log", "mode": "source_unavailable"}
   ]}
   ```
   `status: partial`, `failed_repairs: ["gw-02.log"]`.
7. `GET /jobs/demo-1/results` → `api.example.com`, `www.example.com`, … appear
   under **`blocked`** with `blocked_shards: ["gw-02.log"]`; `ledger.example.io`
   and `media.example.dev` (not served by `gw-02.log`) stay published.
8. `POST /jobs/demo-1/sources/restore` `{"all": true}`
9. `POST /jobs/demo-1/run` `{}` → `completed`, all totals restored and equal to
   the baseline.

The same sequence is scripted: `BASE_URL=http://localhost:8000 bash scripts/demo.sh`.

Stop with `docker compose down` (add `-v` to also drop the work volume).

## 2. Run without Docker

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
LOG_DATA_DIR=./samples/data WORK_DIR=./work ALLOW_SIMULATION=true \
  uvicorn app.asgi:app --reload --port 8000
pytest -q
```

## 3. API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | liveness + directory writability |
| GET | `/sources` | source shard files available under `LOG_DATA_DIR` |
| POST | `/jobs` | create a job with a fixed, ordered shard list |
| GET | `/jobs` | list jobs with partition counters |
| GET | `/jobs/{id}` | full state: shard manifests, scan status, last run report |
| POST | `/jobs/{id}/run` | scan → partition → verify → repair → merge; returns run report |
| GET | `/jobs/{id}/results` | published per-domain results; `blocked` and `pending` lists |
| POST | `/jobs/{id}/sources/unavailable` | demo: simulate an unreadable source (needs `ALLOW_SIMULATION=true`) |
| POST | `/jobs/{id}/sources/restore` | demo: clear simulated outages, then `POST /run` to retry |

`POST /jobs/{id}/run` body (all optional):

```json
{
  "fault_injection": [
    {"shard_id": "gw-02.log", "mode": "delete_part", "domain": "api.example.com"},
    {"shard_id": "gw-02.log", "mode": "truncate_part", "domain": "api.example.com",
     "truncate_bytes": 10},
    {"shard_id": "gw-02.log", "mode": "source_unavailable"}
  ],
  "rescan_shards": ["gw-01.log"]
}
```

* `delete_part` / `truncate_part` need a `domain`; they act on that shard's
  temporary count file.
* `source_unavailable` is sticky: it persists in the job until
  `/sources/restore` is called (or the job is re-created). It is also available
  from request bodies even when the REST simulation endpoints are disabled.
* `rescan_shards` forces a fresh read of listed shards (e.g. after log
  rotation); their parts are replaced, never appended.

Run report fields relevant to the brief:

| field | meaning |
|---|---|
| `reread_shards` | sources re-read in this run (repairs, retry scans, rescans) |
| `reread_shard_numbers` | their 1-based positions in the job's fixed shard list |
| `repaired_shards` / `failed_repairs` | repair outcome per source |
| `source_read_counts` | successful full reads per source **in this run** (max 1) |
| `faults` | applied / no-op / failed simulations |
| `warnings`, `errors` | malformed-line samples, withheld partitions, failures |

`GET /jobs/{id}/results` also exposes cumulative `reread_shards` /
`reread_shard_numbers` and `unscanned_shards`.

## 4. Accepted log line shapes

Blank lines and `#` comments are ignored. Everything else must resolve to a host
and an HTTP status in 100–599; otherwise it is counted as malformed and excluded
from totals (samples are reported in `run.warnings`).

| Shape | Example |
|---|---|
| gateway | `api.example.com 200 10.0.0.1 GET /v1/items` |
| timestamped | `2026-11-22T10:00:00Z api.example.com 503` |
| combined log | `api.example.com - - [22/Nov/2026:10:00:00 +0000] "GET / HTTP/1.1" 500 512` |
| CSV | `api.example.com,201` |
| JSON | `{"domain": "api.example.com", "status": 502}` / `status_code` / `host` |

Hosts are lower-cased and a trailing root dot is dropped; `host:port`,
`https://host/path` and bracketed IPv6 are accepted. 5xx means 500–599.

## 5. Design

### Pipeline

```
source shards (read-only, LOG_DATA_DIR)
   │  one streaming pass per shard: sha256 + per-domain counters
   ▼
parts/<shard>/<sha256(domain)>.json     temporary count files, atomic writes
   │  verify size + sha256 + JSON schema + manifest totals
   ▼
repair: for each shard owning a damaged part → re-read that source once,
        rewrite all of its parts, replace its contribution
   ▼
merge: for each domain, sum at most one contribution per shard in fixed order
   ▼
job.json (atomic) ──► GET /results
```

### Why no double counting

* A partition is always **recomputed** from the manifest as
  `sum(contribution[shard] for shard in job.shard_ids)`; nothing is accumulated
  in place. Repair overwrites the shard's parts and manifest entry.
* Counters live in `parts/`, keyed by shard and domain, one file per pair. A
  re-read deletes parts for domains that vanished.
* Each `POST /run` reads a given source at most once: the correlation between
  scan phase and repair phase is tracked (`attempted` set), so a rescan followed
  by a fault never produces a second read.
* `job.json` and every part file are written temp-file + fsync + `os.replace`,
  so a crash leaves either the old or the new file, never a torn one.

### Withholding instead of half results

`_merge` publishes a domain only if **every** shard either provides a verified
part or has verified zero coverage:

1. a shard that never completed a scan has unknown coverage → **all** domains
   are withheld while it is in that state;
2. a shard that has a verified snapshot but whose part for this domain is
   damaged/unverifiable → this domain is withheld;
3. a shard whose last successful scan did not contain the domain correctly
   contributes zero.

Blocked partitions are zeroed (`total_requests: 0`) and only listed under
`blocked` with the responsible shards, so a consumer never sees a partial sum.
Other partitions keep their previous published values.

### Interruption and retry

* Cross-process `flock` on `jobs/<id>/.lock` plus an in-process thread lock;
  a second run gets `409` (`JobBusy`) immediately (non-blocking).
* Scan progress is persisted after every shard. A killed process (SIGKILL, no
  cleanup) leaves `status: processing`; the OS releases the lock, and the next
  `POST /run` resumes, reusing trusted shard snapshots and reporting an
  `interrupted` warning.
* A crash between writing a part and updating `job.json` is harmless: parts are
  content-addressed and verified, and the manifest is rewritten on the next
  successful scan.
* Retry after `source_unavailable` is a plain `POST /run`; only the still-unread
  shards are read (reported in `reread_shards`).

### Persistence layout

```
WORK_DIR/jobs/<job_id>/job.json                  authoritative state (atomic)
WORK_DIR/jobs/<job_id>/.lock                     flock target
WORK_DIR/jobs/<job_id>/parts/<shard>/<hash>.json temporary count files
```

`LOG_DATA_DIR` is mounted read-only; shard IDs are validated as plain file names
and resolved paths must stay inside it (symlink escapes are rejected).

## 6. Verification performed

* `pytest -q` → **75 passed** (unit + API + crash/retry tests).
* Live `uvicorn` run on the sample shards (`scripts/demo.sh`):
  * baseline: `api.example.com 48/3`, `cdn.shop.example.org 47/2`,
    `ledger.example.io 29/1`, `media.example.dev 9/1`, `shop.example.org 47/3`,
    `static.example.net 47/3`, `www.example.com 48/2` (total/5xx);
  * truncated part → `reread_shards: ["gw-02.log"]`, numbers `[2]`, totals
    unchanged after repair;
  * deleted part + unavailable source → `status: partial`, five domains blocked
    with `gw-02.log`, two unaffected domains still published;
  * repeated retry while unavailable → still withheld, no extra reads;
  * restore + retry → `completed`, identical to baseline;
  * extra runs → no reads, no change (idempotent).
* Crash tests: SIGKILL of a real child process mid-run (400k-line shard) followed
  by a successful retry; state rewound to "never scanned gw-c" followed by a
  retry that reads only `gw-c.log` and lands on the exact baseline totals.

## 7. Remaining items / limits

* **No source-diff detection without `rescan_shards`.** If a source file is
  replaced in place between jobs (log rotation), totals stay at the previous
  snapshot until a rescan is requested. Automatic detection would need mtime/size
  comparison per run; deliberately not implied, because readers may replay
  immutable archives.
* **One job at a time per job id.** Concurrency is serialized by design (local
  aggregation); there is no worker queue. Different jobs *can* run concurrently.
* **Simulation endpoints are demo-only.** They are gated by
  `ALLOW_SIMULATION` (enabled in Compose); production use should disable them.
  `fault_injection` in `POST /run` is also test-only and should be refused at the
  ingress in production.
* **Whole-file reads.** Shards are read in a single pass and held per-domain in
  memory (one small dict per shard), which is fine for the sample sizes; a shard
  with an extreme number of distinct domains would need external sorting.
* **No auth/TLS.** The service is intended for a trusted local network; put it
  behind a reverse proxy if exposed.
* **Docker not executable in this sandbox.** The image and Compose file were
  written and reviewed but `docker compose up` could not be executed here (no
  Docker daemon/socket). The equivalent live run was done with `uvicorn` against
  the same `LOG_DATA_DIR` / `WORK_DIR` environment variables.
