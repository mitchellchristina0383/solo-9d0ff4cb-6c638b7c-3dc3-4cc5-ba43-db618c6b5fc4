# Gateway Log Aggregator

FastAPI service that sums gateway access-log shards **by domain** and reports
**total requests** and **5xx responses**. Each job owns a fixed set of source
shards. Shards are first scanned into per-domain temporary count files, then
merged per domain.

It also answers the pre-maintenance question **"can this gateway be taken
offline without dropping requests?"** via a *diversion trial* (section 4).

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
5. `POST /jobs/demo-1/diversion-trials` → can `gw-03.log` go offline?
   ```json
   {
     "offline_shard_id": "gw-03.log",
     "gateway_request_limits": {"gw-01.log": 160, "gw-02.log": 160},
     "domain_allowed_targets": {
       "api.example.com": ["gw-01.log", "gw-02.log"],
       "cdn.shop.example.org": ["gw-01.log", "gw-02.log"],
       "ledger.example.io": ["gw-01.log", "gw-02.log"],
       "media.example.dev": ["gw-01.log", "gw-02.log"],
       "shop.example.org": ["gw-01.log", "gw-02.log"],
       "static.example.net": ["gw-01.log", "gw-02.log"],
       "www.example.com": ["gw-01.log", "gw-02.log"]
     }
   }
   ```
   → `plan_status: full`, all 65 requests move (40 to gw-01, 25 to gw-02;
   `shop.example.org` split 3/6). Then
   `POST /jobs/demo-1/diversion-trials/{trial_id}/confirm`.
   See section 4 for the full contract.

To demo repair with lost/truncated temp files:

6. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "truncate_part", "domain": "api.example.com"}
   ]}
   ```
   The damaged part is detected and the same run re-reads only `gw-02.log`.
   The response contains
   `"reread_shards": ["gw-02.log"], "reread_shard_numbers": [2]`.

To demo an unrecoverable source (partition withheld, then retried):

7. `POST /jobs/demo-1/run`
   ```json
   {"fault_injection": [
     {"shard_id": "gw-02.log", "mode": "delete_part", "domain": "api.example.com"},
     {"shard_id": "gw-02.log", "mode": "source_unavailable"}
   ]}
   ```
   `status: partial`, `failed_repairs: ["gw-02.log"]`.
8. `GET /jobs/demo-1/results` → `api.example.com`, `www.example.com`, … appear
   under **`blocked`** with `blocked_shards: ["gw-02.log"]`; `ledger.example.io`
   and `media.example.dev` (not served by `gw-02.log`) stay published.
9. `POST /jobs/demo-1/sources/restore` `{"all": true}`
10. `POST /jobs/demo-1/run` `{}` → `completed`, all totals restored and equal to
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
| POST | `/jobs/{id}/diversion-trials` | calculate a diversion trial for one gateway (section 4) |
| GET | `/jobs/{id}/diversion-trials` | list a job's trials |
| GET | `/jobs/{id}/diversion-trials/{trial_id}` | read one trial's missing items / plan |
| POST | `/jobs/{id}/diversion-trials/{trial_id}/confirm` | confirm a fully divertible, still-current trial |

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

## 4. Diversion trial (can this gateway go offline?)

Before maintenance on a gateway, the operator picks a completed job and the
gateway about to go offline. **Each job shard is one gateway.** The trial
answers: with the other gateways' period request limits as given, how many of
the offline gateway's requests can be moved, and where to?

### Request

`POST /jobs/{id}/diversion-trials`

```json
{
  "offline_shard_id": "gw-03.log",
  "gateway_request_limits": {"gw-01.log": 160, "gw-02.log": 160},
  "domain_allowed_targets": {
    "api.example.com": ["gw-01.log", "gw-02.log"],
    "cdn.shop.example.org": ["gw-01.log", "gw-02.log"],
    "ledger.example.io": ["gw-01.log", "gw-02.log"],
    "media.example.dev": ["gw-01.log", "gw-02.log"],
    "shop.example.org": ["gw-01.log", "gw-02.log"],
    "static.example.net": ["gw-01.log", "gw-02.log"],
    "www.example.com": ["gw-01.log", "gw-02.log"]
  }
}
```

* `gateway_request_limits` — the period request ceiling of **every remaining
  gateway**, all domains included. Must name exactly the job's shards except
  the offline one (400 otherwise). Integers ≥ 0.
* `domain_allowed_targets` — for each affected domain, which remaining gateways
  may receive it. When the summary is complete it must name exactly the domains
  the offline gateway serves (400 otherwise); `[]` means "move nothing for this
  domain". A target may appear in several domains' lists.

### Calculation rules

1. The trial is taken from **one complete published snapshot**
   (`GET /results`). Per-domain, per-shard request counts are read from
   published partitions only; a snapshot digest is stored with the trial.
2. Each remaining gateway's **existing** requests are **all** published requests
   it serves in that snapshot (summed across every domain), not only the
   offline gateway's domains. Remaining capacity is
   `max(0, limit - existing)`; a gateway already **over** its limit is treated
   as having zero capacity and gets a warning.
3. Remaining capacity is **shared by all affected domains**. One domain may be
   split across several gateways.
4. The maximum number of migratable requests is an **integral max-flow** value
   (source → domain → allowed gateway → sink), solved with Dinic in
   `app/flow.py`. All capacities are integers, so every assignment is a whole
   number of requests. Ties are broken deterministically (domains sorted,
   gateways in the job's shard order).
5. Only the offline gateway's *request* counts are moved; the 5xx split is not
   re-planned.

### Plan response

```json
{
  "trial_id": "trial-311279b12e8c",
  "summary_complete": true,
  "plan_status": "full",
  "fully_divertible": true,
  "missing": [],
  "total_requests_to_move": 65,
  "total_migrated_requests": 65,
  "total_unmigrated_requests": 0,
  "gateway_capacity": [
    {"shard_id": "gw-01.log", "request_limit": 160, "existing_requests": 120,
     "remaining_capacity": 40, "over_limit": false,
     "assigned_requests": 40, "final_requests": 160},
    {"shard_id": "gw-02.log", "request_limit": 160, "existing_requests": 90,
     "remaining_capacity": 70, "over_limit": false,
     "assigned_requests": 25, "final_requests": 115}
  ],
  "domains": [
    {"domain": "api.example.com", "requests_to_move": 10,
     "migrated_requests": 10, "unmigrated_requests": 0,
     "assignments": [{"shard_id": "gw-01.log", "requests": 10}]},
    {"domain": "shop.example.org", "requests_to_move": 9,
     "migrated_requests": 9, "unmigrated_requests": 0,
     "assignments": [{"shard_id": "gw-01.log", "requests": 3},
                     {"shard_id": "gw-02.log", "requests": 6}]}
  ],
  "confirmed_at": null
}
```

`plan_status` is `full` (everything moves), `partial` (some requests cannot
move), or `incomplete` (no plan; only `missing` is filled).

### Incomplete summary

When the published aggregation is not complete, no plan is computed and
`missing` lists **only** the missing items:

| `kind` | meaning | carries |
|---|---|---|
| `unscanned_shard` | a shard has no successful verified snapshot | `shard_ids` |
| `stale_shard` | parts are stale after a failed forced rescan | `shard_ids` |
| `blocked_domain` | a domain is withheld by the merge | `domain`, `shard_ids` |
| `pending_domain` | a domain has no published sum | `domain` |
| `no_published_results` | the job has never produced results | — |

A trial is calculation-only: it reads the atomic `job.json` and **does not take
the run lock**, so the source job can keep being rescanned while a trial is
open. An in-flight run does not by itself make the summary incomplete — the
published partitions still reflect the last verified scan.

### Confirmation

`POST /jobs/{id}/diversion-trials/{trial_id}/confirm` (body optional:
`{"note": "..."}`) is accepted only when **all** of these hold:

1. the trial was complete and `fully_divertible`;
2. the summary is still complete now; and
3. the per-domain, per-shard request counts are identical to the trial snapshot
   (same digest).

If the counts changed (for example because the job was rescanned after log
rotation) or the summary is no longer complete, the answer is **409** with
`{"code": "results_changed" | "summary_incomplete_now", "recalculate": true}`.
A trial that is itself incomplete or not fully divertible is **400** with
`code: trial_incomplete` / `trial_not_fully_divertible`. Confirmation is
idempotent and is serialized with runs by the job lock.

## 5. Accepted log line shapes

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

## 6. Design

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
WORK_DIR/jobs/<job_id>/trials/trial-<hex>.json   diversion trials (atomic)
```

`LOG_DATA_DIR` is mounted read-only; shard IDs are validated as plain file names
and resolved paths must stay inside it (symlink escapes are rejected).

## 7. Verification performed

* `pytest -q` → **109 passed** (unit + API + crash/retry + diversion tests).
* Diverted-trial tests (`tests/test_diversion.py`, `tests/test_flow.py`):
  * max-flow value cross-checked against an exhaustive oracle on 150 random
    small instances; assignments are feasible and deterministic;
  * three domains competing for one shared gateway's remaining capacity (15
    requested, 8 free → 8 migrated, 7 unmigrated, no gateway over its limit);
  * one domain split across two gateways while another domain uses one of them;
  * limits deducted from **all** existing requests, over-limit gateway → zero
    capacity, over-capacity results as `partial` and cannot be confirmed;
  * incomplete summary returns only `unscanned_shard` / `blocked_domain` items
    and an empty plan;
  * a trial can be calculated while the job's run lock is held (rescan in
    flight) but confirmation is serialized behind it;
  * after a rescan changes the published counts, confirming the old trial is
    **409 `results_changed`** ("recalculate"); a fresh trial confirms;
  * old 75 tests still pass unchanged.
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
  * extra runs → no reads, no change (idempotent);
  * diversion, offline `gw-03.log` (65 requests), limits 160/160 vs existing
    120/90 → `full`, assigned 40 to gw-01 and 25 to gw-02, `shop.example.org`
    split 3/6 across both, `final_requests` within limits; confirm → 200;
  * same trial with limits 130/110 (30 free) → `partial`, 30 migrated, 35
    unmigrated; confirm → HTTP 400 `trial_not_fully_divertible`, `recalculate:
    false`;
  * append one line to `gw-01.log`, `rescan_shards: ["gw-01.log"]`, then confirm
    the old trial → 409 `results_changed` with `recalculate: true`; a fresh
    trial on the new snapshot → 200;
  * source unavailable + damaged part → trial `incomplete`, `missing` lists
    `gw-02.log` and the six blocked domains; confirm → 400 `trial_incomplete`.
* Allocator scale check: 400 domains × 60 gateways (≈1.0M requests) solved in
  ~0.02 s; every gateway stayed within capacity.
* Crash tests: SIGKILL of a real child process mid-run (400k-line shard) followed
  by a successful retry; state rewound to "never scanned gw-c" followed by a
  retry that reads only `gw-c.log` and lands on the exact baseline totals.

## 8. Remaining items / limits

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
* **Diversion plan is advisory.** Confirming a trial records the decision and
  the snapshot it was based on; it does **not** reconfigure gateways. The 5xx
  counts are not re-planned, only request totals. Limits are period totals, so
  the operator is responsible for supplying values for the same statistical
  period as the log snapshot.
* **Docker not executable in this sandbox.** The image and Compose file were
  written and reviewed but `docker compose up` could not be executed here (no
  Docker daemon/socket). The equivalent live run was done with `uvicorn` against
  the same `LOG_DATA_DIR` / `WORK_DIR` environment variables.
