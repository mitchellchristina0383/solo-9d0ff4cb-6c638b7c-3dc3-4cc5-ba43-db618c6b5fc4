#!/usr/bin/env bash
# End-to-end demo against a running service (default http://localhost:8000).
# Usage: BASE_URL=http://localhost:8000 ./scripts/demo.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
JOB_ID="${JOB_ID:-demo-$RANDOM}"
PYTHON="${PYTHON:-python3}"

say() { printf '\n== %s ==\n' "$*"; }
post() { curl -sS -X POST "$BASE_URL$1" -H 'content-type: application/json' -d "$2"; }
get() { curl -sS "$BASE_URL$1"; }

jq_like() {
  "$PYTHON" -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps(eval(sys.argv[1]), ensure_ascii=False))' "$1"
}

say "health"
get /health

say "create job $JOB_ID (fixed shard set: 3 gateways)"
post /jobs "{\"shard_ids\":[\"gw-01.log\",\"gw-02.log\",\"gw-03.log\"],\"job_id\":\"$JOB_ID\"}" >/dev/null
echo created

say "baseline run"
post "/jobs/$JOB_ID/run" '{}' | jq_like "{'status': d['job']['status'], 'reads': d['run']['source_read_counts']}"
get "/jobs/$JOB_ID/results" | jq_like "[(r['domain'], r['total_requests'], r['status_5xx']) for r in d['results']]"

say "damage gw-02's api.example.com temp part (truncate), merge auto-repairs"
post "/jobs/$JOB_ID/run" '{"fault_injection":[{"shard_id":"gw-02.log","mode":"truncate_part","domain":"api.example.com"}]}' \
  | jq_like "{'faults': d['run']['faults'], 'reread': d['run']['reread_shards'], 'numbers': d['run']['reread_shard_numbers']}"
get "/jobs/$JOB_ID/results" | jq_like "[(r['domain'], r['total_requests']) for r in d['results']]"

say "damage a part AND take gw-02 offline: partition withheld, no half sums"
post "/jobs/$JOB_ID/run" '{"fault_injection":[{"shard_id":"gw-02.log","mode":"delete_part","domain":"api.example.com"},{"shard_id":"gw-02.log","mode":"source_unavailable"}]}' \
  | jq_like "{'status': d['job']['status'], 'failed_repairs': d['run']['failed_repairs']}"
get "/jobs/$JOB_ID/results" \
  | jq_like "{'published': [(r['domain'], r['total_requests']) for r in d['results']], 'blocked': [(b['domain'], b['blocked_shards']) for b in d['blocked']]}"

say "restore gw-02 and retry"
post "/jobs/$JOB_ID/sources/restore" '{"all":true}' >/dev/null
echo restored
post "/jobs/$JOB_ID/run" '{}' | jq_like "{'status': d['job']['status'], 'reads': d['run']['source_read_counts']}"
get "/jobs/$JOB_ID/results" | jq_like "[(r['domain'], r['total_requests'], r['status_5xx']) for r in d['results']]"

say "done: open $BASE_URL/docs"
