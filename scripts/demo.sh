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

say "diversion trial: take gw-03 offline; limits leave enough shared capacity"
# gw-01 already serves 120, gw-02 90; limits 160 each leave 40 + 70 = 110 free
# for gw-03's 65 requests. Every domain may use both gateways.
TRIAL=$(post "/jobs/$JOB_ID/diversion-trials" '{
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
}')
echo "$TRIAL" | jq_like "{'status': d['plan_status'], 'fully_divertible': d['fully_divertible'],
  'move': d['total_requests_to_move'], 'migrated': d['total_migrated_requests'],
  'unmigrated': d['total_unmigrated_requests']}"
echo "$TRIAL" | jq_like "[(p['domain'], p['requests_to_move'], p['migrated_requests'], p['assignments']) for p in d['domains']]"
TRIAL_ID=$(echo "$TRIAL" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["trial_id"])')

say "confirm trial $TRIAL_ID"
post "/jobs/$JOB_ID/diversion-trials/$TRIAL_ID/confirm" '{"note":"maintenance window"}' \
  | jq_like "{'confirmed_at': d['confirmed_at'], 'note': d['confirmation_note']}"

say "diversion with too little capacity: partial plan is not confirmable"
PARTIAL=$(post "/jobs/$JOB_ID/diversion-trials" '{
  "offline_shard_id": "gw-03.log",
  "gateway_request_limits": {"gw-01.log": 120, "gw-02.log": 100},
  "domain_allowed_targets": {
    "api.example.com": ["gw-02.log"],
    "cdn.shop.example.org": ["gw-02.log"],
    "ledger.example.io": ["gw-02.log"],
    "media.example.dev": ["gw-02.log"],
    "shop.example.org": ["gw-02.log"],
    "static.example.net": ["gw-02.log"],
    "www.example.com": ["gw-02.log"]
  }
}')
echo "$PARTIAL" | jq_like "{'status': d['plan_status'], 'migrated': d['total_migrated_requests'], 'unmigrated': d['total_unmigrated_requests']}"
PARTIAL_ID=$(echo "$PARTIAL" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["trial_id"])')
printf 'confirm partial trial -> HTTP %s: ' "$(curl -sS -o /tmp/trial-confirm.json -w '%{http_code}' \
  -X POST "$BASE_URL/jobs/$JOB_ID/diversion-trials/$PARTIAL_ID/confirm" \
  -H 'content-type: application/json' -d '{}')"
cat /tmp/trial-confirm.json
echo

say "done: open $BASE_URL/docs"
