#!/usr/bin/env bash
# Plan 005 (Dapr sibling runtime) W9: end-to-end latency benchmark orchestrator.
#
# Runs the SAME seeded `order_states` table's end-to-end SignalR-delta latency on both runtimes,
# SEQUENTIALLY — never concurrently, so both benchmarks get the whole machine's CPU (fairness; a
# Dapr run competing with a live Orleans silo for cores would be a dishonest comparison in either
# direction). Each phase: start a FRESH isolated instance, wait for it to be healthy and for
# `order_states` to be Running and accumulating rows, run tools/bench/latency.ts against it, then
# tear the instance all the way down (including the Dapr sidecar containers-adjacent processes) and
# verify its ports are free before starting the next phase.
#
# Orleans instance: isolated ports 6199 (http) / 6299 (grpc), a fresh temp --DataDir. NEVER touches
# 5199/5299 (AGENTS.md hard rule).
# Dapr instance: the fixed dev ports 5399 (app) / 3599 (sidecar http) / 4599 (sidecar grpc) — Dapr's
# own tooling (dapr/tools/run.sh) hardcodes these; reset via dapr/tools/reset.sh first for a
# comparable fresh-seed state.
#
# Usage: tools/bench/run-bench.sh [--min-samples 500] [--max-duration-ms 90000] [--warmup-ms 10000]

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

DOTNET_BIN="${DOTNET_BIN:-$HOME/.dotnet/dotnet}"
BUN_BIN="${BUN_BIN:-bun}"

MIN_SAMPLES="500"
MAX_DURATION_MS="90000"
WARMUP_MS="10000"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --min-samples) MIN_SAMPLES="$2"; shift 2 ;;
    --max-duration-ms) MAX_DURATION_MS="$2"; shift 2 ;;
    --warmup-ms) WARMUP_MS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { echo "[run-bench] $*" >&2; }

port_free() {
  ! lsof -tnP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

wait_port_free() {
  local port="$1" tries=0
  while ! port_free "$port"; do
    tries=$((tries + 1))
    if [[ $tries -gt 30 ]]; then
      log "WARNING: port $port still bound after waiting; forcing kill"
      lsof -tnP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | xargs -r kill -9 || true
    fi
    sleep 1
  done
}

login() {
  local base="$1"
  curl -s -X POST "$base/api/auth/login" -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"admin123!"}' | \
    "$BUN_BIN" -e 'const d=await Bun.stdin.text(); console.log(JSON.parse(d).token)'
}

wait_healthy_and_seeded() {
  # Poll until login works AND order_states reports Status=Running with growing DeltasIn.
  local base="$1" label="$2" timeout_s="$3"
  local start
  start=$(date +%s)
  local token="" last_deltas=-1 stable_running_seen=0
  while true; do
    local now
    now=$(date +%s)
    if [[ $((now - start)) -gt $timeout_s ]]; then
      log "$label: timed out after ${timeout_s}s waiting for healthy+seeded order_states"
      return 1
    fi
    token=$(login "$base" 2>/dev/null || true)
    if [[ -n "$token" && "$token" != "undefined" && "$token" != "null" ]]; then
      local tables_json
      tables_json=$(curl -s -H "Authorization: Bearer $token" "$base/api/tables" 2>/dev/null || true)
      # /api/tables/{id} resolves by the table's REST id (a GUID), NOT its name — GetTableAsync does
      # not accept the name as a path param (confirmed live: /api/tables/order_states 404s, while
      # /api/tables/{that GUID} 200s) — so pull both status and id out of the list response in one
      # pass rather than hitting a name-keyed URL that doesn't exist.
      local status_and_id
      status_and_id=$(echo "$tables_json" | "$BUN_BIN" -e '
        const d = await Bun.stdin.text();
        try {
          const arr = JSON.parse(d);
          const t = arr.find(x => x.name === "order_states");
          console.log(t ? `${t.status}\t${t.id}` : "missing\t");
        } catch { console.log("parse-error\t"); }
      ' 2>/dev/null || echo -e "error\t")
      local tbl_status="${status_and_id%%$'\t'*}"
      local table_id="${status_and_id#*$'\t'}"
      if [[ "$tbl_status" == "Running" && -n "$table_id" ]]; then
        local metrics_json deltas
        metrics_json=$(curl -s -H "Authorization: Bearer $token" "$base/api/tables/$table_id/metrics" 2>/dev/null || true)
        deltas=$(echo "$metrics_json" | "$BUN_BIN" -e '
          const d = await Bun.stdin.text();
          try { console.log(JSON.parse(d).deltasIn ?? -1); } catch { console.log(-1); }
        ' 2>/dev/null || echo -1)
        if [[ "$deltas" =~ ^[0-9]+$ ]] && [[ "$deltas" -gt 0 ]]; then
          if [[ "$last_deltas" -ge 0 && "$deltas" -gt "$last_deltas" ]]; then
            stable_running_seen=$((stable_running_seen + 1))
            if [[ $stable_running_seen -ge 2 ]]; then
              log "$label: order_states Running, deltasIn=$deltas and growing — healthy"
              echo "$token"
              return 0
            fi
          fi
          last_deltas="$deltas"
        fi
      fi
    fi
    sleep 2
  done
}

ORLEANS_RAW="$RESULTS_DIR/orleans-raw.json"
DAPR_RAW="$RESULTS_DIR/dapr-raw.json"
ORLEANS_SRC_RAW="$RESULTS_DIR/orleans-sourceevent-raw.json"
DAPR_SRC_RAW="$RESULTS_DIR/dapr-sourceevent-raw.json"
rm -f "$ORLEANS_RAW" "$DAPR_RAW" "$ORLEANS_SRC_RAW" "$DAPR_SRC_RAW"

# ---------------------------------------------------------------------------
# Phase 1: Orleans, isolated instance on 6199/6299
# ---------------------------------------------------------------------------
log "=== Phase 1: Orleans (isolated, :6199/:6299) ==="
ORLEANS_DATADIR="$(mktemp -d)"
log "DataDir: $ORLEANS_DATADIR"

if ! port_free 6199 || ! port_free 6299; then
  log "FATAL: 6199 or 6299 already bound — refusing to start (never touches 5199/5299 either)."
  exit 1
fi

(
  cd "$REPO_ROOT" && exec "$DOTNET_BIN" run --project orleans/src/StreamForge.Host -- \
    --Http:Port 6199 --Grpc:Port 6299 --DataDir "$ORLEANS_DATADIR"
) > "$RESULTS_DIR/orleans-host.log" 2>&1 &
ORLEANS_PID=$!
log "Orleans host PID $ORLEANS_PID (log: $RESULTS_DIR/orleans-host.log)"

ORLEANS_TOKEN=""
if ORLEANS_TOKEN=$(wait_healthy_and_seeded "http://localhost:6199" "orleans" 120); then
  log "Running latency.ts against Orleans (primary: tableDelta on order_states)..."
  "$BUN_BIN" "$SCRIPT_DIR/latency.ts" \
    --url http://localhost:6199 --token "$ORLEANS_TOKEN" --runtime orleans --signal tableDelta \
    --out "$ORLEANS_RAW" --min-samples "$MIN_SAMPLES" \
    --max-duration-ms "$MAX_DURATION_MS" --warmup-ms "$WARMUP_MS"
  log "Running latency.ts against Orleans (supplementary: sourceEvent on order_events)..."
  "$BUN_BIN" "$SCRIPT_DIR/latency.ts" \
    --url http://localhost:6199 --token "$ORLEANS_TOKEN" --runtime orleans --signal sourceEvent \
    --source order_events --field _ts \
    --out "$ORLEANS_SRC_RAW" --min-samples "$MIN_SAMPLES" \
    --max-duration-ms "$MAX_DURATION_MS" --warmup-ms "$WARMUP_MS"
else
  log "Orleans phase FAILED health/seed check — see $RESULTS_DIR/orleans-host.log"
fi

log "Tearing down Orleans instance (PID $ORLEANS_PID)..."
kill "$ORLEANS_PID" 2>/dev/null || true
wait "$ORLEANS_PID" 2>/dev/null || true
# dotnet run spawns a child apphost; make sure the actual listener is gone too.
lsof -tnP -iTCP:6199 -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
lsof -tnP -iTCP:6299 -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
wait_port_free 6199
wait_port_free 6299
log "Orleans instance down, 6199/6299 free."

# ---------------------------------------------------------------------------
# Phase 2: Dapr, fixed dev ports 5399/3599/4599
# ---------------------------------------------------------------------------
log "=== Phase 2: Dapr (:5399, sidecar :3599/:4599) ==="

if ! port_free 5399 || ! port_free 3599 || ! port_free 4599; then
  log "FATAL: 5399/3599/4599 not all free before starting — refusing to start."
  exit 1
fi

log "Resetting Dapr Redis state (fresh seed)..."
bash "$REPO_ROOT/dapr/tools/reset.sh"

(
  cd "$REPO_ROOT" && exec bash "$REPO_ROOT/dapr/tools/run.sh"
) > "$RESULTS_DIR/dapr-host.log" 2>&1 &
DAPR_PID=$!
log "Dapr run.sh PID $DAPR_PID (log: $RESULTS_DIR/dapr-host.log)"

DAPR_TOKEN=""
if DAPR_TOKEN=$(wait_healthy_and_seeded "http://localhost:5399" "dapr" 120); then
  log "Running latency.ts against Dapr (primary: tableDelta on order_states)..."
  "$BUN_BIN" "$SCRIPT_DIR/latency.ts" \
    --url http://localhost:5399 --token "$DAPR_TOKEN" --runtime dapr --signal tableDelta \
    --out "$DAPR_RAW" --min-samples "$MIN_SAMPLES" \
    --max-duration-ms "$MAX_DURATION_MS" --warmup-ms "$WARMUP_MS"
  log "Running latency.ts against Dapr (supplementary: sourceEvent on order_events)..."
  "$BUN_BIN" "$SCRIPT_DIR/latency.ts" \
    --url http://localhost:5399 --token "$DAPR_TOKEN" --runtime dapr --signal sourceEvent \
    --source order_events --field _ts \
    --out "$DAPR_SRC_RAW" --min-samples "$MIN_SAMPLES" \
    --max-duration-ms "$MAX_DURATION_MS" --warmup-ms "$WARMUP_MS"
else
  log "Dapr phase FAILED health/seed check — see $RESULTS_DIR/dapr-host.log"
fi

log "Tearing down Dapr instance..."
dapr stop --app-id streamforge-dapr 2>/dev/null || true
kill "$DAPR_PID" 2>/dev/null || true
wait "$DAPR_PID" 2>/dev/null || true
# Documented in dapr/ARCHITECTURE.md: `dapr stop` sometimes leaves the plain dotnet process holding
# the app port — kill explicitly and re-check.
lsof -tnP -iTCP:5399 -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
lsof -tnP -iTCP:3599 -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
lsof -tnP -iTCP:4599 -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
wait_port_free 5399
wait_port_free 3599
wait_port_free 4599
log "Dapr instance down, 5399/3599/4599 free."

# ---------------------------------------------------------------------------
# Merge results
# ---------------------------------------------------------------------------
TS="$(date -u +%Y%m%dT%H%M%SZ)"
COMBINED="$RESULTS_DIR/$TS.json"

"$BUN_BIN" -e "
const fs = require('fs');
const readIfExists = (p) => fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null;
const combined = {
  generatedAtIso: new Date().toISOString(),
  primary: {
    signal: 'tableDelta',
    table: 'order_states',
    field: 'stage_ts',
    methodology: 'client wall clock at SignalR tableDelta arrival minus row.stage_ts (event generation time); ASSERT rows only (weight===1); first 10s + pre-benchmark-start rows discarded; >=500 samples or 90s, whichever first.',
    orleans: readIfExists('$ORLEANS_RAW'),
    dapr: readIfExists('$DAPR_RAW'),
  },
  supplementary: {
    signal: 'sourceEvent',
    source: 'order_events',
    field: '_ts',
    methodology: 'client wall clock at SignalR sourceEvent arrival minus evt._ts (generator-tick to SignalR-relay hop only, NOT the full table-processing pipeline); added because the primary tableDelta signal hit a live Orleans-flavor bug this wave (see report) and this path is unaffected by it, giving a real comparable number on both runtimes.',
    orleans: readIfExists('$ORLEANS_SRC_RAW'),
    dapr: readIfExists('$DAPR_SRC_RAW'),
  },
};
fs.writeFileSync('$COMBINED', JSON.stringify(combined, null, 2) + '\n');
fs.writeFileSync('$RESULTS_DIR/latest.json', JSON.stringify(combined, null, 2) + '\n');
console.log(JSON.stringify(combined, null, 2));
"

log "Combined results written to $COMBINED and $RESULTS_DIR/latest.json"
log "Final port check: 5199=$(port_free 5199 && echo free || echo BOUND) 5299=$(port_free 5299 && echo free || echo BOUND) 6199=$(port_free 6199 && echo free || echo BOUND) 6299=$(port_free 6299 && echo free || echo BOUND) 5399=$(port_free 5399 && echo free || echo BOUND) 3599=$(port_free 3599 && echo free || echo BOUND) 4599=$(port_free 4599 && echo free || echo BOUND)"
