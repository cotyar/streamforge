#!/usr/bin/env bash
# Plan 011 wave C: memory-stability soak harness.
#
# The bench harness next door (tools/bench/run-bench.sh) answers "how fast is one delta?"; this one
# answers the question that actually killed a machine: "does a long run stay flat?". Same isolated-
# instance discipline, verbatim — a FRESH temp --DataDir, high ports (7511/7512 by default, never
# 5199/5299/5399), a health-wait before any measurement, an unconditional teardown, and a port-free
# verification afterwards.
#
# WHAT IT MEASURES, and why these three series and not a heap profiler:
#   * process RSS (`ps -o rss=`) — the number that decides whether the machine survives. No profiler
#     dependency, no diagnostics port, works against a plain `dotnet run`. Reported as a least-squares
#     slope in MB/min over the whole post-warmup window, which is the acceptance criterion: a
#     memory-stable build's slope is bounded by its row growth, not by its DELTA volume.
#   * the state directory's size on disk (`du -sk`) — the write amplifier's visible half. A table
#     rewriting its whole snapshot every FlushMs shows up here as bytes-per-flush.
#   * the soak table's RowCount / DeltasIn — the honest denominator. RSS growing while rows grow is
#     expected (the row set IS the state); RSS growing FASTER than rows is the amplifier.
#
# WHAT IT DRIVES: the stock seed alone (~5 order_events/s) needs hours to show anything, so the
# harness additionally creates ONE high-rate source + ONE table with exactly the seeded `order_states`
# shape — `LATEST BY (<unbounded key>)`, history enabled, LastN(8) — at ~200x the seeded rate. That is
# not a synthetic worst case: it is the seeded demo's own configuration with the clock sped up.
#
# Usage: tools/soak/run-soak.sh --label before [--minutes 8] [--interval-s 5] [--eps 200]
#                               [--http-port 7511] [--grpc-port 7512] [--persistence Batched]
#                               [--retention-max-rows 0]
#
# --retention-max-rows (plan 011 wave C2) sets the soak table's opt-in ROW RETENTION bound. 0 (the default)
# reproduces the C1 runs exactly — an unbounded table — so the recorded before/after numbers stay
# comparable; a positive value makes the soak table a BOUNDED VIEW (oldest rows evicted by event timestamp,
# with real retractions), which is the configuration whose slope answers "does the bound actually bound
# memory, or only the row count the console shows?".
#
# --persistence selects the soak table's TablePersistenceMode. It matters: Batched (the default, and the
# seeded default) rewrites the WHOLE state file every FlushMs by contract, so its residual cost stays
# O(rows) per flush no matter how cheap the in-memory capture gets; Journaled writes only the keys that
# changed. Running the same soak under both is how you tell "the capture is fixed" apart from "the write
# is inherent".
# Results: tools/soak/results/<label>-<timestamp>.{tsv,json} plus results/<label>-latest.json.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

DOTNET_BIN="${DOTNET_BIN:-$HOME/.dotnet/dotnet}"
BUN_BIN="${BUN_BIN:-bun}"

LABEL="unlabeled"
MINUTES="8"
INTERVAL_S="5"
EPS="200"
HTTP_PORT="7511"
GRPC_PORT="7512"
WARMUP_S="20"
PERSISTENCE="Batched"
RETENTION_MAX_ROWS="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label) LABEL="$2"; shift 2 ;;
    --minutes) MINUTES="$2"; shift 2 ;;
    --interval-s) INTERVAL_S="$2"; shift 2 ;;
    --eps) EPS="$2"; shift 2 ;;
    --http-port) HTTP_PORT="$2"; shift 2 ;;
    --grpc-port) GRPC_PORT="$2"; shift 2 ;;
    --warmup-s) WARMUP_S="$2"; shift 2 ;;
    --persistence) PERSISTENCE="$2"; shift 2 ;;
    --retention-max-rows) RETENTION_MAX_ROWS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { echo "[run-soak] $*" >&2; }

# The dev servers are off limits, always — same hard rule the bench harness states (AGENTS.md).
for forbidden in 5199 5299 5399; do
  if [[ "$HTTP_PORT" == "$forbidden" || "$GRPC_PORT" == "$forbidden" ]]; then
    log "FATAL: refusing to bind $forbidden (dev server port)."
    exit 1
  fi
done

port_free() { ! lsof -tnP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

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

BASE="http://localhost:$HTTP_PORT"

login() {
  curl -s -X POST "$BASE/api/auth/login" -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"admin123!"}' |
    "$BUN_BIN" -e 'const d=await Bun.stdin.text(); try { console.log(JSON.parse(d).token) } catch { console.log("") }'
}

if ! port_free "$HTTP_PORT" || ! port_free "$GRPC_PORT"; then
  log "FATAL: $HTTP_PORT or $GRPC_PORT already bound — refusing to start."
  exit 1
fi

DATADIR="$(mktemp -d)"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
TSV="$RESULTS_DIR/$LABEL-$TS.tsv"
JSON="$RESULTS_DIR/$LABEL-$TS.json"
HOSTLOG="$RESULTS_DIR/$LABEL-$TS.host.log"

log "=== soak '$LABEL': ${MINUTES}m @ ${INTERVAL_S}s samples, ${EPS} eps, $PERSISTENCE, retentionMaxRows=$RETENTION_MAX_ROWS, :$HTTP_PORT/:$GRPC_PORT ==="
log "DataDir: $DATADIR"
log "Host log: $HOSTLOG"

(
  cd "$REPO_ROOT" && exec "$DOTNET_BIN" run --project orleans/src/StreamForge.Host -- \
    --Http:Port "$HTTP_PORT" --Grpc:Port "$GRPC_PORT" --DataDir "$DATADIR"
) > "$HOSTLOG" 2>&1 &
RUNNER_PID=$!

teardown() {
  log "Tearing down (runner PID $RUNNER_PID)..."
  kill "$RUNNER_PID" 2>/dev/null || true
  wait "$RUNNER_PID" 2>/dev/null || true
  lsof -tnP -iTCP:"$HTTP_PORT" -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
  lsof -tnP -iTCP:"$GRPC_PORT" -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
  wait_port_free "$HTTP_PORT"
  wait_port_free "$GRPC_PORT"
  rm -rf "$DATADIR"
  log "Down. $HTTP_PORT=$(port_free "$HTTP_PORT" && echo free || echo BOUND) $GRPC_PORT=$(port_free "$GRPC_PORT" && echo free || echo BOUND); DataDir removed."
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Health wait
# ---------------------------------------------------------------------------
TOKEN=""
DEADLINE=$(( $(date +%s) + 180 ))
while [[ -z "$TOKEN" ]]; do
  if [[ $(date +%s) -gt $DEADLINE ]]; then
    log "FATAL: host never became healthy — see $HOSTLOG"
    exit 1
  fi
  TOKEN="$(login 2>/dev/null || true)"
  [[ "$TOKEN" == "undefined" || "$TOKEN" == "null" ]] && TOKEN=""
  [[ -z "$TOKEN" ]] && sleep 2
done
log "Healthy; logged in."

AUTH=(-H "Authorization: Bearer $TOKEN")

# ---------------------------------------------------------------------------
# The load: one high-rate source ("orders" profile — its orderId is a fresh GUID prefix per event, i.e.
# the same effectively-unbounded key space the seeded order_events/order_states pair has) plus one
# table with the seeded order_states shape (LATEST BY that key, history LastN(8)).
# ---------------------------------------------------------------------------
log "Creating soak source (eps=$EPS) and table..."
curl -s -o /dev/null -X POST "$BASE/api/sources" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
  "name": "soak_orders",
  "description": "Plan 011 wave C soak driver: order_states-shaped load at a configurable rate.",
  "generatorProfile": "orders",
  "eventsPerSecond": '"$EPS"',
  "fields": [
    {"name":"symbol","type":"String"},
    {"name":"orderId","type":"String"},
    {"name":"side","type":"String"},
    {"name":"qty","type":"Long"},
    {"name":"limitPrice","type":"Double"},
    {"name":"status","type":"String"}
  ]
}'

TABLE_JSON=$(curl -s -X POST "$BASE/api/tables" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
  "name": "soak_states",
  "description": "Plan 011 wave C soak driver: the seeded order_states shape at soak rate.",
  "sql": "SELECT orderId, symbol, side, qty, limitPrice, status FROM soak_orders LATEST BY (orderId)",
  "historyEnabled": true,
  "historyMode": "LastN",
  "historyLimit": 8,
  "persistence": "'"$PERSISTENCE"'",
  "retentionMaxRows": '"$RETENTION_MAX_ROWS"'
}')
TABLE_ID=$(echo "$TABLE_JSON" | "$BUN_BIN" -e 'const d=await Bun.stdin.text(); try{console.log(JSON.parse(d).id)}catch{console.log("")}')
if [[ -z "$TABLE_ID" ]]; then
  log "FATAL: could not create soak table: $TABLE_JSON"
  exit 1
fi
curl -s -o /dev/null -X POST "$BASE/api/tables/$TABLE_ID/start" "${AUTH[@]}"
log "Soak table $TABLE_ID started."

log "Warming up ${WARMUP_S}s before the first sample (JIT, seed catalog start, first flushes)..."
sleep "$WARMUP_S"

# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------
APP_PID="$(lsof -tnP -iTCP:"$HTTP_PORT" -sTCP:LISTEN 2>/dev/null | head -1)"
if [[ -z "$APP_PID" ]]; then
  log "FATAL: could not resolve the listening PID on $HTTP_PORT."
  exit 1
fi
log "Sampling PID $APP_PID for ${MINUTES}m..."

printf 'elapsed_s\trss_mb\tstate_kb\tstate_files\trows\tdeltas_in\n' > "$TSV"

START=$(date +%s)
END=$(( START + MINUTES * 60 ))
while [[ $(date +%s) -lt $END ]]; do
  NOW=$(date +%s)
  ELAPSED=$(( NOW - START ))
  RSS_KB="$(ps -o rss= -p "$APP_PID" 2>/dev/null | tr -d ' ')"
  if [[ -z "$RSS_KB" ]]; then
    log "WARNING: process $APP_PID vanished at ${ELAPSED}s — the host died mid-soak (see $HOSTLOG)."
    break
  fi
  RSS_MB=$(( RSS_KB / 1024 ))
  STATE_KB="$(du -sk "$DATADIR/state" 2>/dev/null | awk '{print $1}')"
  STATE_KB="${STATE_KB:-0}"
  STATE_FILES="$(ls -1 "$DATADIR/state" 2>/dev/null | wc -l | tr -d ' ')"
  METRICS="$(curl -s --max-time 10 "${AUTH[@]}" "$BASE/api/tables/$TABLE_ID/metrics" 2>/dev/null || true)"
  read -r ROWS DELTAS <<<"$(echo "$METRICS" | "$BUN_BIN" -e '
    const d = await Bun.stdin.text();
    try { const m = JSON.parse(d); console.log(`${m.rowCount ?? -1} ${m.deltasIn ?? -1}`); }
    catch { console.log("-1 -1"); }
  ' 2>/dev/null || echo "-1 -1")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$ELAPSED" "$RSS_MB" "$STATE_KB" "$STATE_FILES" "$ROWS" "$DELTAS" >> "$TSV"
  sleep "$INTERVAL_S"
done

log "Sampling done: $TSV"

# ---------------------------------------------------------------------------
# Summary — least-squares slope over the whole sampled window, per series.
# ---------------------------------------------------------------------------
"$BUN_BIN" -e "
const fs = require('fs');
const lines = fs.readFileSync('$TSV', 'utf8').trim().split('\n').slice(1).filter(Boolean);
const rows = lines.map(l => l.split('\t').map(Number));
const col = (i) => rows.map(r => r[i]);
const slopePerMin = (ys) => {
  const xs = col(0);
  const n = xs.length;
  if (n < 2) return 0;
  const mx = xs.reduce((a,b)=>a+b,0)/n, my = ys.reduce((a,b)=>a+b,0)/n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (xs[i]-mx)*(ys[i]-my); den += (xs[i]-mx)**2; }
  return den === 0 ? 0 : (num/den) * 60;
};
const stats = (name, i) => {
  const ys = col(i);
  return {
    name,
    first: ys[0], last: ys[ys.length-1], max: Math.max(...ys),
    slopePerMin: Number(slopePerMin(ys).toFixed(3)),
  };
};
const rss = stats('rss_mb', 1), state = stats('state_kb', 2), rowsS = stats('rows', 4), deltas = stats('deltas_in', 5);
const rowsGrown = Math.max(1, rowsS.last - rowsS.first);
const out = {
  label: '$LABEL',
  generatedAtIso: new Date().toISOString(),
  window: { minutes: Number('$MINUTES'), intervalSeconds: Number('$INTERVAL_S'), warmupSeconds: Number('$WARMUP_S'), samples: rows.length },
  load: { eventsPerSecond: Number('$EPS'), persistence: '$PERSISTENCE', retentionMaxRows: Number('$RETENTION_MAX_ROWS'), table: 'soak_states', shape: 'LATEST BY (orderId), history LastN(8) — the seeded order_states configuration' },
  series: { rss, state, rows: rowsS, deltas },
  derived: {
    rssKbPerRowAdded: Number(((rss.last - rss.first) * 1024 / rowsGrown).toFixed(2)),
    stateKbPerRowAdded: Number(((state.last - state.first) / rowsGrown).toFixed(3)),
  },
  tsv: '$TSV',
};
fs.writeFileSync('$JSON', JSON.stringify(out, null, 2) + '\n');
fs.writeFileSync('$RESULTS_DIR/$LABEL-latest.json', JSON.stringify(out, null, 2) + '\n');
console.log(JSON.stringify(out, null, 2));
"

log "Summary written to $JSON and $RESULTS_DIR/$LABEL-latest.json"
