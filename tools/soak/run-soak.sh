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
#                               [--retention-max-rows 0] [--shard-by orderId] [--shard-idle-s 90]
#                               [--shard-quantum-s 0] [--shape orders|instruments|trails]
#                               [--trail-cohort-ms 4300] [--trail-concurrency 8]
#
# --shape (plan 011 wave D2) picks WHICH TABLE is under load, because the two shapes answer different
# questions and reporting only one of them would flatter the feature:
#
#   orders (default, and what every recorded run before D2 used) — `LATEST BY (<fresh GUID>)`, history
#     LastN(8), i.e. an UNBOUNDED key space where each key gets exactly one row and one version. This is
#     the WORST case for sharding: per-shard overhead (a grain, a state file, an activation) is paid per
#     key and there is almost no per-key content to amortise it against. It is kept as the default
#     precisely because it is the unflattering one and because the C1/C2/D1 numbers are comparable to it.
#
#   instruments — a BOUNDED key space (~10k) with LONG version trails (history All, unbounded), which is
#     the shape the feature was actually built for: a financial instrument as a state machine, where the
#     query is "everything for this key" and the memory that hurts is versions x keys. Here the resident
#     set is bounded by the ACTIVE key set while the unsharded control holds every version of every key
#     resident forever.
#
#   trails (plan 011 wave D3) — the shape D2's conclusion pointed at and never measured. D2 found the
#     shard tier LOSING at ~8.5 versions per key and estimated a crossover at HUNDREDS of versions per
#     key; the workload this feature exists for (a financial instrument as a state machine: bounded key
#     set, ~1000 changes each, full history kept) sits on the far side of that estimate. This shape puts
#     the run there. Two knobs decide the point on the curve, and TOTAL EVENT VOLUME IS HELD FIXED across
#     the whole sweep, so every point stores the same number of versions in total and only their
#     DISTRIBUTION over keys changes — which is precisely the variable the crossover is a function of:
#
#       --trail-cohort-ms B   how long one COHORT of keys stays live. The key is a wall-clock bucket
#                             (`ts_ms - ts_ms % B`), so a cohort takes ~E*B/T events and then is never
#                             touched again. Bigger B = deeper trails and fewer keys.
#       --trail-concurrency G how many keys are live AT ONCE inside a cohort (`instrument % G`), i.e. the
#                             working set. G>1 keeps this from degenerating into "exactly one hot key".
#
#     Distinct keys K = (T/B)*G and versions per key = E/K, so B alone sweeps trail depth at fixed E:
#     B=430ms -> ~10 versions/key, B=4300ms -> ~100, B=43000ms -> ~1000 (at T=8min, E~90k).
#
#     WHY A RETIRING COHORT AND NOT A UNIFORMLY-HOT BOUNDED KEY SET: the shard tier's entire mechanism is
#     deactivating an IDLE key. With uniform random keys over a bounded space, mean inter-arrival per key
#     is K/eps seconds, so at the deep-trail end (K=90, 200 eps) every key is touched every 0.45s and
#     NOTHING is ever idle long enough to deactivate — the mechanism is switched off by construction and
#     the run measures nothing. A cohort that goes quiet forever is both realistic (an instrument's state
#     machine finishes) and the BEST case for sharding, which is what makes a negative result here
#     conclusive rather than an artefact of an unfriendly access pattern.
#
# --shard-by (plan 011 wave D1) turns the soak table into a SHARDED table keyed by the named column(s),
# and adds two sampled series that only mean anything for one: shard_count (distinct keys the directory
# knows) and resident_shards (shards ACTIVATED right now). The claim under test is not "RSS is flat" —
# a sharded table's row set still grows, and its shard files still accumulate on disk, which is the
# design ("keep everything, just not resident"). The claim is that RESIDENT memory tracks the ACTIVE key
# set rather than the total, and the direct evidence is resident_shards sitting far below shard_count
# while shard_activations climbs past both. If resident_shards tracks shard_count instead, nothing is
# being swapped out and the slope is not worth reading.
#
# --shard-idle-s (default 90) is passed to the host as --Shards:IdleSeconds. Orleans' stock collection
# age is 15 MINUTES, so without lowering it a soak of any reasonable length would observe exactly zero
# deactivations and prove nothing. The silo-wide collection QUANTUM (how often the collector scans, 60s
# by default) must be strictly smaller than any collection age — hence the 90s floor unless the quantum
# is lowered too, which --shard-quantum-s does.
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
SHARD_BY=""
SHARD_IDLE_S="90"
SHARD_QUANTUM_S="0"
SHAPE="orders"
TRAIL_COHORT_MS="4300"
TRAIL_CONCURRENCY="8"

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
    --shard-by) SHARD_BY="$2"; shift 2 ;;
    --shard-idle-s) SHARD_IDLE_S="$2"; shift 2 ;;
    --shard-quantum-s) SHARD_QUANTUM_S="$2"; shift 2 ;;
    --shape) SHAPE="$2"; shift 2 ;;
    --trail-cohort-ms) TRAIL_COHORT_MS="$2"; shift 2 ;;
    --trail-concurrency) TRAIL_CONCURRENCY="$2"; shift 2 ;;
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

[[ "$SHAPE" == "trails" ]] && log "trails: cohort=${TRAIL_COHORT_MS}ms, concurrency=$TRAIL_CONCURRENCY (keys ~ (${MINUTES}m/cohort)*concurrency)"
log "=== soak '$LABEL': shape=$SHAPE, ${MINUTES}m @ ${INTERVAL_S}s samples, ${EPS} eps, $PERSISTENCE, retentionMaxRows=$RETENTION_MAX_ROWS, shardBy='${SHARD_BY:-none}', :$HTTP_PORT/:$GRPC_PORT ==="
log "DataDir: $DATADIR"
log "Host log: $HOSTLOG"

HOST_ARGS=(--Http:Port "$HTTP_PORT" --Grpc:Port "$GRPC_PORT" --DataDir "$DATADIR")
if [[ -n "$SHARD_BY" ]]; then
  # Only lowered for a sharded run: the shard grain class's collection age (and, when asked, the
  # silo-wide scan quantum) is what decides whether an idle shard is ever actually reclaimed.
  HOST_ARGS+=(--Shards:IdleSeconds "$SHARD_IDLE_S")
  [[ "$SHARD_QUANTUM_S" != "0" ]] && HOST_ARGS+=(--Shards:QuantumSeconds "$SHARD_QUANTUM_S")
fi

(
  cd "$REPO_ROOT" && exec "$DOTNET_BIN" run --project orleans/src/StreamForge.Host -- "${HOST_ARGS[@]}"
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
# The table payload's shardBy is a JSON array: [] for an unsharded run (byte-identical to the C1/C2
# runs, so those recorded numbers stay comparable), ["col", ...] for a sharded one.
if [[ -n "$SHARD_BY" ]]; then
  SHARD_BY_JSON="[$(echo "$SHARD_BY" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"", (i>1?",":""), $i}}')]"
else
  SHARD_BY_JSON="[]"
fi

log "Creating soak source (shape=$SHAPE, eps=$EPS) and table..."
if [[ "$SHAPE" == "trails" ]]; then
  # The key is a RETIRING COHORT (see the header): a wall-clock bucket of width --trail-cohort-ms, crossed
  # with --trail-concurrency live keys inside it. `ts_ms` is a declared Timestamp field, which the generic
  # generator synthesises as epoch-millis, and `x - x % B` floors it WITHOUT leaving long arithmetic (the
  # evaluator's `/` always promotes to double, which would make every distinct millisecond its own key).
  #
  # The SELECT expression text must be character-identical (modulo whitespace) to the LATEST BY key text:
  # TableGroupKeyExtractor maps LATEST BY keys onto output columns by normalised expression text, and a
  # mismatch silently falls back to whole-row identity — which would make every event its own logical row
  # and destroy the very thing being measured (a long version trail per key).
  curl -s -o /dev/null -X POST "$BASE/api/sources" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "name": "soak_trails",
    "description": "Plan 011 wave D3 soak driver: bounded key set, ~1000-version trails per key.",
    "eventsPerSecond": '"$EPS"',
    "fields": [
      {"name":"ts_ms","type":"Timestamp"},
      {"name":"instrument","type":"Long"},
      {"name":"stage","type":"String"},
      {"name":"notional","type":"Double"}
    ]
  }'
  TRAIL_KEY_SQL="ts_ms - ts_ms % $TRAIL_COHORT_MS, instrument % $TRAIL_CONCURRENCY"
  TABLE_JSON=$(curl -s -X POST "$BASE/api/tables" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "name": "soak_states",
    "description": "Plan 011 wave D3 soak driver: long version trails over a bounded, retiring key set.",
    "sql": "SELECT ts_ms - ts_ms % '"$TRAIL_COHORT_MS"' AS cohort, instrument % '"$TRAIL_CONCURRENCY"' AS grp, stage, notional FROM soak_trails LATEST BY ('"$TRAIL_KEY_SQL"')",
    "historyEnabled": true,
    "historyMode": "All",
    "persistence": "'"$PERSISTENCE"'",
    "retentionMaxRows": '"$RETENTION_MAX_ROWS"',
    "shardBy": '"$SHARD_BY_JSON"'
  }')
elif [[ "$SHAPE" == "instruments" ]]; then
  # The generic generator branch synthesises a Long field as Random(0, 10_000) — a BOUNDED key space,
  # which is the whole point of this shape. Each key is therefore revisited many times over the run and
  # accumulates a long version trail (history All, unbounded), while at any moment only the keys touched
  # inside the collection age are active.
  curl -s -o /dev/null -X POST "$BASE/api/sources" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "name": "soak_instr",
    "description": "Plan 011 wave D2 soak driver: bounded key space, long version trails.",
    "eventsPerSecond": '"$EPS"',
    "fields": [
      {"name":"instrument","type":"Long"},
      {"name":"stage","type":"String"},
      {"name":"notional","type":"Double"}
    ]
  }'
  TABLE_JSON=$(curl -s -X POST "$BASE/api/tables" "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "name": "soak_states",
    "description": "Plan 011 wave D2 soak driver: instrument-with-legs shape.",
    "sql": "SELECT instrument, stage, notional FROM soak_instr LATEST BY (instrument)",
    "historyEnabled": true,
    "historyMode": "All",
    "persistence": "'"$PERSISTENCE"'",
    "retentionMaxRows": '"$RETENTION_MAX_ROWS"',
    "shardBy": '"$SHARD_BY_JSON"'
  }')
else
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
  "retentionMaxRows": '"$RETENTION_MAX_ROWS"',
  "shardBy": '"$SHARD_BY_JSON"'
}')
fi
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

printf 'elapsed_s\trss_mb\tstate_kb\tstate_files\trows\tdeltas_in\tshard_count\tresident_shards\tshard_activations\n' > "$TSV"

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
  # The shard sample uses GET /shards, which is answered by the router and the directory and touches NO
  # shard — sampling it must not be what keeps shards resident, or the measurement destroys what it
  # measures. (limit=0 also skips serialising the key list, which is O(keys).)
  SHARDS_JSON="{}"
  if [[ -n "$SHARD_BY" ]]; then
    SHARDS_JSON="$(curl -s --max-time 10 "${AUTH[@]}" "$BASE/api/tables/$TABLE_ID/shards?limit=0" 2>/dev/null || true)"
  fi
  [[ -z "$SHARDS_JSON" ]] && SHARDS_JSON="{}"
  read -r SHARD_COUNT RESIDENT ACTIVATIONS <<<"$(echo "$SHARDS_JSON" | "$BUN_BIN" -e '
    const d = await Bun.stdin.text();
    try { const s = JSON.parse(d); console.log(`${s.shardCount ?? -1} ${s.residentShardCount ?? -1} ${s.activations ?? -1}`); }
    catch { console.log("-1 -1 -1"); }
  ' 2>/dev/null || echo "-1 -1 -1")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$ELAPSED" "$RSS_MB" "$STATE_KB" "$STATE_FILES" "$ROWS" "$DELTAS" "$SHARD_COUNT" "$RESIDENT" "$ACTIVATIONS" >> "$TSV"
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
const shardCount = stats('shard_count', 6), resident = stats('resident_shards', 7), activations = stats('shard_activations', 8);
const rowsGrown = Math.max(1, rowsS.last - rowsS.first);
const out = {
  label: '$LABEL',
  generatedAtIso: new Date().toISOString(),
  window: { minutes: Number('$MINUTES'), intervalSeconds: Number('$INTERVAL_S'), warmupSeconds: Number('$WARMUP_S'), samples: rows.length },
  load: { eventsPerSecond: Number('$EPS'), persistence: '$PERSISTENCE', retentionMaxRows: Number('$RETENTION_MAX_ROWS'), shardBy: '$SHARD_BY', shardIdleSeconds: Number('$SHARD_IDLE_S'), shardQuantumSeconds: Number('$SHARD_QUANTUM_S'), table: 'soak_states', shape: '$SHAPE', trailCohortMs: Number('$TRAIL_COHORT_MS'), trailConcurrency: Number('$TRAIL_CONCURRENCY'), shapeNote: '$SHAPE' === 'trails' ? 'LATEST BY (retiring wall-clock cohort x concurrency) — a bounded key set whose keys accumulate long version trails and then go quiet, history All; trail depth swept by --trail-cohort-ms at fixed total event volume' : '$SHAPE' === 'instruments' ? 'LATEST BY (instrument) over a ~10k bounded key space, history All (unbounded trails) — the instrument-with-legs shape the shard tier targets' : 'LATEST BY (orderId), history LastN(8) — the seeded order_states configuration, an unbounded key space with one row and one version per key' },
  series: { rss, state, rows: rowsS, deltas, shardCount, resident, activations },
  derived: {
    rssKbPerRowAdded: Number(((rss.last - rss.first) * 1024 / rowsGrown).toFixed(2)),
    stateKbPerRowAdded: Number(((state.last - state.first) / rowsGrown).toFixed(3)),
    // Sharded runs only. residentFractionAtEnd near 1 means NOTHING is being swapped out and the RSS
    // slope below is not evidence of anything; well under 1, with activations above shardCount (keys
    // being reloaded), is the shape the wave claims.
    residentFractionAtEnd: shardCount.last > 0 ? Number((resident.last / shardCount.last).toFixed(4)) : null,
    residentPeak: resident.max > 0 ? resident.max : null,
    // The point of the 'trails' sweep: rows = distinct keys (LATEST BY yields one row per key), so
    // deltas/rows is the ACHIEVED trail depth. Reported rather than assumed — the cohort width only
    // predicts it, the generator's actual rate decides it.
    versionsPerKeyAtEnd: rowsS.last > 0 ? Number((deltas.last / rowsS.last).toFixed(1)) : null,
  },
  tsv: '$TSV',
};
fs.writeFileSync('$JSON', JSON.stringify(out, null, 2) + '\n');
fs.writeFileSync('$RESULTS_DIR/$LABEL-latest.json', JSON.stringify(out, null, 2) + '\n');
console.log(JSON.stringify(out, null, 2));
"

log "Summary written to $JSON and $RESULTS_DIR/$LABEL-latest.json"
