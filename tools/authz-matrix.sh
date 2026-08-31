#!/usr/bin/env bash
# Plan 015 wave 2-B: the LIVE half of the authorization coverage work.
#
# orleans/tests/StreamsForge.Host.Tests/AuthorizationCoverageTests.cs pins which policy is ATTACHED to
# which route, by reading the EndpointDataSource without ever binding a port. That test cannot tell you
# whether the policy is ENFORCED — a policy registered to require nothing at all satisfies it. This
# script answers the other half: it starts an isolated instance, logs in as each of the three seeded
# users, hits a representative route per REST group with each, and asserts the 200/403 it gets back.
#
# Isolated-instance discipline is copied verbatim from tools/soak/run-soak.sh: a FRESH temp --DataDir
# (seeds only apply to an empty data dir), high ports, a health-wait before any request, an
# unconditional teardown that kills only the PID we started, and a port-free check afterwards.
#
# Usage: tools/authz-matrix.sh [--http-port 7811] [--grpc-port 7812] [-v]
#        AUTHZ_HTTP_PORT / AUTHZ_GRPC_PORT / DOTNET_BIN are the environment equivalents.
#
# Exit 0 = every cell matched. Exit 1 = at least one cell did not (each printed as FAIL with the
# route, the role, what was expected and what came back).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
DOTNET_BIN="${DOTNET_BIN:-$HOME/.dotnet/dotnet}"

HTTP_PORT="${AUTHZ_HTTP_PORT:-7811}"
GRPC_PORT="${AUTHZ_GRPC_PORT:-7812}"
VERBOSE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --http-port) HTTP_PORT="$2"; shift 2 ;;
    --grpc-port) GRPC_PORT="$2"; shift 2 ;;
    -v|--verbose) VERBOSE=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { echo "[authz-matrix] $*" >&2; }

# Other sessions own hosts on these — the two dev servers (5199/5299 Orleans, 5399 + 3599/4599 Dapr
# sidecar), the two containerized stacks (6199/6399) and the admin app (5599). Never bind them.
for forbidden in 5199 5299 5399 3599 4599 6199 6399 5599; do
  if [[ "$HTTP_PORT" == "$forbidden" || "$GRPC_PORT" == "$forbidden" ]]; then
    log "FATAL: refusing to bind $forbidden (owned by a dev server / container stack / admin app)."
    exit 1
  fi
done
for p in "$HTTP_PORT" "$GRPC_PORT"; do
  if [[ "$p" -lt 6000 || "$p" -gt 9999 ]]; then
    log "FATAL: port $p is outside the 6xxx-9xxx range this repo reserves for test instances."
    exit 1
  fi
done

port_free() { ! lsof -tnP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

if ! port_free "$HTTP_PORT" || ! port_free "$GRPC_PORT"; then
  log "FATAL: $HTTP_PORT or $GRPC_PORT already bound — refusing to start."
  exit 1
fi

BASE="http://localhost:$HTTP_PORT"
DATADIR="$(mktemp -d)"
HOSTLOG="$DATADIR/host.log"

log "=== authz matrix on :$HTTP_PORT (grpc :$GRPC_PORT), DataDir $DATADIR ==="

(
  cd "$REPO_ROOT" && exec "$DOTNET_BIN" run --project orleans/src/StreamsForge.Host -- \
    --Http:Port "$HTTP_PORT" --Grpc:Port "$GRPC_PORT" --DataDir "$DATADIR"
) > "$HOSTLOG" 2>&1 &
RUNNER_PID=$!

teardown() {
  log "Tearing down (runner PID $RUNNER_PID)..."
  kill "$RUNNER_PID" 2>/dev/null || true
  wait "$RUNNER_PID" 2>/dev/null || true
  # Only ports we asked for, and only after our own PID is gone.
  local tries=0
  while ! port_free "$HTTP_PORT"; do
    tries=$((tries + 1)); [[ $tries -gt 30 ]] && break; sleep 1
  done
  rm -rf "$DATADIR"
  log "Down. $HTTP_PORT=$(port_free "$HTTP_PORT" && echo free || echo BOUND); DataDir removed."
}
trap teardown EXIT

# --------------------------------------------------------------------------------------------------
# Health wait + logins
# --------------------------------------------------------------------------------------------------
login() {
  curl -s --max-time 10 -X POST "$BASE/api/auth/login" -H 'Content-Type: application/json' \
    -d "{\"username\":\"$1\",\"password\":\"$2\"}" |
    sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

ADMIN_TOKEN=""
DEADLINE=$(( $(date +%s) + 180 ))
while [[ -z "$ADMIN_TOKEN" ]]; do
  if [[ $(date +%s) -gt $DEADLINE ]]; then
    log "FATAL: host never became healthy in 180s — see $HOSTLOG"
    tail -30 "$HOSTLOG" >&2
    exit 1
  fi
  ADMIN_TOKEN="$(login admin 'admin123!' 2>/dev/null || true)"
  [[ -z "$ADMIN_TOKEN" ]] && sleep 2
done
EDITOR_TOKEN="$(login editor 'editor123!')"
VIEWER_TOKEN="$(login viewer 'viewer123!')"
for pair in "admin:$ADMIN_TOKEN" "editor:$EDITOR_TOKEN" "viewer:$VIEWER_TOKEN"; do
  if [[ -z "${pair#*:}" ]]; then
    log "FATAL: could not log in as ${pair%%:*}."
    exit 1
  fi
done
log "Healthy; logged in as admin, editor, viewer."

# --------------------------------------------------------------------------------------------------
# The matrix
# --------------------------------------------------------------------------------------------------
PASS=0
FAIL=0

token_for() {
  case "$1" in
    admin) echo "$ADMIN_TOKEN" ;;
    editor) echo "$EDITOR_TOKEN" ;;
    viewer) echo "$VIEWER_TOKEN" ;;
    anon) echo "" ;;
  esac
}

# check <role> <method> <path> <expected-codes,comma-separated> [json-body]
check() {
  local role="$1" method="$2" path="$3" expect="$4" body="${5:-}"
  local token args=(-s -o /dev/null -w '%{http_code}' --max-time 20 -X "$method")
  token="$(token_for "$role")"
  [[ -n "$token" ]] && args+=(-H "Authorization: Bearer $token")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' -d "$body")

  local code
  code="$(curl "${args[@]}" "$BASE$path" 2>/dev/null || echo 000)"

  if [[ ",$expect," == *",$code,"* ]]; then
    PASS=$((PASS + 1))
    [[ $VERBOSE -eq 1 ]] && printf '  ok   %-7s %-6s %-45s %s\n' "$role" "$method" "$path" "$code"
    return 0
  fi
  FAIL=$((FAIL + 1))
  printf 'FAIL   %-7s %-6s %-45s expected %s, got %s\n' "$role" "$method" "$path" "$expect" "$code"
  return 1
}

VALIDATE_SQL='{"sql":"SELECT * FROM order_events"}'
MAPPING='{"sourceFields":[],"mappings":[]}'

log "--- unauthenticated: a Viewer route must challenge, an anonymous one must answer ---"
check anon GET /api/healthz 200
check anon GET /api/sources/ 401
check anon GET /api/users/ 401

log "--- Viewer routes: all three roles get through ---"
for role in admin editor viewer; do
  check "$role" GET /api/auth/me 200
  check "$role" GET /api/sources/ 200
  check "$role" GET /api/pipelines/ 200
  check "$role" GET /api/tables/ 200
  check "$role" GET /api/config/export 200
  check "$role" GET /api/meta/arrangements 200
  check "$role" GET /api/transports 200
  check "$role" GET /api/sql/functions 200
done

log "--- Editor routes: admin + editor 2xx, viewer 403 ---"
for role in admin editor; do
  check "$role" POST /api/pipelines/validate 200 "$VALIDATE_SQL"
  check "$role" POST /api/tables/validate 200 "$VALIDATE_SQL"
  check "$role" POST /api/sources/schema/mapping-validate 200 "$MAPPING"
done
check viewer POST /api/pipelines/validate 403 "$VALIDATE_SQL"
check viewer POST /api/tables/validate 403 "$VALIDATE_SQL"
check viewer POST /api/sources/schema/mapping-validate 403 "$MAPPING"

log "--- Editor mutation: a real create + delete, refused for viewer ---"
mk_source() {
  printf '{"name":"authzmx_%s","description":"authz-matrix probe","eventsPerSecond":1,"fields":[{"name":"v","type":"Long"}]}' "$1"
}
for role in admin editor; do
  check "$role" POST /api/sources/ 201 "$(mk_source "$role")"
  check "$role" DELETE "/api/sources/authzmx_$role" 204
done
check viewer POST /api/sources/ 403 "$(mk_source viewer)"
check viewer DELETE /api/sources/order_events 403

log "--- Admin routes: admin only ---"
check admin GET /api/users/ 200
check editor GET /api/users/ 403
check viewer GET /api/users/ 403
check admin POST /api/users/ 400,201 '{"username":"","displayName":"x","role":"Viewer","password":"x"}'
check editor POST /api/users/ 403 '{"username":"a","displayName":"x","role":"Viewer","password":"pw12345!"}'
check viewer POST /api/users/ 403 '{"username":"a","displayName":"x","role":"Viewer","password":"pw12345!"}'

# /api/chat is Editor-gated and returns 503 when GEMINI_API_KEY is unset, which is the normal case in
# CI and locally. The cell under test is the GATE, not the model: viewer must be refused BEFORE the
# 503, admin/editor must get past the gate and hit the missing-key answer.
log "--- Chat: Editor gate, 503 past it without a Gemini key ---"
check admin POST /api/chat/ 200,503 '{"messages":[{"role":"user","content":"hi"}]}'
check editor POST /api/chat/ 200,503 '{"messages":[{"role":"user","content":"hi"}]}'
check viewer POST /api/chat/ 403 '{"messages":[{"role":"user","content":"hi"}]}'

echo
log "=== $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] || exit 1
