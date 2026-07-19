#!/usr/bin/env bash
# Plan 005 (Dapr sibling runtime) W4: runs StreamForge.Dapr.Host under its Dapr sidecar.
#
# Ports (never overlap the Orleans flavor's 5199/5299 — see AGENTS.md):
#   app       5399  REST + SignalR + SPA (this script's --app-port)
#   grpc      5499  reserved (not yet served — decision D-F, phase 2)
#   sidecar   3599  Dapr HTTP API
#   sidecar   4599  Dapr gRPC API
#
# Robust from either the repo root or dapr/ itself: resolves paths relative to this script's own
# location rather than $PWD.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
DAPR_DIR="$(cd "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"

DOTNET_BIN="${DOTNET_BIN:-$HOME/.dotnet/dotnet}"
if [[ ! -x "$DOTNET_BIN" ]]; then
  echo "error: dotnet not found at $DOTNET_BIN (set DOTNET_BIN to override)" >&2
  exit 1
fi

exec dapr run \
  --app-id streamforge-dapr \
  --app-port 5399 \
  --dapr-http-port 3599 \
  --dapr-grpc-port 4599 \
  --resources-path "$DAPR_DIR/components" \
  --config "$DAPR_DIR/components/config.yaml" \
  -- "$DOTNET_BIN" run --project "$DAPR_DIR/src/StreamForge.Dapr.Host" "$@"
