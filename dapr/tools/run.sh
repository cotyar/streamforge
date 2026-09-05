#!/usr/bin/env bash
# Plan 005 (Dapr sibling runtime) W4: runs StreamsForge.Dapr.Host under its Dapr sidecar.
#
# Ports (never overlap the Orleans flavor's 5199/5299 — see AGENTS.md):
#   app       5399  REST + SignalR + SPA (this script's --app-port)
#   grpc      5499  gRPC — served since plan 025 G2 (was "reserved, phase 2"). The sidecar knows
#                   nothing about this port and never touches it; it is for platform clients.
#   sidecar   3599  Dapr HTTP API
#   sidecar   4599  Dapr gRPC API
#
# TLS (plan 025 G2). Two things have to change together, and forgetting the second one is the failure
# this comment exists to prevent:
#
#   1. The HOST gets the flag and a certificate, after the `--`:
#        ./tools/run.sh --Tls:Enabled true \
#            --Kestrel:Certificates:Default:Path  /path/cert.pem \
#            --Kestrel:Certificates:Default:KeyPath /path/key.pem
#      (`tools/tls/dev-cert.sh <out-dir>` mints a development pair and prints these arguments.)
#
#   2. The SIDECAR has to be told the app now speaks https, because daprd calls BACK into the app for
#      every actor activation/method-invocation and every pub/sub topic delivery:
#        DAPR_RUN_EXTRA_ARGS="--app-protocol https" ./tools/run.sh --Tls:Enabled true ...
#      Without it daprd keeps dialling http:// on 5399 against a TLS listener: curl still returns 200
#      over https, the SPA still loads, and NOTHING actor- or topic-driven works — no generator ticks,
#      no pipeline output, no table deltas. daprd does not verify the app's certificate on that
#      channel, so a self-signed development pair needs nothing further.
#
# DAPR_RUN_EXTRA_ARGS is word-split on purpose (it is a list of `dapr run` flags, not one argument).
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

# shellcheck disable=SC2206  # deliberate word splitting — see DAPR_RUN_EXTRA_ARGS above.
EXTRA_ARGS=(${DAPR_RUN_EXTRA_ARGS:-})

exec dapr run \
  --app-id streamsforge-dapr \
  --app-port 5399 \
  --dapr-http-port 3599 \
  --dapr-grpc-port 4599 \
  --resources-path "$DAPR_DIR/components" \
  --config "$DAPR_DIR/components/config.yaml" \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} \
  -- "$DOTNET_BIN" run --project "$DAPR_DIR/src/StreamsForge.Dapr.Host" "$@"
