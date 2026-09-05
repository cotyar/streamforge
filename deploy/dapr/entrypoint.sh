#!/usr/bin/env bash
# Plan 007 W1B — app container entrypoint.
#
# StreamsForge.Dapr.Host.Services.CatalogInitializationService seeds the demo catalog/users exactly
# once, on ApplicationStarted, with NO retry loop (by design — see its own doc comment: "a failure
# here just means the demo world isn't seeded yet"). That's correct for local dev (tools/run.sh starts
# the Dapr sidecar, THEN execs the app as its child process), but this container topology has a real
# ordering hazard.
#
# FOUND LIVE, NOT THEORETICAL — an earlier version of this script tried to fix that hazard by
# blocking here, waiting for daprd's own /v1.0/healthz, BEFORE ever launching `dotnet`. That is WRONG
# and deadlocks: daprd's own log is explicit about this — right after it starts, it prints
# "application protocol: http. waiting on port 8080. This will block until the app is listening on
# that port." and does NOT proceed to load the app's actor/subscription config, register with
# placement, or do anything else actor-related until it can reach the app on 8080 (it needs to call
# the app's own Dapr SDK-mapped endpoints, e.g. GET /dapr/config, to learn what actor types/topics
# exist). So: app-waits-for-daprd + daprd-waits-for-app is a genuine circular deadlock, not just a
# race — confirmed by watching `docker compose logs daprd` sit on repeated "waiting for application to
# listen on port 8080" lines forever with no actor-registration/placement-connection log ever
# appearing, for as long as the app entrypoint refused to start it.
#
# THE ACTUAL FIX: never make the app wait to start. Launch `dotnet` immediately (this is what
# unblocks daprd's own gate). Separately, poll daprd's health in parallel; once daprd reports ready,
# do ONE clean restart of the app process so a FRESH ApplicationStarted fires with daprd definitely
# up, guaranteeing CatalogInitializationService's one-shot seed a working attempt. This is safe to do
# unconditionally (even if the very first attempt already happened to succeed) because
# EnsureInitializedAsync (both RegistryActor and UserStoreActor) is idempotent — it checks Count == 0
# first, so a redundant call after a successful seed is a harmless no-op. The brief restart happens
# early in boot, well within Dockerfile.app's HEALTHCHECK start-period budget.
set -uo pipefail

dapr_http_port="${DAPR_HTTP_PORT:-3500}"
port="${PORT:-8080}"
# Plan 025 G2: this flavor serves gRPC now, and Kestrel cannot put HTTP/1.1 and h2c on ONE cleartext
# endpoint (see the Orleans Program.cs's --urls branch note). So the app is launched on the host's
# two-listener branch — one port per protocol, both on 0.0.0.0 — instead of the single --urls it used
# while there was no gRPC to serve. Both are container ports published by compose.yaml / declared in
# service.yaml; Cloud Run routes only the first, which is fine (the gRPC listener is simply unreachable
# there, exactly as it was before).
grpc_port="${GRPC_PORT:-8081}"
timeout_s="${DAPR_WAIT_TIMEOUT_S:-100}"

check_dapr_ready() {
  local response
  exec 3<>"/dev/tcp/127.0.0.1/${dapr_http_port}" 2>/dev/null || return 1
  printf 'GET /v1.0/healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
  if ! IFS= read -r response <&3; then
    exec 3<&- 2>/dev/null || true
    exec 3>&- 2>/dev/null || true
    return 1
  fi
  exec 3<&- 2>/dev/null || true
  exec 3>&- 2>/dev/null || true
  [[ "$response" =~ \ 2[0-9][0-9]\  ]]
}

app_pid=""
term_handler() {
  echo "entrypoint: received termination signal — forwarding to the app (pid ${app_pid})"
  [[ -n "$app_pid" ]] && kill -TERM "$app_pid" 2>/dev/null
  wait "$app_pid" 2>/dev/null
  exit 0
}
trap term_handler TERM INT

launch_app() {
  # Dockerfile.app publishes with Publish.props' single-file/self-contained settings (RID pinned to
  # linux-x64 at both restore and publish time) — /app has StreamsForge.Dapr.Host, a native
  # executable, not a StreamsForge.Dapr.Host.dll for `dotnet` to load. WORKDIR is /app and this
  # script never cd's, so the relative path resolves at both call sites below.
  ./StreamsForge.Dapr.Host --Http:Port "${port}" --Grpc:Port "${grpc_port}" &
  app_pid=$!
}

echo "entrypoint: starting the app immediately (this is what unblocks daprd's own 'waiting for the app' gate)."
launch_app

echo "entrypoint: waiting up to ${timeout_s}s for daprd (localhost:${dapr_http_port}/v1.0/healthz) to report ready..."
deadline=$(( $(date +%s) + timeout_s ))
daprd_ready=false
while true; do
  if check_dapr_ready; then
    daprd_ready=true
    break
  fi
  if ! kill -0 "$app_pid" 2>/dev/null; then
    echo "entrypoint: app process exited before daprd became ready — propagating its exit code." >&2
    wait "$app_pid"
    exit $?
  fi
  if [[ "$(date +%s)" -ge "$deadline" ]]; then
    echo "entrypoint: daprd never reported healthy within ${timeout_s}s — leaving this boot's catalog" \
         "seed as best-effort (see CatalogInitializationService's own doc comment). A later restart of" \
         "this container will retry once daprd is up." >&2
    break
  fi
  sleep 0.5
done

if [[ "$daprd_ready" == true ]]; then
  echo "entrypoint: daprd ready — restarting the app once to guarantee a clean, retried catalog seed."
  kill -TERM "$app_pid" 2>/dev/null
  wait "$app_pid" 2>/dev/null
  trap - TERM INT
  exec ./StreamsForge.Dapr.Host --Http:Port "${port}" --Grpc:Port "${grpc_port}"
fi

wait "$app_pid"
