#!/usr/bin/env bash
# Plan 005 (Dapr sibling runtime) W4: wipes this app's Redis-backed state (catalog + users actor state,
# and later pub/sub consumer-group bookkeeping) so the next `run.sh` reseeds the demo world from empty —
# the Dapr-flavor equivalent of "delete orleans/src/StreamsForge.Host/data/ to reseed" (AGENTS.md).
#
# Scoped to THIS app only: the statestore component sets keyPrefix=appid (see
# dapr/components/statestore.yaml), so every key this app ever writes — actor state
# ("streamsforge-dapr||RegistryActor||catalog||catalog", "streamsforge-dapr||UserStoreActor||users||users",
# etc.) — starts with "streamsforge-dapr". SCAN (not KEYS) so this is safe against a large keyspace shared
# with other apps on the same dev Redis.
set -euo pipefail

APP_ID="streamsforge-dapr"
REDIS_CONTAINER="${REDIS_CONTAINER:-dapr_redis}"

if ! docker ps --format '{{.Names}}' | grep -qx "$REDIS_CONTAINER"; then
  echo "error: redis container '$REDIS_CONTAINER' is not running (set REDIS_CONTAINER to override)" >&2
  exit 1
fi

echo "Scanning for keys matching '${APP_ID}*' in $REDIS_CONTAINER..."

# redis-cli --scan prints one key per line; xargs -n 100 batches the DELs instead of one round trip per
# key. No-op (prints nothing, deletes nothing) when the app has never run / was already reset.
KEYS=$(docker exec "$REDIS_CONTAINER" redis-cli --scan --pattern "${APP_ID}*")

if [[ -z "$KEYS" ]]; then
  echo "No keys found for app id '$APP_ID' — nothing to reset."
  exit 0
fi

COUNT=$(echo "$KEYS" | wc -l | tr -d ' ')
echo "$KEYS" | xargs -L 100 docker exec "$REDIS_CONTAINER" redis-cli DEL >/dev/null
echo "Deleted $COUNT key(s) for app id '$APP_ID'. Restart via tools/run.sh to reseed."
