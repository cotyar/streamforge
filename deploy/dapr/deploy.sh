#!/usr/bin/env bash
# Plan 007 W1B — prepares (and, without --dry-run, executes) a Cloud Run deployment of the Dapr flavor's
# multi-container service. Builds+pushes the two custom images (app, daprd — placement/redis are public
# images referenced directly in service.yaml) to Artifact Registry, then `gcloud run services replace`s
# the envsubst'd manifest.
#
# NEVER run for real without an explicit ask — this bills the configured GCP project. Default project id
# is whatever `gcloud config get-value project` currently reports (e.g. total-casing-445522-j8 on this
# machine) — nothing here assumes or hardcodes a project.
#
# Usage:
#   deploy/dapr/deploy.sh --dry-run                 # print every command, run nothing
#   deploy/dapr/deploy.sh                            # build, push, and deploy for real
#   PROJECT_ID=my-proj REGION=us-central1 deploy/dapr/deploy.sh --dry-run
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    -h|--help)
      echo "Usage: $0 [--dry-run]"
      echo "Env overrides: PROJECT_ID, REGION, AR_REPO, SERVICE_NAME, GEMINI_API_KEY"
      exit 0
      ;;
    *)
      echo "error: unknown argument '$arg' (see --help)" >&2
      exit 1
      ;;
  esac
done

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${REGION:-europe-west1}"
AR_REPO="${AR_REPO:-streamforge}"
SERVICE_NAME="${SERVICE_NAME:-streamforge-dapr}"
GEMINI_API_KEY="${GEMINI_API_KEY:-}"
DAPRD_TAG="1.18.1" # keep in lockstep with deploy/dapr/Dockerfile.daprd's FROM line

if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  echo "error: no GCP project configured — set PROJECT_ID or run 'gcloud config set project <id>'" >&2
  exit 1
fi

AR_HOST="${REGION}-docker.pkg.dev"
# Tagged by git SHA, not :latest — `gcloud run services replace` only creates a new revision when
# the template CHANGES, so :latest silently re-fails the old revision instead of picking up newly
# pushed bytes (observed live on the second deploy attempt).
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo dev)"
APP_IMAGE="${AR_HOST}/${PROJECT_ID}/${AR_REPO}/dapr-app:${GIT_SHA}"
DAPRD_IMAGE="${AR_HOST}/${PROJECT_ID}/${AR_REPO}/dapr-daprd:${GIT_SHA}"

run() {
  echo "+ $*"
  if [[ "$DRY_RUN" == false ]]; then
    "$@"
  fi
}

echo "== StreamForge Dapr flavor — Cloud Run deploy =="
echo "   project:       $PROJECT_ID"
echo "   region:        $REGION"
echo "   service:       $SERVICE_NAME"
echo "   artifact repo: ${AR_HOST}/${PROJECT_ID}/${AR_REPO}"
echo "   app image:     $APP_IMAGE"
echo "   daprd image:   $DAPRD_IMAGE"
echo "   dry-run:       $DRY_RUN"
echo

# 1. Ensure the Artifact Registry repo exists (idempotent — 'already exists' is not a failure here).
run gcloud artifacts repositories create "$AR_REPO" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --repository-format=docker \
  --description="StreamForge container images" \
  --quiet || true

run gcloud auth configure-docker "$AR_HOST" --quiet

# 2. Build + push both custom images. Build context is the repo root (see Dockerfile.app/.daprd headers).
# linux/amd64 pinned: Cloud Run runs amd64; a native arm64 build from an Apple Silicon host
# fails the startup probe with "Application exec likely failed" (observed live on first deploy).
run docker build --platform linux/amd64 -f "$SCRIPT_DIR/Dockerfile.app" -t "$APP_IMAGE" "$REPO_ROOT"
run docker push "$APP_IMAGE"

run docker build --platform linux/amd64 -f "$SCRIPT_DIR/Dockerfile.daprd" -t "$DAPRD_IMAGE" "$REPO_ROOT"
run docker push "$DAPRD_IMAGE"

# 2b. Mirror the public sidecar images into Artifact Registry. Cloud Run only runs images from
# Artifact Registry — Docker Hub references pass deploy validation but the containers never start
# (observed live: redis exit(255), placement "Application exec likely failed", zero stdout).
PLACEMENT_IMAGE="${AR_HOST}/${PROJECT_ID}/${AR_REPO}/placement:1.18.1"
REDIS_IMAGE="${AR_HOST}/${PROJECT_ID}/${AR_REPO}/redis:7-alpine"
run docker pull --platform linux/amd64 daprio/placement:1.18.1
run docker tag daprio/placement:1.18.1 "$PLACEMENT_IMAGE"
run docker push "$PLACEMENT_IMAGE"
run docker pull --platform linux/amd64 redis:7-alpine
run docker tag redis:7-alpine "$REDIS_IMAGE"
run docker push "$REDIS_IMAGE"

# 3. Render service.yaml (every image comes from this project's Artifact Registry).
RENDERED="$(mktemp -t streamforge-dapr-service-XXXXXX.yaml)"
trap 'rm -f "$RENDERED"' EXIT

export APP_IMAGE DAPRD_IMAGE PLACEMENT_IMAGE REDIS_IMAGE REGION GEMINI_API_KEY
if [[ "$DRY_RUN" == true ]]; then
  echo "+ envsubst '\${APP_IMAGE} \${DAPRD_IMAGE} \${PLACEMENT_IMAGE} \${REDIS_IMAGE} \${REGION} \${GEMINI_API_KEY}' < $SCRIPT_DIR/service.yaml > $RENDERED"
  envsubst '${APP_IMAGE} ${DAPRD_IMAGE} ${PLACEMENT_IMAGE} ${REDIS_IMAGE} ${REGION} ${GEMINI_API_KEY}' < "$SCRIPT_DIR/service.yaml" > "$RENDERED"
  echo "----- rendered service.yaml (dry-run preview) -----"
  cat "$RENDERED"
  echo "----------------------------------------------------"
else
  envsubst '${APP_IMAGE} ${DAPRD_IMAGE} ${PLACEMENT_IMAGE} ${REDIS_IMAGE} ${REGION} ${GEMINI_API_KEY}' < "$SCRIPT_DIR/service.yaml" > "$RENDERED"
fi

# 4. Replace (create-or-update) the Cloud Run service from the rendered manifest.
run gcloud run services replace "$RENDERED" \
  --project="$PROJECT_ID" \
  --region="$REGION"

# 5. Multi-container Cloud Run services still need this flag set once out-of-band — 'replace' does not
#    carry min/max-instances the way the YAML's autoscaling annotations might suggest on every gcloud
#    version; run it idempotently every deploy to be sure.
run gcloud run services update "$SERVICE_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --min-instances=0 \
  --max-instances=1

if [[ "$DRY_RUN" == true ]]; then
  echo
  echo "Dry run complete — no gcloud/docker mutation actually ran."
else
  # Demo services are woken by plain GETs — allow unauthenticated invocations (app auth is the
  # platform's own JWT login). `services replace` does not manage the IAM policy, so bind here.
  run gcloud run services add-iam-policy-binding "$SERVICE_NAME" \
    --member=allUsers --role=roles/run.invoker \
    --project="$PROJECT_ID" --region="$REGION" --quiet

  echo
  echo "Deployed. Fetch the URL with:"
  echo "  gcloud run services describe $SERVICE_NAME --project=$PROJECT_ID --region=$REGION --format='value(status.url)'"
fi
