#!/usr/bin/env bash
# StreamForge — Orleans flavor — Cloud Run deploy script.
#
# PREPARES the deployment; per plan 007 ("prepare, not deploy") this script does NOT run itself
# automatically anywhere — you run it by hand when you're ready to bill your own GCP project.
#
# Usage:
#   deploy/orleans/deploy.sh [--dry-run] [--project PROJECT_ID] [--region REGION] [--tag TAG]
#
# Env var overrides (flags win if both given):
#   PROJECT_ID   default: `gcloud config get-value project`
#   REGION       default: europe-west1
#   TAG          default: git short SHA (falls back to `local` outside a git repo)
#
# Steps:
#   1. Build the image via `gcloud builds submit` (repo root context, deploy/orleans/Dockerfile)
#      and push it to Artifact Registry at
#      ${REGION}-docker.pkg.dev/${PROJECT_ID}/streamforge/orleans:${TAG}
#   2. Render deploy/orleans/service.yaml (envsubst ${IMAGE} / ${ANTHROPIC_API_KEY}) and apply it
#      with `gcloud run services replace`.
#
# --dry-run prints every command this script would run (including the rendered service.yaml) and
# exits 0 without touching gcloud/GCP state at all — safe to run any time to sanity-check inputs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DRY_RUN=0
PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-europe-west1}"
TAG="${TAG:-}"

usage() {
    grep '^#' "${BASH_SOURCE[0]}" | sed -E '1d;s/^# ?//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --project) PROJECT_ID="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --tag) TAG="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi
if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
    echo "error: no PROJECT_ID given and no default gcloud project configured (gcloud config set project ...)" >&2
    exit 1
fi

if [[ -z "$TAG" ]]; then
    TAG="$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo local)"
fi

REPOSITORY="streamforge"
IMAGE_NAME="orleans"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"
SERVICE_NAME="streamforge-orleans"

run() {
    if [[ "$DRY_RUN" -eq 1 ]]; then
        printf '[dry-run]'
        printf ' %q' "$@"
        printf '\n'
    else
        "$@"
    fi
}

echo "== StreamForge Orleans → Cloud Run =="
echo "PROJECT_ID = $PROJECT_ID"
echo "REGION     = $REGION"
echo "IMAGE      = $IMAGE"
echo "SERVICE    = $SERVICE_NAME"
[[ "$DRY_RUN" -eq 1 ]] && echo "(--dry-run: no gcloud state will be touched)"
echo

echo "-- 1. ensure Artifact Registry repository exists (idempotent, skips if present) --"
run gcloud artifacts repositories create "$REPOSITORY" \
    --project="$PROJECT_ID" \
    --location="$REGION" \
    --repository-format=docker \
    --description="StreamForge container images" \
    --quiet

echo
echo "-- 2. build + push the image (Cloud Build, repo root context) --"
run gcloud builds submit "$REPO_ROOT" \
    --project="$PROJECT_ID" \
    --config=/dev/stdin \
    --substitutions="_IMAGE=$IMAGE" <<'EOF'
steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-f', 'deploy/orleans/Dockerfile', '-t', '${_IMAGE}', '.']
images: ['${_IMAGE}']
EOF

echo
echo "-- 3. render service.yaml and apply it --"
RENDERED="$(mktemp -t streamforge-orleans-service.XXXXXX.yaml)"
export IMAGE
export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"
if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] envsubst '\${IMAGE} \${ANTHROPIC_API_KEY}' < deploy/orleans/service.yaml > $RENDERED"
    envsubst '${IMAGE} ${ANTHROPIC_API_KEY}' < "$SCRIPT_DIR/service.yaml" > "$RENDERED"
    echo "[dry-run] --- rendered service.yaml ---"
    cat "$RENDERED"
    echo "[dry-run] --- end rendered service.yaml ---"
    echo "[dry-run] gcloud run services replace $RENDERED --project=$PROJECT_ID --region=$REGION"
else
    envsubst '${IMAGE} ${ANTHROPIC_API_KEY}' < "$SCRIPT_DIR/service.yaml" > "$RENDERED"
    gcloud run services replace "$RENDERED" --project="$PROJECT_ID" --region="$REGION"
fi
rm -f "$RENDERED"

echo
echo "Done. Service URL: gcloud run services describe $SERVICE_NAME --project=$PROJECT_ID --region=$REGION --format='value(status.url)'"
