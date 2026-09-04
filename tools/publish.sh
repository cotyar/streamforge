#!/usr/bin/env bash
# Publishes a StreamsForge host as a single self-contained executable (plus appsettings.json), with
# plugins/ and ui-plugins/ carried alongside it as loose, still-installable directories.
#
# The actual publish knobs (PublishSingleFile, SelfContained, trimming off, embedded SPA/docs/protos
# fallback resources) live in orleans/src/StreamsForge.Host/Publish.props and
# dapr/src/StreamsForge.Dapr.Host/Publish.props, gated on `dotnet publish`'s own `_IsPublishing`
# property — this script only picks the RID/output directory and does the file shuffling `dotnet
# publish` itself has no opinion about: building the SPA first if it's missing, and carrying the
# build output's plugins/ directory (merged out-of-tree connector DLLs, see TRANSPORTS.md) into the
# publish output since single-file publish does not automatically include a loose sibling directory.
#
# Usage: tools/publish.sh <orleans|dapr> [rid] [out-dir] [--dry-run]
#   rid      default: linux-x64 (the container image target). osx-arm64 works too (used to verify
#            this script locally on a Mac with no Linux host to hand).
#   out-dir  default: out/<flavor>-<rid>/ (repo-root out/, gitignored)
#   --dry-run  print the commands this script would run, without running them.
#
# Examples:
#   tools/publish.sh orleans
#   tools/publish.sh dapr osx-arm64 /tmp/dapr-out
#   tools/publish.sh orleans linux-x64 out/orleans-linux-x64 --dry-run

set -euo pipefail

DOTNET="${DOTNET_BIN:-$HOME/.dotnet/dotnet}"

usage() {
  echo "Usage: $0 <orleans|dapr> [rid] [out-dir] [--dry-run]" >&2
  exit 1
}

DRY_RUN=false
POSITIONAL=()
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) POSITIONAL+=("$arg") ;;
  esac
done

FLAVOR="${POSITIONAL[0]:-}"
RID="${POSITIONAL[1]:-linux-x64}"
[[ "$FLAVOR" == "orleans" || "$FLAVOR" == "dapr" ]] || usage

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${POSITIONAL[2]:-$REPO_ROOT/out/${FLAVOR}-${RID}}"

case "$FLAVOR" in
  orleans) PROJECT="$REPO_ROOT/orleans/src/StreamsForge.Host/StreamsForge.Host.csproj" ;;
  dapr)    PROJECT="$REPO_ROOT/dapr/src/StreamsForge.Dapr.Host/StreamsForge.Dapr.Host.csproj" ;;
esac
PROJECT_DIR="$(dirname "$PROJECT")"

run() {
  if $DRY_RUN; then
    printf '+'; printf ' %q' "$@"; printf '\n'
  else
    echo "+ $*"
    "$@"
  fi
}

# --- 1. Build the SPA if it's missing, or older than its sources (bun, never npm — see CLAUDE.md). ---
SPA_DIST="$REPO_ROOT/web/dist"
SPA_STALE=false
if [[ ! -f "$SPA_DIST/index.html" ]]; then
  SPA_STALE=true
elif [[ -n "$(find "$REPO_ROOT/web/src" "$REPO_ROOT/web/package.json" -newer "$SPA_DIST/index.html" 2>/dev/null)" ]]; then
  SPA_STALE=true
fi
if $SPA_STALE; then
  echo "== web/dist missing or stale — building the SPA =="
  run bun run --cwd "$REPO_ROOT/web" build
else
  echo "== web/dist is up to date, skipping SPA build =="
fi

# --- 2. Publish. Single-file/self-contained/trim-off/embedded-resource properties all come from this
#        host's Publish.props (see that file) — this command only supplies RID, config and output dir. ---
echo "== dotnet publish ($FLAVOR, $RID) -> $OUT_DIR =="
run "$DOTNET" publish "$PROJECT" -c Release -r "$RID" -o "$OUT_DIR"

if $DRY_RUN; then
  echo "== dry-run: skipping plugins/ui-plugins staging and file listing =="
  exit 0
fi

# --- 3. Strip whatever local dev data/ the SDK's default item globbing carried into the publish
#        output. The host projects have no explicit Content item for it — it rides along because
#        Microsoft.NET.Sdk.Web's default "copy everything that isn't code to the output dir" globbing
#        does not exclude arbitrary top-level folders (only bin/obj), so a checkout that has ever run
#        the dev server (data/ holds its persisted grain/actor state and instance identity) publishes
#        that dev catalog as if it were shipped seed data. Confirmed pre-existing and independent of
#        Publish.props: `dotnet build`'s own bin/ output already carries the identical data/ before
#        this script's changes. A published host creates its own fresh data/ from --DataDir on first
#        boot, so nothing here is lost by removing it. ---
if [[ -d "$OUT_DIR/data" ]]; then
  echo "== removing local dev data/ that rode along into the publish output =="
  rm -rf "$OUT_DIR/data"
fi
# Debug symbols of the host and every referenced project ride along too; a deployable is the exe,
# appsettings.json and the two plugin directories. Keep a build's pdbs in bin/, not in the deliverable.
rm -f "$OUT_DIR"/*.pdb

# --- 4. Carry the build output's plugins/ (merged out-of-tree connector DLLs) into the publish
#        output, if a copy target produced one — dotnet publish does not pull in a loose sibling
#        directory next to the framework-dependent build output on its own. Search both the
#        RID-specific and the plain build output trees under bin/, since which one a copy target
#        lands in depends on whether the build step that produced it saw a RuntimeIdentifier. ---
PUBLISH_PLUGINS_DIR="$OUT_DIR/plugins"
if [[ -d "$PUBLISH_PLUGINS_DIR" ]] && compgen -G "$PUBLISH_PLUGINS_DIR/*.dll" > /dev/null; then
  echo "== plugins/ already present in publish output, nothing to carry =="
else
  BUILD_PLUGINS_DIR="$(find "$PROJECT_DIR/bin" -type d -name plugins 2>/dev/null | head -1)"
  if [[ -n "$BUILD_PLUGINS_DIR" ]] && compgen -G "$BUILD_PLUGINS_DIR"/*.dll > /dev/null; then
    echo "== carrying build output's plugins/ ($BUILD_PLUGINS_DIR) into $PUBLISH_PLUGINS_DIR =="
    mkdir -p "$PUBLISH_PLUGINS_DIR"
    cp "$BUILD_PLUGINS_DIR"/*.dll "$PUBLISH_PLUGINS_DIR/"
  else
    echo "== no plugins/ found in the build output — publishing without one (drop DLLs into $PUBLISH_PLUGINS_DIR/ later) =="
  fi
fi

# --- 5. An empty ui-plugins/ so an operator has an obvious place to drop console UI modules. ---
mkdir -p "$OUT_DIR/ui-plugins"
cat > "$OUT_DIR/ui-plugins/README.txt" <<'EOF'
Drop one .js/.mjs ES module or one .ts/.tsx file per out-of-tree console UI plugin here (GET /api/ui-plugins lists them); see TRANSPORTS.md.
EOF

# --- 6. Report what got published. ---
echo
echo "== published $FLAVOR ($RID) to $OUT_DIR =="
find "$OUT_DIR" -type f -exec ls -la {} \; | awk '{printf "%10d  %s\n", $5, $NF}' | sort -k2
echo "-- total --"
du -sh "$OUT_DIR"
