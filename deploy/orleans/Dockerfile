# StreamForge — Orleans flavor — Cloud Run / compose image.
#
# Build from the REPO ROOT (context matters — the SPA and shared/ live outside orleans/):
#   docker build --platform linux/amd64 -f deploy/orleans/Dockerfile -t streamforge-orleans .
#
# --platform linux/amd64 matters even on Apple Silicon: grpc.tools' bundled protoc binary
# segfaults (SIGSEGV, exit 139) under native linux/arm64 in this SDK image as of grpc.tools
# 2.80.0 — amd64 builds fine (emulated locally via QEMU, native on Cloud Build/Cloud Run).
#
# Three stages: (1) bun builds the SPA, (2) dotnet SDK publishes StreamForge.Host, (3) a slim
# aspnet runtime bakes the publish output + SPA + docs together. See deploy/README.md for the
# full local-run / Cloud Run story; see plans/007-cloudrun-admin-aichat.md decision D-A for why
# this is one self-contained container (single-silo localhost clustering, ephemeral data dir).

# ---------------------------------------------------------------------------
# Stage 1 — SPA build (bun only, never npm — see CLAUDE.md)
# ---------------------------------------------------------------------------
FROM oven/bun:1 AS web-build
WORKDIR /web

# Install first from the lockfile alone so this layer caches across source-only changes.
COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile

COPY web/ ./
RUN bun run build
# → /web/dist

# ---------------------------------------------------------------------------
# Stage 2 — publish StreamForge.Host (and its shared/ + orleans project references)
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS dotnet-build
WORKDIR /src

# Only the pieces StreamForge.Host's ProjectReferences touch — shared/ + orleans/ — keeping the
# dapr/ and web/ trees out of this stage's context entirely (irrelevant to this publish and, for
# web/, already built by stage 1).
COPY shared/ shared/
COPY orleans/ orleans/

RUN dotnet publish orleans/src/StreamForge.Host -c Release -o /app/publish

# ---------------------------------------------------------------------------
# Stage 3 — runtime image
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# curl is not present in the base aspnet image; installed for the compose/Cloud Run healthcheck
# (`GET /healthz`, decision D-B). Kept minimal (no-install-recommends, apt cache dropped).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=dotnet-build /app/publish ./
COPY --from=web-build /web/dist ./web-dist
COPY orleans/docs ./docs

# Absolute paths so StreamForgeApiOptions resolves them regardless of ContentRootPath — see
# Program.cs (Web:Dist was already configurable; Docs:File is the additive key this wave added,
# mirroring the Dapr host's existing pattern one-for-one).
ENV Web__Dist=/app/web-dist
ENV Docs__File=/app/docs/index.html
ENV ASPNETCORE_ENVIRONMENT=Production
# Data dir defaults to "./data" (appsettings.json) — relative to the working directory below,
# so it lands at /app/data: ephemeral inside the container. A cold start reseeds the demo
# catalog (admin/editor/viewer + sample sources/pipelines/tables) — expected, documented in
# deploy/README.md, and acceptable for a demo platform (persistent storage is future work).

# gRPC (Grpc:Port, default 5299) stays internal-only — Cloud Run is one-port-per-service. Setting
# --urls below makes Program.cs skip its explicit two-Kestrel-endpoint block (the
# `if (string.IsNullOrEmpty(builder.Configuration["urls"]))` guard), so this image never opens a
# second listening port; only the single $PORT endpoint is bound (and EXPOSEd) below. gRPC service
# code is still mapped on the app, but with no second port to reach it on externally, it's
# effectively internal-only — consistent with plan 007 decision D-A's "one port per service".
EXPOSE 8080

# Cloud Run injects PORT (default 8080 locally/compose too); shell form so ${PORT} expands, and
# `exec` so dotnet becomes PID 1 (proper SIGTERM handling on `docker stop` / Cloud Run scale-down).
ENTRYPOINT ["/bin/sh", "-c", "exec dotnet StreamForge.Host.dll --urls http://0.0.0.0:${PORT:-8080}"]
