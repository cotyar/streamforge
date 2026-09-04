# StreamsForge — Orleans flavor — Cloud Run / compose image.
#
# Build from the REPO ROOT (context matters — the SPA and shared/ live outside orleans/):
#   docker build --platform linux/amd64 -f deploy/orleans/Dockerfile -t streamsforge-orleans .
#
# --platform linux/amd64 matters even on Apple Silicon: grpc.tools' bundled protoc binary
# segfaults (SIGSEGV, exit 139) under native linux/arm64 in this SDK image as of grpc.tools
# 2.80.0 — amd64 builds fine (emulated locally via QEMU, native on Cloud Build/Cloud Run).
#
# Three stages: (1) bun builds the SPA, (2) dotnet SDK publishes StreamsForge.Host, (3) a slim
# aspnet runtime bakes the publish output + SPA + docs together. See deploy/README.md for the
# full local-run / Cloud Run story; see plans/007-cloudrun-admin-aichat.md decision D-A for why
# this is one self-contained container (single-silo localhost clustering, ephemeral data dir).

# ---------------------------------------------------------------------------
# Stage 1 — SPA build (bun only, never npm — see CLAUDE.md)
# ---------------------------------------------------------------------------
FROM oven/bun:1.4 AS web-build
WORKDIR /src

# The console is a member of the repo's bun WORKSPACE (root package.json): @streamsforge/client and
# @streamsforge/tanstack-db are linked, not copied, and web's own prebuild compiles them. So the build
# context is the workspace root, not web/ — copying web/ alone leaves those two dependencies unresolvable.
# Manifests first, so this layer caches across source-only changes.
COPY package.json bun.lock ./
COPY web/package.json web/
COPY clients/typescript/package.json clients/typescript/
COPY clients/tanstack-db/package.json clients/tanstack-db/
COPY clients/react/package.json clients/react/
RUN bun install --frozen-lockfile

COPY web/ web/
COPY clients/ clients/
RUN bun run --cwd web build
# → /src/web/dist

# ---------------------------------------------------------------------------
# Stage 2 — publish StreamsForge.Host (and its shared/ + orleans project references)
# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS dotnet-build
WORKDIR /src

# Only the pieces StreamsForge.Host's ProjectReferences touch — shared/ + orleans/ — keeping the
# dapr/ and web/ trees out of this stage's context entirely (irrelevant to this publish and, for
# web/, already built by stage 1).
COPY shared/ shared/
COPY orleans/ orleans/
COPY external/ycs/ external/ycs/
# plugins/ holds the built-in server plugins (Quant, Fix) the host csproj builds and publishes into
# /app/publish/plugins — see StreamsForge.Host.csproj's PublishBuiltInPlugins target.
COPY plugins/ plugins/

# -r linux-x64: orleans/src/StreamsForge.Host/Publish.props (imported by the csproj when it exists)
# turns on PublishSingleFile+SelfContained for any `dotnet publish`, defaulting the RID to linux-x64
# when unset — but that default only fires once MSBuild has already picked a RID-less restore graph,
# so pass it explicitly here too rather than lean on the fallback.
RUN dotnet publish orleans/src/StreamsForge.Host -c Release -r linux-x64 -o /app/publish

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
COPY --from=web-build /src/web/dist ./web-dist
COPY orleans/docs ./docs

# Wishlist #19: ship the .proto contract alongside the image, so a non-.NET client can compile
# against the exact version this image runs instead of vendoring a file out of a host project.
# Cheap (a handful of small text files) — no separate publishing pipeline built for it. Pull with
# `docker cp <container>:/app/protos - | tar -x`, or `docker run --rm --entrypoint cat <image>
# /app/protos/streamsforge.proto`.
COPY orleans/src/StreamsForge.Host/Protos ./protos

# Absolute paths so StreamsForgeApiOptions resolves them regardless of ContentRootPath — see
# Program.cs (Web:Dist was already configurable; Docs:File is the additive key this wave added,
# mirroring the Dapr host's existing pattern one-for-one).
ENV Web__Dist=/app/web-dist
ENV Docs__File=/app/docs/index.html
ENV ASPNETCORE_ENVIRONMENT=Production
# Data dir defaults to "./data" (appsettings.json) — relative to the working directory below,
# so it lands at /app/data: ephemeral inside the container. A cold start reseeds the demo
# catalog (admin/editor/viewer + sample sources/pipelines/tables) — expected, documented in
# deploy/README.md, and acceptable for a demo platform (persistent storage is future work).

# Wishlist #19: --urls below now makes Program.cs bind Kestrel's SINGLE $PORT endpoint with
# HttpProtocols.Http1AndHttp2 (still exactly one listening port — plan 007 decision D-A's "one port
# per service" is unchanged), instead of skipping gRPC entirely the way it used to (the pre-#19
# two-Kestrel-endpoint block only ran when --urls was UNSET, so this image never opened a gRPC
# listener at all).
#
# THAT ALONE DOES NOT MAKE gRPC REACHABLE HERE on a CLEARTEXT endpoint, though — measured against a
# real running instance while building this: Kestrel does not multiplex HTTP/1.1 and HTTP/2 on a
# single cleartext endpoint (no TLS) in any shipped ASP.NET Core version; it always falls back to
# HTTP/1.1 and a genuine h2c "prior knowledge" gRPC call times out (open, unresolved upstream:
# dotnet/aspnetcore#56984). REST/SignalR on $PORT are completely unaffected either way. Two paths
# close the gap:
#
# (1) PUT A CERTIFICATE ON THIS ENDPOINT — this now works. Change the ENTRYPOINT's --urls scheme to
#     https and give Kestrel a certificate via the same Kestrel:Certificates:Default section the
#     two-listener dev path uses (a PEM pair, PFX, or OS-store lookup — see
#     shared/StreamsForge.AppCore/Net/OutboundTls.cs's sibling, orleans/src/StreamsForge.Host/
#     Program.cs, for the branch that reads it), e.g.:
#         ENTRYPOINT ["/bin/sh", "-c", "exec ./StreamsForge.Host --urls https://0.0.0.0:${PORT:-8080}"]
#     with Kestrel__Certificates__Default__Path and Kestrel__Certificates__Default__KeyPath set (see
#     compose.yaml's commented-out TLS block for the exact env vars and a cert volume mount). With a
#     real certificate, Http1AndHttp2 gets real TLS+ALPN negotiation and gRPC becomes reachable on
#     this single port — note this is the --urls branch specifically: Tls:Enabled (the flag that turns
#     TLS on for the OTHER, two-listener branch) has no effect here, since the https:// scheme in
#     --urls is what switches this branch's Kestrel to TLS. Full recipe and the outbound-trust axis
#     (Tls:TrustedCaPath, for connections this container makes outward) are documented in
#     orleans/docs/index.html § TLS and TRANSPORTS.md's "Outbound TLS trust" section.
# (2) Deploy the Cloud Run SERVICE with HTTP/2 end-to-end (`--use-http2` / a port named "h2c" — an
#     infra-level `gcloud run deploy` flag, not something this file controls), which makes Cloud Run's
#     own edge terminate the client's TLS and forward EVERY request to the container as HTTP/2
#     cleartext, so the container's single Http1AndHttp2 listener only ever has to speak one protocol
#     for real and REST traffic rides along on it transparently. See deploy/orleans/service.yaml for
#     the related note on ASPNETCORE_FORWARDEDHEADERS_ENABLED when Cloud Run itself terminates TLS.
EXPOSE 8080

# Cloud Run injects PORT (default 8080 locally/compose too); shell form so ${PORT} expands, and
# `exec` so the app becomes PID 1 (proper SIGTERM handling on `docker stop` / Cloud Run scale-down).
#
# Publish.props (see orleans/src/StreamsForge.Host/Publish.props) makes the publish above a
# self-contained single-file native executable, not a framework-dependent .dll — there is no
# StreamsForge.Host.dll in /app/publish to hand to `dotnet`. Run the executable directly instead.
ENTRYPOINT ["/bin/sh", "-c", "exec ./StreamsForge.Host --urls http://0.0.0.0:${PORT:-8080}"]
