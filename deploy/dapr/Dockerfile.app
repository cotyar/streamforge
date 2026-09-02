# Plan 007 W1B — Dapr flavor app image. Multi-stage: bun builds the SPA, the .NET SDK publishes the
# host, and a slim ASP.NET runtime image serves everything. Build context is the REPO ROOT (not
# deploy/dapr/) so this stage can COPY across shared/, dapr/src/, web/, and orleans/docs/ — see
# ../../.dockerignore for what's excluded from that context.
#
#   docker build -f deploy/dapr/Dockerfile.app -t streamsforge-dapr-app .
#
# Runs identically under `docker compose -f deploy/dapr/compose.yaml up` and as the ingress container of
# the Cloud Run multi-container service in deploy/dapr/service.yaml (see deploy/dapr/README.md).

# ---- Stage 1: web SPA (bun only — never npm, see AGENTS.md) ----------------------------------------
FROM oven/bun:1.4 AS web-build
WORKDIR /src
# Workspace root, not web/ — see the same stage in deploy/orleans/Dockerfile for why.
COPY package.json bun.lock ./
COPY web/package.json web/
COPY clients/typescript/package.json clients/typescript/
COPY clients/tanstack-db/package.json clients/tanstack-db/
COPY clients/react/package.json clients/react/
RUN bun install --frozen-lockfile
COPY web/ web/
COPY clients/ clients/
RUN bun run --cwd web build

# ---- Stage 2: .NET publish --------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS dotnet-build
WORKDIR /src
# Preserve the repo's relative directory layout — StreamsForge.Dapr.Host.csproj's ProjectReference
# entries point at ..\..\..\shared\* (dapr/src/StreamsForge.Dapr.Host -> repo root -> shared), so the
# copied tree must keep the same shape for those relative paths to resolve inside the build context.
COPY shared/ shared/
COPY dapr/src/ dapr/src/
# plugins/ holds the built-in server plugins (Quant, Fix) the host csproj builds and publishes into
# /app/publish/plugins — see StreamsForge.Dapr.Host.csproj's PublishBuiltInPlugins target.
COPY plugins/ plugins/
WORKDIR /src/dapr/src/StreamsForge.Dapr.Host
# -r linux-x64 on both: Publish.props (imported by this csproj when it exists) turns on
# PublishSingleFile+SelfContained for any `dotnet publish`, defaulting the RID to linux-x64 when
# unset — but a RID-specific publish needs a RID-specific restore graph, so pin it at restore time
# too rather than lean on the publish-time fallback picking a mismatched (or absent) one.
RUN dotnet restore StreamsForge.Dapr.Host.csproj -r linux-x64
RUN dotnet publish StreamsForge.Dapr.Host.csproj -c Release -r linux-x64 -o /app/publish --no-restore

# ---- Stage 3: runtime ----------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

COPY --from=dotnet-build /app/publish ./
# Baked-in static assets (decision D-A): the SPA and the shared docs page, pointed at by the two config
# keys StreamsForge.Dapr.Host.Program.cs already reads (Web:Dist / Docs:File, env-mapped as Web__Dist /
# Docs__File — ASP.NET Core's double-underscore convention for nested config keys).
COPY --from=web-build /src/web/dist ./web-dist
COPY orleans/docs/ ./docs/

COPY deploy/dapr/healthcheck.sh ./healthcheck.sh
COPY deploy/dapr/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./healthcheck.sh ./entrypoint.sh

ENV Web__Dist=/app/web-dist
ENV Docs__File=/app/docs/index.html
# The Dapr .NET SDK's default DaprClientBuilder reads these two env vars to find its sidecar — see
# Actors/GeneratorRuntimeSetup.cs's services.AddDaprClient() call and every ActorProxy.Create<T>() site.
# Values match the daprd container's own --dapr-http-port/--dapr-grpc-port args (deploy/dapr/compose.yaml,
# deploy/dapr/service.yaml) — all four containers share one network namespace, so "localhost" resolves.
ENV DAPR_HTTP_PORT=3500
ENV DAPR_GRPC_PORT=50001

EXPOSE 8080

# No curl/wget in this image (Ubuntu-based mcr.microsoft.com/dotnet/aspnet:10.0 ships neither) — bash's
# /dev/tcp is used instead (healthcheck.sh). start-period is generous: entrypoint.sh starts `dotnet`
# immediately, but may then restart it once (a fast, sub-second blip) after waiting up to
# DAPR_WAIT_TIMEOUT_S (default 100s) for daprd to report ready — see that script's own comment for why
# this restart exists and why it's safe.
HEALTHCHECK --interval=5s --timeout=3s --start-period=110s --retries=6 \
    CMD ["bash", "/app/healthcheck.sh"]

# entrypoint.sh starts `dotnet` immediately (this is what unblocks daprd's own "waiting for the app"
# gate — see that script's header comment for the live-verified deadlock this avoids), then restarts
# it exactly once after confirming daprd's own health, guaranteeing
# CatalogInitializationService's one-shot (no-retry) catalog/users seed a clean attempt with daprd
# definitely up.
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
