namespace StreamForge.Api;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W3: host-specific facts <see cref="StreamForgeApiExtensions.MapStreamForgeApi"/>
/// needs to serve the identical REST/SignalR/SPA surface on either runtime. Everything else (routes,
/// policies, JSON shapes, hub route/method names) lives verbatim in the shared endpoint/hub code —
/// only these facts differ between the Orleans host (today) and a future Dapr host.
/// </summary>
/// <param name="ProtosDir">Directory containing the hand-authored static .proto files
/// (streamforge.proto / streamforge_dynamic.proto) served raw by GET /api/meta/protos/static. The
/// Orleans host points this at its own Protos/ folder; a Dapr host without static gRPC serving (plan
/// decision D-F) can point at an empty directory — the endpoint already tolerates missing files.</param>
/// <param name="GrpcPort">Reported in GET /api/meta/grpc's response (informational only — this
/// project maps no gRPC endpoints itself; the host maps its own gRPC services on this port).</param>
/// <param name="GrpcStaticServices">The fixed static gRPC service name list reported by GET
/// /api/meta/grpc. Orleans populates today's six control-plane + reflection services; per decision
/// D-F, a Dapr host keeps the response shape but reports an empty list (gRPC serving is phase 2 there).</param>
/// <param name="DocsFilePath">Absolute path to docs/index.html for GET /docs, or null to not map the
/// route at all (mirrors the Orleans host's existing File.Exists guard). Per decision D-F, /docs stays
/// Orleans-served — a Dapr host passes null.</param>
/// <param name="SpaDistPath">Absolute path to the built console SPA (web/dist) served as static files
/// + SPA fallback, or null to skip SPA serving entirely (mirrors the Orleans host's existing
/// Directory.Exists guard).</param>
/// <param name="Flavor">Runtime flavor name ("orleans" / "dapr") reported by the anonymous
/// GET /healthz endpoint (plan 007 W0) so the admin app, compose healthchecks, and Cloud Run
/// startup probes can tell instances apart. Optional (additive evolution) — defaults keep
/// pre-007 construction sites compiling unchanged.</param>
public sealed record StreamForgeApiOptions(
    string ProtosDir,
    int GrpcPort,
    IReadOnlyList<string> GrpcStaticServices,
    string? DocsFilePath,
    string? SpaDistPath,
    string Flavor = "unknown");
