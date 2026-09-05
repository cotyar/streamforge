using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Plugins;
using StreamsForge.AppCore.Discovery;
using StreamsForge.AppCore.Net;
using StreamsForge.Connectors.Database;
using StreamsForge.Host.Grpc;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Ingest;
using StreamsForge.Dapr.Host.Lifecycle;
using StreamsForge.Dapr.Host.Services;
using StreamsForge.Dapr.Host.Streaming;

var builder = WebApplication.CreateBuilder(args);

// Dapr flavor owns :5399 (REST/SignalR/SPA, HTTP/1.1) and :5499 (gRPC — HTTP/2 only). Plan 025 G2
// closed decision D-F's "phase 2": 5499 is SERVED now, by the same seven services the Orleans host maps
// (shared/StreamsForge.Api/Grpc/**). The Dapr sidecar's own HTTP/gRPC ports (3599/4599) are a separate
// process started by tools/run.sh and are unrelated to either of these. ASPNETCORE_URLS (if set) wins
// and takes the single-port branch below, exactly like the Orleans host's Program.cs.
//
// Everything about the two branches, TLS included, is deliberately the same shape as
// orleans/src/StreamsForge.Host/Program.cs — read that file's much longer comments for the full
// reasoning; only what is specific to this flavor is repeated here.
var envPort = builder.Configuration.GetValue<int?>("PORT");
var httpPort = builder.Configuration.GetValue("Http:Port", envPort ?? 5399);
// Resolved out here rather than inside the branch, because StreamsForgeApiOptions below reports this
// same number to clients — computing it twice is how the reported port and the bound port drift apart.
var grpcPort = builder.Configuration.GetValue("Grpc:Port", envPort is { } p ? p + 100 : 5499);

// Outbound TLS trust, configured ONCE for every client this process dials with (url source, http sink,
// federated grpc source, peer probes, OpenAPI derive). Must run before anything can dial: each of those
// call sites captures its handler in a static Lazy the first time it is used, and OutboundTls.Configure
// throws rather than silently apply to only some of them. Until plan 025 this flavor never called it at
// all, which meant Tls:TrustedCaPath and Tls:AcceptAnyCertificate were silently ignored here — a
// federated grpc source pointed at a privately-signed peer simply failed with no way to fix it.
OutboundTls.Configure(
    builder.Configuration[OutboundTls.TrustedCaPathKey],
    builder.Configuration.GetValue(OutboundTls.AcceptAnyCertificateKey, false));

var tlsEnabled = builder.Configuration.GetValue("Tls:Enabled", false);

if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    // Fail fast, loudly: booting cleartext with Tls:Enabled=true in the config is a security
    // misconfiguration that looks exactly like a working server.
    if (tlsEnabled
        && string.IsNullOrWhiteSpace(builder.Configuration["Kestrel:Certificates:Default:Path"])
        && string.IsNullOrWhiteSpace(builder.Configuration["Kestrel:Certificates:Default:Subject"]))
    {
        throw new InvalidOperationException(
            "Tls:Enabled is true but no server certificate is configured. Set either "
          + "Kestrel:Certificates:Default:Path (+ :KeyPath for a PEM pair, or + :Password for a PFX) "
          + "or Kestrel:Certificates:Default:Subject (+ :Store) so Kestrel has a certificate to serve. "
          + "For a development pair: tools/tls/dev-cert.sh <out-dir> [host-or-ip ...], which prints the "
          + "exact arguments to pass.");
    }

    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        // IPAddress.Any, not ListenLocalhost (which is what this host did until plan 025) and NOT
        // ListenAnyIP. Two separate reasons, both already paid for on the Orleans side:
        //
        // - Not loopback: a loopback-only listener is unreachable from outside this process's network
        //   namespace. That was survivable while this flavor served only REST to a browser on the same
        //   machine, but it makes a published container port a dead end — and deploy/dapr/compose.yaml
        //   publishes both of these — and it makes the gRPC port unreachable from any peer, which is
        //   the entire point of serving it.
        // - Not ListenAnyIP: it binds the IPv6 wildcard in DUAL-STACK mode, and on this platform an
        //   IPv4-mapped accept throws out of Kestrel's accept loop UNHANDLED, killing the listener and
        //   taking the host down with it. See the Orleans Program.cs comment for the measurement. The
        //   stated cost is the same here: this host does not answer on IPv6.
        kestrel.Listen(IPAddress.Any, httpPort, o =>
        {
            o.Protocols = HttpProtocols.Http1;
            if (tlsEnabled)
            {
                o.UseHttps();
            }
        });
        kestrel.Listen(IPAddress.Any, grpcPort, o =>
        {
            // HTTP/2 only. Cleartext that is prior-knowledge h2c; with TLS it is ALPN-negotiated h2,
            // which is what a gRPC client dialling https:// expects anyway.
            o.Protocols = HttpProtocols.Http2;
            if (tlsEnabled)
            {
                o.UseHttps();
            }
        });
    });
}
else
{
    // Single-port branch (--urls / PORT), same as the Orleans host: Http1AndHttp2 on the one endpoint.
    // On a CLEARTEXT endpoint Kestrel falls back to HTTP/1.1 and the gRPC half simply is not served
    // (dotnet/aspnetcore#56984); with `--urls https://…` plus a Kestrel:Certificates:Default section it
    // becomes real ALPN multiplexing and both halves work on one port. Read the Orleans Program.cs's
    // long note in this same branch before changing anything here.
    builder.WebHost.ConfigureKestrel(kestrel =>
        kestrel.ConfigureEndpointDefaults(o => o.Protocols = HttpProtocols.Http1AndHttp2));
}

// IMPORTANT, and it has no Orleans equivalent: with TLS on the app port, the DAPR SIDECAR must be told
// so. daprd calls back into this app for actor activation/deactivation/method-invocation and for every
// pub/sub topic delivery, and it speaks plain http:// unless started with `--app-protocol https`. Get
// this wrong and the host looks healthy over curl while every actor call and every topic delivery fails
// — see dapr/tools/run.sh, which passes it through DAPR_RUN_EXTRA_ARGS. daprd also does not verify the
// app's certificate on that channel, so a self-signed development pair needs nothing else.

builder.Services.AddStreamsForgeApi(builder.Configuration);
// Plan 025 G2: the gRPC surface, shared with the Orleans host (shared/StreamsForge.Api/Grpc/**). This is
// AddGrpc() plus the ONE dependency of those services that is genuinely runtime-specific — the live
// per-entity subscription primitive the two streaming services need. On this flavor that is
// EntityStreamFanout, registered by StreamingRuntimeSetup.AddServices below alongside the other sinks,
// because it IS one (see that class's doc).
builder.Services.AddStreamsForgeGrpc();

// Actor state + method-invocation payloads serialize via System.Text.Json with default settings
// (enums as ints) — this is an internal actor wire (RegistryActor/UserStoreActor's own state, and the
// request/response records in Actors/I*Actor.cs), independent of the public REST/SignalR JSON contract
// (StreamsForgeApiExtensions.AddStreamsForgeApi configures JsonStringEnumConverter separately for that
// surface — see dapr/ARCHITECTURE.md's serialization note).
builder.Services.AddActors(options =>
{
    options.Actors.RegisterActor<RegistryActor>();
    // Plan 021: the environment directory — see EnvironmentRegistryActor's own doc comment for why it is
    // one more singleton actor registered right next to RegistryActor rather than folded into it.
    options.Actors.RegisterActor<EnvironmentRegistryActor>();
    options.Actors.RegisterActor<UserStoreActor>();
    options.Actors.RegisterActor<AccessPolicyActor>();
    // Plan 015 W4-C. Two types, not three: AuditLogActor is BOTH the per-day shard (audit:yyyyMMdd) and
    // the day index (audit:index) — see IAuditLogActor for why the index lives at a reserved id of the
    // same type rather than in an actor type of its own.
    options.Actors.RegisterActor<ApprovalActor>();
    options.Actors.RegisterActor<AuditLogActor>();
    GeneratorRuntimeSetup.RegisterActors(options);
    ConnectorRuntimeSetup.RegisterActors(options);
    PipelineRuntimeSetup.RegisterActors(options);
    TableRuntimeSetup.RegisterActors(options);
    TableHistoryRuntimeSetup.RegisterActors(options);
    // See Actors/ActorProxyDefaults.cs for the client-side half of this decision — both sides of every
    // actor call in this project must agree on System.Text.Json, not the SDK's legacy DataContract default.
    options.UseJsonSerialization = true;
});

builder.Services.AddSingleton<ILifecycleOrchestrator, NoopLifecycleOrchestrator>();
builder.Services.AddDaprFacades();
builder.Services.AddHostedService<CatalogInitializationService>();
// Wave seams (see the *RuntimeSetup classes) — registered after the Noop orchestrator so a real
// ILifecycleOrchestrator registered inside wins. ConnectorRuntimeSetup.AddServices also registers the
// real IConnectorStatusFacade (see that method's doc comment for why it isn't in AddDaprFacades above) —
// order relative to the other *RuntimeSetup calls doesn't matter (it doesn't override anything), placed
// right after GeneratorRuntimeSetup since both are source-lifecycle wave seams.
GeneratorRuntimeSetup.AddServices(builder.Services);
ConnectorRuntimeSetup.AddServices(builder.Services);
IngestRuntimeSetup.AddServices(builder.Services);
StreamingRuntimeSetup.AddServices(builder.Services);
PipelineRuntimeSetup.AddServices(builder.Services);
TableRuntimeSetup.AddServices(builder.Services);
TableHistoryRuntimeSetup.AddServices(builder.Services);

// Plan 014-I: the out-of-core database connectors' only call site. InboundTransports/PolledTransports
// both document "before any source starts" as the registration deadline; nothing in this process can
// start a source before builder.Build() returns and the hosted services above get to Run(), so anywhere
// before this line satisfies it — here, immediately before Build(), keeps it visibly paired with the
// rest of the *RuntimeSetup wiring above rather than buried at the top of the file.
DatabaseConnectors.RegisterAll();
// Quant (pricing scalars) and Fix (the `fix`/`fix-duplex` transports) used to register here directly;
// they are now install-time server plugins under plugins/, loaded by StreamsForgePlugins.LoadFrom below
// like any other out-of-tree connector.

// Out-of-tree connectors, installed rather than referenced: every IStreamsForgePlugin in `plugins/` next
// to the binaries (or wherever `Plugins:Path` points) registers its own kinds here, at the same
// "before any source starts" deadline the three lines above satisfy — and AFTER them, so a plugin
// shipping a kind name a built-in already owns loses to the built-in instead of shadowing it. The report
// is logged, never thrown: a plugin that fails to load must not keep this host from starting, and the
// line is the only diagnostic an operator has for "I copied the DLL and nothing happened".
foreach (var line in StreamsForge.AppCore.Plugins.StreamsForgePlugins.LoadFrom(builder.Configuration["Plugins:Path"]))
{
    Console.WriteLine($"[plugins] {line}");
}
// A plugin that implements IStreamsForgeWebPlugin (StreamsForge.Api) gets its two host hooks here:
// services now, endpoints after Build(). Loaded plugins are also what a runtime adds to its own type
// manifest (Orleans: grains/serializers) — see the UseOrleans block.
StreamsForge.Api.Plugins.StreamsForgePluginHosting.ConfigureServices(builder.Services, builder.Configuration);

// Plan 016 wave 5: same shape, same config keys as the Orleans host's Program.cs — see that file's
// comment for why "Discovery:Peers" is a section (binds as an array from appsettings.json, CLI
// `--Discovery:Peers:0:Name ...`, or `DISCOVERY__PEERS__0__NAME` env vars alike) and why binding
// straight onto PeerRecord is enough. PeerDirectory is process-static, so this line is the whole of
// this flavor's peer wiring; the federated `grpc` source's driver (ConnectorActor) reads it the same
// way ConnectorGrain does on the other flavor.
PeerDirectory.Configure(builder.Configuration.GetSection("Discovery:Peers").Get<List<PeerRecord>>() ?? []);

// Plan 016 wave 6: named external endpoints, read from the SAME configuration providers by the same
// rules — `--Endpoints:primary-oltp=host:5432` on the command line, Endpoints__PRIMARY_OLTP as an env
// var, or an "Endpoints" object in appsettings.json. A flat name->value map rather than a section of
// objects, because that is all an alias is. Deliberately NOT read from the catalog: a document that
// carried environment-specific endpoints would defeat the indirection it is asking for, and keeping the
// map here is what lets one exported catalog import into prod and dev alike.
NamedEndpoints.Configure(
    builder.Configuration.GetSection("Endpoints").GetChildren()
        .Where(c => c.Value is not null)
        .Select(c => new KeyValuePair<string, string>(c.Key, c.Value!)));

var app = builder.Build();

// Re-emitted through the real logging pipeline (OutboundTls.Configure ran before it existed). A
// development-only escape hatch that disables outbound certificate validation entirely must be visible
// in whatever log an operator actually reads.
if (builder.Configuration.GetValue(OutboundTls.AcceptAnyCertificateKey, false))
{
    app.Logger.LogWarning(
        "{Key} is TRUE: every outbound HTTPS/gRPC connection from this instance accepts ANY server "
      + "certificate. Development only.",
        OutboundTls.AcceptAnyCertificateKey);
}

// Host-specific facts StreamsForgeApiOptions carries so the shared endpoints stay byte-identical across
// runtimes (plan 005 W3, decision D-B). Plan 025 G2 retired decision D-F's "no gRPC on this flavor":
//   - ProtosDir is AppContext.BaseDirectory/Protos, the same as the Orleans host — the .proto files live
//     in shared/StreamsForge.Api now and that project copies them into every referencing project's
//     output directory, so /api/meta/protos/static answers with the real files here too instead of the
//     empty list decision D-F left it returning.
//   - GrpcStaticServices is the shared list, so GET /api/meta/instance advertises `grpc` in its
//     capabilities and its endpoints — honestly, because MapStreamsForgeGrpc below maps exactly those
//     services. The list and the mapping are one definition; see StreamsForgeGrpc.
//   - DocsFilePath serves the SAME flavor-aware docs the Orleans host serves (orleans/docs/ covers both
//     runtimes since plan 006's docs sync — the original W4 "no /docs here" descope is obsolete), so the
//     SPA's Documentation link works on :5399 too. Sibling pages (comparison.html) come along for free.
var apiOptions = new StreamsForgeApiOptions(
    ProtosDir: Path.Combine(AppContext.BaseDirectory, "Protos"),
    GrpcPort: grpcPort,
    GrpcStaticServices: StreamsForgeGrpc.StaticServiceNames,
    DocsFilePath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Docs:File"] ?? Path.Combine("..", "..", "..", "orleans", "docs", "index.html"))),
    SpaDistPath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Web:Dist"] ?? Path.Combine("..", "..", "..", "web", "dist"))),
    Flavor: "dapr",
    // Plan 016 wave 5: same config key, same default as the Orleans host — this flavor keeps its
    // catalog/actor state in Redis, not here, but still needs somewhere to persist its one identity
    // file (see StreamsForgeApiOptions.DataDir's own doc comment for why that is honest even though the
    // directory otherwise does nothing for this flavor).
    DataDir: app.Configuration["DataDir"] ?? "./data",
    InstanceName: app.Configuration["InstanceName"] ?? "",
    Version: app.Configuration["Version"] ?? "");

app.MapStreamsForgeApi(apiOptions);
app.MapPluginEndpoints();

// gRPC control plane + streaming + ingest + dynamic reflection — served on the HTTP/2-only endpoint
// configured above (Grpc:Port, default 5499); doesn't share the REST/SignalR/SPA port. Identical service
// set to the Orleans host: the mapping lives in shared/StreamsForge.Api so the two cannot diverge.
app.MapStreamsForgeGrpc();

// Dapr actor HTTP endpoints the sidecar calls for activation/deactivation/method-invocation.
app.MapActorsHandlers();

// Pub/sub subscription handshake (the sidecar GETs /dapr/subscribe on startup) — the five fixed envelope
// topics (decision D-D) are mapped by StreamingRuntimeSetup.MapTopicEndpoints (W5-B) ahead of this call,
// so MapSubscribeHandler's discovery pass picks them all up.
app.UseCloudEvents();
StreamingRuntimeSetup.MapTopicEndpoints(app);
app.MapSubscribeHandler();

app.Run();
