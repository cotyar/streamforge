using Microsoft.AspNetCore.Server.Kestrel.Core;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Discovery;
using StreamForge.Connectors.Database;
using StreamForge.Connectors.Fix;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Facades;
using StreamForge.Dapr.Host.Ingest;
using StreamForge.Dapr.Host.Lifecycle;
using StreamForge.Dapr.Host.Services;
using StreamForge.Dapr.Host.Streaming;

var builder = WebApplication.CreateBuilder(args);

// Dapr flavor owns :5399 (REST/SignalR/SPA, HTTP/1.1) — gRPC (:5499) is reserved but not served by this
// process yet (phase 2, decision D-F); the Dapr sidecar's own HTTP/gRPC ports (3599/4599) are separate
// processes started by tools/run.sh. ASPNETCORE_URLS (if set) wins and skips the explicit Kestrel
// endpoint below, exactly like the Orleans host's Program.cs.
if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    var httpPort = builder.Configuration.GetValue("Http:Port", 5399);

    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.ListenLocalhost(httpPort, o => o.Protocols = HttpProtocols.Http1);
    });
}

builder.Services.AddStreamForgeApi(builder.Configuration);

// Actor state + method-invocation payloads serialize via System.Text.Json with default settings
// (enums as ints) — this is an internal actor wire (RegistryActor/UserStoreActor's own state, and the
// request/response records in Actors/I*Actor.cs), independent of the public REST/SignalR JSON contract
// (StreamForgeApiExtensions.AddStreamForgeApi configures JsonStringEnumConverter separately for that
// surface — see dapr/ARCHITECTURE.md's serialization note).
builder.Services.AddActors(options =>
{
    options.Actors.RegisterActor<RegistryActor>();
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
// Plan 018-D: same deadline, same shape, same reasoning — the out-of-core FIX session transport's
// only call site, registering the `fix` inbound kind before any source can open one.
FixConnectors.RegisterAll();
// Same deadline, same shape: the pricing scalars (QLNet-backed Black family, closed-form
// flat-curve bond/swap/FX) register into the Engine's SqlFunctions seam so the Engine itself
// never links a pricing library. Must precede anything that compiles SQL.
StreamForge.Quant.QuantFunctions.RegisterAll();

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

// Host-specific facts StreamForgeApiOptions carries so the shared endpoints stay byte-identical across
// runtimes (plan 005 W3, decision D-B). Per decision D-F (this flavor has no static gRPC serving):
//   - ProtosDir points at a directory that doesn't exist — /api/meta/protos/static already guards each
//     file with File.Exists and returns an empty list, so the response SHAPE is unchanged, just empty.
//   - GrpcStaticServices is empty (gRPC serving is phase 2 here); GrpcPort is still reported (5499,
//     reserved) so the API Explorer UI can show it as "not yet serving" rather than omitting it. Plan
//     016 wave 5's GET /api/meta/instance reads GrpcStaticServices being empty as "omit the grpc
//     endpoint key entirely" for exactly this reason — see MetaEndpoints' comment on servesGrpc.
//   - DocsFilePath serves the SAME flavor-aware docs the Orleans host serves (orleans/docs/ covers both
//     runtimes since plan 006's docs sync — the original W4 "no /docs here" descope is obsolete), so the
//     SPA's Documentation link works on :5399 too. Sibling pages (comparison.html) come along for free.
var apiOptions = new StreamForgeApiOptions(
    ProtosDir: Path.Combine(app.Environment.ContentRootPath, "Protos"),
    GrpcPort: app.Configuration.GetValue("Grpc:Port", 5499),
    GrpcStaticServices: [],
    DocsFilePath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Docs:File"] ?? Path.Combine("..", "..", "..", "orleans", "docs", "index.html"))),
    SpaDistPath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Web:Dist"] ?? Path.Combine("..", "..", "..", "web", "dist"))),
    Flavor: "dapr",
    // Plan 016 wave 5: same config key, same default as the Orleans host — this flavor keeps its
    // catalog/actor state in Redis, not here, but still needs somewhere to persist its one identity
    // file (see StreamForgeApiOptions.DataDir's own doc comment for why that is honest even though the
    // directory otherwise does nothing for this flavor).
    DataDir: app.Configuration["DataDir"] ?? "./data",
    InstanceName: app.Configuration["InstanceName"] ?? "",
    Version: app.Configuration["Version"] ?? "");

app.MapStreamForgeApi(apiOptions);

// Dapr actor HTTP endpoints the sidecar calls for activation/deactivation/method-invocation.
app.MapActorsHandlers();

// Pub/sub subscription handshake (the sidecar GETs /dapr/subscribe on startup) — the five fixed envelope
// topics (decision D-D) are mapped by StreamingRuntimeSetup.MapTopicEndpoints (W5-B) ahead of this call,
// so MapSubscribeHandler's discovery pass picks them all up.
app.UseCloudEvents();
StreamingRuntimeSetup.MapTopicEndpoints(app);
app.MapSubscribeHandler();

app.Run();
