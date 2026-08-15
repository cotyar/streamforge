using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans;
using Orleans.Hosting;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Connectors.Database;
using StreamForge.Host.Facades;
using StreamForge.Host.Grpc;
using StreamForge.Host.Grpc.Dynamic;
using StreamForge.Host.Services;
using StreamForge.Host.Storage;
using StreamForge.Host.Streaming;

var builder = WebApplication.CreateBuilder(args);

// Co-hosted process listens on http://localhost:5199 (REST/SignalR/SPA, HTTP/1.1) and
// http://localhost:5299 (gRPC, cleartext h2c — HTTP/2-only, no ALPN without TLS) by default;
// ASPNETCORE_URLS (if set) wins and skips both explicit Kestrel endpoints below.
//
// PORT (the PaaS convention: Cloud Run, Heroku, fly.io all set it) moves the HTTP port, and the gRPC
// port follows at PORT+100 — the same +100 relationship the two defaults already have. Without this,
// `PORT=6199 dotnet run` silently still bound 5199/5299, which on a developer machine means landing on
// whatever else already owns those ports. Http:Port / Grpc:Port still win where they are set, so an
// explicit pair can always split the two apart.
var envPort = builder.Configuration.GetValue<int?>("PORT");
var httpPort = builder.Configuration.GetValue("Http:Port", envPort ?? 5199);
// Resolved out here, not inside the `if`, because StreamForgeApiOptions below reports this same number
// to clients — computing it twice is how the reported port and the bound port drift apart.
var grpcPort = builder.Configuration.GetValue("Grpc:Port", envPort is { } p ? p + 100 : 5299);

if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.ListenLocalhost(httpPort, o => o.Protocols = HttpProtocols.Http1);
        kestrel.ListenLocalhost(grpcPort, o => o.Protocols = HttpProtocols.Http2);
    });
}

// Streams:Transport selects the stream transport. "pull" (DEFAULT) is Orleans' stock memory-stream
// path, untouched. "push" swaps in StreamForge.Host.Streaming's in-process push bus under the SAME
// provider name — every producer/consumer call site is identical in both modes (see PushStreamBus and
// PushStreamProvider's class docs for the ordering/deadlock/backpressure reasoning).
var streamTransport = (builder.Configuration["Streams:Transport"] ?? "pull").Trim().ToLowerInvariant();
if (streamTransport is not ("pull" or "push"))
{
    throw new InvalidOperationException(
        $"Unknown Streams:Transport '{streamTransport}'. Valid values: 'pull' (default, Orleans memory streams) or 'push'.");
}

builder.Host.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    if (streamTransport == "push")
    {
        // PUSH: no pulling agents at all — publish is a non-blocking channel write, one pump task per
        // subscriber delivers (into the grain's own turn, via a grain extension, for grain subscribers).
        // Streams:PushCapacity bounds each subscriber's backlog; overflow drops the incoming item and
        // logs a throttled counter (see PushStreamBus's backpressure paragraph).
        siloBuilder.AddPushStreams(
            StreamConstants.ProviderName,
            builder.Configuration.GetValue("Streams:PushCapacity", 10_000));
    }
    else
    {
        // Memory streams are PULL-based: pulling agents poll the in-memory queues every
        // GetQueueMessagesTimerPeriod (Orleans default 100ms). Every stream hop therefore adds
        // Uniform(0, period) latency — the table path pays it twice (sources → TableGrain,
        // tableDelta → SignalR bridge), which is exactly the 122ms p50 / 209ms p90 the 005-W9
        // benchmark measured vs Dapr's push-based 7ms. Streams:PullPeriodMs makes the cadence
        // tunable; default keeps Orleans' stock behavior.
        var pullPeriodMs = builder.Configuration.GetValue("Streams:PullPeriodMs", 100);
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName, configurator =>
            configurator.ConfigurePullingAgent(ob => ob.Configure(o =>
                o.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(pullPeriodMs))));
    }
    siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
    siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);

    // Plan 011 wave D1 — HOW LONG AN IDLE SHARD STAYS RESIDENT.
    //
    // TableShardGrain is the one grain in the table path that does NOT call DelayDeactivation, so it is
    // the one grain Orleans' activation collector can actually reclaim — and how quickly it does is the
    // difference between "sharding bounds resident memory" and "sharding is a nice API". Orleans'
    // default CollectionAge is 15 minutes, which is a reasonable default for grains generally and far
    // too long for a soak run or a live check to observe anything, so it is configurable here.
    //
    // Shards:IdleSeconds sets the shard class's own collection age. Shards:QuantumSeconds sets the
    // silo-wide scan interval, which Orleans requires to be strictly SMALLER than any collection age;
    // its default (60s) is left alone unless asked for, since it applies to every grain type. Both are
    // pass-through knobs with no behavioral default change: at the default 120s a shard becomes eligible
    // after two minutes idle and is collected on the next 60s scan.
    var shardIdleSeconds = builder.Configuration.GetValue("Shards:IdleSeconds", 120);
    var shardQuantumSeconds = builder.Configuration.GetValue("Shards:QuantumSeconds", 0);
    siloBuilder.Configure<Orleans.Configuration.GrainCollectionOptions>(o =>
    {
        if (shardQuantumSeconds > 0)
        {
            o.CollectionQuantum = TimeSpan.FromSeconds(shardQuantumSeconds);
        }
        if (shardIdleSeconds > 0)
        {
            o.ClassSpecificCollectionAge[typeof(StreamForge.Host.Grains.TableShardGrain).FullName!] =
                TimeSpan.FromSeconds(shardIdleSeconds);
        }
    });
});

builder.Services.AddStreamForgeApi(builder.Configuration);
builder.Services.AddOrleansFacades();
builder.Services.AddHostedService<GeneratorSupervisorService>();
// Plan 008 W4: drives SourceIngressBuffer.DrainAsync — without it a buffered push is admitted and
// never published. See the service's own doc comment.
builder.Services.AddHostedService<IngestDrainPumpService>();
builder.Services.AddHostedService<StreamBridgeService>();
// Plan 009 B2: a second, independent consumer at the same stream seam as StreamBridgeService —
// fire-and-forget republishes pipeline results / table deltas to NATS for entities with Sinks
// configured. See the service's own doc comment.
builder.Services.AddHostedService<NatsPublisherService>();

builder.Services.AddGrpc();

// Plan 014-I: the out-of-core database connectors' only call site. InboundTransports/PolledTransports
// both document "before any source starts" as the registration deadline; nothing in this process can
// start a source before builder.Build() returns and the hosted services below get to Run(), so anywhere
// before this line satisfies it — here, immediately before Build(), keeps it visibly paired with the
// rest of the transport wiring above rather than buried at the top of the file.
DatabaseConnectors.RegisterAll();

var app = builder.Build();

// Host-specific facts StreamForgeApiOptions carries so the shared endpoints stay byte-identical
// across runtimes (plan 005 W3, decision D-B). Values below reproduce exactly what the pre-W3
// Program.cs resolved inline.
var apiOptions = new StreamForgeApiOptions(
    ProtosDir: Path.Combine(app.Environment.ContentRootPath, "Protos"),
    GrpcPort: grpcPort,
    GrpcStaticServices:
    [
        "SourceService", "PipelineService", "TableService", "StreamService", "IngestService", "DynamicStreamService", "ServerReflection",
    ],
    DocsFilePath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Docs:File"] ?? Path.Combine("..", "..", "docs", "index.html"))),
    SpaDistPath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Web:Dist"] ?? Path.Combine("..", "..", "..", "web", "dist"))),
    Flavor: "orleans");

app.MapStreamForgeApi(apiOptions);

// gRPC control plane + streaming (see Protos/streamforge.proto) — served on the HTTP/2-only
// endpoint configured above (Grpc:Port, default 5299); doesn't share the REST/SignalR/SPA port.
app.MapGrpcService<SourceGrpcService>();
app.MapGrpcService<PipelineGrpcService>();
app.MapGrpcService<TableGrpcService>();
app.MapGrpcService<StreamGrpcService>();
app.MapGrpcService<IngestGrpcService>();

// Tier 2 — dynamic (runtime-typed) gRPC surface: server reflection over BOTH the static streamforge.v1
// descriptors and per-entity descriptors generated on the fly for the current catalog (see
// Grpc/Dynamic/DynamicReflectionService.cs for why this replaces the built-in
// Grpc.AspNetCore.Server.Reflection package), plus one generic typed-streaming RPC
// (Grpc/Dynamic/DynamicStreamService.cs) whose row payloads are encoded against those descriptors.
app.MapGrpcService<DynamicReflectionService>();
app.MapGrpcService<DynamicStreamService>();

app.Lifetime.ApplicationStarted.Register(() => _ = InitializeGrainsAsync(app.Services));

app.Run();

static async Task InitializeGrainsAsync(IServiceProvider services)
{
    try
    {
        var client = services.GetRequiredService<IClusterClient>();
        await client.GetGrain<IUserStoreGrain>(StreamConstants.UsersKey).EnsureInitializedAsync();
        await client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).EnsureInitializedAsync();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[StreamForge.Host] grain initialization failed: {ex}");
    }
}
