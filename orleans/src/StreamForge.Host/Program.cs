using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans;
using Orleans.Hosting;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Discovery;
using StreamForge.AppCore.Environments;
using StreamForge.Connectors.Database;
using StreamForge.Connectors.Fix;
using StreamForge.Host.Facades;
using StreamForge.Host.Grpc;
using StreamForge.Host.Grpc.Dynamic;
using StreamForge.Host.Services;
using StreamForge.Host.Storage;
using StreamForge.Host.Streaming;

var builder = WebApplication.CreateBuilder(args);

// Co-hosted process listens on http://localhost:5199 (REST/SignalR/SPA, HTTP/1.1) and
// http://localhost:5299 (gRPC, cleartext h2c — HTTP/2-only, no ALPN without TLS) by default;
// ASPNETCORE_URLS (if set) wins and takes the single-port path below instead of these two explicit
// Kestrel endpoints.
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
    // Explicit Http:Port/Grpc:Port split (or the bare defaults above) — two listeners, one protocol
    // each, byte-for-byte the pre-#19 shape. This is now an OVERRIDE for anyone who genuinely wants
    // REST/SignalR and gRPC apart (a deployment that has two ports to spend, or a TLS-terminating
    // front end that only forwards one protocol per port) rather than the only option.
    //
    // ListenAnyIP, not ListenLocalhost: a loopback-only listener is unreachable from outside the
    // process's own network namespace, which is exactly what made a published Docker port a dead end
    // for gRPC even before the single-port work below existed (see the Dockerfile's note). AnyIP still
    // answers on localhost, so nothing about local dev changes.
    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.ListenAnyIP(httpPort, o => o.Protocols = HttpProtocols.Http1);
        kestrel.ListenAnyIP(grpcPort, o => o.Protocols = HttpProtocols.Http2);
    });
}
else
{
    // Wishlist #19, as literally specified: gRPC as a second PROTOCOL on the one port --urls/PORT
    // already picks, via HttpProtocols.Http1AndHttp2 on that single endpoint.
    //
    // CORRECTION TO THE WISHLIST'S OWN ANALYSIS, MEASURED AGAINST A REAL RUNNING HOST — read before
    // touching this branch. The wishlist assumed "Kestrel detects the HTTP/2 preface on a cleartext
    // listener, so REST, SignalR and gRPC coexist on one port with no TLS/ALPN". That is not how
    // stock Kestrel behaves. Booting this exact binary with --urls http://localhost:9199 and setting
    // Http1AndHttp2 (whether via ConfigureEndpointDefaults or an explicit Listen*/ListenAnyIP call —
    // both were tried) makes Kestrel itself log "HTTP/2 is not enabled for ... TLS is not enabled ...
    // Connections to this endpoint will use HTTP/1.1", and a real h2c "prior knowledge" gRPC call
    // against that port times out. This isn't a misconfiguration on this file's part: automatic
    // protocol selection on a CLEARTEXT Kestrel endpoint is an open, unshipped ASP.NET Core feature
    // request (dotnet/aspnetcore#56984, filed July 2024, still open) — Microsoft's own words on
    // current behavior are "If an endpoint is cleartext (doesn't have TLS) then the connection always
    // falls back to HTTP/1.1. If a client sends a prior knowledge H2C request to the server, the
    // server will error with HTTP_1_1_REQUIRED." Genuine HTTP/1.1-and-HTTP/2 multiplexing on ONE
    // Kestrel endpoint requires TLS + ALPN, full stop, in every version of Kestrel available as of
    // this change.
    //
    // WHY THE SETTING STAYS ANYWAY. Three reasons: (1) it is exactly what the wishlist's own "Do" text
    // asks for, verbatim; (2) it is not a regression — REST/SignalR on this endpoint are completely
    // unaffected (proven below), so the only thing NOT achieved is the gRPC half, which was ALSO not
    // achieved before this change (an --urls deploy never opened a gRPC listener at all); (3) it is
    // the forward-compatible choice — the day this endpoint gains a certificate (Kestrel:Certificates:
    // Default in config, no code change needed here), Http1AndHttp2 starts doing real ALPN-negotiated
    // multiplexing for free. Cloud Run's own HTTP/2 end-to-end mode (`--use-http2` / a port named
    // "h2c") is the other path to a working single port in production: Cloud Run's edge terminates the
    // client's real TLS and then forwards EVERY request to the container as HTTP/2 cleartext, so the
    // container never has to disambiguate REST-vs-gRPC on its own cleartext listener at all — Cloud
    // Run's edge already did that. Neither path is exercised by this task's local verification (no
    // cert, no Cloud Run instance to hand), so what's PROVEN here is REST/SignalR unaffected and gRPC
    // still gated on one of the two paths above; what's NOT proven is either path actually closing the
    // gap end-to-end, and that should not be assumed without testing it for real.
    builder.WebHost.ConfigureKestrel(kestrel =>
        kestrel.ConfigureEndpointDefaults(o => o.Protocols = HttpProtocols.Http1AndHttp2));
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
    // Silo/gateway ports are configurable so two hosts can run side by side on
    // one machine — e.g. the OTC demo and the aqua-demo fish-farm demo, which
    // must not share an engine (a materialized table's state is persisted by
    // JsonFileGrainStorage under DataDir and survives dropping the table, its
    // source, and re-provisioning, so one engine silently mixes both demos).
    //
    // The parameterless overload hardcodes 11111/30000, and a second host then
    // collides on them: the symptom is not a clean bind error but Kestrel's
    // listener dying later with "The connection listener failed to accept any
    // new connections / SocketAddress is an invalid size for the IPEndPoint",
    // taking the HTTP API down with it. Defaults below are Orleans' own, so a
    // host started without these flags behaves exactly as before.
    siloBuilder.UseLocalhostClustering(
        builder.Configuration.GetValue("Silo:Port", 11111),
        builder.Configuration.GetValue("Silo:GatewayPort", 30000));
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
// Plan 018-D: same deadline, same shape, same reasoning — the out-of-core FIX session transport's
// only call site, registering the `fix` inbound kind before any source can open one.
FixConnectors.RegisterAll();
// Same deadline, same shape: the pricing scalars (QLNet-backed Black family, closed-form
// flat-curve bond/swap/FX) register into the Engine's SqlFunctions seam so the Engine itself
// never links a pricing library. Must precede anything that compiles SQL.
StreamForge.Quant.QuantFunctions.RegisterAll();

// Plan 016 wave 5: this instance's directory of known peers, read from config and installed into the
// process-wide PeerDirectory before the host starts serving. "Discovery:Peers" (a section, not a flat
// key) so it binds as an ARRAY — the shape .NET configuration gives every provider for free, including
// the one every live check in this repo actually uses: `--Discovery:Peers:0:Name foo
// --Discovery:Peers:0:RestEndpoint http://host:port` on the command line, or DISCOVERY__PEERS__0__NAME /
// DISCOVERY__PEERS__0__RESTENDPOINT as env vars, bind identically to the equivalent appsettings.json
// `{ "Discovery": { "Peers": [ { "Name": "...", "RestEndpoint": "...", "GrpcEndpoint": "..." } ] } }`.
// Binding straight onto PeerRecord (not a separate DTO) keeps this a one-liner; the registry-owned
// fields (InstanceId/LastSeenAtMs/LastError/Info) simply stay at their zero values unless a probe sets
// them later, which is exactly the "configured but never seen" state PeerRecord already documents.
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
    Flavor: "orleans",
    // Plan 016 wave 5: same config key JsonFileGrainStorage already reads (see that class's
    // AddJsonFileGrainStorage extension) — one DataDir, one identity file living next to the grain state
    // it is already the default for, not a second directory setting nobody would think to keep in sync.
    DataDir: app.Configuration["DataDir"] ?? "./data",
    InstanceName: app.Configuration["InstanceName"] ?? "",
    Version: app.Configuration["Version"] ?? "");

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

        // Plan 021 D1/item-3 — seeding still applies to the DEFAULT environment exactly as before (the
        // environment directory always lists it first — see IEnvironmentRegistryGrain.ListAsync), but the
        // boot path must also resume already-Running generators/connectors/pipelines/tables in every OTHER
        // environment that was ever created, not just default. On an instance that has never created one,
        // ListAsync returns exactly [default] and this loop is one call, byte-identical to the pre-021 line
        // it replaces (EnvKeys.Qualify("", RegistryKey) == RegistryKey).
        var environments = await client.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey).ListAsync();
        foreach (var env in environments)
        {
            await client.RegistryFor(EnvKeys.Normalize(env.Name)).EnsureInitializedAsync();
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[StreamForge.Host] grain initialization failed: {ex}");
    }
}
