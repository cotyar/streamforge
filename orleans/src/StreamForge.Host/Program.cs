using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans;
using Orleans.Hosting;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Host.Facades;
using StreamForge.Host.Grpc;
using StreamForge.Host.Grpc.Dynamic;
using StreamForge.Host.Services;
using StreamForge.Host.Storage;

var builder = WebApplication.CreateBuilder(args);

// Co-hosted process listens on http://localhost:5199 (REST/SignalR/SPA, HTTP/1.1) and
// http://localhost:5299 (gRPC, cleartext h2c — HTTP/2-only, no ALPN without TLS) by default;
// ASPNETCORE_URLS (if set) wins and skips both explicit Kestrel endpoints below.
if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    var httpPort = builder.Configuration.GetValue("Http:Port", 5199);
    var grpcPort = builder.Configuration.GetValue("Grpc:Port", 5299);

    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.ListenLocalhost(httpPort, o => o.Protocols = HttpProtocols.Http1);
        kestrel.ListenLocalhost(grpcPort, o => o.Protocols = HttpProtocols.Http2);
    });
}

builder.Host.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
    siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
    siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);
});

builder.Services.AddStreamForgeApi(builder.Configuration);
builder.Services.AddOrleansFacades();
builder.Services.AddHostedService<GeneratorSupervisorService>();
builder.Services.AddHostedService<StreamBridgeService>();

builder.Services.AddGrpc();

var app = builder.Build();

// Host-specific facts StreamForgeApiOptions carries so the shared endpoints stay byte-identical
// across runtimes (plan 005 W3, decision D-B). Values below reproduce exactly what the pre-W3
// Program.cs resolved inline.
var apiOptions = new StreamForgeApiOptions(
    ProtosDir: Path.Combine(app.Environment.ContentRootPath, "Protos"),
    GrpcPort: app.Configuration.GetValue("Grpc:Port", 5299),
    GrpcStaticServices:
    [
        "SourceService", "PipelineService", "TableService", "StreamService", "DynamicStreamService", "ServerReflection",
    ],
    DocsFilePath: Path.GetFullPath(Path.Combine(app.Environment.ContentRootPath, "..", "..", "docs", "index.html")),
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
