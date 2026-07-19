using System.Text;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.FileProviders;
using Microsoft.IdentityModel.Tokens;
using Orleans;
using Orleans.Hosting;
using Scalar.AspNetCore;
using StreamForge.Abstractions;
using StreamForge.Host.Api;
using StreamForge.Host.Auth;
using StreamForge.Host.Grpc;
using StreamForge.Host.Grpc.Dynamic;
using StreamForge.Host.Hubs;
using StreamForge.Host.Services;
using StreamForge.Host.Storage;

const string SpaCorsPolicy = "SpaDev";

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

var jwtKey = builder.Configuration["Jwt:Key"]!;
var jwtIssuer = builder.Configuration["Jwt:Issuer"]!;
var jwtAudience = builder.Configuration["Jwt:Audience"]!;

builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = jwtIssuer,
            ValidateAudience = true,
            ValidAudience = jwtAudience,
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey)),
            ValidateLifetime = true,
            ClockSkew = TimeSpan.FromMinutes(1),
        };
        options.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                var accessToken = context.Request.Query["access_token"];
                if (!string.IsNullOrEmpty(accessToken) &&
                    context.HttpContext.Request.Path.StartsWithSegments("/hubs"))
                {
                    context.Token = accessToken;
                }

                return Task.CompletedTask;
            },
        };
    });

builder.Services.AddAuthorizationBuilder()
    .AddPolicy("Viewer", p => p.RequireAuthenticatedUser())
    .AddPolicy("Editor", p => p.RequireRole("Editor", "Admin"))
    .AddPolicy("Admin", p => p.RequireRole("Admin"));

builder.Services.AddCors(options =>
{
    options.AddPolicy(SpaCorsPolicy, p => p
        .WithOrigins("http://localhost:5173")
        .AllowAnyHeader()
        .AllowAnyMethod()
        .AllowCredentials());
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

builder.Services
    .AddSignalR()
    .AddJsonProtocol(options =>
    {
        options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });

builder.Services.AddOpenApi(options =>
{
    options.AddDocumentTransformer((document, _, _) =>
    {
        document.Info.Title = "StreamForge API";
        document.Info.Description =
            "Streaming-SQL platform on Microsoft Orleans. Authenticate via POST /api/auth/login, " +
            "then use the returned JWT as a Bearer token.";
        document.Components ??= new Microsoft.OpenApi.OpenApiComponents();
        document.Components.SecuritySchemes ??= new Dictionary<string, Microsoft.OpenApi.IOpenApiSecurityScheme>();
        document.Components.SecuritySchemes["Bearer"] = new Microsoft.OpenApi.OpenApiSecurityScheme
        {
            Type = Microsoft.OpenApi.SecuritySchemeType.Http,
            Scheme = "bearer",
            BearerFormat = "JWT",
        };
        document.Security ??= new List<Microsoft.OpenApi.OpenApiSecurityRequirement>();
        document.Security.Add(new Microsoft.OpenApi.OpenApiSecurityRequirement
        {
            [new Microsoft.OpenApi.OpenApiSecuritySchemeReference("Bearer", document)] = [],
        });
        return Task.CompletedTask;
    });
});

builder.Services.AddSingleton<JwtTokenService>();
builder.Services.AddHostedService<GeneratorSupervisorService>();
builder.Services.AddHostedService<StreamBridgeService>();

builder.Services.AddGrpc();

var app = builder.Build();

app.UseCors(SpaCorsPolicy);
app.UseAuthentication();
app.UseAuthorization();

app.MapOpenApi();
app.MapScalarApiReference(options =>
{
    options.Title = "StreamForge API";
    options.Theme = ScalarTheme.Kepler;
});

app.MapAuthEndpoints();
app.MapSourcesEndpoints();
app.MapPipelinesEndpoints();
app.MapTablesEndpoints();
app.MapUsersEndpoints();
app.MapMetaEndpoints();
app.MapHub<StreamHub>("/hubs/stream");

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

// Interactive user documentation (docs/index.html), served at /docs.
var docsFile = Path.GetFullPath(Path.Combine(app.Environment.ContentRootPath, "..", "..", "docs", "index.html"));
if (File.Exists(docsFile))
{
    app.MapGet("/docs", () => Results.File(docsFile, "text/html"));
}

// Serve the built SPA (repo-root web/dist) if present, without swallowing /api or /hubs routes.
// "Web:Dist" is configurable (relative to ContentRootPath) so the Dapr host can point at the same
// directory from its own content root; default is the path from orleans/src/StreamForge.Host up to
// repo-root web/dist.
var spaDist = Path.GetFullPath(Path.Combine(
    app.Environment.ContentRootPath,
    app.Configuration["Web:Dist"] ?? Path.Combine("..", "..", "..", "web", "dist")));
if (Directory.Exists(spaDist))
{
    var spaFiles = new PhysicalFileProvider(spaDist);
    app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = spaFiles });
    app.UseStaticFiles(new StaticFileOptions { FileProvider = spaFiles });
    app.MapFallbackToFile("index.html", new StaticFileOptions { FileProvider = spaFiles });
}

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
