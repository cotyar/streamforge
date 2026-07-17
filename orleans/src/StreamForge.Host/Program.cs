using System.Text;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.FileProviders;
using Microsoft.IdentityModel.Tokens;
using Orleans;
using Orleans.Hosting;
using Scalar.AspNetCore;
using StreamForge.Abstractions;
using StreamForge.Host.Api;
using StreamForge.Host.Auth;
using StreamForge.Host.Hubs;
using StreamForge.Host.Services;
using StreamForge.Host.Storage;

const string SpaCorsPolicy = "SpaDev";

var builder = WebApplication.CreateBuilder(args);

// Co-hosted process listens on http://localhost:5199 by default; ASPNETCORE_URLS (if set) wins.
if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    builder.WebHost.UseUrls("http://localhost:5199");
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
app.MapHub<StreamHub>("/hubs/stream");

// Interactive user documentation (docs/index.html), served at /docs.
var docsFile = Path.GetFullPath(Path.Combine(app.Environment.ContentRootPath, "..", "..", "docs", "index.html"));
if (File.Exists(docsFile))
{
    app.MapGet("/docs", () => Results.File(docsFile, "text/html"));
}

// Serve the built SPA (web/dist) if present, without swallowing /api or /hubs routes.
var spaDist = Path.Combine(app.Environment.ContentRootPath, "..", "..", "web", "dist");
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
