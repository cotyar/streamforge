using System.Text;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.IdentityModel.Tokens;
using Scalar.AspNetCore;
using StreamForge.Api.Auth;
using StreamForge.Api.Hubs;

namespace StreamForge.Api;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W3: the entire REST/SignalR/SPA surface (routes, JWT auth wiring,
/// authorization policies, CORS, JSON options, OpenAPI/Scalar) lifted verbatim from the Orleans host's
/// former Program.cs so both runtimes serve byte-identical responses. <see cref="AddStreamForgeApi"/>
/// registers services (call from the host's builder.Services); <see cref="MapStreamForgeApi"/> wires
/// middleware + endpoint routes (call once <c>WebApplication</c> is built), taking a
/// <see cref="StreamForgeApiOptions"/> for the handful of host-specific facts (protos dir, gRPC port +
/// static service list, docs file, SPA dist path).
/// </summary>
public static class StreamForgeApiExtensions
{
    private const string SpaCorsPolicy = "SpaDev";

    public static void AddStreamForgeApi(this IServiceCollection services, IConfiguration configuration)
    {
        var jwtKey = configuration["Jwt:Key"]!;
        var jwtIssuer = configuration["Jwt:Issuer"]!;
        var jwtAudience = configuration["Jwt:Audience"]!;

        services
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

        services.AddAuthorizationBuilder()
            .AddPolicy("Viewer", p => p.RequireAuthenticatedUser())
            .AddPolicy("Editor", p => p.RequireRole("Editor", "Admin"))
            .AddPolicy("Admin", p => p.RequireRole("Admin"));

        services.AddCors(options =>
        {
            options.AddPolicy(SpaCorsPolicy, p => p
                .WithOrigins("http://localhost:5173")
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials());
        });

        services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
        });

        services
            .AddSignalR()
            .AddJsonProtocol(options =>
            {
                options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter());
            });

        services.AddOpenApi(options =>
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

        services.AddSingleton<JwtTokenService>();

        // Plan 007 W1C, decision D-D: AI control chat (POST /api/chat) over Google Gemini's native
        // REST API. Plain HttpClient via IHttpClientFactory, no new NuGet dependency. Config
        // Gemini:ApiKey|BaseUrl|Model with GEMINI_API_KEY/GOOGLE_API_KEY env fallback for the key —
        // unset key means GeminiChatService.IsConfigured is false and ChatEndpoints returns 503.
        services.AddHttpClient();
        services.AddScoped(sp =>
        {
            var config = sp.GetRequiredService<IConfiguration>();
            var apiKey = config["Gemini:ApiKey"]
                ?? Environment.GetEnvironmentVariable("GEMINI_API_KEY")
                ?? Environment.GetEnvironmentVariable("GOOGLE_API_KEY");
            var baseUrl = config["Gemini:BaseUrl"] ?? "https://generativelanguage.googleapis.com";
            var model = config["Gemini:Model"] ?? "gemini-2.5-flash";
            var httpClient = sp.GetRequiredService<IHttpClientFactory>().CreateClient(nameof(GeminiChatService));
            return new GeminiChatService(
                httpClient,
                baseUrl,
                model,
                apiKey,
                sp.GetRequiredService<StreamForge.Abstractions.ICatalogFacade>(),
                sp.GetRequiredService<StreamForge.Abstractions.ITableReadFacade>(),
                sp.GetRequiredService<StreamForge.Abstractions.ITableHistoryFacade>());
        });
    }

    public static void MapStreamForgeApi(this WebApplication app, StreamForgeApiOptions options)
    {
        app.UseCors(SpaCorsPolicy);
        app.UseAuthentication();
        app.UseAuthorization();

        app.MapOpenApi();
        app.MapScalarApiReference(o =>
        {
            o.Title = "StreamForge API";
            o.Theme = ScalarTheme.Kepler;
        });

        // Anonymous liveness/readiness probe (plan 007 W0): used by the admin app, docker compose
        // healthchecks, and Cloud Run startup probes. Deliberately unauthenticated and cheap.
        app.MapGet("/healthz", () => Results.Ok(new
        {
            status = "ok",
            flavor = options.Flavor,
            time = DateTimeOffset.UtcNow,
        })).AllowAnonymous();

        app.MapAuthEndpoints();
        app.MapSourcesEndpoints();
        app.MapPipelinesEndpoints();
        app.MapTablesEndpoints();
        app.MapUsersEndpoints();
        app.MapConfigEndpoints();
        app.MapChatEndpoints();
        app.MapMetaEndpoints(options);
        app.MapHub<StreamHub>("/hubs/stream");

        // Interactive user documentation (docs/index.html), served at /docs. Per decision D-F, /docs
        // stays Orleans-served — options.DocsFilePath is null on a host that doesn't want the route.
        if (options.DocsFilePath is not null && File.Exists(options.DocsFilePath))
        {
            app.MapGet("/docs", () => Results.File(options.DocsFilePath, "text/html"));

            // Sibling pages next to index.html (e.g. comparison.html, linked from its sidebar) are
            // served under /docs/{page}.html from the same directory — nothing else.
            var docsDir = Path.GetDirectoryName(options.DocsFilePath)!;
            app.MapGet("/docs/{page}.html", (string page) =>
            {
                var file = Path.Combine(docsDir, page + ".html");
                return Path.GetDirectoryName(Path.GetFullPath(file)) == Path.GetFullPath(docsDir) && File.Exists(file)
                    ? Results.File(file, "text/html")
                    : Results.NotFound();
            });
        }

        // Serve the built SPA (repo-root web/dist) if present, without swallowing /api or /hubs routes.
        if (options.SpaDistPath is not null && Directory.Exists(options.SpaDistPath))
        {
            var spaFiles = new PhysicalFileProvider(options.SpaDistPath);
            app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = spaFiles });
            app.UseStaticFiles(new StaticFileOptions { FileProvider = spaFiles });
            app.MapFallbackToFile("index.html", new StaticFileOptions { FileProvider = spaFiles });
        }
    }
}
