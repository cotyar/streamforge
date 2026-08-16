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
                        var request = context.HttpContext.Request;

                        // SignalR cannot send an Authorization header on its WebSocket/SSE handshake, so
                        // the hubs — and only the hubs — take the token from the query string.
                        var accessToken = context.Request.Query["access_token"];
                        if (!string.IsNullOrEmpty(accessToken) && request.Path.StartsWithSegments("/hubs"))
                        {
                            context.Token = accessToken;
                            return Task.CompletedTask;
                        }

                        // The same problem one shape over: a Scalar page cannot send a header either, on
                        // its own navigation or on the plain fetch it loads its document with. So the
                        // per-entity documentation paths — and only those, read-only every one of them —
                        // fall back to the httpOnly cookie login issued alongside the token. A header, when
                        // present, still wins; see DocsAuthCookie for why this cannot authenticate anything
                        // that changes state.
                        if (string.IsNullOrEmpty(request.Headers.Authorization) &&
                            DocsAuthCookie.IsDocumentationPath(request.Path) &&
                            request.Cookies.TryGetValue(DocsAuthCookie.Name, out var docsToken) &&
                            !string.IsNullOrEmpty(docsToken))
                        {
                            context.Token = docsToken;
                        }

                        return Task.CompletedTask;
                    },
                };
            });

        services.AddAuthorizationBuilder()
            .AddPolicy("Viewer", p => p.RequireAuthenticatedUser())
            .AddPolicy("Editor", p => p.RequireRole("Editor", "Admin"))
            .AddPolicy("Admin", p => p.RequireRole("Admin"));

        // Cors:AllowedOrigins (env: Cors__AllowedOrigins__0, __1, ...) extends/replaces the
        // dev-SPA default so an external console (e.g. an Office add-in or another web app)
        // can call REST + negotiate SignalR cross-origin. Unset = the historical 5173-only.
        var corsOrigins = configuration.GetSection("Cors:AllowedOrigins").Get<string[]>();
        if (corsOrigins is null || corsOrigins.Length == 0)
        {
            corsOrigins = ["http://localhost:5173"];
        }

        services.AddCors(options =>
        {
            options.AddPolicy(SpaCorsPolicy, p => p
                .WithOrigins(corsOrigins)
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
            var model = config["Gemini:Model"] ?? "gemini-3.6-flash";
            var httpClient = sp.GetRequiredService<IHttpClientFactory>().CreateClient(nameof(GeminiChatService));
            return new GeminiChatService(
                httpClient,
                baseUrl,
                model,
                apiKey,
                sp.GetRequiredService<StreamForge.Abstractions.ICatalogFacade>(),
                sp.GetRequiredService<StreamForge.Abstractions.ITableReadFacade>(),
                sp.GetRequiredService<StreamForge.Abstractions.ITableHistoryFacade>(),
                thinkingBudget: config.GetValue("Gemini:ThinkingBudget", 0),
                thinkingLevel: config["Gemini:ThinkingLevel"] ?? "LOW");
        });

        // Per-login budget for /api/chat (Chat:MaxRequestsPerSession, 0 or less = unlimited).
        // Singleton: the counters have to outlive the request scope to mean anything.
        services.AddSingleton(sp => new ChatRateLimiter(
            sp.GetRequiredService<IConfiguration>().GetValue("Chat:MaxRequestsPerSession", 10)));
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

        // Per-entity interactive references: /scalar/tables/{id}, /scalar/pipelines/{id},
        // /scalar/sources/{name}. Same Scalar UI as /scalar above, pointed at that entity's own document
        // (EntityOpenApiEndpoints) instead of the whole application's — the REST twin of the per-entity
        // .proto downloads, and what the console's API Explorer links to per entity.
        //
        // The mechanism is Scalar's (options, HttpContext) overload: the endpoint prefix carries a normal
        // route parameter, and OpenApiRoutePattern is chosen per request from its value. No CDN is
        // involved — Scalar.AspNetCore serves its own bundle from embedded resources under each prefix,
        // and each MapScalarApiReference call maps that static-asset route under its own prefix, so the
        // four pages coexist.
        //
        // These pages are Viewer-gated, like the documents they render — a per-entity document describes
        // one named entity's shape and sources are addressed by a guessable name, so it is not something
        // to hand to the open internet. Scalar cannot send an Authorization header, so what satisfies the
        // policy here is the httpOnly cookie issued at login (DocsAuthCookie): the browser sends it on
        // this navigation and on the document fetch, and nowhere it would matter.
        foreach (var (segment, parameter) in
                 new[] { ("tables", "id"), ("pipelines", "id"), ("sources", "name") })
        {
            app.MapScalarApiReference($"/scalar/{segment}/{{{parameter}}}", (o, httpContext) =>
            {
                var key = httpContext.Request.RouteValues[parameter] as string ?? "";
                o.Title = $"StreamForge — {segment}/{key}";
                o.Theme = ScalarTheme.Kepler;
                o.OpenApiRoutePattern = $"/api/{segment}/{Uri.EscapeDataString(key)}/{EntityOpenApiEndpoints.RouteSuffix}";
            }).RequireAuthorization("Viewer");
        }

        // Anonymous liveness/readiness probe (plan 007 W0): used by the admin app, docker compose
        // healthchecks, and Cloud Run startup probes. Deliberately unauthenticated and cheap.
        // /api/healthz alias: Google Frontend intercepts external /healthz requests on run.app URLs
        // (reserved path — returns Google's own 404 before reaching the container; internal probes
        // are unaffected), so anything polling over the public internet must use the alias.
        var healthz = () => Results.Ok(new
        {
            status = "ok",
            flavor = options.Flavor,
            time = DateTimeOffset.UtcNow,
        });
        app.MapGet("/healthz", healthz).AllowAnonymous();
        app.MapGet("/api/healthz", healthz).AllowAnonymous();

        app.MapAuthEndpoints();
        app.MapSourcesEndpoints();
        app.MapSourceRunEndpoints();
        app.MapPipelinesEndpoints();
        app.MapTablesEndpoints(options);
        app.MapUsersEndpoints();
        app.MapConfigEndpoints();
        app.MapChatEndpoints();
        app.MapMetaEndpoints(options);
        app.MapTransportsEndpoints();
        app.MapSqlFunctionsEndpoints();
        app.MapEntityOpenApiEndpoints();
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
