using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Logging;

namespace StreamForge.Client;

/// <summary>Which live transport <see cref="StreamForgeClient.ConnectAsync"/> should use.</summary>
public enum TransportKind
{
    /// <summary>Try gRPC first; on failure (or on explicit <see cref="TransportKind.SignalR"/>
    /// below) fall back to SignalR, walking WebSockets -&gt; Server-Sent Events -&gt; Long Polling
    /// and logging which one actually worked. Never degrades silently.</summary>
    Auto,
    Grpc,
    SignalR,
}

/// <summary>
/// Connection options for <see cref="StreamForgeClient.ConnectAsync"/>. A small record instead of
/// a dozen constructor parameters; every string property falls back to the environment variable
/// named in its doc comment when left null, mirroring the Python client's resolution order
/// (explicit argument -&gt; environment).
/// </summary>
public sealed record ConnectOptions
{
    /// <summary>REST/SignalR base URL, e.g. <c>http://localhost:9199</c>. Falls back to
    /// <c>STREAMFORGE_BASE_URL</c>.</summary>
    public string? Url { get; init; }

    /// <summary>gRPC target, e.g. <c>localhost:9299</c>. Falls back to <c>STREAMFORGE_GRPC</c>,
    /// then to <c>Url</c>'s host with port+100 (Program.cs's own PORT/PORT+100 convention) --
    /// only correct when the two ports actually follow that relationship.</summary>
    public string? GrpcTarget { get; init; }

    /// <summary>Falls back to <c>STREAMFORGE_ADMIN_USER</c>.</summary>
    public string? User { get; init; }

    /// <summary>Falls back to <c>STREAMFORGE_ADMIN_PASS</c>.</summary>
    public string? Password { get; init; }

    /// <summary>A pre-minted JWT, skipping login until it expires (~11h after construction, same
    /// as a freshly logged-in token).</summary>
    public string? Token { get; init; }

    /// <summary>Preferred over the admin JWT for <see cref="StreamForgeClient.PushAsync"/> when
    /// set, so a caller that only feeds a source never needs to hold one. Falls back to
    /// <c>SF_INGEST_KEY</c>.</summary>
    public string? IngestKey { get; init; }

    public TransportKind Transport { get; init; } = TransportKind.Auto;

    /// <summary>Which SignalR wire mode(s) are acceptable. All three by default -- the hub
    /// restricts none. A single flag pins that exact mode with no fallback; multiple flags are
    /// tried in WebSockets -&gt; ServerSentEvents -&gt; LongPolling order.</summary>
    public HttpTransportType SignalRTransports { get; init; } =
        HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling;

    /// <summary>Used for the "connected via X transport" log and any transport warnings. Defaults
    /// to no-op logging.</summary>
    public ILoggerFactory? LoggerFactory { get; init; }
}
