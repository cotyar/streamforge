using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace StreamsForge.Client;

/// <summary>
/// Entry point. <see cref="ConnectAsync"/> resolves a <see cref="TransportKind"/> into a live
/// <see cref="ITransport"/> (gRPC or one SignalR wire mode), and this class exposes tables,
/// ad-hoc SQL, and ingest push over it.
///
/// Catalog reads (<see cref="ListTablesAsync"/>, <see cref="SearchAsync"/>, <see cref="ValidateAsync"/>,
/// the one-shot <see cref="SnapshotAsync"/>) and ad-hoc SQL (<see cref="SqlAsync"/> and friends) are
/// ALWAYS REST, regardless of which live transport got picked -- ported as a deliberate
/// simplification from clients/python/src/streamsforge/tables.py / sql.py: these are cheap,
/// infrequent calls, there is no gRPC RPC for config import at all, and one REST code path is
/// simpler than duplicating them per transport. <see cref="PushAsync"/> is the one place transport
/// choice matters: gRPC's bidi <c>IngestService.Ingest</c> gets real HTTP/2 backpressure that REST's
/// buffered POST does not.
/// </summary>
public sealed class StreamsForgeClient : IAsyncDisposable
{
    private readonly AuthHttpClient _http;
    private readonly GrpcTransport? _grpc;
    private readonly ITransport _liveTransport;
    private readonly string? _ingestKey;
    private readonly ILogger _logger;

    /// <summary>"grpc", "signalr:ws", "signalr:sse" or "signalr:lp" -- whichever <see cref="ConnectAsync"/>
    /// actually landed on.</summary>
    public string TransportName { get; }

    private StreamsForgeClient(AuthHttpClient http, GrpcTransport? grpc, ITransport live, string? ingestKey, string transportName, ILogger logger)
    {
        _http = http;
        _grpc = grpc;
        _liveTransport = live;
        _ingestKey = ingestKey;
        TransportName = transportName;
        _logger = logger;
    }

    // ============================================================================
    // Connect
    // ============================================================================

    public static async Task<StreamsForgeClient> ConnectAsync(ConnectOptions options, CancellationToken ct = default)
    {
        var url = options.Url ?? Environment.GetEnvironmentVariable("STREAMSFORGE_BASE_URL");
        if (string.IsNullOrEmpty(url))
            throw new StreamsForgeException("no base URL: set ConnectOptions.Url or STREAMSFORGE_BASE_URL");

        var user = options.User ?? Environment.GetEnvironmentVariable("STREAMSFORGE_ADMIN_USER");
        var password = options.Password ?? Environment.GetEnvironmentVariable("STREAMSFORGE_ADMIN_PASS");
        var ingestKey = options.IngestKey ?? Environment.GetEnvironmentVariable("SF_INGEST_KEY");
        var logger = (options.LoggerFactory ?? NullLoggerFactory.Instance).CreateLogger("StreamsForge");

        var http = new AuthHttpClient(url, user, password, options.Token);

        GrpcTransport? grpc = null;
        if (options.Transport is TransportKind.Grpc or TransportKind.Auto)
        {
            var target = options.GrpcTarget ?? Environment.GetEnvironmentVariable("STREAMSFORGE_GRPC") ?? DefaultGrpcTarget(url);
            try
            {
                var candidate = new GrpcTransport(target, http.GetTokenAsync);
                using var probeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                probeCts.CancelAfter(TimeSpan.FromSeconds(3));
                await candidate.ListTablesAsync(probeCts.Token).ConfigureAwait(false); // proves the channel AND the JWT work
                grpc = candidate;
            }
            catch (Exception ex)
            {
                if (options.Transport == TransportKind.Grpc)
                {
                    throw new StreamsForgeException(
                        $"gRPC channel to {target} refused. If the host was started with --urls, Program.cs's " +
                        "guard binds no gRPC port at all -- start it with --Http:Port/--Grpc:Port instead.", ex);
                }
                logger.LogWarning(ex, "streamsforge: gRPC unavailable, falling back to SignalR");
            }
        }

        ITransport live;
        string chosen;
        if (grpc is not null)
        {
            live = grpc;
            chosen = "grpc";
        }
        else
        {
            live = await ResolveSignalRAsync(url, http, options, logger, ct).ConfigureAwait(false);
            chosen = live.Name;
        }

        // Never degrade silently: this is the one line that says which transport a caller on
        // TransportKind.Auto actually landed on.
        logger.LogInformation("streamsforge: connected via {Transport} transport ({Url})", chosen, url);

        return new StreamsForgeClient(http, grpc, live, ingestKey, chosen, logger);
    }

    private static string DefaultGrpcTarget(string baseUrl)
    {
        var uri = new Uri(baseUrl);
        var httpPort = uri.Port > 0 ? uri.Port : (uri.Scheme == "https" ? 443 : 80);
        return $"{uri.Host}:{httpPort + 100}";
    }

    private static readonly (HttpTransportType Flag, string Label)[] SignalRPriority =
    [
        (HttpTransportType.WebSockets, "ws"),
        (HttpTransportType.ServerSentEvents, "sse"),
        (HttpTransportType.LongPolling, "lp"),
    ];

    private static bool IsSingleFlag(HttpTransportType t) => t is HttpTransportType.WebSockets or HttpTransportType.ServerSentEvents or HttpTransportType.LongPolling;

    /// <summary>Resolves <see cref="ConnectOptions.SignalRTransports"/> into one working
    /// <see cref="SignalRTransport"/>. A single flag is used exactly as given, no fallback -- the
    /// caller pinned it. Multiple flags (the default -- all three) are tried in
    /// WebSockets -&gt; ServerSentEvents -&gt; LongPolling order, each probed before committing, and
    /// a warning is logged for every mode that did not work -- mirrors the gRPC-unavailable log
    /// above: never let a caller believe they are on the fast path while quietly riding a fallback.
    /// Long polling needs no upgrade and no probe connection, so it is tried last and unconditionally
    /// (if plain REST works, long polling does too).</summary>
    private static async Task<ITransport> ResolveSignalRAsync(string url, AuthHttpClient http, ConnectOptions options, ILogger logger, CancellationToken ct)
    {
        var requested = options.SignalRTransports;
        if (IsSingleFlag(requested)) return new SignalRTransport(url, http.GetTokenAsync, requested, http);

        foreach (var (flag, label) in SignalRPriority)
        {
            if (!requested.HasFlag(flag)) continue;
            var candidate = new SignalRTransport(url, http.GetTokenAsync, flag, http);
            if (flag == HttpTransportType.LongPolling) return candidate;
            try
            {
                await candidate.ProbeAsync(ct).ConfigureAwait(false);
                return candidate;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "streamsforge: signalr:{Mode} unavailable, trying next", label);
            }
        }
        throw new StreamsForgeException("no SignalR transport in the requested set could connect");
    }

    // ============================================================================
    // Tables / live
    // ============================================================================

    /// <summary><paramref name="keyFields"/> omitted (null, the default) resolves the table's
    /// row-identity key from its own definition instead -- wishlist #18's <c>GET /api/tables</c>
    /// <c>keyFields</c>, recomputed by the engine on every successful compile. A non-empty list is
    /// the resolved GROUP BY/LATEST BY key; <c>[]</c> is an unkeyed global aggregate (one row, one
    /// group); an engine build that predates wishlist #18 (the field is simply absent from the
    /// JSON) or a table this engine doesn't know about both resolve to <c>null</c> here, which
    /// <see cref="RowIdentity.GroupKeyOf"/> already treats as whole-row identity -- this client
    /// never had a hand-maintained key map to fall back to, so that was always its behavior for an
    /// unknown table and stays exactly that. Pass <paramref name="keyFields"/> explicitly to
    /// bypass resolution entirely and always win.
    ///
    /// <paramref name="flush"/> is the leading-edge/trailing-coalesce window applied to the
    /// returned table's <see cref="LiveTable.Changed"/> (and <see cref="LiveTable.WatchAsync"/>)
    /// notifications -- see <see cref="LiveTable"/>'s class doc. Omitted (null, the default) uses
    /// <see cref="LiveTable.DefaultFlushWindow"/> (16ms); <see cref="TimeSpan.Zero"/> disables
    /// coalescing entirely, emitting once per applied batch.</summary>
    public async Task<LiveTable> TableAsync(
        string name, IReadOnlyList<string>? keyFields = null, TimeSpan? timeout = null, TimeSpan? flush = null, CancellationToken ct = default)
    {
        var resolvedKeyFields = keyFields ?? await ResolveKeyFieldsAsync(name, ct).ConfigureAwait(false);
        var table = new LiveTable(_liveTransport, name, resolvedKeyFields, _logger, flush);
        await table.StartAsync(timeout ?? TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
        return table;
    }

    public async Task<IReadOnlyList<TableSummary>> ListTablesAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync("api/tables", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        return doc.RootElement.EnumerateArray()
            .Select(t => new TableSummary(t.GetProperty("id").GetString()!, t.GetProperty("name").GetString()!))
            .ToList();
    }

    /// <summary>Reads one table's <c>keyFields</c> straight off <c>GET /api/tables</c> (no
    /// dedicated by-name endpoint, same as <see cref="ResolveTableIdAsync"/>). <c>null</c> covers
    /// both an explicit JSON <c>null</c> (whole-row identity) and the property being absent
    /// entirely (an engine older than wishlist #18) -- this client has no per-table key map to
    /// fall back to either way, so the two cases need no distinguishing here (contrast web/'s
    /// console, which does still have a heuristic fallback for the latter case only).</summary>
    private async Task<IReadOnlyList<string>?> ResolveKeyFieldsAsync(string name, CancellationToken ct)
    {
        using var resp = await _http.GetAsync("api/tables", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        foreach (var t in doc.RootElement.EnumerateArray())
        {
            if (t.GetProperty("name").GetString() != name) continue;
            if (!t.TryGetProperty("keyFields", out var kf) || kf.ValueKind == JsonValueKind.Null) return null;
            return kf.EnumerateArray().Select(e => e.GetString()!).ToList();
        }
        return null;
    }

    private async Task<string> ResolveTableIdAsync(string name, CancellationToken ct)
    {
        foreach (var t in await ListTablesAsync(ct).ConfigureAwait(false))
        {
            if (t.Name == name) return t.Id;
        }
        throw new StreamsForgeException($"no such table '{name}'");
    }

    /// <summary>One-shot REST read, no subscription, no background task. Drops weight&lt;=0 rows,
    /// same as the reducer would.</summary>
    public async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> SnapshotAsync(
        string name, int limit = 500, CancellationToken ct = default)
    {
        var id = await ResolveTableIdAsync(name, ct).ConfigureAwait(false);
        using var resp = await _http.GetAsync($"api/tables/{id}/rows?limit={limit}", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        return doc.RootElement.GetProperty("rows").EnumerateArray()
            .Where(r => !r.TryGetProperty("weight", out var w) || w.GetInt64() > 0)
            .Select(r => RowCodec.FromJson(r.GetProperty("row")))
            .ToList();
    }

    public async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> SearchAsync(
        string name, string query, int limit = 50, CancellationToken ct = default)
    {
        var id = await ResolveTableIdAsync(name, ct).ConfigureAwait(false);
        using var resp = await _http.GetAsync($"api/tables/{id}/search?q={Uri.EscapeDataString(query)}&limit={limit}", ct)
            .ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        return doc.RootElement.GetProperty("rows").EnumerateArray()
            .Where(r => !r.TryGetProperty("weight", out var w) || w.GetInt64() > 0)
            .Select(r => RowCodec.FromJson(r.GetProperty("row")))
            .ToList();
    }

    public async Task<ValidateResult> ValidateAsync(string sqlText, CancellationToken ct = default)
    {
        using var resp = await _http.PostJsonAsync("api/tables/validate", new { sql = sqlText }, ct).ConfigureAwait(false);
        var body = await ReadJsonOrNullAsync(resp, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode || body is null)
            throw new SqlException($"validate failed: {(int)resp.StatusCode}", Array.Empty<SqlDiagnostic>(), sqlText);

        var root = body.Value;
        var ok = root.GetProperty("ok").GetBoolean();
        var diagnostics = root.TryGetProperty("diagnostics", out var diagsEl)
            ? diagsEl.EnumerateArray().Select(ParseDiagnostic).ToList()
            : new List<SqlDiagnostic>();
        var planSummary = root.TryGetProperty("planSummary", out var ps) && ps.ValueKind != JsonValueKind.Null ? ps.GetString() : null;
        return new ValidateResult(ok, diagnostics, planSummary);
    }

    private static SqlDiagnostic ParseDiagnostic(JsonElement d) => new(
        d.GetProperty("message").GetString() ?? "",
        d.TryGetProperty("line", out var l) ? l.GetInt32() : 0,
        d.TryGetProperty("column", out var c) ? c.GetInt32() : 0,
        d.TryGetProperty("severity", out var s) ? s.GetString() ?? "Error" : "Error");

    // ============================================================================
    // Ad-hoc SQL: validate -> POST /api/config/import?mode=merge -> LiveTable.
    // Always REST: there is no gRPC RPC for config import.
    // ============================================================================

    private const string AdhocPrefix = "adhoc_";

    /// <summary>"Exposure vs Ostrava!" -&gt; "adhoc_exposure_vs_ostrava". Already-prefixed names
    /// pass through, so re-running an edited query updates the same table.</summary>
    internal static string AdhocTableName(string raw)
    {
        var slug = raw.Trim().ToLowerInvariant();
        slug = Regex.Replace(slug, "^adhoc_", "");
        slug = Regex.Replace(slug, "[^a-z0-9]+", "_");
        slug = slug.Trim('_');
        if (slug.Length > 48) slug = slug[..48];
        return AdhocPrefix + (slug.Length == 0 ? "scratch_1" : slug);
    }

    /// <summary><paramref name="flush"/> is forwarded to <see cref="TableAsync"/> unchanged -- see
    /// its doc.</summary>
    public async Task<LiveTable> SqlAsync(
        string sqlText, string name, IReadOnlyList<string>? keyFields = null, TimeSpan? timeout = null, TimeSpan? flush = null, CancellationToken ct = default)
    {
        var tableName = AdhocTableName(name);
        var validated = await ValidateAsync(sqlText, ct).ConfigureAwait(false);
        if (!validated.Ok) throw DiagnosticsError(sqlText, validated.Diagnostics);

        var payload = new
        {
            version = 1,
            sources = Array.Empty<object>(),
            pipelines = Array.Empty<object>(),
            tables = new[]
            {
                new { name = tableName, description = "Ad-hoc query from the .NET client", sql = sqlText, running = true },
            },
        };
        using var resp = await _http.PostJsonAsync("api/config/import?mode=merge", payload, ct).ConfigureAwait(false);
        var body = await ReadJsonOrNullAsync(resp, ct).ConfigureAwait(false);
        var entries = body?.TryGetProperty("entries", out var entriesEl) == true
            ? entriesEl.EnumerateArray().ToList()
            : new List<JsonElement>();
        var errored = entries.Where(e => e.TryGetProperty("action", out var a) && a.GetString() == "error").ToList();

        if (!resp.IsSuccessStatusCode || errored.Count > 0)
        {
            var diagnostics = new List<SqlDiagnostic>();
            foreach (var e in errored)
            {
                if (e.TryGetProperty("diagnostics", out var diagsEl) && diagsEl.ValueKind == JsonValueKind.Array && diagsEl.GetArrayLength() > 0)
                {
                    diagnostics.AddRange(diagsEl.EnumerateArray().Select(m => new SqlDiagnostic(m.GetString() ?? "SQL rejected", 0, 0, "Error")));
                }
                else
                {
                    var rejectedName = e.TryGetProperty("name", out var n) ? n.GetString() : tableName;
                    diagnostics.Add(new SqlDiagnostic($"import rejected '{rejectedName}'", 0, 0, "Error"));
                }
            }
            throw DiagnosticsError(sqlText, diagnostics);
        }

        return await TableAsync(tableName, keyFields, timeout, flush, ct).ConfigureAwait(false);
    }

    private static SqlException DiagnosticsError(string sqlText, IReadOnlyList<SqlDiagnostic> diagnostics)
    {
        var message = diagnostics.Count > 0 ? diagnostics[0].Message : "SQL rejected";
        return new SqlException(message, diagnostics, sqlText);
    }

    public async Task<IReadOnlyList<TableSummary>> AdhocTablesAsync(CancellationToken ct = default)
    {
        var all = await ListTablesAsync(ct).ConfigureAwait(false);
        return all.Where(t => t.Name.StartsWith(AdhocPrefix, StringComparison.Ordinal)).ToList();
    }

    public async Task<bool> DropAdhocAsync(string name, CancellationToken ct = default)
    {
        if (!name.StartsWith(AdhocPrefix, StringComparison.Ordinal))
            throw new StreamsForgeException($"refusing to drop non-ad-hoc table '{name}'");

        var tables = await ListTablesAsync(ct).ConfigureAwait(false);
        var match = tables.FirstOrDefault(t => t.Name == name);
        if (match.Id is null) return false;

        using var resp = await _http.DeleteAsync($"api/tables/{match.Id}", ct).ConfigureAwait(false);
        if (resp.StatusCode == HttpStatusCode.NotFound) return false;
        if (resp.StatusCode is not (HttpStatusCode.OK or HttpStatusCode.NoContent))
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new StreamsForgeException($"drop '{name}' failed: {(int)resp.StatusCode} {body}");
        }
        return true;
    }

    // ============================================================================
    // Ingest push: gRPC bidi (real backpressure) when connected via gRPC, REST otherwise.
    // ============================================================================

    public async Task<IngestAckResult> PushAsync(
        string source,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        string? idempotencyKey = null,
        bool partial = false,
        CancellationToken ct = default)
    {
        if (_grpc is not null)
            return await _grpc.IngestAsync(source, rows, idempotencyKey, partial, ct).ConfigureAwait(false);

        return await PushRestAsync(source, rows, idempotencyKey, partial, ct).ConfigureAwait(false);
    }

    private async Task<IngestAckResult> PushRestAsync(
        string source, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows, string? idempotencyKey, bool partial, CancellationToken ct)
    {
        // Prefer the ingest key over the admin JWT when configured, so a caller that only feeds a
        // source never forces a login -- the route is AllowAnonymous with its own header check.
        var useIngestKey = !string.IsNullOrEmpty(_ingestKey);
        var body = new Dictionary<string, object?> { ["events"] = rows, ["partial"] = partial };
        if (!string.IsNullOrEmpty(idempotencyKey)) body["idempotencyKey"] = idempotencyKey;

        using var resp = await _http.SendAsync(
            HttpMethod.Post,
            $"api/sources/{source}/events",
            () => JsonContent.Create(body),
            auth: !useIngestKey,
            ct,
            configure: req => { if (useIngestKey) req.Headers.Add("X-SF-Ingest-Key", _ingestKey); }
        ).ConfigureAwait(false);

        if (resp.StatusCode != HttpStatusCode.Accepted)
        {
            var errBody = await ReadJsonOrNullAsync(resp, ct).ConfigureAwait(false);
            var error = errBody?.TryGetProperty("error", out var e) == true ? e.GetString() : null;
            var rowErrors = errBody?.TryGetProperty("rowErrors", out var re) == true
                ? re.EnumerateArray().Select(x => x.GetString() ?? "").ToList()
                : new List<string>();
            throw new IngestRejectedException(error ?? $"{source} ingest push failed: {(int)resp.StatusCode}", rowErrors);
        }

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false));
        var root = doc.RootElement;
        return new IngestAckResult(
            "INGEST_OUTCOME_ACCEPTED",
            root.GetProperty("accepted").GetInt32(),
            root.GetProperty("dropped").GetInt32(),
            root.GetProperty("invalid").GetInt32(),
            null,
            Array.Empty<string>());
    }

    // ============================================================================

    private static async Task<JsonElement?> ReadJsonOrNullAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        var text = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(text)) return null;
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.Clone();
    }

    public async ValueTask DisposeAsync()
    {
        if (_liveTransport is IAsyncDisposable liveDisposable) await liveDisposable.DisposeAsync().ConfigureAwait(false);
        if (_grpc is not null && !ReferenceEquals(_grpc, _liveTransport)) await _grpc.DisposeAsync().ConfigureAwait(false);
        await _http.DisposeAsync().ConfigureAwait(false);
    }
}
