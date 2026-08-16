using System.Text;
using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Wishlist item 9(a): the outbound connection for ONE configured HTTP sink (one <c>SinkSpec.Http</c>
/// entry on one pipeline or table) — the smaller of the wishlist's two "bounded feedback loop" options,
/// and the one this wave implements (see <c>docs/otc-demo-wishlist.md</c> §9; the native in-process
/// loopback sink→source pair, option (b), is NOT attempted here). Modeled directly on
/// <see cref="NatsSinkClient"/>, whose class doc explains the fire-and-forget contract this type also
/// carries verbatim: NEVER throws, NEVER blocks the caller past its own timeout, counts and (throttled)
/// reports its own failures. Read that class first — this one follows its shape field-for-field rather
/// than reinventing it.
///
/// <para><b>Why HttpClient instead of NatsConnection's built-in reconnect.</b> There is no persistent
/// connection to keep here — each publish is one independent HTTP request — so there is nothing to
/// "reconnect"; the per-request linked <see cref="CancellationTokenSource"/> below is what makes "never
/// blocks past its own timeout" true, exactly as it is for <see cref="NatsSinkClient"/>, just without a
/// long-lived socket underneath it. <c>SharedHttp</c> is one process-lifetime <see cref="HttpClient"/>
/// shared by every <see cref="HttpSinkClient"/> instance — the same pattern
/// <c>GrpcSubscriberCore.SharedHttp</c> already uses in this project, for the same reason (one client's
/// pooled connections beat one-per-sink socket exhaustion under Kestrel's default limits).</para>
///
/// <para><b>The body is shaped for the loop's own endpoint, not a bare row.</b> Every POST body is
/// <c>{ "events": [ &lt;row&gt; ] }</c> — see <see cref="HttpSinkConfig"/>'s class doc for why: it is
/// wire-identical to <c>IngestEventsRequest</c> (<c>StreamForge.Api/Dtos.cs</c>), the body
/// <c>POST /api/sources/{name}/events</c> actually expects, which is the loop's own destination and the
/// concrete reason this sink kind exists. <see cref="HttpSinkIngestBody"/> below is a hand-duplicated,
/// wire-identical shape rather than a reference to that type, because <c>StreamForge.AppCore</c> sits
/// below <c>StreamForge.Api</c> in the project graph and cannot reference it (see
/// <c>StreamForge.AppCore.csproj</c> — no <c>ProjectReference</c> to <c>StreamForge.Api</c>).</para>
///
/// <para><b>The maxDepth guard runs BEFORE the request is built</b> — a dropped row never reaches the
/// network at all. It is deliberately folded into this class's existing failure-counting machinery
/// (<see cref="Counters"/>'s <c>Failed</c>/<c>LastError</c>, and the same throttled <c>onFailure</c>
/// callback every other publish failure goes through) rather than a new counter: <see cref="ISinkClient"/>
/// and <see cref="SinkPublishCounters"/> are shared, frozen shapes every sink kind returns through the
/// exact same publisher-service log line (<c>NatsPublisherService.NewClient</c>'s warning), and a
/// dedicated "dropped" bucket would need a wire/shape change reaching well outside this wave's ownership
/// for a distinction no caller reads today. The tradeoff, named plainly: a guard-drop and a genuine
/// network failure are indistinguishable in <see cref="Counters"/> alone — both show up as
/// <c>Failed</c> — but <see cref="SinkPublishCounters.LastError"/>'s text always says which one happened,
/// so nothing is actually silent, just not separately countable without a contract change.</para>
/// </summary>
public sealed class HttpSinkClient : ISinkClient
{
    /// <summary>Minimum gap between two <c>onFailure</c> invocations for the SAME client — identical
    /// value and reason to <see cref="NatsSinkClient.LogThrottleWindow"/>.</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    /// <summary>Fallback when <see cref="HttpSinkConfig.TimeoutMs"/> is non-positive (should not happen
    /// through the console, which enforces the field's default, but a hand-edited/imported config could
    /// zero it) — same value <see cref="NatsSinkClient.PublishTimeout"/> uses, so an unconfigured timeout
    /// degrades to the platform's usual publish bound rather than an unbounded wait.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(3);

    private static readonly HttpClient SharedHttp = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly HttpSinkConfig _config;
    private readonly string _url;
    private readonly TimeSpan _timeout;
    private readonly Action<string, Exception>? _onFailure;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    /// <param name="config">Non-null <c>SinkSpec.Http</c> — the caller filters for that before
    /// constructing a client.</param>
    /// <param name="entityKind">"pipeline" | "table" — carried for log/failure-callback context only,
    /// same role it plays for <see cref="NatsSinkClient"/>.</param>
    /// <param name="entityName">Pipeline id or table name; also what <c>{name}</c> in the URL expands to.</param>
    /// <param name="onFailure">Invoked (throttled) with (url, exception) on a publish failure or a
    /// maxDepth drop. Never invoked on success. May be null.</param>
    public HttpSinkClient(
        HttpSinkConfig config, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        _config = config;
        _url = config.Url.Replace("{name}", entityName, StringComparison.Ordinal);
        _timeout = config.TimeoutMs > 0 ? TimeSpan.FromMilliseconds(config.TimeoutMs) : DefaultTimeout;
        _onFailure = onFailure;
        EntityName = entityName;
        EntityKind = entityKind;
    }

    /// <summary>The expanded destination URL — <c>{name}</c> already substituted.</summary>
    public string Url => _url;

    public string EntityName { get; }

    /// <summary>"pipeline" | "table" — see <see cref="NatsSinkClient.EntityName"/>'s sibling doc for why
    /// this is carried at all (log context, since one host runs many sinks of the same kind at once).</summary>
    public string EntityKind { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>Posts <paramref name="payload"/>, wrapped as one <see cref="HttpSinkIngestBody"/> event.
    /// NEVER throws — see this class's doc comment. <paramref name="ct"/> already cancelled when this is
    /// called is the one case treated as "caller is shutting down", exactly as
    /// <see cref="NatsSinkClient.PublishAsync{T}"/> treats it; every other outcome (maxDepth drop, DNS
    /// failure, connection refused, a non-2xx response, this class's own timeout firing) is caught,
    /// counted and reported via <c>onFailure</c>.</summary>
    public async Task PublishAsync<T>(T payload, CancellationToken ct)
    {
        try
        {
            var row = RowOf(payload);

            if (_config.MaxDepth > 0 && TryGetStep(row, _config.StepField, out var step) && step >= _config.MaxDepth)
            {
                // Dropped BEFORE any network call — see this class's doc comment for why this folds into
                // the same Failed/LastError counters every other publish failure uses.
                Fail(new InvalidOperationException(
                    $"dropped by maxDepth guard: {_config.StepField}={step} >= maxDepth={_config.MaxDepth}"));
                return;
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_timeout);

            using var request = new HttpRequestMessage(HttpMethod.Post, _url);
            if (!string.IsNullOrEmpty(_config.HeaderName) && !string.IsNullOrEmpty(_config.HeaderValue))
            {
                // TryAddWithoutValidation, not Add: an operator-supplied header name/value pair must not
                // be rejected by HttpClient's own format checks (e.g. a value containing characters
                // HttpHeaders.Add is picky about) — the receiver gets the final say on whether it's valid,
                // not this sink.
                request.Headers.TryAddWithoutValidation(_config.HeaderName, _config.HeaderValue);
            }

            var json = JsonSerializer.Serialize(new HttpSinkIngestBody([row]), JsonOptions);
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");

            using var response = await SharedHttp
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token)
                .ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            Interlocked.Increment(ref _published);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Caller (host shutdown, sink config changed mid-publish) is tearing this client down — not
            // an endpoint problem, so not counted as a failure. Mirrors NatsSinkClient's identical guard.
        }
        catch (Exception ex)
        {
            // Every other outcome — this class's own timeout firing, DNS/connect failure, a non-2xx
            // response via EnsureSuccessStatusCode, a malformed URL — folds into the same "the sink
            // failed" bucket, same call as NatsSinkClient.PublishAsync makes for the identical reasons.
            Fail(ex);
        }
    }

    /// <summary>Flattens a sink message to the row this sink POSTs. Deliberately the same shape
    /// <see cref="FileSinkClient"/>'s private <c>RowOf</c> produces (including <c>_weight</c> for a table
    /// delta, so a retraction is not indistinguishable from an insert to whatever reads it downstream) —
    /// duplicated rather than shared because neither sink's <c>RowOf</c> is on a public seam and this
    /// wave's file ownership does not include touching <see cref="FileSinkClient"/>.</summary>
    private static Dictionary<string, object?> RowOf<T>(T payload) => payload switch
    {
        NatsTableDeltaMessage d => new Dictionary<string, object?>(d.Row, StringComparer.Ordinal) { ["_weight"] = d.Weight },
        NatsPipelineRowMessage p => new Dictionary<string, object?>(p.Row, StringComparer.Ordinal),
        // No other payload type reaches a sink today; round-tripping it through JSON keeps a future one
        // visible in the posted event instead of silently empty.
        _ => JsonSerializer.Deserialize<Dictionary<string, object?>>(JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions))
             ?? new Dictionary<string, object?>(StringComparer.Ordinal),
    };

    /// <summary>Reads <paramref name="field"/> off <paramref name="row"/> as an integer step counter.
    /// Absent, null, or a value that is not one of the numeric CLR shapes the engine's own coercion
    /// produces (see <c>FieldValueConversion</c>) returns false — a row without a recognizable step is
    /// let through, not dropped, per this class's (and <see cref="HttpSinkConfig.MaxDepth"/>'s) doc
    /// comment. Double→long uses the same UNCHECKED narrowing cast <c>FieldValueConversion</c> documents
    /// using for the identical conversion elsewhere in this codebase, for the same consistency reason.</summary>
    private static bool TryGetStep(Dictionary<string, object?> row, string field, out long step)
    {
        step = 0;
        if (!row.TryGetValue(field, out var value) || value is null)
        {
            return false;
        }

        switch (value)
        {
            case long l:
                step = l;
                return true;
            case int i:
                step = i;
                return true;
            case double d:
                step = (long)d;
                return true;
            case decimal m:
                step = (long)m;
                return true;
            case string s when long.TryParse(s, out var parsed):
                step = parsed;
                return true;
            default:
                return false;
        }
    }

    private void Fail(Exception ex)
    {
        Interlocked.Increment(ref _failed);
        Volatile.Write(ref _lastError, $"{ex.GetType().Name}: {ex.Message}");
        Interlocked.Exchange(ref _lastFailureAtMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        if (_onFailure is null)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var last = Interlocked.Read(ref _lastLoggedAtMs);
        if (now - last < LogThrottleWindow.TotalMilliseconds)
        {
            return;
        }

        // Only the thread that wins this race actually logs — same reasoning as
        // NatsSinkClient.MaybeReportFailure: a burst of near-simultaneous failures must produce one log
        // line, not one per row.
        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(_url, ex);
    }

    /// <summary>No-op: <see cref="SharedHttp"/> is process-lifetime and shared across every
    /// <see cref="HttpSinkClient"/>, so no per-instance resource needs releasing — unlike
    /// <see cref="NatsSinkClient"/>, which owns its own connection and must dispose it.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>Wire-identical to <c>StreamForge.Api</c>'s <c>IngestEventsRequest</c> record
/// (<c>Events</c>/<c>Partial</c>/<c>IdempotencyKey</c>) — see <see cref="HttpSinkClient"/>'s class doc for
/// why it is duplicated here instead of referenced. Only <see cref="Events"/> is declared: the real
/// record's <c>Partial</c>/<c>IdempotencyKey</c> parameters default to <c>false</c>/<c>null</c> when the
/// JSON omits them (System.Text.Json honors a record constructor's own default parameter values for a
/// missing member), so encoding those defaults a second time here would be one more place for them to
/// drift from the real contract instead of zero.</summary>
internal sealed record HttpSinkIngestBody(List<Dictionary<string, object?>> Events);
