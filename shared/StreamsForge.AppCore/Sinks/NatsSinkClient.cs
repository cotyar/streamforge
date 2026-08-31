using System.Text.Json;
using NATS.Client.Core;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Nats;

namespace StreamsForge.AppCore.Sinks;

/// <summary>
/// Plan 009 B2: the outbound connection for ONE configured NATS sink (one <c>SinkSpec.Nats</c> entry on
/// one pipeline or table). Owns a single <see cref="NatsConnection"/> for its lifetime and reuses it
/// across every publish — <see cref="NatsConnection"/> manages its own reconnection internally
/// (<c>NatsOpts.ReconnectWaitMin</c>/<c>ReconnectWaitMax</c> give it exactly the "back off, never spin"
/// behavior this wave asks for), so this class does not hand-roll a second reconnect loop on top of it.
/// Its own job is narrower and is the harder half of the fire-and-forget contract: survive an
/// individual publish failure — or a publish that would otherwise hang forever against a disconnected
/// broker — without ever propagating an exception to the caller, and without ever blocking the caller
/// past <see cref="PublishTimeout"/>.
///
/// <para><b>HONESTY (see <c>SinkSpec</c>'s doc comment — this is the same limit, restated in code):</b>
/// <see cref="PublishAsync{T}"/> never throws and never awaits longer than <see cref="PublishTimeout"/>.
/// By default <c>NatsConnection</c> does NOT throw when disconnected — it waits to publish until
/// reconnected, which without a bound here would mean an absent broker turns every publish into an
/// unbounded hang. Wrapping every call in its own short-lived, linked <see cref="CancellationTokenSource"/>
/// is therefore not an optimization, it is what makes "drop, never slow the platform down" true. There
/// is no acknowledgement, no retry, no redelivery: at-most-once, best-effort, by design.</para>
///
/// <para><b>A broken sink must not break the entity</b> (plan 009 B2's other named requirement): every
/// failure — bad URL, unreachable broker, bad credentials, a malformed subject — is caught here,
/// counted in <see cref="Counters"/>, and reported through <paramref name="onFailure"/> at most once
/// per <see cref="LogThrottleWindow"/> (so a broker that is down for an hour produces one log line a
/// minute, not one per row). The caller — <c>NatsPublisherService</c> (Orleans) /
/// <c>NatsSinkPublisherService</c> (Dapr) — never sees an exception from this type.</para>
///
/// <para>Deliberately takes an <see cref="Action{T1,T2}"/> callback rather than an
/// <c>ILogger</c>: <c>StreamsForge.AppCore</c> has no logging-framework dependency today (it is a pure
/// connector/config library — see e.g. <c>GrpcSubscriberCore</c>, which reports status the same
/// callback way), and this type follows that same convention rather than being the first file in the
/// project to add one.</para>
/// </summary>
public sealed class NatsSinkClient : ISinkClient
{
    /// <summary>Upper bound on how long ONE publish attempt may take, including any time
    /// <see cref="NatsConnection"/> would otherwise spend waiting for a disconnected connection to come
    /// back — the mechanism behind this class's "never blocks the caller" guarantee.</summary>
    public static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Minimum gap between two <paramref name="onFailure"/> invocations for the SAME client —
    /// keeps a continuously-down broker from producing one log line per row.</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly NatsConnection _connection;
    private readonly string _subjectTemplate;
    private readonly Action<string, Exception>? _onFailure;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    /// <param name="config">Non-null <c>SinkSpec.Nats</c> — the caller filters for that before
    /// constructing a client.</param>
    /// <param name="entityKind">"pipeline" | "table" — folded into the connection's <c>Name</c> (visible
    /// server-side in <c>nats server report connections</c>) and into failure callbacks, purely for
    /// operator legibility.</param>
    /// <param name="entityName">Pipeline id or table name — also used to expand <c>{name}</c> in the
    /// subject template via <see cref="NatsConnectionSettings.ExpandSubject"/>.</param>
    /// <param name="onFailure">Invoked (throttled) with (subject, exception) on a publish failure. Never
    /// invoked on success. May be null (failures are still counted, just not reported anywhere).</param>
    public NatsSinkClient(
        NatsPubConfig config, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        _subjectTemplate = config.Subject;
        _onFailure = onFailure;
        var opts = NatsConnectionSettings.Build(
            config.Url, config.Token, config.Username, config.Password, config.Credentials,
            name: $"streamsforge-sink-{entityKind}-{entityName}");
        _connection = new NatsConnection(opts);
        EntityName = entityName;
    }

    /// <summary>The entity name this client was constructed for — exposed so a caller iterating many
    /// clients doesn't need to keep a parallel dictionary just to log which one failed.</summary>
    public string EntityName { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>Publishes <paramref name="payload"/> (JSON-serialized, camelCase — same convention as
    /// every other wire shape in this codebase) to this sink's subject (after <c>{name}</c> expansion).
    /// NEVER throws: <paramref name="ct"/> being already cancelled when this is called is the ONE case
    /// treated as "caller is shutting down", not a broker failure — everything else (including this
    /// class's own <see cref="PublishTimeout"/> firing) is caught, counted, and reported via
    /// <c>onFailure</c>.</summary>
    public async Task PublishAsync<T>(T payload, CancellationToken ct)
    {
        var subject = NatsConnectionSettings.ExpandSubject(_subjectTemplate, EntityName);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(PublishTimeout);
            var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);
            await _connection.PublishAsync(subject, bytes, cancellationToken: cts.Token).ConfigureAwait(false);
            Interlocked.Increment(ref _published);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Caller (host shutdown, sink config changed mid-publish) is tearing this client down — not
            // a broker problem, so not counted as a failure.
        }
        catch (Exception ex)
        {
            // Every other outcome: our own PublishTimeout firing, DNS/connect failure, auth rejection,
            // an invalid subject — all fold into the same "the sink failed" bucket. Distinguishing them
            // further would need per-exception-type dashboards this wave deliberately doesn't build
            // (out of scope — see plan 009 B2's "no per-sink backpressure/CRUD page" note).
            Interlocked.Increment(ref _failed);
            Volatile.Write(ref _lastError, $"{ex.GetType().Name}: {ex.Message}");
            Interlocked.Exchange(ref _lastFailureAtMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            MaybeReportFailure(subject, ex);
        }
    }

    private void MaybeReportFailure(string subject, Exception ex)
    {
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

        // Only the thread that wins this race actually logs — avoids a burst of concurrent publish
        // failures (multiple rows failing around the same moment) each independently deciding the
        // window has elapsed.
        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(subject, ex);
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
