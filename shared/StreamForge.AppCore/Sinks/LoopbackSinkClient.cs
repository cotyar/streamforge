using StreamForge.Abstractions;
using StreamForge.Host.Generators;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Wishlist item 9(b): the outbound connection for ONE configured loopback sink — the native, in-process
/// twin of <see cref="HttpSinkClient"/> (option (a), wishlist #9(a)). Where <see cref="HttpSinkClient"/>
/// serializes a row to JSON and POSTs it to <c>/api/sources/{name}/events</c>, this class writes the SAME
/// flattened row directly into <see cref="LoopbackHub"/> — no HTTP, no JSON, no network round trip at
/// all. Read <see cref="LoopbackHub"/>'s class doc first: it is where the interesting design decisions
/// (why a hub, why draining is timer-driven, and the explicit "what happens on an unbounded cycle"
/// argument) actually live; this class is the thin sink-side half.
///
/// <para><b>Fire-and-forget contract, identical to every other <see cref="ISinkClient"/>.</b> NEVER
/// throws, NEVER blocks — there is nothing to block ON here: <see cref="LoopbackHub.TryPublish"/> is a
/// synchronous, non-blocking channel write, so this class doesn't even need the per-publish
/// <see cref="CancellationTokenSource"/>/timeout machinery <see cref="HttpSinkClient"/> and
/// <see cref="NatsSinkClient"/> need for their actual I/O — there is no I/O.</para>
///
/// <para><b>The maxDepth guard is <see cref="SinkStepGuard"/>'s, not this class's own</b> — see that
/// class's doc comment. It runs BEFORE <see cref="LoopbackHub.TryPublish"/>, so a dropped row never
/// reaches the hub at all, exactly mirroring where <see cref="HttpSinkClient"/> runs it relative to its
/// own network call.</para>
///
/// <para><b>An unknown target is ALSO a reported failure, not a silent no-op.</b> <see cref="LoopbackHub.TryPublish"/>
/// returns false when no generator has attached under <see cref="LoopbackSinkConfig.TargetSourceName"/> —
/// not started yet, wrong kind, or already stopped. This class counts that exactly like a maxDepth drop or
/// an HTTP sink's network failure: <see cref="Counters"/>'s <c>Failed</c> increments and
/// <see cref="SinkPublishCounters.LastError"/> names the reason, so "the loop's target source isn't
/// running" is visible the same way every other publish failure is, never a row that silently vanishes.</para>
/// </summary>
public sealed class LoopbackSinkClient : ISinkClient
{
    /// <summary>Same value and reason as <see cref="HttpSinkClient.LogThrottleWindow"/> — a burst of
    /// near-simultaneous failures (e.g. every row of one delta batch hitting the same unknown target)
    /// should produce one <c>onFailure</c> call, not one per row.</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    private readonly LoopbackSinkConfig _config;
    private readonly string _targetSourceName;
    private readonly Action<string, Exception>? _onFailure;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    /// <param name="config">Non-null <c>SinkSpec.Loopback</c> — the caller filters for that before
    /// constructing a client, same convention as <see cref="HttpSinkClient"/>.</param>
    /// <param name="entityKind">"pipeline" | "table" — carried for log/failure-callback context only.</param>
    /// <param name="entityName">Pipeline id or table name; also what <c>{name}</c> in
    /// <see cref="LoopbackSinkConfig.TargetSourceName"/> expands to.</param>
    /// <param name="onFailure">Invoked (throttled) with (targetSourceName, exception) on a maxDepth drop
    /// or an unattached target. Never invoked on success. May be null.</param>
    public LoopbackSinkClient(
        LoopbackSinkConfig config, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        _config = config;
        _targetSourceName = config.TargetSourceName.Replace("{name}", entityName, StringComparison.Ordinal);
        _onFailure = onFailure;
        EntityName = entityName;
        EntityKind = entityKind;
    }

    /// <summary>The expanded target source name — <c>{name}</c> already substituted.</summary>
    public string TargetSourceName => _targetSourceName;

    public string EntityName { get; }

    /// <summary>"pipeline" | "table".</summary>
    public string EntityKind { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>Flattens <paramref name="payload"/> (see <see cref="SinkStepGuard.RowOf{T}"/>), applies
    /// the shared maxDepth guard, and — if neither dropped it — writes the row straight into
    /// <see cref="LoopbackHub"/>. Entirely synchronous (there is no I/O to await); the <c>Task</c> return
    /// keeps this class's shape identical to every other <see cref="ISinkClient"/>. NEVER throws — see
    /// this class's doc comment.</summary>
    public Task PublishAsync<T>(T payload, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
        {
            // Caller (host shutdown, sink config changed mid-publish) is tearing this client down — not
            // a target problem, so not counted as a failure. Mirrors HttpSinkClient's identical guard.
            return Task.CompletedTask;
        }

        try
        {
            var row = SinkStepGuard.RowOf(payload);

            if (SinkStepGuard.ShouldDrop(row, _config.StepField, _config.MaxDepth, out var step))
            {
                // Dropped BEFORE the hub write — see this class's and SinkStepGuard's doc comments.
                Fail(new InvalidOperationException(
                    $"dropped by maxDepth guard: {_config.StepField}={step} >= maxDepth={_config.MaxDepth}"));
                return Task.CompletedTask;
            }

            if (!LoopbackHub.TryPublish(_targetSourceName, row))
            {
                Fail(new InvalidOperationException(
                    $"loopback target source '{_targetSourceName}' has no attached generator (not started, not a generator-kind source, or already stopped)"));
                return Task.CompletedTask;
            }

            Interlocked.Increment(ref _published);
        }
        catch (Exception ex)
        {
            // Defensive only — SinkStepGuard.RowOf/ShouldDrop and LoopbackHub.TryPublish are documented
            // not to throw, but this class's own contract (never throw) does not get to assume that of
            // its dependencies forever.
            Fail(ex);
        }

        return Task.CompletedTask;
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
        // HttpSinkClient.Fail/NatsSinkClient.MaybeReportFailure.
        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(_targetSourceName, ex);
    }

    /// <summary>No-op: there is no connection or handle to release — see this class's doc comment.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
