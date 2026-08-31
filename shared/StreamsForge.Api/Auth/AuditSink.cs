using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamsForge.Abstractions;

namespace StreamsForge.Api.Auth;

// =================================================================================================
// Plan 015 wave 4-D — audit that never costs a request.
//
// The rule the plan states and this file exists to keep: **audit must never make a request fail or
// slow.** So the request thread does exactly one thing — a non-blocking TryWrite into a bounded
// in-process Channel — and a BackgroundService carries the entries to IAuditFacade on its own time.
// Nothing on the request path awaits a grain call, a sidecar round trip, or a disk write, and nothing
// on the request path can throw because of audit.
//
// The three things that makes non-obvious are all here:
//   1. AuditChannelSink   — the bounded queue, the drop policy, and the counter that makes a drop
//                           visible instead of silent.
//   2. AuditActionPolicy  — WHAT gets recorded, which matters more than how fast it is recorded.
//   3. AuditWriterService — the drain, which tolerates a store that does not exist yet, is not ready,
//                           or throws.
// =================================================================================================

/// <summary>Where an audit row goes from the request path. One method, non-blocking, never throws —
/// that trio is the whole contract, and it is what lets <see cref="AccessGuard"/> call it without a
/// try/catch being a fig leaf.</summary>
public interface IAuditSink
{
    void Record(AuditEntry entry);
}

/// <summary>
/// The bounded in-process queue behind <see cref="IAuditFacade.AppendAsync"/>.
///
/// <para><b>The drop policy is drop-the-incoming-entry (<see cref="BoundedChannelFullMode.DropWrite"/>),
/// not drop-oldest, and the two are not equivalent for an audit log.</b> A queue only overflows during
/// a burst, and a burst is almost always many rows of one kind — one caller hammering one refused
/// route. Three reasons the incoming row is the right one to lose:</para>
/// <list type="number">
///   <item><b>The onset is the forensically valuable part.</b> After an incident the question is "when
///   did this start and what was the first thing that failed". Drop-oldest destroys precisely that and
///   keeps the least informative tail; drop-write keeps the beginning.</item>
///   <item><b>The hole is a middle, not a tail.</b> When the burst subsides the queue drains and normal
///   recording resumes, so drop-write loses a counted middle and keeps both the onset AND the recovery.
///   Drop-oldest keeps only whatever happened to be in flight at the end.</item>
///   <item><b>An entry already queued has already been accepted.</b> Evicting it throws away work
///   nothing else knows about, and reorders nothing's benefit against real loss.</item>
/// </list>
///
/// <para>(The <i>store's</i> per-day cap is drop-oldest with a persisted counter — a different problem:
/// there the competing rows are a whole day apart, not milliseconds.)</para>
///
/// <para><b>A drop that nobody counts is exactly the silence the audit log was added to prevent</b>, so
/// every dropped row increments <see cref="Dropped"/>, and <see cref="AuditWriterService"/> turns any
/// movement in that counter into a real audit row of its own (<c>audit.dropped</c>) as soon as the queue
/// has room again. The count therefore reaches the log itself and not only a log line — which is the
/// same job <see cref="AuditPage.Truncated"/> does one layer down.</para>
/// </summary>
public sealed class AuditChannelSink : IAuditSink, IChatAuditSink
{
    /// <summary>Config keys, named once so the code, the tests and the report cannot disagree.</summary>
    public const string EnabledKey = "Audit:Enabled";
    public const string QueueCapacityKey = "Audit:QueueCapacity";
    public const string RecordAllowedMutationsKey = "Audit:RecordAllowedMutations";

    /// <summary>Safe on a small host: ~2 000 pending rows is a fraction of a megabyte and absorbs a
    /// multi-second stall of the store without dropping anything under any normal load.</summary>
    public const int DefaultQueueCapacity = 2048;

    private readonly Channel<AuditEntry>? _channel;
    private readonly ILogger _logger;
    private long _dropped;
    private long _offered;
    private long _reportedDrops;

    /// <param name="capacity">Queue depth. Values below 1 are clamped to 1 — a zero-capacity bounded
    /// channel is not a thing, and "audit off" is <paramref name="enabled"/>, not a capacity of 0.</param>
    /// <param name="enabled"><c>Audit:Enabled</c>. When false <see cref="Record"/> is a no-op and no
    /// queue is allocated at all, so turning audit off costs nothing rather than costing a drained
    /// channel.</param>
    public AuditChannelSink(int capacity, bool enabled, ILogger logger)
    {
        _logger = logger;
        if (!enabled)
        {
            return;
        }

        _channel = Channel.CreateBounded<AuditEntry>(
            new BoundedChannelOptions(Math.Max(1, capacity))
            {
                FullMode = BoundedChannelFullMode.DropWrite,
                SingleReader = true,
                SingleWriter = false,
            },
            // Fires for the row that could not be admitted. This is the only place a drop is observable,
            // which is why it counts rather than logs: one log line per dropped row during a storm is a
            // second denial-of-service on the thing that is already struggling.
            _ => OnDropped());
    }

    /// <summary>False when <c>Audit:Enabled=false</c>: <see cref="Record"/> does nothing and
    /// <see cref="AuditWriterService"/> is not registered.</summary>
    public bool Enabled => _channel is not null;

    /// <summary>How many rows the queue refused. Monotonic, never reset.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>How many rows were handed to <see cref="Record"/>. Diagnostics and tests.</summary>
    public long Offered => Interlocked.Read(ref _offered);

    /// <summary>How many rows made it into the queue. Diagnostics and tests.</summary>
    public long Enqueued => Math.Max(0, Offered - Dropped);

    internal ChannelReader<AuditEntry>? Reader => _channel?.Reader;

    /// <summary>Non-blocking, and it cannot throw. <see cref="Channel"/>'s <c>TryWrite</c> on a bounded
    /// <see cref="BoundedChannelFullMode.DropWrite"/> channel neither blocks nor allocates a task, which
    /// is the entire reason the request path is allowed to call this without ceremony.</summary>
    public void Record(AuditEntry entry)
    {
        if (_channel is null || entry is null)
        {
            return;
        }

        Interlocked.Increment(ref _offered);

        // Note the shape: on a DropWrite channel TryWrite returns TRUE even when the row was refused —
        // "succeeded, by dropping". The itemDropped callback given to CreateBounded is what actually
        // reports a drop, and it has already run by the time TryWrite returns. A `false` here means
        // something else: the channel has been completed, i.e. the host is shutting down.
        if (!_channel.Writer.TryWrite(entry))
        {
            OnDropped();
        }
    }

    /// <summary>Ends the queue so <see cref="AuditWriterService"/>'s drain loop finishes what is left and
    /// exits, instead of the host waiting out its shutdown timeout on a read that never completes.</summary>
    public void Complete() => _channel?.Writer.TryComplete();

    /// <summary>Reads and clears the outstanding drop count, so the writer can turn it into one audit row
    /// per burst rather than one per dropped entry.</summary>
    internal long TakeDropReport()
    {
        var total = Interlocked.Read(ref _dropped);
        var reported = Interlocked.Exchange(ref _reportedDrops, total);
        return total - reported;
    }

    private void OnDropped()
    {
        var total = Interlocked.Increment(ref _dropped);

        // One line at the moment the queue first overflows, and then silence: the counter and the
        // synthetic audit row carry the magnitude, and a per-row log during a storm helps nobody.
        if (total == 1)
        {
            _logger.LogWarning(
                "Audit queue is full — entries are being dropped (drop-write: the newest row is refused, the queued ones survive). "
                + "Raise {CapacityKey} or find out why the audit store is not keeping up. Dropped rows are counted and reported as an "
                + "'audit.dropped' entry once the queue drains.",
                QueueCapacityKey);
        }
    }
}

/// <summary>
/// <b>What gets audited, which is the decision that matters.</b>
///
/// <para>An <see cref="AccessDecision.Allowed"/> on every read route would write thousands of rows a
/// minute and bury the one that matters, so:</para>
/// <list type="bullet">
///   <item><b>Every <see cref="AccessDecision.Denied"/> and every
///   <see cref="AccessDecision.RequiresApproval"/> is recorded</b>, for every action — reads included.
///   A refusal is rare by construction (the system is configured so people can do their jobs), and a
///   refusal that is NOT rare is itself the thing worth seeing.</item>
///   <item><b>An <see cref="AccessDecision.Allowed"/> is recorded only for a mutation</b>, and only
///   when <c>Audit:RecordAllowedMutations</c> is on. "alice changed the prod pipeline" is the row an
///   incident review needs; "alice listed the pipelines" is the row that hides it.</item>
/// </list>
///
/// <para>Three exclusions from the allowed-mutation set are worth naming, because each is a decision
/// and not an oversight:</para>
/// <list type="bullet">
///   <item><c>source.ingest</c> — a WRITE, and excluded anyway. It is the platform's hottest path, one
///   check per message; an audit row per push would make the audit log the bottleneck it was designed
///   never to be, and would drown every other row in the day shard. Wave 3 already refuses to pay a
///   catalog read there for the same reason. A <i>denied</i> ingest is still recorded — that one is
///   rare and interesting.</item>
///   <item><c>catalog.write</c> / <c>catalog.read</c> — the legacy-equivalent <i>bundles</i>
///   (<see cref="Actions.CatalogWrite"/>'s own doc comment calls them that), which are what the coarse
///   Editor policy asks for at the door. Passing a door is not doing a thing; the route's own scoped
///   check, a few microseconds later, is the row that says what was actually done. A denial at the door
///   is still recorded.</item>
///   <item><c>chat.use</c> — the door to <c>POST /api/chat</c>. Everything the model then does is
///   audited individually by <c>ChatToolGate</c>, with the model as Actor and the human as
///   OnBehalfOf.</item>
/// </list>
/// </summary>
public static class AuditActionPolicy
{
    private static readonly HashSet<string> NeverRecordedWhenAllowed = new(StringComparer.Ordinal)
    {
        Actions.ConfigExport,   // a read wearing a verb
        Actions.SourceIngest,   // the hot path — see the type comment
        Actions.CatalogRead,
        Actions.CatalogWrite,   // coarse policy bundles, not operations
        Actions.ChatUse,
    };

    /// <summary>True for an action whose successful use is worth one row. Reads (<c>*.read</c>) and the
    /// five exclusions above are not.</summary>
    public static bool RecordsAllowed(string action) =>
        !string.IsNullOrEmpty(action)
        && !action.EndsWith(".read", StringComparison.Ordinal)
        && !NeverRecordedWhenAllowed.Contains(action);

    /// <summary>The <see cref="AuditEntry.Outcome"/> string for a decision, from the vocabulary
    /// <see cref="AuditEntry.Outcome"/> documents.</summary>
    public static string OutcomeOf(AccessDecision decision) => decision switch
    {
        AccessDecision.Allowed => "allowed",
        AccessDecision.RequiresApproval => "requires-approval",
        _ => "denied",
    };
}

/// <summary>
/// The drain: one row at a time out of <see cref="AuditChannelSink"/> and into
/// <see cref="IAuditFacade"/>, off the request path entirely.
///
/// <para><b>Registered from <c>AddStreamsForgeApi</c></b> for the same reason
/// <see cref="AccessBootstrapService"/> is: both hosts call it, so it is the one place that reaches
/// Orleans and Dapr at once without editing either <c>Program.cs</c>.</para>
///
/// <para><b>It tolerates a store that does not exist.</b> <see cref="IAuditFacade"/> is registered by
/// each host's own facade wiring, which may not have landed on the flavour this runs on; the facade is
/// therefore resolved through a delegate and may be null or may throw. Either way the loop keeps
/// draining (discarding, loudly, once) rather than letting the queue sit permanently full — and the
/// host never goes down because an audit row could not be written.</para>
/// </summary>
public sealed class AuditWriterService(
    AuditChannelSink sink,
    Func<IAuditFacade?> facade,
    ILogger<AuditWriterService> logger) : BackgroundService
{
    private IAuditFacade? _resolved;
    private bool _warnedMissing;
    private long _failed;

    /// <summary>Rows the store refused. Diagnostics and tests.</summary>
    public long Failed => Interlocked.Read(ref _failed);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // No Task.Yield() here. A hosted service is awaited up to its first await, and the drain's first
        // await — ReadAllAsync on an empty queue — is inherently asynchronous, so the host is already
        // released without one.
        //
        // Cancellation COMPLETES the queue rather than being passed to the read: that is what makes
        // shutdown drain what is left instead of abandoning it.
        using var registration = stoppingToken.Register(sink.Complete);
        await DrainAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// The loop itself: rows out of the queue and into the store until the queue is completed.
    ///
    /// <para>Public and separate from <see cref="ExecuteAsync"/> so a test can await the drain directly
    /// instead of racing a <see cref="BackgroundService"/>'s start/stop plumbing. What is left in
    /// <see cref="ExecuteAsync"/> is exactly that plumbing and nothing else, which is the point.</para>
    /// </summary>
    public async Task DrainAsync()
    {
        var reader = sink.Reader;
        if (reader is null)
        {
            logger.LogInformation("{Key}=false — the audit writer is not running and no audit rows are recorded.", AuditChannelSink.EnabledKey);
            return;
        }

        try
        {
            await foreach (var entry in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
            {
                await AppendAsync(entry).ConfigureAwait(false);

                // A drop is only reportable once there is room again, which is exactly here: we just
                // took a row out. Reported as a real audit row so the hole is visible to whoever reads
                // the log, not only to whoever reads the metrics.
                var dropped = sink.TakeDropReport();
                if (dropped > 0)
                {
                    await AppendAsync(DropReport(dropped)).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            // Nothing above is expected to throw — AppendAsync swallows — but a writer loop that can
            // take the host down is the one failure this whole design exists to prevent.
            logger.LogError(ex, "The audit writer loop stopped unexpectedly. Audit rows will be queued and dropped until the host restarts.");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        sink.Complete();
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    private static AuditEntry DropReport(long dropped) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Actor = "system",
        Action = "audit.dropped",
        Scope = "*",
        Outcome = "failed",
        Origin = "system",
        Detail = $"{dropped} audit entr{(dropped == 1 ? "y was" : "ies were")} dropped because the in-process audit queue was full. "
            + "Nothing in this gap was recorded; the entries either side of it are intact.",
    };

    private async Task AppendAsync(AuditEntry entry)
    {
        var store = _resolved ??= Resolve();
        if (store is null)
        {
            return;
        }

        try
        {
            await store.AppendAsync(entry).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var failed = Interlocked.Increment(ref _failed);
            // ponytail: log the first failure and then every 100th, and drop the row. Ceiling: a store
            // that is down for a minute loses that minute of audit with no retry. Upgrade path is a
            // bounded retry (or writing the row back into the queue's head) on the day someone needs
            // durability across a store outage — which is a different guarantee from "never costs a
            // request", and the one this wave was not asked for.
            if (failed == 1 || failed % 100 == 0)
            {
                logger.LogWarning(ex, "Audit store rejected an entry ({Failed} so far); the entry is dropped.", failed);
            }
        }
    }

    private IAuditFacade? Resolve()
    {
        try
        {
            var store = facade();
            if (store is null && !_warnedMissing)
            {
                _warnedMissing = true;
                logger.LogWarning(
                    "No IAuditFacade is registered on this host — audit rows are being discarded. The decision path is unaffected.");
            }

            return store;
        }
        catch (Exception ex)
        {
            if (!_warnedMissing)
            {
                _warnedMissing = true;
                logger.LogWarning(ex, "Resolving IAuditFacade failed — audit rows are being discarded. The decision path is unaffected.");
            }

            return null;
        }
    }
}
