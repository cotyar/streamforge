using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// One source's client-push ingress buffer (plan 008 W4): explicit row-count accounting under a
/// lock, plus a drain pump the host calls to hand queued batches to the stream/pub-sub layer.
/// Deliberately NOT <c>Channel.CreateBounded</c> — its <c>FullMode</c> is fixed at construction and
/// it cannot do the atomic all-or-nothing batch reservation <see cref="IngressOverflowPolicy.Reject"/>
/// requires (a bounded channel's per-item TryWrite has no notion of "reserve N slots or none").
/// Time (both the wall clock and the wait between poll attempts) is injectable so
/// <see cref="IngressOverflowPolicy.Block"/>'s timeout and the drain-rate estimate are testable
/// without sleeping for real.
/// </summary>
/// <summary>Result of <see cref="SourceIngressBuffer.FilterRowLevelDuplicates"/>: <paramref name="Kept"/>
/// is what should reach admission; <paramref name="DuplicateCount"/> is reported as
/// <see cref="IngestResult.Duplicate"/>.</summary>
public sealed record RowLevelDedupResult(List<Dictionary<string, object?>> Kept, int DuplicateCount);

public sealed class SourceIngressBuffer
{
    public string SourceName { get; }
    public IngestConfig Config { get; }

    /// <summary>Fingerprint of <see cref="Config"/> this buffer was built from — see
    /// <see cref="SourceIngressRegistry"/>.</summary>
    public string ConfigFingerprint { get; }

    private readonly object _gate = new();
    private readonly Queue<Dictionary<string, object?>> _queue = new();
    private readonly Func<long> _clock;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task> _drain;
    private readonly long _createdAtMs;

    /// <summary>Plan 009 A1.1: per-source row-level dedup tracker, present only when
    /// <see cref="IngestConfig.DedupKeyField"/> is configured — reuses
    /// <see cref="DedupTracker"/> rather than a second implementation (per the plan brief).
    /// Rebuilt (fresh, empty) whenever this buffer itself is rebuilt (config fingerprint change),
    /// same "no stale state survives an edit" tradeoff <see cref="SourceIngressRegistry"/>'s own doc
    /// comment already accepts for admission policy.</summary>
    private readonly DedupTracker? _dedup;

    private int _depth;
    private long _totalAccepted;
    private long _totalRejected;
    private long _totalDropped;
    private long _totalInvalid;
    private long _totalPublished;
    private long _downstreamDropped;
    private long _totalDuplicate;
    private long _lastPushMs;

    private TaskCompletionSource<bool> _spaceSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <param name="sourceName">The ingest-kind source this buffer serves.</param>
    /// <param name="config">Effective policy/capacity — immutable for this buffer's lifetime; a
    /// config edit goes through <see cref="SourceIngressRegistry"/>, which builds a fresh buffer.</param>
    /// <param name="configFingerprint">Pre-computed fingerprint of <paramref name="config"/>.</param>
    /// <param name="drain">Hands one drained batch to the host's publish path. Orleans publishes one
    /// EventRecord per row; Dapr publishes one envelope per batch — that choice stays on the host
    /// side, so this always receives the whole batch.</param>
    /// <param name="clock">Epoch-ms clock; defaults to <see cref="DateTimeOffset.UtcNow"/>.</param>
    /// <param name="delay">Wait primitive used while a <see cref="IngressOverflowPolicy.Block"/> push
    /// is waiting for space; defaults to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</param>
    public SourceIngressBuffer(
        string sourceName,
        IngestConfig config,
        string configFingerprint,
        Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task> drain,
        Func<long>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        SourceName = sourceName;
        Config = config;
        ConfigFingerprint = configFingerprint;
        _drain = drain;
        _clock = clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        _delay = delay ?? Task.Delay;
        _createdAtMs = _clock();
        _dedup = string.IsNullOrEmpty(config.DedupKeyField)
            ? null
            : new DedupTracker(maxKeys: config.DedupWindow > 0 ? config.DedupWindow : null);
    }

    public int DepthRows { get { lock (_gate) return _depth; } }

    /// <summary>Coerced, admission-eligible rows in, per <see cref="Config"/>'s policy. Never blocks
    /// forever: <see cref="IngressOverflowPolicy.Block"/> waits up to
    /// <c>min(Config.MaxWaitMs, IngressAdmission.MaxBlockWaitMs)</c> then reports Overloaded.</summary>
    public async Task<IngestResult> PushAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct = default)
    {
        var batchSize = rows.Count;
        if (batchSize == 0)
        {
            return new IngestResult { Outcome = IngestOutcome.Accepted, Accepted = 0 };
        }

        var effectiveWaitMs = Math.Min(Config.MaxWaitMs, IngressAdmission.MaxBlockWaitMs);
        var deadlineMs = _clock() + effectiveWaitMs;

        while (true)
        {
            IngressAdmission.Decision decision;
            List<Dictionary<string, object?>>? admitted = null;
            Task? spaceTask = null;

            lock (_gate)
            {
                decision = IngressAdmission.Decide(_depth, batchSize, Config, DrainRatePerMsLocked());

                switch (decision.Kind)
                {
                    case IngressAdmission.AdmissionKind.TooLarge:
                        _totalRejected += batchSize;
                        return Result(IngestOutcome.TooLarge, 0, 0,
                            $"batch of {batchSize} rows exceeds MaxBatchRows ({Config.MaxBatchRows}) or buffer capacity ({Config.CapacityRows})", 0);

                    case IngressAdmission.AdmissionKind.Reject:
                        _totalRejected += batchSize;
                        return Result(IngestOutcome.Overloaded, 0, 0, "ingress buffer has no room for this batch", decision.RetryAfterMs);

                    case IngressAdmission.AdmissionKind.Wait:
                        spaceTask = _spaceSignal.Task;
                        break;

                    case IngressAdmission.AdmissionKind.Admit:
                        admitted = ApplyAdmitLocked(decision, rows);
                        break;
                }
            }

            if (admitted is not null)
            {
                if (Config.Policy == IngressOverflowPolicy.Inline)
                {
                    await _drain(admitted, ct).ConfigureAwait(false);
                    lock (_gate) { _totalPublished += admitted.Count; }
                }

                return Result(IngestOutcome.Accepted, decision.Admit, decision.Drop, null, 0);
            }

            // AdmissionKind.Wait: block up to the deadline, then re-Decide.
            var nowMs = _clock();
            if (nowMs >= deadlineMs)
            {
                lock (_gate) { _totalRejected += batchSize; }
                return Result(IngestOutcome.Overloaded, 0, 0, "timed out waiting for ingress buffer space", decision.RetryAfterMs);
            }

            var remaining = TimeSpan.FromMilliseconds(Math.Max(1, deadlineMs - nowMs));
            await Task.WhenAny(spaceTask!, _delay(remaining, ct)).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
        }
    }

    /// <summary>Drains up to <paramref name="maxRows"/> queued rows (default: everything queued) as
    /// one batch and hands them to the drain delegate. Rows leave <see cref="DepthRows"/> the moment
    /// they are dequeued (under the lock), before the delegate runs, so waiting
    /// <see cref="PushAsync"/> callers wake up as soon as space is freed rather than after the
    /// delegate completes. Returns 0 (a no-op) when the buffer is empty.</summary>
    public async Task<int> DrainAsync(int maxRows = int.MaxValue, CancellationToken ct = default)
    {
        List<Dictionary<string, object?>>? batch = null;

        lock (_gate)
        {
            if (_queue.Count > 0)
            {
                var take = Math.Min(maxRows, _queue.Count);
                batch = new List<Dictionary<string, object?>>(take);
                for (var i = 0; i < take; i++)
                {
                    batch.Add(_queue.Dequeue());
                }
                _depth -= batch.Count;
                SignalSpaceLocked();
            }
        }

        if (batch is null || batch.Count == 0)
        {
            return 0;
        }

        await _drain(batch, ct).ConfigureAwait(false);

        lock (_gate) { _totalPublished += batch.Count; }
        return batch.Count;
    }

    /// <summary>Rows that failed coercion before ever reaching <see cref="PushAsync"/> — recorded
    /// here so a single per-source counters object backs the whole of <see cref="IngestStatus"/>.</summary>
    public void RecordInvalid(int count)
    {
        if (count <= 0) return;
        lock (_gate) { _totalInvalid += count; }
    }

    /// <summary>Plan 009 A1.1: row-level dedup — called with the already-COERCED rows of a batch
    /// (i.e. after <c>IngressRowAcceptance.AcceptBatch</c>, before <see cref="PushAsync"/>), so a
    /// duplicate never consumes buffer capacity and never shows up as coercion-invalid either. A no-op
    /// (every row kept, zero duplicates) when this source has no <see cref="IngestConfig.DedupKeyField"/>
    /// configured. A row missing the configured key field, or holding a null value for it, cannot be
    /// deduped and is kept as-is — silently admitting it is more forgiving than failing the whole
    /// row over an absent optional field.</summary>
    public RowLevelDedupResult FilterRowLevelDuplicates(IReadOnlyList<Dictionary<string, object?>> rows)
    {
        if (_dedup is null || rows.Count == 0)
        {
            return new RowLevelDedupResult([.. rows], 0);
        }

        var kept = new List<Dictionary<string, object?>>(rows.Count);
        var duplicateCount = 0;
        foreach (var row in rows)
        {
            if (row.TryGetValue(Config.DedupKeyField!, out var value) && value is not null && _dedup.Seen(DedupKeyToString(value)))
            {
                duplicateCount++;
            }
            else
            {
                kept.Add(row);
            }
        }

        if (duplicateCount > 0)
        {
            lock (_gate) { _totalDuplicate += duplicateCount; }
        }

        return new RowLevelDedupResult(kept, duplicateCount);
    }

    private static string DedupKeyToString(object value) => value switch
    {
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>The SECOND loss point (IngestModels.cs's header): rows the transport dropped AFTER
    /// a successful publish. The host reads this from its own transport (e.g. Orleans'
    /// PushStreamBus.TotalDropped) and reports the delta here.</summary>
    public void RecordDownstreamDropped(int count)
    {
        if (count <= 0) return;
        lock (_gate) { _downstreamDropped += count; }
    }

    public IngestStatus GetStatus()
    {
        lock (_gate)
        {
            return new IngestStatus
            {
                Policy = Config.Policy,
                CapacityRows = Config.CapacityRows,
                DepthRows = _depth,
                MaxBatchRows = Config.MaxBatchRows,
                TotalAccepted = _totalAccepted,
                TotalRejected = _totalRejected,
                TotalDropped = _totalDropped,
                TotalInvalid = _totalInvalid,
                TotalPublished = _totalPublished,
                DownstreamDropped = _downstreamDropped,
                TotalDuplicate = _totalDuplicate,
                LastPushMs = _lastPushMs,
            };
        }
    }

    /// <summary>Applies an <see cref="IngressAdmission.AdmissionKind.Admit"/> decision while holding
    /// <see cref="_gate"/>: evicts (DropOldest), enqueues (unless Inline — Inline never touches the
    /// queue), and updates every counter <see cref="Decision"/> implies. Returns the rows the caller
    /// admitted (needed by the Inline path, which publishes them itself outside the lock).</summary>
    private List<Dictionary<string, object?>> ApplyAdmitLocked(IngressAdmission.Decision decision, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var admitted = decision.Admit == rows.Count ? [.. rows] : rows.Take(decision.Admit).ToList();

        if (Config.Policy != IngressOverflowPolicy.Inline)
        {
            if (decision.Evict > 0)
            {
                EvictHeadLocked(decision.Evict);
            }

            foreach (var row in admitted)
            {
                _queue.Enqueue(row);
            }
            _depth += admitted.Count;
        }

        _totalAccepted += admitted.Count;
        if (decision.Drop > 0)
        {
            _totalDropped += decision.Drop;
        }
        _lastPushMs = _clock();

        return admitted;
    }

    private void EvictHeadLocked(int n)
    {
        var evicted = 0;
        while (evicted < n && _queue.Count > 0)
        {
            _queue.Dequeue();
            evicted++;
        }
        _depth -= evicted;
    }

    private void SignalSpaceLocked()
    {
        var old = _spaceSignal;
        _spaceSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        old.TrySetResult(true);
    }

    /// <summary>Cumulative rows-published / elapsed-ms since this buffer was created — simple by
    /// design (no sliding window): good enough to turn a raw deficit into an honest RetryAfter
    /// estimate without pretending to more precision than a buffer actually has.</summary>
    private double DrainRatePerMsLocked()
    {
        var elapsed = _clock() - _createdAtMs;
        return elapsed > 0 && _totalPublished > 0 ? (double)_totalPublished / elapsed : 0.0;
    }

    private static IngestResult Result(IngestOutcome outcome, int accepted, int dropped, string? error, int retryAfterMs) => new()
    {
        Outcome = outcome,
        Accepted = accepted,
        Dropped = dropped,
        Invalid = 0,
        RetryAfterMs = retryAfterMs,
        Error = error,
    };
}
