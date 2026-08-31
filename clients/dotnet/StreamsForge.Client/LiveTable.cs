using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace StreamsForge.Client;

public sealed class LiveTableChangedEventArgs(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows) : EventArgs
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; } = rows;
}

/// <summary>
/// One table's Z-set state, kept current by a background reader loop: subscribe -&gt; buffer ->
/// snapshot -&gt; replay (see <see cref="ZSet"/>'s class doc for why buffering is necessary), then
/// live deltas. <see cref="Rows"/> (and <see cref="Value"/>, <see cref="WaitForAsync"/>) reflect a
/// batch the instant it is applied -- that is never delayed. Only the <see cref="Changed"/>
/// notification (and its <see cref="WatchAsync"/> twin) follows a leading-edge/trailing-coalesce
/// window, <see cref="FlushWindow"/> (default <see cref="DefaultFlushWindow"/>, 16ms -- one frame
/// at 60Hz, the natural ceiling since no UI consumer can display more than one update per frame
/// anyway): a batch that lands at least a window after the last emit fires <see cref="Changed"/>
/// immediately, no artificial delay -- a lone update on an otherwise-quiet table is never held
/// back. A batch that lands sooner merges into a single pending emit scheduled for (last emit +
/// window), so a burst of batches inside one window still costs exactly one event, not one per
/// batch -- handing a consumer a fresh snapshot per delta melts the event under a Monte-Carlo-style
/// firehose (tens of thousands of deltas/sec). <c>flush = TimeSpan.Zero</c> disables coalescing
/// entirely: one <see cref="Changed"/> per applied batch. In short: the window changes WHEN a
/// consumer is told, never WHAT the state is -- see <see cref="WaitForAsync"/>'s doc for the
/// consequence of that split.
///
/// DESIGN CHOICE -- <see cref="Changed"/> is an event, and <see cref="WatchAsync"/> is offered
/// ALONGSIDE it, not instead of it: an async-enumerable of change notifications forces exactly one
/// owner to enumerate it, which is awkward for the normal .NET shape of "several independent
/// listeners attach and detach over the object's lifetime" (a UI binding, a logger, a test
/// assertion, none of which should have to coordinate over who owns the single enumeration). A
/// plain event lets each attach/detach freely, matching the idiom other live .NET types use
/// (<c>FileSystemWatcher</c>, <c>INotifyCollectionChanged</c>). The cost is the usual one: handlers
/// run synchronously on the internal reader loop and must not block it -- an exception from a
/// handler is caught and logged, never allowed to kill the reader. <see cref="WatchAsync"/> exists
/// for the opposite, equally normal shape: a single caller that wants <c>await foreach</c> instead
/// of an event handler, at the cost of being exactly that -- single-owner.
///
/// <see cref="Rows"/> is a fresh immutable snapshot on every read, not a live-mutating collection --
/// same reasoning as the Python client's <c>.df</c>: a collection consumers could observe mutating
/// out from under them, with no built-in change notification, is a footgun; <see cref="Changed"/>
/// exists precisely so a consumer never has to poll <see cref="Rows"/> to find out something moved.
/// </summary>
public sealed class LiveTable : IAsyncDisposable
{
    /// <summary>16ms -- one frame at 60Hz. Used when <c>flush</c> is omitted at
    /// <see cref="StreamsForgeClient.TableAsync"/> / <see cref="StreamsForgeClient.SqlAsync"/>.</summary>
    public static readonly TimeSpan DefaultFlushWindow = TimeSpan.FromMilliseconds(16);

    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(15);

    private readonly ITransport _transport;
    private readonly string _tableName;
    private readonly IReadOnlyList<string>? _keyFields;
    private readonly ILogger _logger;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ---- Changed coalescing state (guarded by _emitGate, deliberately separate from _lock so
    // scheduling an emit never has to contend with the zset-mutation path) ----
    private readonly object _emitGate = new();
    private DateTime _lastEmitUtc = DateTime.MinValue;
    private bool _trailingPending;
    private Task? _trailingTask;

    private ZSet _zset;
    private long _seq;
    private int _reconnects;
    private Task? _readerTask;

    public event EventHandler<LiveTableChangedEventArgs>? Changed;

    /// <summary>The leading-edge/trailing-coalesce window applied to <see cref="Changed"/> (and
    /// <see cref="WatchAsync"/>) notifications. See the class doc.</summary>
    public TimeSpan FlushWindow { get; }

    internal LiveTable(ITransport transport, string tableName, IReadOnlyList<string>? keyFields, ILogger logger, TimeSpan? flush = null)
    {
        _transport = transport;
        _tableName = tableName;
        _keyFields = keyFields;
        _logger = logger;
        _zset = new ZSet(keyFields);

        FlushWindow = flush ?? DefaultFlushWindow;
        if (FlushWindow < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(flush), "flush window must be >= TimeSpan.Zero");
    }

    internal async Task StartAsync(TimeSpan timeout, CancellationToken ct)
    {
        _readerTask = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);

        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token, _cts.Token);
        try
        {
            await _ready.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            await DisposeAsync().ConfigureAwait(false);
            throw new NotReadyException(
                $"table '{_tableName}' did not fill within {timeout.TotalSeconds:0}s -- a brand-new " +
                "table gets no backfill, so this is expected until something pushes to it");
        }
    }

    // ---- public surface ----

    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows
    {
        get { lock (_lock) return _zset.Rows(); }
    }

    public bool Ready => _ready.Task.IsCompletedSuccessfully;

    public int Reconnects => Volatile.Read(ref _reconnects);

    public long Seq
    {
        get { lock (_lock) return _seq; }
    }

    /// <summary>The first row whose fields match every entry in <paramref name="keys"/>, or null.</summary>
    public object? Value(string column, IReadOnlyDictionary<string, object?> keys)
    {
        foreach (var row in Rows)
        {
            var match = true;
            foreach (var (k, v) in keys)
            {
                if (!row.TryGetValue(k, out var rowValue) || !Equals(rowValue, v)) { match = false; break; }
            }
            if (match) return row.TryGetValue(column, out var value) ? value : null;
        }
        return null;
    }

    /// <summary>Poll <paramref name="predicate"/> against <see cref="Rows"/> every 50ms until it
    /// returns true. A predicate that indexes a column that does not exist yet is treated the same
    /// as "not yet" (an empty table has no columns at all), not as a bug in the predicate.
    ///
    /// LATENCY NOTE: <see cref="Rows"/> is updated the instant a batch is applied, independent of
    /// the <see cref="Changed"/>/<see cref="WatchAsync"/> coalescing window (see the class doc) --
    /// so this method's worst case is the 50ms poll interval alone, NOT the poll interval plus
    /// <see cref="FlushWindow"/>. That makes polling here strictly slower than reacting to
    /// <see cref="Changed"/> or <see cref="WatchAsync"/> once <see cref="FlushWindow"/> is at or
    /// below the poll interval (true of the 16ms default): a caller that cares about latency should
    /// prefer those over this method. This method stays poll-based rather than being rebuilt on top
    /// of <see cref="Changed"/> because a plain "block until predicate(Rows) is true" is simpler to
    /// reason about and to use from a non-event-driven caller (e.g. a test) than wiring a temporary
    /// handler would be -- the 50ms cost of that simplicity was true before this change and remains
    /// true after it.</summary>
    public async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> WaitForAsync(
        Func<IReadOnlyList<IReadOnlyDictionary<string, object?>>, bool> predicate, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var rows = Rows;
            try
            {
                if (predicate(rows)) return rows;
            }
            catch (KeyNotFoundException)
            {
                // not yet -- the column doesn't exist yet, exactly the state this method waits out
            }
            if (DateTime.UtcNow >= deadline)
                throw new NotReadyException($"WaitForAsync on '{_tableName}' timed out after {timeout.TotalSeconds:0}s");
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// An <see cref="IAsyncEnumerable{T}"/> view of <see cref="Changed"/>, for <c>await foreach</c>
    /// consumers -- offered ALONGSIDE the event, not instead of it (see the class doc's DESIGN
    /// CHOICE note: several independent listeners is the normal case, and an enumerable models that
    /// badly).
    ///
    /// Each call returns an INDEPENDENT enumerator backed by its own
    /// <c>Channel.CreateBounded&lt;...&gt;(1)</c> with <see cref="BoundedChannelFullMode.DropOldest"/>,
    /// so two concurrent <c>await foreach</c> loops never steal items from each other. These channel
    /// items are STATE SNAPSHOTS, not queued events -- latest wins: only the newest matters, and a
    /// slow consumer must never (a) block the reader loop, which is obliged to keep draining the
    /// transport, nor (b) accumulate snapshots it no longer wants. A back-pressured, unbounded, or
    /// drop-newest queue would get either of those wrong; capacity-1 drop-oldest gets neither wrong,
    /// at the honest cost below.
    ///
    /// WARNING: intermediate snapshots WILL be skipped by design under a burst -- if three batches
    /// land before this enumerator's consumer calls <c>MoveNextAsync</c> again, it observes only the
    /// last of the three. This is therefore NOT an audit log of every batch; a caller that needs to
    /// see every one (a logger, a test assertion counting events) must use <see cref="Changed"/>
    /// instead.
    ///
    /// Subscribing hooks <see cref="Changed"/>; the enumeration finishing, being cancelled via
    /// <paramref name="ct"/>, or being disposed (<c>await foreach</c>'s implicit
    /// <c>DisposeAsync</c>) unhooks it and completes the channel -- no dangling handler keeps this
    /// <see cref="LiveTable"/> observing on the caller's behalf after the caller is done.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyList<IReadOnlyDictionary<string, object?>>> WatchAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateBounded<IReadOnlyList<IReadOnlyDictionary<string, object?>>>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                // Not truly single-writer: an immediate (leading-edge) emit on the reader-loop
                // thread and a trailing emit firing on a threadpool thread can, in rare timing,
                // both call Emit() close enough together to race here.
                SingleWriter = false,
            });

        void OnChanged(object? _, LiveTableChangedEventArgs e) => channel.Writer.TryWrite(e.Rows);

        Changed += OnChanged;
        try
        {
            await foreach (var rows in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return rows;
        }
        finally
        {
            Changed -= OnChanged;
            channel.Writer.TryComplete();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_readerTask is not null)
        {
            try { await _readerTask.ConfigureAwait(false); }
            catch { /* the reader loop's own exceptions are expected once cancellation fires */ }
        }
        // By the time _readerTask above has completed, nothing can schedule a NEW trailing task
        // (only the reader loop does that) -- so _trailingTask here, if set, is the final one and
        // awaiting it both guarantees no post-dispose Changed fires and leaves no task unobserved.
        var trailing = _trailingTask;
        if (trailing is not null)
        {
            try { await trailing.ConfigureAwait(false); }
            catch { /* cancelled before its window elapsed -- expected */ }
        }
        _cts.Dispose();
    }

    // ---- reader loop ----

    private async Task RunAsync(CancellationToken ct)
    {
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await SubscribeSnapshotReplayAsync(ct).ConfigureAwait(false);
                backoff = TimeSpan.FromSeconds(1);
            }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested) return;
                var attempt = Interlocked.Increment(ref _reconnects);
                _logger.LogWarning(ex, "streamsforge: {Table} reader error (reconnect #{Attempt} in {Backoff}s)",
                    _tableName, attempt, backoff.TotalSeconds);
                try { await Task.Delay(backoff, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                backoff = TimeSpan.FromSeconds(Math.Min(MaxBackoff.TotalSeconds, backoff.TotalSeconds * 2));
            }
        }
    }

    private async Task SubscribeSnapshotReplayAsync(CancellationToken ct)
    {
        // A resumed connection without a fresh snapshot silently corrupts the Z-set (deltas
        // emitted while the connection was down are gone) -- every (re)connect starts clean.
        lock (_lock) _zset = new ZSet(_keyFields);

        var channel = Channel.CreateUnbounded<(IReadOnlyList<RowDelta> Deltas, long Seq)>();
        Exception? readerError = null;

        // Awaited HERE, before the snapshot read and before spawning the reader task -- this is
        // the load-bearing ordering the buffer/snapshot/replay contract depends on. Getting this
        // wrong (e.g. starting the transport's subscribe-and-iterate inside the Task.Run below,
        // racing it against the SnapshotAsync call that follows) was a real bug: a delta the
        // server emits before our subscription is registered is never sent to us at all -- not
        // buffered, not replayable, just gone -- which is a different failure than "arrived before
        // the snapshot" (what buffering below actually handles). ITransport.SubscribeAsync's own
        // doc has the full account of why its signature forces this.
        var subscription = await _transport.SubscribeAsync(_tableName, ct).ConfigureAwait(false);

        var readerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in subscription.WithCancellation(ct).ConfigureAwait(false))
                    await channel.Writer.WriteAsync(item, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                readerError = ex;
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        }, CancellationToken.None);

        try
        {
            var (snapshotRows, snapshotSeq) = await _transport.SnapshotAsync(_tableName, 500, ct).ConfigureAwait(false);

            // Drain whatever the reader buffered while the snapshot read was in flight -- these
            // deltas may predate, straddle or postdate the snapshot; AlreadyReflected() is the
            // content-based heuristic that decides which (see ZSet's class doc).
            var buffered = new List<(IReadOnlyList<RowDelta> Deltas, long Seq)>();
            while (channel.Reader.TryRead(out var item)) buffered.Add(item);

            lock (_lock)
            {
                _zset.Seed(snapshotRows);
                _seq = snapshotSeq;
                foreach (var (deltas, seq) in buffered)
                {
                    if (_zset.AlreadyReflected(deltas)) continue;
                    _zset.Apply(deltas);
                    _seq = seq;
                }
            }

            if (ct.IsCancellationRequested) return;
            _ready.TrySetResult();

            await LiveLoopAsync(channel.Reader, ct).ConfigureAwait(false);

            if (readerError is not null) throw readerError;
            throw new StreamsForgeException($"'{_tableName}' subscription stream ended");
        }
        finally
        {
            await readerTask.ConfigureAwait(false);
        }
    }

    private async Task LiveLoopAsync(ChannelReader<(IReadOnlyList<RowDelta> Deltas, long Seq)> reader, CancellationToken ct)
    {
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            // Apply every currently-available batch AS IT IS READ -- Rows must never lag behind
            // the transport just because a Changed notification is being coalesced (see the class
            // doc: the window changes WHEN a consumer is told, never WHAT the state is). Scheduling
            // the (possibly deferred) emit per batch, rather than delaying the drain itself, is also
            // what keeps this loop free to keep reading while a trailing emit is pending.
            while (reader.TryRead(out var item))
            {
                lock (_lock)
                {
                    _zset.Apply(item.Deltas);
                    _seq = item.Seq;
                }
                ScheduleEmit(ct);
            }
        }
    }

    /// <summary>Leading-edge/trailing-coalesce decision for one applied batch. At least
    /// <see cref="FlushWindow"/> since the last emit -&gt; emit right now, no delay. Sooner than
    /// that -&gt; merge into the single pending trailing emit (scheduling one if none is already
    /// pending; a batch that arrives while one IS pending does nothing further -- the pending emit
    /// will read <see cref="Rows"/> fresh when it fires, so this batch's effect is already covered).</summary>
    private void ScheduleEmit(CancellationToken ct)
    {
        if (ct.IsCancellationRequested) return;

        bool emitNow;
        var wait = TimeSpan.Zero;
        lock (_emitGate)
        {
            var elapsed = DateTime.UtcNow - _lastEmitUtc;
            if (elapsed >= FlushWindow)
            {
                _lastEmitUtc = DateTime.UtcNow;
                emitNow = true;
            }
            else
            {
                emitNow = false;
                if (_trailingPending) return; // already scheduled -- this batch just merges into it
                _trailingPending = true;
                wait = FlushWindow - elapsed;
            }
        }

        if (emitNow)
        {
            Emit();
            return;
        }

        _trailingTask = TrailingEmitAsync(wait, ct);
    }

    private async Task TrailingEmitAsync(TimeSpan wait, CancellationToken ct)
    {
        try { await Task.Delay(wait, ct).ConfigureAwait(false); }
        catch (OperationCanceledException) { return; } // disposed before the window elapsed -- no emit, no leak

        lock (_emitGate)
        {
            _trailingPending = false;
            _lastEmitUtc = DateTime.UtcNow;
        }
        if (ct.IsCancellationRequested) return; // dispose raced the delay's own completion
        Emit();
    }

    private void Emit()
    {
        var handler = Changed;
        if (handler is null) return;
        var rows = Rows;
        try
        {
            handler(this, new LiveTableChangedEventArgs(rows));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "streamsforge: Changed handler for '{Table}' raised", _tableName);
        }
    }
}
