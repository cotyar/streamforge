using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace StreamForge.Client;

public sealed class LiveTableChangedEventArgs(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows) : EventArgs
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; } = rows;
}

/// <summary>
/// One table's Z-set state, kept current by a background reader loop: subscribe -&gt; buffer ->
/// snapshot -&gt; replay (see <see cref="ZSet"/>'s class doc for why buffering is necessary), then
/// live deltas, coalesced to roughly one <see cref="Changed"/> per <see cref="FlushMs"/> regardless
/// of how fast deltas arrive -- handing a consumer a fresh snapshot per delta melts the event under
/// a Monte-Carlo-style firehose (tens of thousands of deltas/sec), while at most one window of
/// staleness is free.
///
/// DESIGN CHOICE -- event, not <c>IAsyncEnumerable&lt;Changed&gt;</c>: an async-enumerable of
/// change notifications forces exactly one owner to enumerate it, which is awkward for the normal
/// .NET shape of "several independent listeners attach and detach over the object's lifetime" (a
/// UI binding, a logger, a test assertion, none of which should have to coordinate over who owns
/// the single enumeration). A plain event lets each attach/detach freely, matching the idiom other
/// live .NET types use (<c>FileSystemWatcher</c>, <c>INotifyCollectionChanged</c>). The cost is the
/// usual one: handlers run synchronously on the internal reader loop and must not block it -- an
/// exception from a handler is caught and logged, never allowed to kill the reader.
///
/// <see cref="Rows"/> is a fresh immutable snapshot on every read, not a live-mutating collection --
/// same reasoning as the Python client's <c>.df</c>: a collection consumers could observe mutating
/// out from under them, with no built-in change notification, is a footgun; <see cref="Changed"/>
/// exists precisely so a consumer never has to poll <see cref="Rows"/> to find out something moved.
/// </summary>
public sealed class LiveTable : IAsyncDisposable
{
    private const int FlushMs = 120;
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(15);

    private readonly ITransport _transport;
    private readonly string _tableName;
    private readonly IReadOnlyList<string>? _keyFields;
    private readonly ILogger _logger;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private ZSet _zset;
    private long _seq;
    private int _reconnects;
    private Task? _readerTask;

    public event EventHandler<LiveTableChangedEventArgs>? Changed;

    internal LiveTable(ITransport transport, string tableName, IReadOnlyList<string>? keyFields, ILogger logger)
    {
        _transport = transport;
        _tableName = tableName;
        _keyFields = keyFields;
        _logger = logger;
        _zset = new ZSet(keyFields);
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

    /// <summary>Poll <paramref name="predicate"/> against <see cref="Rows"/> until it returns true.
    /// A predicate that indexes a column that does not exist yet is treated the same as "not yet"
    /// (an empty table has no columns at all), not as a bug in the predicate.</summary>
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

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_readerTask is not null)
        {
            try { await _readerTask.ConfigureAwait(false); }
            catch { /* the reader loop's own exceptions are expected once cancellation fires */ }
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
                _logger.LogWarning(ex, "streamforge: {Table} reader error (reconnect #{Attempt} in {Backoff}s)",
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
            throw new StreamForgeException($"'{_tableName}' subscription stream ended");
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
            var touchedAny = false;
            while (reader.TryRead(out var item))
            {
                lock (_lock)
                {
                    _zset.Apply(item.Deltas);
                    _seq = item.Seq;
                }
                touchedAny = true;
            }
            if (!touchedAny) continue;

            // Coalesce: give FlushMs a chance to pick up whatever else lands right after, so a
            // burst of batches costs one Changed event, not one per batch.
            try { await Task.Delay(FlushMs, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { /* fall through and emit what we have */ }
            while (reader.TryRead(out var item2))
            {
                lock (_lock)
                {
                    _zset.Apply(item2.Deltas);
                    _seq = item2.Seq;
                }
            }

            Emit();
            if (ct.IsCancellationRequested) return;
        }
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
            _logger.LogError(ex, "streamforge: Changed handler for '{Table}' raised", _tableName);
        }
    }
}
