using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StreamsForge.Client.Tests.Fixtures;

/// <summary>
/// An in-memory <see cref="ITransport"/> for <see cref="LiveTable"/> unit tests that need no
/// engine: <see cref="SnapshotAsync"/> returns whatever <see cref="SetSnapshot"/> last set (empty
/// by default), and <see cref="Push"/> writes one (deltas, seq) batch straight onto the live
/// subscription channel <see cref="LiveTable"/>'s reader loop drains -- letting a test control
/// exactly when and how many batches "arrive" without any real network or engine involved.
///
/// <see cref="SubscribeAsync"/>'s own contract (see <see cref="ITransport"/>'s class doc) is
/// honored literally: establishment (recording that a subscription now exists) happens eagerly,
/// before the method returns, and only the mechanical read loop is lazy.
/// </summary>
internal sealed class FakeTransport : ITransport
{
    private readonly Channel<(IReadOnlyList<RowDelta> Deltas, long Seq)> _live =
        Channel.CreateUnbounded<(IReadOnlyList<RowDelta> Deltas, long Seq)>();

    private IReadOnlyList<RowDelta> _snapshotRows = Array.Empty<RowDelta>();
    private long _snapshotSeq;

    public string Name => "fake";

    /// <summary>Completes once <see cref="SubscribeAsync"/> has been called at least once --
    /// lets a test await "the reader loop is definitely subscribed" without a fixed sleep.</summary>
    public TaskCompletionSource Subscribed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void SetSnapshot(IReadOnlyList<RowDelta> rows, long seq = 0)
    {
        _snapshotRows = rows;
        _snapshotSeq = seq;
    }

    /// <summary>Pushes one batch onto the live subscription stream. Never blocks -- the underlying
    /// channel is unbounded, mirroring the real transports' own subscription channel.</summary>
    public void Push(IReadOnlyList<RowDelta> deltas, long seq) => _live.Writer.TryWrite((deltas, seq));

    public Task<IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)>> SubscribeAsync(string table, CancellationToken ct)
    {
        Subscribed.TrySetResult();
        return Task.FromResult(ReadLiveAsync(ct));
    }

    private async IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)> ReadLiveAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var item in _live.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            yield return item;
    }

    public Task<(IReadOnlyList<RowDelta> Rows, long Seq)> SnapshotAsync(string table, int limit, CancellationToken ct) =>
        Task.FromResult((_snapshotRows, _snapshotSeq));
}
