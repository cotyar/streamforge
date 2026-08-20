using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using StreamForge.Engine;
using Xunit;
using Ycs;

namespace StreamForge.Host.Tests;

/// <summary>Silo config mirroring ConnectorGrainClusterTests'/ConnectorGrainPolledClusterTests' own
/// configurator (memory streams + memory grain storage) — duplicated here (not shared), same reason those
/// files give: xunit test classes shouldn't share cluster state.</summary>
internal sealed class CrdtTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class CrdtTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 020 wave B-2: cluster tests for the Orleans CRDT document driver
/// (<see cref="ICrdtDocGrain"/>/<see cref="StreamForge.Host.Grains.CrdtDocGrain"/>) against a real
/// <see cref="TestCluster"/>, mirroring <c>ConnectorGrainClusterTests</c>'/
/// <c>ConnectorGrainPolledClusterTests</c>' own harness shape. Every document here is a REAL
/// <see cref="YDoc"/> built through the Ycs API — the same "no mock where there is no socket" standard
/// <c>CrdtProjectorTests</c> already holds itself to.
///
/// <para><b>How a test document is produced.</b> Each test keeps ONE persistent "edge" <see cref="YDoc"/>
/// (simulating the disconnected client) and, after every edit, calls
/// <see cref="YDoc.EncodeStateAsUpdateV1"/> with NO target state vector — i.e. the edge's ENTIRE current
/// state, deletes included, not an incremental diff. Feeding successive full-state snapshots into the
/// SAME grain document converges exactly like successive incremental updates would (CRDT merge is
/// order- and redundancy-tolerant by construction — see <c>YcsPinTests.ReApplyingTheSameUpdateChangesNothing</c>),
/// and it is simpler to construct than tracking per-merge state vectors.</para>
/// </summary>
public sealed class CrdtDocGrainClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<CrdtTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<CrdtTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition MakeCrdtSource(string name) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Enabled = true,
        Fields =
        [
            new FieldDef("id", FieldType.String),
            new FieldDef("name", FieldType.String),
        ],
        Connector = new ConnectorConfig
        {
            Crdt = new CrdtSourceConfig { RootMap = "root", KeyField = "id" },
        },
    };

    private static YMap Root(YDoc doc) => doc.GetMap("root");

    // ------------------------------------------------------------------
    // 0 — a STOPPED document must not answer like a successful replay.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MergingIntoAStoppedDocumentSaysSoInsteadOfAnsweringLikeAnIdempotentReplay()
    {
        // Found on review of this wave: the grain's defensive floor returned a bare CrdtMergeResult, so
        // "0 applied, 0 rows" came back — byte-identical to the successful idempotent replay the very
        // next test pins. An edge draining its store-and-forward buffer into a stopped document would
        // have read its own data loss as success. The diagnostic is the whole fix, so the diagnostic is
        // what this asserts; the zeroes were never the problem.
        var name = "crdt_stopped_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);

        await grain.StartAsync(MakeCrdtSource(name));
        await grain.StopAsync();

        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var entity = new YMap();
            Root(doc).Set("e1", entity);
            entity.Set("name", "ignored");
        });

        var result = await grain.MergeAsync([doc.EncodeStateAsUpdateV1()]);

        Assert.Equal(0, result.UpdatesApplied);
        Assert.Equal(0, result.RowsEmitted);
        Assert.Contains(result.Diagnostics, d => d.Contains("not running", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // 1 — D7: re-merging the identical batch emits zero rows. The single most important test in this
    // wave — the property the entire store-and-forward design rests on.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ReMergingTheIdenticalBatchEmitsZeroRows()
    {
        var name = "crdt_idem_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update = edge.EncodeStateAsUpdateV1();

            var first = await grain.MergeAsync([update]);
            Assert.Equal(1, first.UpdatesApplied);
            Assert.Equal(1, first.RowsEmitted);

            var replay = await grain.MergeAsync([update]);
            Assert.Equal(1, replay.UpdatesApplied); // the update itself still decodes and applies fine
            Assert.Equal(0, replay.RowsEmitted);     // but changes nothing, so nothing is emitted

            // A THIRD replay, for good measure — the property has to hold indefinitely, not just once.
            var replayAgain = await grain.MergeAsync([update]);
            Assert.Equal(0, replayAgain.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 2 — a document edit reaches a table exactly like any other source's rows.
    // ------------------------------------------------------------------

    [Fact]
    public async Task AMergedEditEmitsARowStampedWithSourceAndTs()
    {
        var name = "crdt_emit_" + Guid.NewGuid().ToString("n")[..8];

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        var subHandle = await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var beforeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var result = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
            Assert.Equal(1, result.RowsEmitted);

            var row = await PollUntilAsync(
                () => Task.FromResult(Snapshot(received).FirstOrDefault(e => (string?)e.GetValueOrDefault("id") == "e1")),
                r => r is not null,
                deadlineSeconds: 15);

            Assert.NotNull(row);
            Assert.Equal(name, row!.Source);
            Assert.True(row.Timestamp >= beforeMs);
            Assert.Equal("Ann", row.GetValueOrDefault("name"));
            Assert.Equal("c", row.GetValueOrDefault("_op"));
            Assert.Equal(1L, Convert.ToInt64(row.GetValueOrDefault("_weight")));
        }
        finally
        {
            await subHandle.UnsubscribeAsync();
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 3 — deleting a key emits the tombstone convention (_op = "d", _weight = -1).
    // ------------------------------------------------------------------

    [Fact]
    public async Task DeletingAKeyEmitsTheTombstone()
    {
        var name = "crdt_delete_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var created = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
            Assert.Equal(1, created.RowsEmitted);

            Root(edge).Delete("e1");
            var deleted = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);

            Assert.Equal(1, deleted.RowsEmitted);
            Assert.DoesNotContain(deleted.Diagnostics, d => d.Contains("failed", StringComparison.OrdinalIgnoreCase));

            var status = await grain.GetStatusAsync();
            Assert.Equal(0, status.EntityCount); // the key no longer enumerates at all
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 4 — a corrupt update in the middle of a batch: the good ones on either side still merge.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ACorruptUpdateInTheMiddleOfABatchDoesNotStrandTheGoodOnes()
    {
        var name = "crdt_corrupt_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update1 = edge.EncodeStateAsUpdateV1();

            var e2 = new YMap();
            Root(edge).Set("e2", e2);
            e2.Set("name", "Bo");
            var update2 = edge.EncodeStateAsUpdateV1(); // full state: e1 (unchanged) + e2 (new)

            // Not a legal v1 update frame — Ycs's decoder is expected to throw decoding it.
            var corrupt = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

            var result = await grain.MergeAsync([update1, corrupt, update2]);

            Assert.Equal(2, result.UpdatesApplied); // update1 and update2 — corrupt is NOT counted
            Assert.Equal(2, result.RowsEmitted);     // e1 created, e2 created
            Assert.Contains(result.Diagnostics, d => d.Contains("update[1]") && d.Contains("skipped"));

            var status = await grain.GetStatusAsync();
            Assert.Equal(2, status.EntityCount);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 5 — self-resume: a real deactivation, and a resume from durable storage.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ADeactivatedDocumentResumesFromPersistedStateAndItsContentSurvives()
    {
        var name = "crdt_resume_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update = edge.EncodeStateAsUpdateV1();

            var merged = await grain.MergeAsync([update]);
            Assert.Equal(1, merged.RowsEmitted);

            // Deactivate for real (the same mechanism ConnectorGrainPolledClusterTests uses) — this is
            // what proves OnActivateAsync's resume path, not just a warm in-memory field surviving.
            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            // Wake it with an ordinary grain call. GetStatusAsync must reflect the SAME document content
            // — 1 entity — read back out of grain storage by the new activation, not an empty fresh doc.
            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);
            Assert.Equal(1, status.UpdatesMerged);
            Assert.Equal(1, status.RowsEmitted);

            // D7 again, now against the RESUMED activation specifically: replaying the same update must
            // still emit nothing, which is only true if the resumed _doc genuinely has e1 already in it
            // (a fresh empty document would instead emit a create row here).
            var replay = await grain.MergeAsync([update]);
            Assert.Equal(0, replay.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    private static List<EventRecord> Snapshot(List<EventRecord> list)
    {
        lock (list) return [.. list];
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last))
            {
                return last;
            }
            await Task.Delay(100);
        }
        return last;
    }
}
