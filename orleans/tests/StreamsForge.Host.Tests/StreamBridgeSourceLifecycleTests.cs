using Microsoft.Extensions.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Services;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Track A section B: SOURCE lifecycle events reach <see cref="StreamBridgeService"/>, and what the bridge
/// does with a source's events once subscribed is PACE them, not drop them.
///
/// <para>Both halves are about the same reported symptom — "messages from non-generator sources appear to
/// vanish" — in the one place that is genuinely only a UI relay. Nothing here is on the pipeline/table data
/// path: this service exists to push a source's raw events at a browser over SignalR, and a row it drops is
/// still in the table. That is why the fix is allowed to be a pacing heuristic rather than a queue.</para>
///
/// <para>Two prior behaviours are under test:
/// <list type="number">
/// <item>A source was discovered ONLY by the bridge's 30 s add-only refresh loop, so a source created in
/// the console produced nothing on the wire for up to 30 seconds, and a deleted one kept its relay
/// forever. <see cref="A_newly_upserted_enabled_source_is_relayed_without_waiting_for_the_30s_poll"/>'s
/// 10 s deadline IS its assertion — it has to stay comfortably under that 30 s poll, or the test proves
/// only that the backstop still works.</item>
/// <item>The relay DROPPED any event arriving under 50 ms after the last one it sent. A polled source
/// emits a whole cycle's rows in a tight loop, so a six-row burst reached the hub as one or two rows —
/// measured on this very fixture before the change: 1 of 6 in three consecutive runs (see the class-level
/// note in the burst test). Now the callback waits out the remainder of its slot and sends, which is
/// correct because Orleans delivers ONE subscription's callbacks sequentially.</item>
/// </list></para>
///
/// <para>Reuses <c>BridgeRaceSiloConfigurator</c> / <c>BridgeRaceClientConfigurator</c> /
/// <c>RecordingHubContext</c> / <c>AlreadyStartedLifetime</c> from
/// <see cref="StreamBridgeServiceStartupRaceTests"/> (same assembly, all <c>internal</c>) but stands up its
/// OWN <see cref="TestCluster"/> — per that file's own "xunit test classes shouldn't share cluster state"
/// rule, which matters more than usual here because this class both seeds and mutates the catalog.</para>
/// </summary>
public sealed class StreamBridgeSourceLifecycleTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratch = null!;

    public async Task InitializeAsync()
    {
        _scratch = Path.Combine(Path.GetTempPath(), "sf-bridge-source-lifecycle", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(_scratch);

        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<BridgeRaceSiloConfigurator>();
        builder.AddClientBuilderConfigurator<BridgeRaceClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try
        {
            Directory.Delete(_scratch, recursive: true);
        }
        catch
        {
            // scratch cleanup is best-effort; a leaked temp dir must never fail a green run.
        }
    }

    // =============================================================================================
    // (a) a lifecycle event, not the 30s poll, is what starts the relay
    // =============================================================================================

    /// <summary>The source is created AFTER the bridge has finished onboarding the default environment —
    /// which is the whole point. Onboarding subscribes every source already in the catalog; the 30 s
    /// <c>PeriodicTimer</c> that would eventually pick up a NEW one has not ticked once by the deadline
    /// asserted here. So the only path by which a <c>sourceEvent</c> can reach the hub inside 10 s is the
    /// <c>source-started</c> lifecycle event this plan added.
    ///
    /// <para>The source deliberately has NO <c>DedupKeyField</c>: <c>RegistryGrain.UpsertSourceAsync</c>
    /// starts the connector grain BEFORE it publishes the lifecycle event, so the very first poll can and
    /// does land before the bridge has subscribed. A dedup key would make those rows unrepeatable and turn
    /// this into a race; without one the file is re-read every second, so the assertion is "the relay
    /// exists well inside 10 s", not "no row was ever missed". <c>IntervalMs = 1000</c> is the platform's
    /// poll FLOOR (<c>ScheduleCalc.MinIntervalMs</c>) — anything smaller is invalid and silently becomes a
    /// 30 s cadence, which would make this test assert the opposite of what it means to.</para></summary>
    [Fact]
    public async Task A_newly_upserted_enabled_source_is_relayed_without_waiting_for_the_30s_poll()
    {
        var sink = new List<(string Method, object?[] Args)>();
        IHostedService bridge = new StreamBridgeService(_cluster.Client, new RecordingHubContext(sink), new AlreadyStartedLifetime());
        await bridge.StartAsync(CancellationToken.None);
        try
        {
            var registry = await WaitForOnboardingAsync();

            var name = "bridge_lifecycle_live";
            var path = Path.Combine(_scratch, $"{name}.ndjson");
            await File.WriteAllTextAsync(path, "{\"id\":1,\"value\":\"hello\"}\n");
            await registry.UpsertSourceAsync(FileSource(name, path, enabled: true));

            var relayed = await PollUntilAsync(
                () => Task.FromResult(CountSourceEvents(sink, name)),
                n => n > 0,
                deadlineSeconds: 10);

            Assert.True(
                relayed > 0,
                "no sourceEvent was relayed within 10s of creating the source — the bridge only learned " +
                "about it from its 30s add-only refresh loop, which is exactly the reported symptom.");
        }
        finally
        {
            await bridge.StopAsync(CancellationToken.None);
        }
    }

    // =============================================================================================
    // (b) pacing: a burst is relayed in full, in order
    // =============================================================================================

    /// <summary>Six events published back-to-back on a source's stream must arrive as six
    /// <c>sourceEvent</c> messages in publish order.
    ///
    /// <para><b>Measured against the pre-change code on this exact fixture: 2, 1 and 1 of 6</b> over three
    /// consecutive runs (the pacing branch in <c>SubscribeToSourceAsync</c> temporarily reverted to its
    /// original "return early if under 50 ms" form; <c>Assert.Equal() Failure: Expected 6, Actual 2 / 1 /
    /// 1</c>). It is 6 every time with the change in place. Which of the six survived was luck — how the
    /// 50 ms window happened to line up with delivery — which is why the old number varies and the new one
    /// does not. The burst is what a polled source's emission loop actually looks like, so that 1-or-2 of
    /// 6 is the "vanishing messages" report in miniature.</para>
    ///
    /// <para>The source is DISABLED on purpose: nothing must be polling it, so every event on its stream
    /// is one this test put there and the count is exact. A disabled source is still subscribed by the
    /// bridge — the 30 s refresh always subscribed every catalogued source regardless of <c>Enabled</c>,
    /// and <c>source-stopped</c> deliberately agrees with it (see
    /// <c>StreamBridgeService.OnSourceLifecycleEventAsync</c>), which is what makes this test possible at
    /// all.</para></summary>
    [Fact]
    public async Task A_tight_burst_of_six_events_is_relayed_six_for_six()
    {
        var sink = new List<(string Method, object?[] Args)>();
        IHostedService bridge = new StreamBridgeService(_cluster.Client, new RecordingHubContext(sink), new AlreadyStartedLifetime());
        await bridge.StartAsync(CancellationToken.None);
        try
        {
            var registry = await WaitForOnboardingAsync();

            var name = "bridge_burst_src";
            await registry.UpsertSourceAsync(FileSource(name, Path.Combine(_scratch, "never-read.ndjson"), enabled: false));

            var stream = SourceStream(name);
            await WaitForRelayAsync(sink, name, stream);

            // Clear only AFTER the relay is provably live, so the six below are the only events counted.
            lock (sink) sink.Clear();

            for (var i = 0; i < 6; i++)
            {
                await stream.OnNextAsync(new EventRecord { ["seq"] = (long)i });
            }

            var count = await PollUntilAsync(
                () => Task.FromResult(CountSourceEvents(sink, name)),
                n => n >= 6,
                deadlineSeconds: 10);

            Assert.Equal(6, count);
            Assert.Equal([0L, 1L, 2L, 3L, 4L, 5L], RelayedSeqs(sink, name));
        }
        finally
        {
            await bridge.StopAsync(CancellationToken.None);
        }
    }

    // =============================================================================================
    // (c) deletion drops the relay
    // =============================================================================================

    /// <summary>After <c>DeleteSourceAsync</c>, events published on the dead source's stream must reach
    /// nobody. Proving an ABSENCE needs a fence rather than a sleep: a SECOND source is upserted after the
    /// delete, and this test waits for THAT source's relay to go live before publishing on the dead
    /// stream. Both lifecycle events travel the same stream to the same subscriber, which Orleans delivers
    /// in order, so "the fence source is relaying" implies "the bridge has already processed the delete".
    /// A fixed <c>Task.Delay</c> would be the weakest possible form of the same claim.</summary>
    [Fact]
    public async Task Deleting_a_source_removes_its_relay()
    {
        var sink = new List<(string Method, object?[] Args)>();
        IHostedService bridge = new StreamBridgeService(_cluster.Client, new RecordingHubContext(sink), new AlreadyStartedLifetime());
        await bridge.StartAsync(CancellationToken.None);
        try
        {
            var registry = await WaitForOnboardingAsync();

            var name = "bridge_deleted_src";
            await registry.UpsertSourceAsync(FileSource(name, Path.Combine(_scratch, "never-read.ndjson"), enabled: false));

            var stream = SourceStream(name);
            await WaitForRelayAsync(sink, name, stream);
            lock (sink) sink.Clear();

            for (var i = 0; i < 6; i++)
            {
                await stream.OnNextAsync(new EventRecord { ["seq"] = (long)i });
            }
            var before = await PollUntilAsync(
                () => Task.FromResult(CountSourceEvents(sink, name)),
                n => n >= 6,
                deadlineSeconds: 10);
            Assert.Equal(6, before);

            Assert.True(await registry.DeleteSourceAsync(name));

            // The fence — see this test's doc comment.
            var fenceName = "bridge_delete_fence";
            await registry.UpsertSourceAsync(FileSource(fenceName, Path.Combine(_scratch, "never-read.ndjson"), enabled: false));
            var fenceStream = SourceStream(fenceName);
            await WaitForRelayAsync(sink, fenceName, fenceStream);

            for (var i = 6; i < 9; i++)
            {
                await stream.OnNextAsync(new EventRecord { ["seq"] = (long)i });
            }

            // A second fence round-trip AFTER the three publishes: the dead source's events were enqueued
            // first, so anything still capable of relaying them has had its turn by the time a later event
            // on a different stream has been relayed twice over.
            await WaitForRelayAsync(sink, fenceName, fenceStream);
            await WaitForRelayAsync(sink, fenceName, fenceStream);

            Assert.Equal(6, CountSourceEvents(sink, name));
        }
        finally
        {
            await bridge.StopAsync(CancellationToken.None);
        }
    }

    // =============================================================================================
    // helpers
    // =============================================================================================

    /// <summary>A file-kind connector source with a declared shape and no dedup key. NDJSON, poll floor
    /// interval — see the first test's doc comment for why both of those are deliberate.</summary>
    private static SourceDefinition FileSource(string name, string path, bool enabled) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = enabled,
        EventsPerSecond = 0,
        Fields = [new FieldDef("id", FieldType.Long), new FieldDef("value", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec { ItemsPath = "$" },
        },
    };

    private Orleans.Streams.IAsyncStream<EventRecord> SourceStream(string name) =>
        _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

    /// <summary>Waits until the bridge has finished onboarding the default environment, using the same
    /// signal <see cref="StreamBridgeServiceStartupRaceTests"/> uses: the seeded "order_states" table
    /// reaching Running proves <c>DiscoverEnvironmentsAsync</c> got past its own
    /// <c>EnsureInitializedAsync</c> — and the lifecycle-stream subscription is established BEFORE that
    /// call, so by then it is certainly in place.</summary>
    private async Task<IRegistryGrain> WaitForOnboardingAsync()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var tables = await PollUntilAsync(
            () => registry.GetTablesAsync(),
            ts => ts.Any(t => t.Name == "order_states" && t.Status == PipelineStatus.Running),
            deadlineSeconds: 20);
        Assert.Contains(tables, t => t.Name == "order_states" && t.Status == PipelineStatus.Running);
        return registry;
    }

    /// <summary>Probes a source's stream until the bridge relays one — proving the subscription is live —
    /// and THEN stops publishing and waits for the relay count to go quiet, so nothing this helper put on
    /// the wire is still in flight when the caller clears the sink.
    ///
    /// <para>Both halves earned their place in a red run. Returning on the first relay alone let a probe
    /// published one poll earlier land after the caller's <c>Clear</c>, turning an exact "6" into a "7".
    /// The obvious repair — wait until <c>relayed &gt;= published</c> — is WRONG here and hung for the
    /// full 20 s: memory streams have no replay, so every probe published before the subscription existed
    /// is gone for good and that count can never be reached. Quiescence is the only claim actually
    /// available: stop producing, then observe that nothing more arrives.</para></summary>
    private static async Task WaitForRelayAsync(
        List<(string Method, object?[] Args)> sink,
        string name,
        Orleans.Streams.IAsyncStream<EventRecord> stream)
    {
        var baseline = CountSourceEvents(sink, name);
        var live = false;
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            await stream.OnNextAsync(new EventRecord { ["probe"] = true });
            await Task.Delay(200);
            if (CountSourceEvents(sink, name) > baseline)
            {
                live = true;
                break;
            }
        }

        Assert.True(live, $"the bridge never relayed a probe event for source '{name}' within 20s — its " +
                          "subscription was never established.");

        var settled = CountSourceEvents(sink, name);
        var quietDeadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < quietDeadline)
        {
            await Task.Delay(300);
            var now = CountSourceEvents(sink, name);
            if (now == settled)
            {
                return;
            }
            settled = now;
        }
    }

    private static int CountSourceEvents(List<(string Method, object?[] Args)> sink, string name)
    {
        lock (sink) return sink.Count(s => s.Method == "sourceEvent" && (string?)s.Args[0] == name);
    }

    /// <summary>The "seq" value of every relayed sourceEvent for a source, in relay order — the ORDER
    /// half of the burst assertion. Probe events (no "seq") are skipped so a caller that fenced with a
    /// probe still gets a clean sequence.</summary>
    private static List<long> RelayedSeqs(List<(string Method, object?[] Args)> sink, string name)
    {
        lock (sink)
        {
            return sink
                .Where(s => s.Method == "sourceEvent" && (string?)s.Args[0] == name)
                .Select(s => s.Args[1] as EventRecord)
                .Where(r => r is not null && r.TryGetValue("seq", out var v) && v is long)
                .Select(r => (long)r!["seq"]!)
                .ToList();
        }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        if (until(last)) return last;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }
}
