using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 014 F: the Dapr flavour's polled (PULL) connector arm — cursor round-trip, restart resumption,
/// failure discipline, and <see cref="PolledBatch.HasMore"/> re-arming through the timer this actor already
/// owns.
///
/// <para><b>What is asserted, and what cannot be.</b> Every test here drives
/// <see cref="ConnectorBookkeeping"/>, not <see cref="ConnectorActor"/> itself — the established pattern of
/// this suite (see ConnectorActorLogicTests, GeneratorBatchingTests, CatalogStoreTests), and not a matter of
/// preference: a Dapr actor's <c>StateManager</c> talks to a sidecar, so instantiating one here would be a
/// test of whether a sidecar happens to be running. <see cref="ConnectorActor.TickAsync"/>'s polled branch is
/// therefore three lines — resolve from <see cref="PolledTransports"/>, call
/// <see cref="ConnectorBookkeeping.RunPolledCycleAsync"/>, apply
/// <see cref="ConnectorBookkeeping.MarkDueNow"/> when it says so — over a body of decisions that all live
/// here and are all exercised below. What genuinely is NOT covered by any test in this file: the
/// <c>daprClient.PublishEventAsync</c> hop itself, which is the same single call site the url/file/folder
/// kinds have always published through and is equally untested for them. What these tests pin instead is
/// that the polled arm hands that call site the identical <see cref="PollCycleResult"/> shape — coerced,
/// stamped, non-empty — so "it lands as events" reduces to a path the flavour already runs in production.</para>
///
/// <para><b>Registration hygiene.</b> <see cref="PolledTransports.Register"/> is process-global and
/// permanent, so exactly one instance of one distinctively-named fake is registered, from a static
/// constructor, and only the two registry tests use it; every other test hands its own throwaway instance
/// straight to <see cref="ConnectorBookkeeping.RunPolledCycleAsync"/>, which takes the transport as a
/// parameter and never consults the registry. Nothing leaks into another test class's run.</para>
/// </summary>
public class ConnectorActorPolledTests
{
    /// <summary>Deliberately not "postgres", and deliberately not the Orleans suite's "fizzdb" either: a
    /// test kind that could ever collide with a kind the host registers for real would turn a wiring bug
    /// into a green test run.</summary>
    private const string FakeKind = "fizzdb-dapr";

    private static readonly FakeDb Registered = new();

    static ConnectorActorPolledTests() => PolledTransports.Register(Registered);

    // ------------------------------------------------------------------
    // The fake: a scripted result-set reader, one page consumed per poll.
    // ------------------------------------------------------------------

    private sealed class FakeDb : IPolledTransport
    {
        public Queue<PolledBatch> Pages { get; } = new();

        /// <summary>Set to make the next poll fail the way a real one does — an exception out of the
        /// driver, not a returned error code. Cleared by the throw, so the recovery cycle is assertable.</summary>
        public Exception? Fail { get; set; }

        /// <summary>Every cursor the driver handed over, in order: the evidence that what was persisted is
        /// what came back.</summary>
        public List<string?> SeenCursors { get; } = [];

        public string Kind => FakeKind;

        public void Validate(SourceDefinition def, List<string> errors)
        {
        }

        public Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
        {
            SeenCursors.Add(cursor);
            if (Fail is not null)
            {
                var fail = Fail;
                Fail = null;
                throw fail;
            }

            return Task.FromResult(Pages.Count > 0 ? Pages.Dequeue() : new PolledBatch([], null, false));
        }

        public TransportDescriptor Describe() => new()
        {
            Kind = FakeKind,
            Label = "FizzDB (dapr tests)",
            ConfigProperty = "db",
            Polled = true,
            Mapping = false,
        };
    }

    private static SourceDefinition PolledSource(int intervalMs = 30_000, string dedupColumn = "id") => new()
    {
        Name = "fdb",
        Kind = FakeKind,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = intervalMs },
            Db = new DbSourceConfig
            {
                Host = "fizz.local",
                Database = "warehouse",
                Table = "orders",
                CursorColumn = "updated_at",
                DedupKeyColumn = dedupColumn,
            },
        },
    };

    private static Dictionary<string, object?> Row(string id, object? qty) => new() { ["id"] = id, ["qty"] = qty };

    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

    /// <summary>The due time <see cref="ConnectorActor"/> would compute for its one-shot timer from this
    /// state — the actor's three arming call sites are literally this expression.</summary>
    private static TimeSpan DueOf(ConnectorActorState state, DateTimeOffset nowUtc) =>
        ConnectorBookkeeping.DueFrom(
            state.NextRunMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(state.NextRunMs.Value) : null,
            nowUtc);

    /// <summary>One tick's worth of the polled arm, in the order <see cref="ConnectorActor.TickAsync"/>
    /// runs it: cycle, dedup snapshot, status bookkeeping, then the HasMore override. Returned so a test can
    /// assert on the rows that tick would have published.</summary>
    private static async Task<PolledTickOutcome> TickAsync(
        ConnectorActorState state, FakeDb transport, DedupTracker dedup, DateTimeOffset nowUtc)
    {
        var outcome = await ConnectorBookkeeping.RunPolledCycleAsync(
            state, transport, dedup, nowUtc.ToUnixTimeMilliseconds(), CancellationToken.None);

        state.DedupKeys = dedup.ToPersistable();
        ConnectorBookkeeping.ApplyPollResult(state, outcome.Result, nowUtc);
        if (outcome.HasMore)
        {
            ConnectorBookkeeping.MarkDueNow(state, nowUtc);
        }

        return outcome;
    }

    // ------------------------------------------------------------------
    // 1 — Rows land as events through the path every other kind publishes through
    // ------------------------------------------------------------------

    [Fact]
    public async Task APolledSourcesRowsReachTheTicksPublishCallCoercedAndStamped()
    {
        // Resolved the way TickAsync resolves it — from the registry, by a kind string this assembly's
        // switch statement has never heard of.
        var transport = Assert.IsType<FakeDb>(PolledTransports.Find(FakeKind));
        transport.Pages.Enqueue(new PolledBatch([Row("a", "7"), Row("b", 9L)], "c1", false));

        var state = new ConnectorActorState { Def = PolledSource(), Running = true };
        var outcome = await TickAsync(state, transport, new DedupTracker([]), Now);

        Assert.Null(outcome.Result.Error);
        Assert.Equal(2, outcome.Result.Rows.Count);

        // TickAsync publishes exactly when Rows.Count > 0, and publishes exactly these dictionaries.
        var first = outcome.Result.Rows[0];
        Assert.Equal("a", first["id"]);
        Assert.Equal(7L, first["qty"]);                    // coerced by the shared path, not by the transport
        Assert.Equal("fdb", first["_source"]);             // stamped by the shared path
        Assert.Equal(Now.ToUnixTimeMilliseconds(), first["_ts"]);

        Assert.Equal("ok", state.LastStatus);
        Assert.Equal(2, state.LastBatchCount);
        Assert.Equal(2, state.EventsEmittedTotal);
    }

    [Fact]
    public void TheBuiltInKindsStillBelongToTheirOwnDriverArms()
    {
        // The "no behaviour change to any existing kind" claim, from the side that could break it: TickAsync
        // consults PolledTransports BEFORE its built-in switch, so a built-in resolving here would silently
        // bypass its own url/file/folder/grpc/ingest path.
        Assert.Null(PolledTransports.Find(SourceKinds.Url));
        Assert.Null(PolledTransports.Find(SourceKinds.File));
        Assert.Null(PolledTransports.Find(SourceKinds.Folder));
        Assert.Null(PolledTransports.Find(SourceKinds.Generator));
        Assert.Null(PolledTransports.Find(SourceKinds.Grpc));
        Assert.Null(PolledTransports.Find(SourceKinds.Ingest));
        Assert.Null(PolledTransports.Find(SourceKinds.Nats));
    }

    // ------------------------------------------------------------------
    // 2 — The cursor advances into the state that gets persisted
    // ------------------------------------------------------------------

    [Fact]
    public async Task TheCursorAdvancesOnTheStateTheTickPersists()
    {
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "c1", false));
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "c2", false));
        transport.Pages.Enqueue(new PolledBatch([], null, false));   // nothing new

        var state = new ConnectorActorState { Def = PolledSource(), Running = true };
        var dedup = new DedupTracker(state.DedupKeys);

        Assert.Null(state.Cursor);                                   // first ever cycle
        await TickAsync(state, transport, dedup, Now);
        Assert.Equal("c1", state.Cursor);

        await TickAsync(state, transport, dedup, Now);
        Assert.Equal("c2", state.Cursor);

        await TickAsync(state, transport, dedup, Now);
        Assert.Equal("c2", state.Cursor);                            // null Cursor = unchanged, never "reset"

        // Each cycle was handed exactly what the previous one left on the state.
        Assert.Equal([null, "c1", "c2"], transport.SeenCursors);
    }

    [Fact]
    public async Task ADedupKeyColumnSuppressesTheRowsAReReadingCursorBringsBack()
    {
        // The companion to a `>=` cursor. The column is read from connector.db, never from a MappingSpec —
        // a polled row source has no mapping document (DedupKeyColumn's own doc argues the point).
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "t1", false));
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L), Row("b", 2L)], "t2", false));

        var state = new ConnectorActorState { Def = PolledSource(), Running = true };
        var dedup = new DedupTracker(state.DedupKeys);

        var first = await TickAsync(state, transport, dedup, Now);
        var second = await TickAsync(state, transport, dedup, Now);

        Assert.Equal(["a"], first.Result.Rows.Select(r => r["id"]));
        Assert.Equal(["b"], second.Result.Rows.Select(r => r["id"]));
        Assert.NotEmpty(state.DedupKeys);   // and the tracker's snapshot rode along on the persisted state

        Assert.Equal("id", ConnectorBookkeeping.DedupKeyColumn(PolledSource()));
        Assert.Null(ConnectorBookkeeping.DedupKeyColumn(PolledSource(dedupColumn: "")));
    }

    // ------------------------------------------------------------------
    // 3 — Restart: the reloaded state resumes, it does not start over
    // ------------------------------------------------------------------

    [Fact]
    public async Task AReloadedStateResumesFromItsStoredCursorRatherThanRestarting()
    {
        // The restart is modelled the way Dapr actually performs one: OnActivateAsync reads the state back
        // through the actor state store, which is System.Text.Json over this exact POCO. Round-tripping it
        // is therefore the whole of "the process died" as far as this arm is concerned — and it is also the
        // assertion that Cursor survives serialization at all, which a field held only in memory would not.
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "snapshot-page-2", false));

        var before = new ConnectorActorState { Def = PolledSource(), Running = true };
        await TickAsync(before, transport, new DedupTracker(before.DedupKeys), Now);
        Assert.Equal("snapshot-page-2", before.Cursor);

        var after = JsonSerializer.Deserialize<ConnectorActorState>(JsonSerializer.Serialize(before))!;
        Assert.Equal("snapshot-page-2", after.Cursor);
        Assert.True(after.Running);
        Assert.Equal(FakeKind, after.Def!.Kind);

        // The next cycle after the restart asks the transport to continue, not to begin.
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "snapshot-page-3", false));
        await TickAsync(after, transport, new DedupTracker(after.DedupKeys), Now);

        Assert.Equal("snapshot-page-3", after.Cursor);
        Assert.Equal([null, "snapshot-page-2"], transport.SeenCursors);
        Assert.DoesNotContain(transport.SeenCursors.Skip(1), c => c is null);   // never re-read from the top
    }

    // ------------------------------------------------------------------
    // 4 — A throwing transport keeps the cursor and reports through the existing status path
    // ------------------------------------------------------------------

    [Fact]
    public async Task AThrowingTransportLeavesTheCursorAloneAndSurfacesTheErrorOnStatus()
    {
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "c1", false));

        var state = new ConnectorActorState { Def = PolledSource(), Running = true };
        var dedup = new DedupTracker(state.DedupKeys);
        await TickAsync(state, transport, dedup, Now);
        Assert.Equal("c1", state.Cursor);

        transport.Fail = new InvalidOperationException("connection reset by peer");
        var failed = await TickAsync(state, transport, dedup, Now.AddSeconds(30));

        // THE load-bearing invariant: a transport bug must not be able to skip data.
        Assert.Equal("c1", state.Cursor);
        Assert.Empty(failed.Result.Rows);
        Assert.False(failed.HasMore);

        // …reported through the same ConnectorRuntimeStatus fields every other kind's failures use — no
        // separate error channel was invented for the polled arm.
        Assert.Equal("error", state.LastStatus);
        Assert.Equal(1, state.ConsecutiveFailures);
        Assert.Contains("connection reset by peer", state.LastError);
        Assert.Contains(nameof(InvalidOperationException), state.LastError);
        Assert.Equal(0, state.LastBatchCount);

        // A failing connector still gets a (backed-off) next run rather than freezing, and the cursor it
        // kept is the one the recovery cycle is handed.
        Assert.True(DueOf(state, Now.AddSeconds(30)) > TimeSpan.Zero);

        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "c2", false));
        await TickAsync(state, transport, dedup, Now.AddSeconds(60));

        Assert.Equal("c2", state.Cursor);
        Assert.Equal("ok", state.LastStatus);
        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal([null, "c1", "c1"], transport.SeenCursors);
    }

    [Fact]
    public async Task ARejectedBatchKeepsItsRowsReReadableAndDoesNotSpin()
    {
        // Coerce-before-admission: a RejectBatch rejection emits nothing, so moving the cursor would skip
        // the very rows the operator is about to go fix — and re-arming immediately would hammer the
        // database with them at full speed while they stay broken.
        var def = PolledSource();
        def.OnCoercionFailure = CoercionFailurePolicy.RejectBatch;

        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", "not-a-number")], "c9", true));

        var state = new ConnectorActorState { Def = def, Running = true, Cursor = "c0" };
        var outcome = await TickAsync(state, transport, new DedupTracker([]), Now);

        Assert.Equal("c0", state.Cursor);
        Assert.False(outcome.HasMore);
        Assert.Equal("error", state.LastStatus);
        Assert.True(DueOf(state, Now) > TimeSpan.Zero);
    }

    // ------------------------------------------------------------------
    // 5 — HasMore re-arms immediately, on timers alone
    // ------------------------------------------------------------------

    [Fact]
    public async Task HasMorePagesASnapshotWithoutWaitingForTheSchedule()
    {
        // Three pages against a 30-SECOND schedule. If HasMore only advanced the cursor, this snapshot
        // would take a minute; the assertion is that the due time the actor arms its one-shot timer with is
        // zero after every page but the last — no reminder involved, which matters because the compose
        // stack runs without the Dapr scheduler.
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "p1", true));
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "p2", true));
        transport.Pages.Enqueue(new PolledBatch([Row("c", 3L)], "p3", false));

        var state = new ConnectorActorState { Def = PolledSource(intervalMs: 30_000), Running = true };
        var dedup = new DedupTracker(state.DedupKeys);

        var cursors = new List<string?>();
        var dues = new List<TimeSpan>();
        var ids = new List<object?>();
        var cycles = 0;

        while (cycles < 10)   // a HasMore that never clears is a hang, not a failing assert
        {
            var outcome = await TickAsync(state, transport, dedup, Now);
            Assert.Null(outcome.Result.Error);
            cursors.Add(state.Cursor);
            dues.Add(DueOf(state, Now));
            ids.AddRange(outcome.Result.Rows.Select(r => r["id"]));
            cycles++;

            if (!outcome.HasMore)
            {
                break;
            }
        }

        Assert.Equal(3, cycles);
        Assert.Equal(["p1", "p2", "p3"], cursors);                 // one durable cursor per page
        Assert.Equal(["a", "b", "c"], ids);
        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero, TimeSpan.FromSeconds(30)], dues);
        Assert.Equal([null, "p1", "p2"], transport.SeenCursors);
    }

    [Fact]
    public async Task AnInterruptedSnapshotResumesImmediatelyAfterReactivation()
    {
        // MarkDueNow writes into the PERSISTED state, not a local, so the "come straight back" instruction
        // outlives a deactivation — OnActivateAsync's ArmTimerFromPersistedNextRunAsync reads the same
        // NextRunMs and computes the same zero due time. Without that, a snapshot interrupted mid-paging
        // would idle out a full schedule interval for no reason it could observe.
        var transport = new FakeDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "p1", true));

        var state = new ConnectorActorState { Def = PolledSource(intervalMs: 3_600_000), Running = true };
        await TickAsync(state, transport, new DedupTracker(state.DedupKeys), Now);

        var reactivated = JsonSerializer.Deserialize<ConnectorActorState>(JsonSerializer.Serialize(state))!;

        Assert.Equal("p1", reactivated.Cursor);
        Assert.Equal(TimeSpan.Zero, DueOf(reactivated, Now));   // not an hour from now
    }

    // ------------------------------------------------------------------
    // 6 — The cursor reaches the wire contract
    // ------------------------------------------------------------------

    [Fact]
    public void TheCursorAppearsOnConnectorRuntimeStatus()
    {
        var state = new ConnectorActorState
        {
            Def = PolledSource(),
            Cursor = "2026-01-01T00:00:00Z|4711",
            LastStatus = "ok",
            LastBatchCount = 3,
            EventsEmittedTotal = 9,
            NextRunMs = Now.ToUnixTimeMilliseconds(),
        };

        var status = ConnectorBookkeeping.ToStatus(state, "fdb");

        Assert.Equal("2026-01-01T00:00:00Z|4711", status.Cursor);
        Assert.Equal("fdb", status.SourceName);
        Assert.Equal("ok", status.LastStatus);
        Assert.Equal(3, status.LastBatchCount);
        Assert.Equal(9, status.EventsEmittedTotal);
        Assert.Equal(Now.ToUnixTimeMilliseconds(), status.NextRunMs);

        // Null for every kind that has no cursor — the field is additive, not a value the console must
        // learn to ignore for url/file/folder.
        Assert.Null(ConnectorBookkeeping.ToStatus(new ConnectorActorState { Def = PolledSource() }, "fdb").Cursor);
    }

    // ------------------------------------------------------------------
    // Guard rails
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunPolledCycleWithoutADefinitionIsARefusalRatherThanANullReference()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => ConnectorBookkeeping.RunPolledCycleAsync(
            new ConnectorActorState(), new FakeDb(), new DedupTracker([]), 1L, CancellationToken.None));
    }

    [Fact]
    public void DueFromClampsAnOverdueRunToNowAndFallsBackWhenNoScheduleComputed()
    {
        Assert.Equal(TimeSpan.Zero, ConnectorBookkeeping.DueFrom(Now.AddMinutes(-5), Now));
        Assert.Equal(TimeSpan.FromSeconds(10), ConnectorBookkeeping.DueFrom(Now.AddSeconds(10), Now));
        Assert.Equal(TimeSpan.FromSeconds(30), ConnectorBookkeeping.DueFrom(null, Now));
    }
}
