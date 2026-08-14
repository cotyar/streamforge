using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 014: the acceptance test for the PULL-shaped seam, and the actual proof of its extensibility claim.
/// A polled kind this repo has never heard of — "fizzdb", backed by a hand-rolled in-memory fake — is
/// registered from a TEST, and the platform then finds it, drives it, turns its result sets into coerced and
/// stamped rows, advances its cursor, refuses to advance it on failure, pages it, and dedups it. Nothing in
/// <c>PolledTransports</c>, <c>PolledSourceCore</c> or <c>ConnectorPollCycle</c> mentions "fizzdb", or
/// Postgres, or a database at all.
///
/// <para><b>Why this test rather than the built-in kinds.</b> Plan 014 deliberately does not migrate
/// <c>url</c>/<c>file</c>/<c>folder</c> onto this SPI, so exercising a built-in would prove nothing about
/// pluggability — it would only re-test code the six existing connector suites already cover. A kind that
/// exists nowhere but in this file is the only version of the claim that can actually fail: if any of these
/// components regains a hardcoded per-kind branch, or if the "never advance the cursor on a failed cycle"
/// rule is relaxed anywhere, these break.</para>
///
/// <para>Registration is process-global and permanent, so the fake kind is named distinctively and
/// registered exactly once from the static constructor — the registry throws on a duplicate, which
/// <see cref="Register_RejectsADuplicateKind"/> pins.</para>
/// </summary>
public class PolledTransportRegistryTests
{
    private const string FizzDbKind = "fizzdb";

    private static readonly FizzDb Shared = new();

    static PolledTransportRegistryTests() => PolledTransports.Register(Shared);

    // ------------------------------------------------------------------
    // The fake transport — what a real one looks like, minus a database.
    // ------------------------------------------------------------------

    /// <summary>A scripted result-set reader. <see cref="Pages"/> is consumed one per
    /// <see cref="PollAsync"/> call, so a test states the cycles it expects rather than the query it would
    /// have run. It rides the existing <c>connector.db</c> config slot for the same reason
    /// <c>TransportRegistryTests</c>' fake rides the nats one — adding an <c>[Id]</c> to
    /// <c>ConnectorConfig</c> for a test fixture would be a contract change; a real transport gets its
    /// own.</summary>
    private sealed class FizzDb : IPolledTransport, ISchemaProbe
    {
        public Queue<PolledBatch> Pages { get; } = new();

        /// <summary>Set to make the next poll fail the way a real one does: an exception out of the driver,
        /// not a returned error code.</summary>
        public Exception? Fail { get; set; }

        /// <summary>Every cursor the driver handed over, in order — the evidence that what was persisted is
        /// what comes back on the next cycle.</summary>
        public List<string?> SeenCursors { get; } = [];

        public string Kind => FizzDbKind;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            if (string.IsNullOrWhiteSpace(def.Connector?.Db?.Table))
            {
                errors.Add("kind 'fizzdb' requires connector.db.table");
            }
        }

        public Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
        {
            SeenCursors.Add(cursor);
            if (Fail is not null)
            {
                var fail = Fail;
                Fail = null; // one scripted failure, so a test can assert the recovery cycle too
                throw fail;
            }

            return Task.FromResult(Pages.Count > 0 ? Pages.Dequeue() : new PolledBatch([], null, false));
        }

        public Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct)
            => Task.FromResult(new SchemaProbeResult(
                [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
                ["price is numeric(18,4); it will be read as Double and lose precision"]));

        public TransportDescriptor Describe() => new()
        {
            Kind = FizzDbKind,
            Label = "FizzDB",
            ConfigProperty = "db",
            Polled = true,
            Mapping = false,
            CanProbe = true,
            Fields =
            [
                new TransportField { Key = "table", Label = "Table", Required = true },
                new TransportField { Key = "query", Label = "Query", Type = TransportFieldTypes.Text },
                new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
            ],
        };
    }

    private static SourceDefinition FizzDbSource() => new()
    {
        Name = "fdb",
        Kind = FizzDbKind,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Db = new DbSourceConfig
            {
                Host = "fizz.local",
                Database = "warehouse",
                Table = "orders",
                CursorColumn = "updated_at",
                DedupKeyColumn = "id",
            },
        },
    };

    private static Dictionary<string, object?> Row(string id, object? qty) => new() { ["id"] = id, ["qty"] = qty };

    /// <summary>One cycle exactly as a flavour driver runs it: resolve the transport from the registry, hand
    /// it the persisted cursor and the persisted dedup tracker, get back the next cursor to persist. The
    /// dedup column comes from the kind's own config, which is the driver's business and not the core's —
    /// <c>PolledSourceCore</c> is handed a field name and never asks where it came from.</summary>
    private static Task<PolledCycleOutcome> RunCycleAsync(FizzDb transport, SourceDefinition def, string? cursor, DedupTracker dedup, long nowMs = 1_700_000_000_000)
        => PolledSourceCore.RunCycleAsync(
            transport, def, cursor, dedup, nowMs, CancellationToken.None,
            dedupKeyField: def.Connector?.Db?.DedupKeyColumn is { Length: > 0 } c ? c : null);

    // ------------------------------------------------------------------
    // 1 — Registry
    // ------------------------------------------------------------------

    [Fact]
    public void Find_ResolvesTheRegisteredKindAndListsIt()
    {
        Assert.Same(Shared, PolledTransports.Find(FizzDbKind));
        Assert.Contains(FizzDbKind, PolledTransports.Kinds);

        Assert.Null(PolledTransports.Find(null));
        Assert.Null(PolledTransports.Find(""));
        Assert.Null(PolledTransports.Find("no-such-kind"));
    }

    [Fact]
    public void TheTwoRegistriesStayDisjoint()
    {
        // A kind resolving in BOTH would be driven twice — once by the subscribe loop and once by the timer.
        // Equally, the kinds that have drivers of their own must not resolve as polled transports, or their
        // own timer/grpc/ingest paths would be bypassed.
        Assert.Null(InboundTransports.Find(FizzDbKind));
        Assert.Null(PolledTransports.Find(SourceKinds.Nats));
        Assert.Null(PolledTransports.Find(SourceKinds.Url));
        Assert.Null(PolledTransports.Find(SourceKinds.File));
        Assert.Null(PolledTransports.Find(SourceKinds.Folder));
        Assert.Null(PolledTransports.Find(SourceKinds.Generator));
        Assert.Null(PolledTransports.Find(SourceKinds.Grpc));
        Assert.Null(PolledTransports.Find(SourceKinds.Ingest));
    }

    [Fact]
    public void Register_RejectsADuplicateKind() =>
        Assert.Throws<InvalidOperationException>(() => PolledTransports.Register(new FizzDb()));

    [Fact]
    public void TheDescriptorCarriesTheFlagsTheConsoleRendersFrom()
    {
        var d = PolledTransports.Find(FizzDbKind)!.Describe();

        Assert.True(d.Polled);        // -> schedule editor
        Assert.False(d.Mapping);      // -> no mapping editor: the SELECT list is the mapping
        Assert.True(d.CanProbe);      // -> "Discover schema" button
        Assert.Contains(d.Fields, f => f.Type == TransportFieldTypes.Text);

        // Mapping defaults to TRUE so that every pre-014 descriptor keeps its editor untouched.
        Assert.True(new TransportDescriptor { Kind = "x", Label = "X", ConfigProperty = "x" }.Mapping);
        Assert.False(new TransportDescriptor { Kind = "x", Label = "X", ConfigProperty = "x" }.Polled);
    }

    [Fact]
    public async Task TheProbeCapabilityIsReachedByTypeTestAlone()
    {
        // How POST /api/transports/{kind}/probe finds it: the registry returns an IPolledTransport, and the
        // capability is an `is` away. StreamForge.Api never learns what a database is.
        var probe = Assert.IsAssignableFrom<ISchemaProbe>(PolledTransports.Find(FizzDbKind));
        var result = await probe.ProbeAsync(FizzDbSource(), CancellationToken.None);

        Assert.Equal(["id", "qty"], result.Fields.Select(f => f.Name));
        Assert.Single(result.Diagnostics);   // a lossy mapping is REPORTED, not silently rounded
    }

    // ------------------------------------------------------------------
    // 2 — Rows land as events through the shared path
    // ------------------------------------------------------------------

    [Fact]
    public async Task PolledSourceCore_DrivesTheTransportIntoCoercedStampedRows()
    {
        var transport = new FizzDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", "7"), Row("b", 9L)], "c1", false));

        var outcome = await RunCycleAsync(transport, FizzDbSource(), null, new DedupTracker([]));

        Assert.Null(outcome.Result.Error);
        Assert.Equal(2, outcome.Result.Rows.Count);

        var first = outcome.Result.Rows[0];
        Assert.Equal("a", first["id"]);
        Assert.Equal(7L, first["qty"]);                     // coerced by the shared path, not the transport
        Assert.Equal("fdb", first["_source"]);              // stamped by the shared path
        Assert.Equal(1_700_000_000_000L, first["_ts"]);

        Assert.Equal([null], transport.SeenCursors);        // first ever cycle: no persisted cursor
    }

    [Fact]
    public async Task ARejectedBatchEmitsNothingAndKeepsItsRowsReReadable()
    {
        // Coerce-before-admission: a RejectBatch rejection leaves nothing behind, so the cursor must not move
        // either — otherwise the rows the operator is about to go fix would already have been skipped.
        var def = FizzDbSource();
        def.OnCoercionFailure = CoercionFailurePolicy.RejectBatch;

        var transport = new FizzDb();
        transport.Pages.Enqueue(new PolledBatch([Row("a", "not-a-number")], "c9", true));

        var outcome = await RunCycleAsync(transport, def, "c0", new DedupTracker([]));

        Assert.Empty(outcome.Result.Rows);
        Assert.NotNull(outcome.Result.Error);
        Assert.Equal("c0", outcome.Cursor);
        Assert.False(outcome.HasMore);   // and it does NOT re-arm against the same failing rows at full speed
    }

    // ------------------------------------------------------------------
    // 3 — The cursor advances across cycles
    // ------------------------------------------------------------------

    [Fact]
    public async Task TheCursorAdvancesAcrossCyclesAndSurvivesAnEmptyPoll()
    {
        var transport = new FizzDb();
        var def = FizzDbSource();
        var dedup = new DedupTracker([]);

        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "c1", false));
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "c2", false));
        transport.Pages.Enqueue(new PolledBatch([], null, false));   // nothing new: null = leave unchanged

        var first = await RunCycleAsync(transport, def, null, dedup);
        var second = await RunCycleAsync(transport, def, first.Cursor, dedup);
        var third = await RunCycleAsync(transport, def, second.Cursor, dedup);

        Assert.Equal("c1", first.Cursor);
        Assert.Equal("c2", second.Cursor);
        Assert.Equal("c2", third.Cursor);   // a null Cursor means "unchanged", never "start over"
        Assert.Empty(third.Result.Rows);

        // What was persisted is exactly what the next cycle was handed.
        Assert.Equal([null, "c1", "c2"], transport.SeenCursors);
    }

    // ------------------------------------------------------------------
    // 4 — A throwing transport does NOT advance the cursor
    // ------------------------------------------------------------------

    [Fact]
    public async Task AThrowingTransportNeverAdvancesTheCursor()
    {
        var transport = new FizzDb();
        var def = FizzDbSource();
        var dedup = new DedupTracker([]);

        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "c1", false));
        var ok = await RunCycleAsync(transport, def, null, dedup);
        Assert.Equal("c1", ok.Cursor);

        transport.Fail = new InvalidOperationException("connection reset by peer");
        var failed = await RunCycleAsync(transport, def, ok.Cursor, dedup);

        // THE load-bearing invariant: a transport bug must not be able to skip data.
        Assert.Equal("c1", failed.Cursor);
        Assert.Empty(failed.Result.Rows);
        Assert.False(failed.HasMore);
        Assert.NotNull(failed.Result.Error);
        Assert.Contains("connection reset by peer", failed.Result.Error);
        Assert.Contains(nameof(InvalidOperationException), failed.Result.Error);

        // …and the very rows that failed are re-read on the next cycle, from the cursor that was kept.
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "c2", false));
        var recovered = await RunCycleAsync(transport, def, failed.Cursor, dedup);

        Assert.Equal("c2", recovered.Cursor);
        Assert.Equal([null, "c1", "c1"], transport.SeenCursors);
    }

    [Fact]
    public async Task ANullBatchIsATransportBugReportedAsOneRatherThanAnNRE()
    {
        var transport = new FizzDb();
        transport.Pages.Enqueue(null!);

        var outcome = await RunCycleAsync(transport, FizzDbSource(), "c0", new DedupTracker([]));

        Assert.Equal("c0", outcome.Cursor);
        Assert.Contains("null batch", outcome.Result.Error);
    }

    // ------------------------------------------------------------------
    // 5 — HasMore paging, one persisted cursor per page
    // ------------------------------------------------------------------

    [Fact]
    public async Task HasMorePagesASnapshotAcrossCyclesEachPersistingItsOwnCursor()
    {
        // The snapshot-resumability claim: paging happens across DRIVER cycles, not inside one PollAsync,
        // so the cursor of every completed page is persisted and a restart resumes mid-snapshot.
        var transport = new FizzDb();
        var def = FizzDbSource();
        var dedup = new DedupTracker([]);

        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "p1", true));
        transport.Pages.Enqueue(new PolledBatch([Row("b", 2L)], "p2", true));
        transport.Pages.Enqueue(new PolledBatch([Row("c", 3L)], "p3", false));

        var persisted = new List<string?>();
        var ids = new List<object?>();
        string? cursor = null;
        var cycles = 0;

        do
        {
            var outcome = await RunCycleAsync(transport, def, cursor, dedup);
            Assert.Null(outcome.Result.Error);
            cursor = outcome.Cursor;
            persisted.Add(cursor);
            ids.AddRange(outcome.Result.Rows.Select(r => r["id"]));
            cycles++;

            if (!outcome.HasMore)
            {
                break;
            }
        }
        while (cycles < 10); // a HasMore that never clears is a hang, not a failing assert

        Assert.Equal(3, cycles);
        Assert.Equal(["p1", "p2", "p3"], persisted);          // one durable cursor per page
        Assert.Equal(["a", "b", "c"], ids);
        Assert.Equal([null, "p1", "p2"], transport.SeenCursors);
    }

    // ------------------------------------------------------------------
    // 6 — Dedup by the supplied key column
    // ------------------------------------------------------------------

    [Fact]
    public async Task TheSuppliedKeyColumnSuppressesAReReadRow()
    {
        // The companion to a `>=` cursor, which re-reads every row sharing the watermark's timestamp. The
        // key column is the transport's own config, handed to the core by the driver — the core never reads
        // MappingSpec.DedupKeyField here, because a polled row source has no mapping document at all.
        var transport = new FizzDb();
        var def = FizzDbSource();
        var dedup = new DedupTracker([]);

        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L)], "t1", false));
        transport.Pages.Enqueue(new PolledBatch([Row("a", 1L), Row("b", 2L)], "t2", false));

        var first = await RunCycleAsync(transport, def, null, dedup);
        var second = await RunCycleAsync(transport, def, first.Cursor, dedup);

        Assert.Equal(["a"], first.Result.Rows.Select(r => r["id"]));
        Assert.Equal(["b"], second.Result.Rows.Select(r => r["id"]));   // "a" re-read, and suppressed

        // Without a key column nothing is suppressed — the honest at-least-once default, not a silent one.
        var noKey = FizzDbSource();
        noKey.Connector!.Db!.DedupKeyColumn = "";
        var plain = new FizzDb();
        plain.Pages.Enqueue(new PolledBatch([Row("a", 1L), Row("a", 1L)], "t1", false));

        var undeduped = await RunCycleAsync(plain, noKey, null, new DedupTracker([]));
        Assert.Equal(2, undeduped.Result.Rows.Count);
    }

    // ------------------------------------------------------------------
    // The shared entry point, used directly
    // ------------------------------------------------------------------

    [Fact]
    public void ExecuteRows_IgnoresTheMappingDocumentsDedupField()
    {
        // ExecuteRows takes its key from the CALLER. If it fell back to MappingSpec.DedupKeyField, a source
        // that happens to carry a stale mapping would start dropping rows for a reason nothing displays.
        var def = FizzDbSource();
        def.Connector!.Mapping = new MappingSpec { ItemsPath = "$", DedupKeyField = "id" };

        var result = ConnectorPollCycle.ExecuteRows(
            def, [Row("a", 1L), Row("a", 1L)], dedupKeyField: null, new DedupTracker([]), 1L);

        Assert.Equal(2, result.Rows.Count);
    }
}
