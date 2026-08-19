using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 2 — the two counters and the pin evaluation, tested where BOTH flavours are covered at
/// once (this project is listed in both solutions). The registries call exactly these methods, so a rule
/// proven here cannot be true on Orleans and false on Dapr.
/// </summary>
public class CatalogRevisionsTests
{
    private static SourceDefinition Source(params FieldDef[] fields) => new()
    {
        Name = "trades",
        EventsPerSecond = 5,
        Fields = [.. fields],
    };

    private static SinkSpec Warehouse(bool enabled) => new()
    {
        Kind = SinkKinds.Nats,
        Name = "warehouse",
        Enabled = enabled,
        Nats = new NatsPubConfig { Url = "nats://h:4222", Subject = "out" },
    };

    // ---------------------------------------------------------------------------------------------
    // The split that makes a pin useful: a knob is not a schema.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void AKnobOnlyEditMovesRevisionAndLeavesSchemaRevisionAlone()
    {
        var existing = Source(new FieldDef("symbol", FieldType.String));
        existing.Revision = 4;
        existing.SchemaRevision = 2;

        var incoming = Source(new FieldDef("symbol", FieldType.String));
        incoming.EventsPerSecond = 50;

        CatalogRevisions.CarryAndBumpSource(existing, incoming);

        Assert.Equal(5, incoming.Revision);
        Assert.Equal(2, incoming.SchemaRevision); // unmoved — this is the entire reason there are two.
    }

    [Fact]
    public void AFieldShapeEditMovesBoth()
    {
        var existing = Source(new FieldDef("symbol", FieldType.String));
        existing.Revision = 4;
        existing.SchemaRevision = 2;

        var incoming = Source(new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double));

        CatalogRevisions.CarryAndBumpSource(existing, incoming);

        Assert.Equal(5, incoming.Revision);
        Assert.Equal(3, incoming.SchemaRevision);
    }

    [Fact]
    public void AnIdenticalUpsertMovesNeither()
    {
        var existing = Source(new FieldDef("symbol", FieldType.String));
        existing.Revision = 4;
        existing.SchemaRevision = 2;

        var incoming = Source(new FieldDef("symbol", FieldType.String));

        CatalogRevisions.CarryAndBumpSource(existing, incoming);

        Assert.Equal(4, incoming.Revision);
        Assert.Equal(2, incoming.SchemaRevision);
    }

    [Fact]
    public void ACallersOwnCounterValuesAreDiscarded()
    {
        // The counters are REGISTRY-assigned. A caller that could choose its own Revision could pin a
        // dependant to a revision that never existed.
        var existing = Source(new FieldDef("symbol", FieldType.String));
        existing.Revision = 4;
        existing.SchemaRevision = 2;

        var incoming = Source(new FieldDef("symbol", FieldType.String));
        incoming.Revision = 999;
        incoming.SchemaRevision = 999;

        CatalogRevisions.CarryAndBumpSource(existing, incoming);

        Assert.Equal(4, incoming.Revision);
        Assert.Equal(2, incoming.SchemaRevision);
    }

    [Fact]
    public void TheCountersThemselvesAreNotPartOfWhatCountsAsAChange()
    {
        // Without this, the first bump would make every later comparison unequal forever: SourceNode
        // serializes the whole SourceDefinition, counters included.
        var a = Source(new FieldDef("symbol", FieldType.String));
        var b = Source(new FieldDef("symbol", FieldType.String));
        a.Revision = 7;
        a.SchemaRevision = 3;

        Assert.False(CatalogRevisions.DefinitionChanged(a, b));
    }

    [Fact]
    public void AReorderedFieldListIsAShapeChangeOnlyIfTheFieldsActuallyDiffer()
    {
        var existing = Source(new FieldDef("a", FieldType.String), new FieldDef("b", FieldType.Long));
        existing.SchemaRevision = 1;
        var incoming = Source(new FieldDef("b", FieldType.Long), new FieldDef("a", FieldType.String));

        CatalogRevisions.CarryAndBumpSource(existing, incoming);

        Assert.Equal(1, incoming.SchemaRevision);   // field NUMBERS are stable across a reorder
        Assert.Equal(1, incoming.Revision);         // …but the stored document did change, so this moves
    }

    // ---------------------------------------------------------------------------------------------
    // Tables and pipelines: carry-then-bump on top of CatalogRecordMerge.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void ATablesSchemaRevisionFollowsItsCompiledOutputFieldsNotItsSql()
    {
        var existing = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT a FROM s", Revision = 2, SchemaRevision = 2 };
        existing.OutputFields = [new FieldDef("a", FieldType.String)];

        // Same output shape, different SQL text (an aliasing/whitespace edit): definition moved, shape did not.
        var incoming = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT  a  FROM s" };
        CatalogRecordMerge.CarryServerOwnedFields(existing, incoming, nowMs: 1);
        incoming.OutputFields = [new FieldDef("a", FieldType.String)];

        CatalogRevisions.BumpTable(existing, incoming, [new FieldDef("a", FieldType.String)]);

        Assert.Equal(3, incoming.Revision);
        Assert.Equal(2, incoming.SchemaRevision);
    }

    [Fact]
    public void ATableThatStopsCompilingCountsAsAShapeChange()
    {
        var existing = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT a FROM s", Revision = 2, SchemaRevision = 2 };
        existing.OutputFields = [new FieldDef("a", FieldType.String)];

        var incoming = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT nope FROM s" };
        CatalogRecordMerge.CarryServerOwnedFields(existing, incoming, nowMs: 1);
        incoming.OutputFields = []; // what ApplyCompileResult does on a failed compile

        CatalogRevisions.BumpTable(existing, incoming, [new FieldDef("a", FieldType.String)]);

        Assert.Equal(3, incoming.SchemaRevision);

        // …and re-saving the same broken draft does not bump again: empty to empty.
        var again = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT nope FROM s" };
        CatalogRecordMerge.CarryServerOwnedFields(incoming, again, nowMs: 2);
        again.OutputFields = [];
        CatalogRevisions.BumpTable(incoming, again, []);
        Assert.Equal(3, again.SchemaRevision);
        Assert.Equal(incoming.Revision, again.Revision);
    }

    [Fact]
    public void APipelineStatusChangeIsNotADefinitionChange()
    {
        // ToConfigPipeline projects Status onto a `running` boolean, so if the counter were computed
        // before CatalogRecordMerge carried the stored Status, every start/stop would look like an edit.
        var existing = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Status = PipelineStatus.Running, Revision = 3 };
        var incoming = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Status = PipelineStatus.Stopped };

        CatalogRecordMerge.CarryServerOwnedFields(existing, incoming, nowMs: 1);
        CatalogRevisions.BumpPipeline(existing, incoming);

        Assert.Equal(3, incoming.Revision);
    }

    // ---------------------------------------------------------------------------------------------
    // Pins.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void APinThatHoldsProducesNoStaleReason()
    {
        var src = Source(new FieldDef("symbol", FieldType.String));
        src.SchemaRevision = 3;

        Assert.Null(CatalogRevisions.EvaluatePins(
            [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 3 }], [src], []));
    }

    [Fact]
    public void APinNamesWhichDependencyMovedAndFromWhat()
    {
        var src = Source(new FieldDef("symbol", FieldType.String));
        src.SchemaRevision = 5;

        var reason = CatalogRevisions.EvaluatePins(
            [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 3 }], [src], []);

        Assert.NotNull(reason);
        Assert.Contains("trades", reason);
        Assert.Contains("3", reason);
        Assert.Contains("5", reason);
    }

    [Fact]
    public void APinAtSchemaRevisionZeroIsADeclaredEdgeAndNeverGoesStale()
    {
        var src = Source(new FieldDef("symbol", FieldType.String));
        src.SchemaRevision = 99;

        Assert.Null(CatalogRevisions.EvaluatePins(
            [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 0 }], [src], []));
    }

    [Fact]
    public void APinToSomethingThatNoLongerExistsIsStale()
    {
        var reason = CatalogRevisions.EvaluatePins(
            [new EntityPin { Kind = "table", Name = "gone", SchemaRevision = 1 }], [], []);

        Assert.NotNull(reason);
        Assert.Contains("gone", reason);
    }

    [Fact]
    public void EveryBrokenPinIsNamed()
    {
        var src = Source(new FieldDef("symbol", FieldType.String));
        src.SchemaRevision = 2;
        var tbl = new TableDefinition { Id = "t1", Name = "positions", SchemaRevision = 4 };

        var reason = CatalogRevisions.EvaluatePins(
        [
            new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 },
            new EntityPin { Kind = "table", Name = "positions", SchemaRevision = 4 },  // still holds
            new EntityPin { Kind = "table", Name = "vanished", SchemaRevision = 1 },
        ], [src], [tbl]);

        Assert.NotNull(reason);
        Assert.Contains("trades", reason);
        Assert.Contains("vanished", reason);
        Assert.DoesNotContain("positions", reason);
    }

    [Fact]
    public void NoPinsMeansNoStaleReasonEver() =>
        Assert.Null(CatalogRevisions.EvaluatePins([], [], []));

    // ---------------------------------------------------------------------------------------------
    // The 014-K interaction: a sugared document must plan as "skipped", or the counter it drives churns.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void ASugaredTableDocumentRoundTripsAsSkippedRatherThanUpdatedForever()
    {
        var stored = new TableDefinition
        {
            Id = "t1",
            Name = "daily_pnl",
            Sql = "SELECT symbol FROM trades",   // what INSERT INTO … stored
            Sinks = [Warehouse(enabled: true)],  // …and the sink the sugar switched on
        };

        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable
                {
                    Name = "daily_pnl",
                    Sql = "INSERT INTO warehouse SELECT symbol FROM trades",
                    Sinks = [Warehouse(enabled: false)],
                },
            ],
        };

        var plan = ImportPlanner.Plan(doc, [], [], [stored], "merge");

        Assert.Equal("skipped", Assert.Single(plan).Action);

        // …and the plan did not mutate the caller's document, which would make mode=validate
        // side-effecting.
        Assert.Equal("INSERT INTO warehouse SELECT symbol FROM trades", doc.Tables[0].Sql);
        Assert.False(doc.Tables[0].Sinks[0].Enabled);
    }

    [Fact]
    public void ASugaredPipelineDocumentRoundTripsAsSkippedToo()
    {
        var stored = new PipelineDefinition
        {
            Id = "p1",
            Name = "feed",
            Sql = "SELECT symbol FROM trades",
            Sinks = [Warehouse(enabled: true)],
        };

        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "feed",
                    Sql = "INSERT INTO warehouse SELECT symbol FROM trades",
                    Sinks = [Warehouse(enabled: false)],
                },
            ],
        };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void ASugaredDocumentWhoseSinkTargetDoesNotExistStillPlansRatherThanThrowing()
    {
        // ConfigImportService reports the unknown target as a real "error" entry a moment later; the
        // pure diff's job is to keep planning, on the untouched text.
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT symbol FROM trades" };
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "t", Sql = "INSERT INTO nowhere SELECT symbol FROM trades" }],
        };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    [Fact]
    public void ARealEditToASugaredDocumentStillPlansAsUpdated()
    {
        var stored = new TableDefinition
        {
            Id = "t1", Name = "t", Sql = "SELECT symbol FROM trades", Sinks = [Warehouse(enabled: true)],
        };
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable
                {
                    Name = "t",
                    Sql = "INSERT INTO warehouse SELECT symbol, price FROM trades",
                    Sinks = [Warehouse(enabled: false)],
                },
            ],
        };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    [Fact]
    public void ASourceWhoseRevisionHasMovedStillPlansAsSkipped()
    {
        // The counter-churn loop this closes: bump -> "updated" -> apply -> bump -> "updated" -> …
        var stored = Source(new FieldDef("symbol", FieldType.String));
        stored.Revision = 12;
        stored.SchemaRevision = 4;

        var doc = new ConfigDocument { Sources = [Source(new FieldDef("symbol", FieldType.String))] };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [stored], [], [], "merge")).Action);
    }

    [Fact]
    public void AnUnsetDescriptionIsNotAChangeWhicheverWayItWasStored()
    {
        // Found live, and it is the commonest churn source there is: POST /api/tables without a
        // description stores null; export prunes the null; re-parsing yields the model default "".
        // Before this normalisation the very first round-trip of a freshly-created table planned as
        // "updated" — which under this plan is what moves Revision, on every single import.
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT symbol FROM trades", Description = null! };
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT symbol FROM trades", Description = "" }],
        };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);

        // …and a description that is genuinely set still reads as a change.
        doc.Tables[0].Description = "now it has one";
        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    [Fact]
    public void PlannerAndCounterAgreeOnEverySourceDocumentTheyAreBothShown()
    {
        // The property the plan actually asks for: "a round-trip import that reports skipped provably
        // does not bump". Asserted as agreement between the two, not as two separate expectations.
        var stored = Source(new FieldDef("symbol", FieldType.String));
        stored.Revision = 12;
        stored.SchemaRevision = 4;

        foreach (var candidate in new[]
                 {
                     Source(new FieldDef("symbol", FieldType.String)),                                    // identical
                     Source(new FieldDef("symbol", FieldType.String), new FieldDef("px", FieldType.Double)), // schema edit
                 })
        {
            var planned = Assert.Single(ImportPlanner.Plan(
                new ConfigDocument { Sources = [candidate] }, [stored], [], [], "merge")).Action;

            var probe = ConfigJsonMapperProbe(candidate);
            CatalogRevisions.CarryAndBumpSource(stored, probe);
            var bumped = probe.Revision != stored.Revision;

            Assert.Equal(planned == "updated", bumped);
        }
    }

    /// <summary>A throwaway copy, so the loop above can bump one without disturbing the next iteration.</summary>
    private static SourceDefinition ConfigJsonMapperProbe(SourceDefinition s) => new()
    {
        Name = s.Name,
        Description = s.Description,
        EventsPerSecond = s.EventsPerSecond,
        GeneratorProfile = s.GeneratorProfile,
        Enabled = s.Enabled,
        Kind = s.Kind,
        Fields = [.. s.Fields],
    };
}
