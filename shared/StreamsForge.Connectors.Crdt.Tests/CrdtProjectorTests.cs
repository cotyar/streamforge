using StreamsForge.Abstractions;
using Xunit;
using StreamsForge.Connectors.Database;
using Ycs;

namespace StreamsForge.Connectors.Crdt.Tests;

/// <summary>
/// Plan 020 wave B. Every hazard the plan's "The projection is the dangerous part" section names is a
/// required deliverable, not a note — each gets its own test below rather than being folded into a
/// single "happy path" case. Every document is a REAL <see cref="YDoc"/> built through the Ycs API
/// (never a mock), the same "fake seam only where a socket would otherwise be needed" standard
/// <c>FixRowMapperTests</c> uses one project over — there is no socket here at all, so there is nothing
/// to fake.
/// </summary>
public class CrdtProjectorTests
{
    private static CrdtSourceConfig DefaultConfig() => new() { RootMap = "root", KeyField = "id" };

    private static YMap RootOf(YDoc doc, CrdtSourceConfig? config = null) =>
        doc.GetMap((config ?? DefaultConfig()).RootMap);

    [Fact]
    public void ReservedColumnCollisionIsRenamedNeverPassedThrough()
    {
        // Plan: "_ts and _source are reserved... The projector renames defensively." An edge's document
        // is entirely likely to carry a field literally named "_ts" (an ERP timestamp column, say); if
        // that value silently overwrote EventRecord.Timestamp downstream nobody would notice until the
        // row's actual arrival time was gone. The rename target ("doc_ts") must itself be declared like
        // any other column -- the projector never invents schema, not even for a renamed one.
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        entity.Set("_ts", "edge-supplied-value");
        entity.Set("name", "Ann");

        var fields = new List<FieldDef>
        {
            new("id", FieldType.String),
            new("name", FieldType.String),
            new("doc_ts", FieldType.String),
        };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        var row = rows["e1"];
        Assert.False(row.ContainsKey("_ts")); // never passed through under the reserved name
        Assert.Equal("edge-supplied-value", row["doc_ts"]);
        Assert.Equal("Ann", row["name"]);
        Assert.Contains(diagnostics, d => d.Contains("_ts") && d.Contains("doc_ts"));
    }

    [Fact]
    public void NestedYMapFlattensToADottedColumnName()
    {
        // Plan: "Nested Y-types flatten recursively... v1 flattens." Pins the joined-key scheme
        // (dot-separated path from the entity's own attributes down to the leaf) that CrdtProjector's
        // class doc commits to.
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        var address = new YMap();
        entity.Set("address", address);
        address.Set("city", "Paris");

        var fields = new List<FieldDef>
        {
            new("id", FieldType.String),
            new("address.city", FieldType.String),
        };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.Equal("Paris", rows["e1"]["address.city"]);
    }

    [Fact]
    public void YTextProjectsAsItsPlainStringWithNoFormatting()
    {
        // Plan: "Y.Text projects as its plain string... Declared out of scope in v1 rather than
        // half-supported." No YTextChangeAttributes/formatting is asserted here on purpose -- there is
        // nothing to assert, which IS the point.
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        entity.Set("notes", new YText("hello world"));

        var fields = new List<FieldDef>
        {
            new("id", FieldType.String),
            new("notes", FieldType.String),
        };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.Equal("hello world", rows["e1"]["notes"]);
    }

    [Fact]
    public void CoercionFailureNullsTheFieldAndKeepsTheRow()
    {
        // Plan: "Type drift... goes through FieldValueCoercion, like any weakly-typed connector." This
        // pins WHICH of ConnectorRowCoercion's three policies CrdtProjector matches: Null (field becomes
        // null, row survives) -- SourceDefinition.OnCoercionFailure's own default, and the only policy
        // reachable here since Flatten's pinned signature carries no policy parameter to select DropRow
        // or RejectBatch with.
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        entity.Set("age", "not-a-number");

        var fields = new List<FieldDef>
        {
            new("id", FieldType.String),
            new("age", FieldType.Long),
        };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.True(rows.ContainsKey("e1")); // the row survives
        Assert.Null(rows["e1"]["age"]); // the failing field becomes null, not dropped, not the raw string
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void UndeclaredDocumentKeyIsDroppedNotInvented()
    {
        // Plan: "Keys present in the document but not declared in fields are a diagnostic and are
        // dropped -- the projector never invents schema."
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        entity.Set("secret", "shh");
        entity.Set("name", "Ann");

        var fields = new List<FieldDef> { new("id", FieldType.String), new("name", FieldType.String) };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.False(rows["e1"].ContainsKey("secret"));
        Assert.Equal("Ann", rows["e1"]["name"]);
        Assert.Contains(diagnostics, d => d.Contains("secret"));
    }

    [Fact]
    public void ScalarEntityProjectsToItsSoleDeclaredNonKeyColumn()
    {
        // Plan: "a scalar entity projects to one column named by the single declared non-key field."
        var doc = new YDoc();
        RootOf(doc).Set("e1", 42);

        var fields = new List<FieldDef> { new("id", FieldType.String), new("count", FieldType.Long) };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.Equal(42L, rows["e1"]["count"]);
        Assert.Equal("e1", rows["e1"]["id"]);
    }

    [Fact]
    public void AmbiguousScalarEntityIsSkippedNeverGuessed()
    {
        // Plan: "if there is more than one candidate, that is a diagnostic and the entity is skipped,
        // never guessed." Two non-key fields declared -- CrdtProjector must not pick one arbitrarily.
        var doc = new YDoc();
        RootOf(doc).Set("e1", 42);

        var fields = new List<FieldDef>
        {
            new("id", FieldType.String),
            new("count", FieldType.Long),
            new("other", FieldType.String),
        };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.False(rows.ContainsKey("e1"));
        Assert.Contains(diagnostics, d => d.Contains("e1"));
    }

    [Fact]
    public void KeyFieldColumnIsAlwaysTheEntityKeyEvenIfContentDisagrees()
    {
        // The entity's own identity is authoritative -- a nested attribute that happens to share the
        // KeyField's name must never let document content overwrite it.
        var doc = new YDoc();
        var entity = new YMap();
        RootOf(doc).Set("e1", entity);
        entity.Set("id", "impostor");

        var fields = new List<FieldDef> { new("id", FieldType.String) };
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Flatten(doc, DefaultConfig(), fields, diagnostics);

        Assert.Equal("e1", rows["e1"]["id"]);
    }

    [Fact]
    public void DeletionProducesATombstoneCarryingOnlyTheKeyField()
    {
        // Plan D8/projection section + CrdtSourceConfig's own doc comment: "emit a row carrying only
        // config.KeyField plus _op = 'd' and _weight = -1L" -- deliberately no stale attribute values.
        //
        // DECLARED BEHAVIOUR CHANGE, wave B-2's live check: the tombstone now carries a THIRD stamp,
        // _retract = true. The original two are not enough and the live run proved it -- deleting a
        // document key left the table holding both the old row and a second all-null row for the same
        // key, each at weight 1, because _weight on an INBOUND row is just a column (CdcEnvelope's class
        // doc has always said so; a Debezium delete has the same limit). _retract is the platform's one
        // real key retraction, honoured by the Engine's TableIngestOp, and with it a LATEST BY table
        // genuinely frees the key -- re-verified live, table went to 0 rows. So this assertion is
        // updated, not weakened: the count moves 3 -> 4 and the new stamp is asserted by name.
        var config = DefaultConfig();
        var before = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Ann" },
        };
        var after = new Dictionary<string, Dictionary<string, object?>>();

        var diff = CrdtProjector.Diff(before, after, config);

        var tombstone = Assert.Single(diff);
        Assert.Equal(4, tombstone.Count); // ONLY id/_op/_weight/_retract -- "name" must not leak through
        Assert.Equal("e1", tombstone["id"]);
        Assert.Equal("d", tombstone["_op"]);
        Assert.Equal(-1L, tombstone["_weight"]);
        Assert.Equal(true, tombstone["_retract"]);
    }

    [Fact]
    public void NewKeyEmitsACreateRowWithTheFullRow()
    {
        var config = DefaultConfig();
        var before = new Dictionary<string, Dictionary<string, object?>>();
        var after = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Ann" },
        };

        var diff = CrdtProjector.Diff(before, after, config);

        var created = Assert.Single(diff);
        Assert.Equal("c", created["_op"]);
        Assert.Equal(1L, created["_weight"]);
        Assert.Equal("Ann", created["name"]); // full row, not just the key
    }

    [Fact]
    public void ChangedKeyEmitsAnUpdateRowWithTheFullRow()
    {
        var config = DefaultConfig();
        var before = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Ann" },
        };
        var after = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Bob" },
        };

        var diff = CrdtProjector.Diff(before, after, config);

        var updated = Assert.Single(diff);
        Assert.Equal("u", updated["_op"]);
        Assert.Equal(1L, updated["_weight"]);
        Assert.Equal("Bob", updated["name"]);
    }

    [Fact]
    public void UnchangedKeyEmitsNothing()
    {
        // Plan D7, asserted directly against Diff: "A key present in both with an identical row: emit
        // nothing." Two SEPARATE dictionary instances with equal content -- reference equality must not
        // be what Diff relies on, or a fresh Flatten() of an unchanged document would wrongly re-emit.
        var config = DefaultConfig();
        var before = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Ann" },
        };
        var after = new Dictionary<string, Dictionary<string, object?>>
        {
            ["e1"] = new() { ["id"] = "e1", ["name"] = "Ann" },
        };

        var diff = CrdtProjector.Diff(before, after, config);

        Assert.Empty(diff);
    }

    [Fact]
    public void RedeliveringTheSameYjsUpdateProducesZeroFurtherDeltas()
    {
        // Plan D7, end to end through the real Ycs merge path rather than through two hand-built
        // dictionaries: "a flaky link that redelivers the same batch four times costs four merges and
        // zero downstream events." Build one real update, apply it to a fresh document, flatten, apply
        // the SAME update AGAIN (simulating redelivery), flatten again, and Diff the two states.
        var config = DefaultConfig();
        var fields = new List<FieldDef> { new("id", FieldType.String), new("name", FieldType.String) };

        var source = new YDoc();
        var entity = new YMap();
        RootOf(source, config).Set("e1", entity);
        entity.Set("name", "Ann");
        var update = source.EncodeStateAsUpdateV1();

        var target = new YDoc();
        target.ApplyUpdateV1(update);
        var before = CrdtProjector.Flatten(target, config, fields, new List<string>());

        target.ApplyUpdateV1(update); // redelivery of the identical batch
        var after = CrdtProjector.Flatten(target, config, fields, new List<string>());

        var diff = CrdtProjector.Diff(before, after, config);

        Assert.Empty(diff);
    }

    [Fact]
    public void DiffOpAndWeightLiteralsMatchCdcStampsSpellingExactly()
    {
        // Plan: "This is the deletion convention -- it is deliberately the same one
        // StreamsForge.Connectors.Database.CdcStamp uses... a test should assert the literals rather than
        // trusting the two to stay in step."
        //
        // CrdtProjector deliberately does NOT reference StreamsForge.Connectors.Database (see its class
        // doc: a database connector has no business being a dependency of a CRDT document, and it would
        // drag a live-database driver stack in for four string constants). This TEST does reference it,
        // which is the whole point -- it reads CdcStamp's REAL declared values rather than a copy of
        // them, so a change on either side turns this red. A pin that restates the other side's literals
        // from memory is a comment wearing a test's clothes.
        var config = DefaultConfig();
        var created = Assert.Single(CrdtProjector.Diff(
            new Dictionary<string, Dictionary<string, object?>>(),
            new Dictionary<string, Dictionary<string, object?>> { ["e1"] = new() { ["id"] = "e1" } },
            config));
        var deleted = Assert.Single(CrdtProjector.Diff(
            new Dictionary<string, Dictionary<string, object?>> { ["e1"] = new() { ["id"] = "e1" } },
            new Dictionary<string, Dictionary<string, object?>>(),
            config));

        Assert.True(created.ContainsKey(CdcStamp.OpColumn));
        Assert.True(created.ContainsKey(CdcStamp.WeightColumn));
        Assert.Equal(CdcStamp.OpCreate, created[CdcStamp.OpColumn]);
        Assert.Equal(1L, created[CdcStamp.WeightColumn]);
        Assert.Equal(CdcStamp.OpDelete, deleted[CdcStamp.OpColumn]);
        Assert.Equal(-1L, deleted[CdcStamp.WeightColumn]);

        // The weight's TYPE is not pinned across the two: CdcStamp.WeightOf returns int and Diff emits
        // long. They are independent implementations and only the spelling is contractual -- but a
        // downstream consumer reading _weight has to cope with both, so the divergence is stated here
        // rather than discovered by whoever writes that consumer.
        Assert.Equal(CdcStamp.OpUpdate, "u");
    }

    [Fact]
    public void ArrayAttributeFlattensToIndexedColumns()
    {
        // The dotted-path join scheme uses an array element's INDEX as its path segment, which is the
        // half of the scheme where the plan's accepted cost actually bites: reorder the array and every
        // column's value changes, because position IS the identity here. Pinned so that cost stays
        // visible rather than being rediscovered as a bug.
        var doc = new YDoc();
        var root = doc.GetMap("root");
        doc.Transact(_ =>
        {
            var entity = new YMap();
            root.Set("e1", entity);
            var tags = new YArray();
            entity.Set("tags", tags);
            tags.Insert(0, ["a", "b"]);
        });

        var diagnostics = new List<string>();
        var flat = CrdtProjector.Flatten(
            doc,
            DefaultConfig(),
            [new FieldDef("id", FieldType.String), new FieldDef("tags.0", FieldType.String), new FieldDef("tags.1", FieldType.String)],
            diagnostics);

        var row = flat["e1"];
        Assert.Equal("a", row["tags.0"]);
        Assert.Equal("b", row["tags.1"]);
    }

    [Fact]
    public void ABareYArrayDirectlyUnderAnEntityKeyIsRefusedHonestlyRatherThanThrowing()
    {
        // A shape plan 020 does not name: the root map's value is neither a YMap of attributes nor a
        // scalar, but a Y-type sitting directly under the entity key. The contract that matters is the
        // one this class states in bold -- NEVER THROWS on document content, because a document is
        // written by somebody else's edge. So this asserts the honest refusal (a diagnostic, and no
        // exception), not a projection: inventing flattening rules for an unspecified shape is how a
        // v1 acquires behaviour nobody designed.
        var doc = new YDoc();
        var root = doc.GetMap("root");
        doc.Transact(_ =>
        {
            var bare = new YArray();
            root.Set("e1", bare);
            bare.Insert(0, ["a"]);
        });

        var diagnostics = new List<string>();
        var flat = CrdtProjector.Flatten(
            doc,
            DefaultConfig(),
            [new FieldDef("id", FieldType.String), new FieldDef("name", FieldType.String)],
            diagnostics);

        // Found live by this test on its first run: before the fix, this projected the literal string
        // "Ycs.YArray" into the row with NO diagnostic, because FieldValueCoercion coerces any object at
        // all into a String field via ToString(). Data that looks like data and is not is the exact
        // failure the projector's design notes exist to prevent, so both halves are asserted — the null
        // AND the diagnostic. A silent null would be only half a fix.
        var row = flat["e1"];
        Assert.Null(row["name"]);
        Assert.Contains(diagnostics, d => d.Contains("YArray", StringComparison.Ordinal));
    }
}
