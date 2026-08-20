using StreamForge.Abstractions;
using Xunit;
using Ycs;

namespace StreamForge.Connectors.Crdt.Tests;

/// <summary>
/// Plan 020 wave D, finding 2. Pins <see cref="CrdtUpdateInspector"/>'s exact boundary — the cases the
/// wave brief demanded be stated rather than assumed: what is honestly recoverable from an update's bytes
/// alone, and what is not. Every document here is a REAL <see cref="YDoc"/>, same standard
/// <see cref="CrdtProjectorTests"/> holds itself to — there is no document to fake, only bytes to decode.
/// </summary>
public class CrdtUpdateInspectorTests
{
    private static CrdtSourceConfig DefaultConfig() => new() { RootMap = "root", KeyField = "id" };

    // ------------------------------------------------------------------
    // Decidable — the whole chain is inside this one update.
    // ------------------------------------------------------------------

    [Fact]
    public void WholeEntityScalarCreateResolvesTheEntityKeyWithNoFieldPath()
    {
        var doc = new YDoc();
        doc.GetMap("root").Set("e1", "just-a-scalar-entity");

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        var touch = Assert.Single(inspection.Touches);
        Assert.Equal("e1", touch.EntityKey);
        Assert.Null(touch.FieldPath);
    }

    [Fact]
    public void TopLevelFieldOnAnEntityCreatedInTheSameUpdateResolvesAOneSegmentPath()
    {
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("qty", 250L);
        });

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        // Two items land in this one update: the entity's own map (whole-entity, FieldPath null) and the
        // "qty" field inside it (FieldPath "qty") — both resolve to the SAME entity key.
        Assert.All(inspection.Touches, t => Assert.Equal("e1", t.EntityKey));
        Assert.Contains(inspection.Touches, t => t.FieldPath == "qty");
    }

    [Fact]
    public void NestedMapFieldCreatedInTheSameUpdateResolvesTheDottedPath()
    {
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            var address = new YMap();
            e1.Set("address", address);
            address.Set("city", "Berlin");
        });

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        Assert.Contains(inspection.Touches, t => t.EntityKey == "e1" && t.FieldPath == "address.city");
    }

    [Fact]
    public void CreateThenDeleteOfTheSameEntityInOneUpdateResolvesViaTheDeleteSet()
    {
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("name", "Ann");
            doc.GetMap("root").Delete("e1");
        });

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        Assert.Contains(inspection.Touches, t => t.EntityKey == "e1");
    }

    [Fact]
    public void MultipleEntitiesInOneUpdateEachProduceTheirOwnTouch()
    {
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var a = new YMap();
            doc.GetMap("root").Set("AAPL", a);
            a.Set("name", "Apple");

            var b = new YMap();
            doc.GetMap("root").Set("MSFT", b);
            b.Set("name", "Microsoft");
        });

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        Assert.Contains(inspection.Touches, t => t.EntityKey == "AAPL");
        Assert.Contains(inspection.Touches, t => t.EntityKey == "MSFT");
    }

    [Fact]
    public void ATouchUnderADifferentRootTypeIsSilentlyNotOurs()
    {
        var doc = new YDoc();
        // A root type this source's CrdtSourceConfig.RootMap ("root") does not name at all — the
        // document may carry an edge's own bookkeeping under it; not this source's business.
        doc.GetMap("other").Set("whatever", "value");

        var inspection = CrdtUpdateInspector.Inspect(doc.EncodeStateAsUpdateV1(), DefaultConfig());

        Assert.False(inspection.Undecidable);
        Assert.Empty(inspection.Touches);
    }

    // ------------------------------------------------------------------
    // Undecidable — the honest boundary the wave brief demanded be named, not guessed past.
    // ------------------------------------------------------------------

    [Fact]
    public void MalformedBytesAreUndecidableNotAThrow()
    {
        var garbage = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

        var inspection = CrdtUpdateInspector.Inspect(garbage, DefaultConfig());

        Assert.True(inspection.Undecidable);
        Assert.Contains("decode", inspection.UndecidableReason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AFieldEditOnAnEntityCreatedByAnEarlierUpdateIsUndecidable()
    {
        // The ordinary case an edge produces: create an entity once, edit it many times afterwards, each
        // edit its own update frame. Inspecting the SECOND frame alone cannot know which entity "qty"
        // belongs to — the item that defines the entity's own map is not in these bytes at all.
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("name", "Ann");
        });
        var svAfterCreate = doc.EncodeStateVectorV1();

        doc.Transact(_ => ((YMap)doc.GetMap("root").Get("e1")).Set("qty", 5L));
        var editOnlyUpdate = doc.EncodeStateAsUpdateV1(svAfterCreate);

        var inspection = CrdtUpdateInspector.Inspect(editOnlyUpdate, DefaultConfig());

        Assert.True(inspection.Undecidable);
        Assert.Contains("not among the structs this update decoded", inspection.UndecidableReason);
    }

    [Fact]
    public void DeletingAnEntityCreatedByAnEarlierUpdateIsUndecidable()
    {
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("name", "Ann");
        });
        var svAfterCreate = doc.EncodeStateVectorV1();

        doc.Transact(_ => doc.GetMap("root").Delete("e1"));
        var deleteOnlyUpdate = doc.EncodeStateAsUpdateV1(svAfterCreate);

        var inspection = CrdtUpdateInspector.Inspect(deleteOnlyUpdate, DefaultConfig());

        Assert.True(inspection.Undecidable);
        Assert.Contains("deletes content this update did not itself create", inspection.UndecidableReason);
    }

    [Fact]
    public void AFieldEditOnAnEntityCreatedInAnEarlierUpdateStaysUndecidableEvenWhenAnotherEntityInTheSameFrameResolves()
    {
        // One undecidable struct makes the WHOLE frame undecidable, per this class's own contract — a
        // partially-authorized update is not something CrdtEndpoints is asked to reason further about.
        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("name", "Ann");
        });
        var svAfterCreate = doc.EncodeStateVectorV1();

        doc.Transact(_ =>
        {
            ((YMap)doc.GetMap("root").Get("e1")).Set("qty", 5L); // undecidable half
            var e2 = new YMap();
            doc.GetMap("root").Set("e2", e2); // decidable half, same frame
            e2.Set("name", "Bo");
        });
        var mixedUpdate = doc.EncodeStateAsUpdateV1(svAfterCreate);

        var inspection = CrdtUpdateInspector.Inspect(mixedUpdate, DefaultConfig());

        Assert.True(inspection.Undecidable);
        Assert.Empty(inspection.Touches);
    }
}
