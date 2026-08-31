using Xunit;
using Ycs;

namespace StreamsForge.Connectors.Crdt.Tests;

/// <summary>
/// Plan 020 wave D, finding 3 — pins the EXACT mechanism <c>CrdtDocGrain.AttributeAcceptedUpdates</c>
/// uses, standalone from Orleans (no grain, no DI — the same "there is no socket here" standard
/// <see cref="CrdtProjectorTests"/> holds itself to), so the documented tension in
/// <c>CrdtSourceConfig.AttributeChanges</c>'s own doc comment is a proven fact rather than an inference:
/// "who wrote this" is answered by <see cref="PermanentUserData.GetUserByClientId"/> for a REMOTELY
/// applied update exactly as it would be for a local one; "who deleted this" is NOT, because
/// <see cref="PermanentUserData.SetUserMapping"/>'s delete-set bookkeeping is gated on
/// <see cref="Transaction.Local"/>, and <see cref="YDoc.ApplyUpdateV1(byte[], object, bool)"/> — the only
/// way <c>CrdtDocGrain</c> ever mutates a document — defaults <c>local</c> to <c>false</c> and the grain
/// never overrides it.
/// </summary>
public class CrdtAttributionTests
{
    [Fact]
    public void GetUserByClientId_answers_correctly_for_a_remotely_applied_update()
    {
        // The "edge": produces an update entirely on its own YDoc, exactly like a browser tab would.
        var edge = new YDoc();
        var e1 = new YMap();
        edge.GetMap("root").Set("e1", e1);
        e1.Set("name", "Ann");
        var update = edge.EncodeStateAsUpdateV1();
        var edgeClientId = edge.ClientId;

        // The "grain": a SEPARATE document that only ever receives bytes over the wire — this is
        // deliberately the ApplyUpdateV1(bytes) call CrdtDocGrain.MergeCoreAsync makes, local defaulted.
        var serverDoc = new YDoc();
        var pud = new PermanentUserData(serverDoc);
        serverDoc.ApplyUpdateV1(update);

        pud.SetUserMapping(serverDoc, edgeClientId, "alice");

        Assert.Equal("alice", pud.GetUserByClientId(edgeClientId));
    }

    /// <summary>Reads the exact <c>(client, clock)</c> a deletion covers straight out of the update
    /// bytes, via the SAME decoder <see cref="CrdtUpdateInspector"/> uses — robust against exactly how
    /// many structs a document happened to create before the deletion (e.g. <see cref="PermanentUserData"/>'s
    /// own "users" map bookkeeping consumes clock space too), rather than assuming a clock offset.</summary>
    private static ID FirstDeletedId(byte[] deleteOnlyUpdate)
    {
        var decoded = UpdateOperations.DecodeUpdate(deleteOnlyUpdate);
        foreach (var (client, ranges) in decoded.DeleteSet)
        {
            foreach (var range in ranges)
            {
                return new ID(client, range.Clock);
            }
        }

        throw new InvalidOperationException("update carries no delete set — test setup is wrong");
    }

    [Fact]
    public void GetUserByDeletedId_does_NOT_answer_for_a_remotely_applied_deletion_because_ApplyUpdateV1_defaults_local_to_false()
    {
        // This is the gap CrdtSourceConfig.AttributeChanges's own doc comment names rather than hides:
        // SetUserMapping's "ds" (delete-set) bookkeeping is written from an AfterTransaction handler
        // gated on Transaction.Local, and ApplyUpdateV1 never sets that flag.
        var edge = new YDoc();
        var e1 = new YMap();
        edge.GetMap("root").Set("e1", e1);
        e1.Set("name", "Ann");
        var svAfterCreate = edge.EncodeStateVectorV1();
        var createUpdate = edge.EncodeStateAsUpdateV1();

        edge.GetMap("root").Delete("e1");
        var deleteUpdate = edge.EncodeStateAsUpdateV1(svAfterCreate);
        var deletedId = FirstDeletedId(deleteUpdate);
        var edgeClientId = edge.ClientId;

        var serverDoc = new YDoc();
        var pud = new PermanentUserData(serverDoc);
        pud.SetUserMapping(serverDoc, edgeClientId, "alice"); // set up BEFORE either update lands, as CrdtDocGrain does (lazily, on first attributed merge)

        serverDoc.ApplyUpdateV1(createUpdate, local: false); // CrdtDocGrain.MergeCoreAsync's exact call shape
        serverDoc.ApplyUpdateV1(deleteUpdate, local: false);

        // GetUserByClientId still knows "alice" touched this document...
        Assert.Equal("alice", pud.GetUserByClientId(edgeClientId));
        // ...but WHO deleted e1 is unanswerable: the delete-set was never recorded, because the
        // transaction that ran ApplyUpdateV1's delete was not local.
        Assert.Null(pud.GetUserByDeletedId(deletedId));
    }

    [Fact]
    public void GetUserByDeletedId_DOES_answer_when_the_exact_same_bytes_are_applied_as_a_genuinely_local_transaction()
    {
        // The control case, isolating the ONE variable: identical document, identical update bytes,
        // identical SetUserMapping call — the only difference from the test above is local: true. This
        // is what proves the gap is specifically "ApplyUpdateV1 defaults local to false", not some other
        // unrelated PermanentUserData limitation.
        var edge = new YDoc();
        var e1 = new YMap();
        edge.GetMap("root").Set("e1", e1);
        e1.Set("name", "Ann");
        var svAfterCreate = edge.EncodeStateVectorV1();
        var createUpdate = edge.EncodeStateAsUpdateV1();

        edge.GetMap("root").Delete("e1");
        var deleteUpdate = edge.EncodeStateAsUpdateV1(svAfterCreate);
        var deletedId = FirstDeletedId(deleteUpdate);
        var edgeClientId = edge.ClientId;

        var serverDoc = new YDoc();
        var pud = new PermanentUserData(serverDoc);
        pud.SetUserMapping(serverDoc, edgeClientId, "alice");

        serverDoc.ApplyUpdateV1(createUpdate, local: true);
        serverDoc.ApplyUpdateV1(deleteUpdate, local: true);

        Assert.Equal("alice", pud.GetUserByClientId(edgeClientId));
        Assert.Equal("alice", pud.GetUserByDeletedId(deletedId));
    }
}
