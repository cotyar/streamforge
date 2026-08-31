using StreamsForge.Abstractions;
using Ycs;

namespace StreamsForge.Connectors.Crdt;

/// <summary>
/// Plan 020 wave D, finding 2 — works out which entity key(s) a Yjs v1 update touches WITHOUT applying
/// it to a document, so <c>CrdtEndpoints</c> can authorize an update before <c>CrdtDocGrain.MergeAsync</c>
/// ever sees it. See <see cref="CrdtSourceConfig.RequireEntityAuthorization"/> for the operator-facing
/// contract (opt-in, cost, and the exact boundary in prose); this class is that boundary in code.
///
/// <para><b>Why this is possible at all without a document.</b> <c>Ycs.UpdateOperations.DecodeUpdate</c>
/// (the parity branch's own doc comment on <c>EncodingUtils.ReadClientStructRefs</c>) decodes an update's
/// structs WITHOUT resolving them against a live <see cref="YDoc"/> — deliberately, because there may not
/// be one to resolve against. So every <see cref="Item"/>'s <c>Parent</c> is either the literal name of a
/// root-level type (a <see cref="string"/>) or the <see cref="ID"/> of the item that defined its
/// container — never an already-resolved <c>AbstractType</c>. Walking that chain from an item up to a
/// root name is exactly "which entity does this belong to", provided every link in the chain is a struct
/// THIS SAME UPDATE also decoded.</para>
///
/// <para><b>The boundary, precisely.</b> The walk below climbs from an item through <c>Item.Parent</c>
/// until it either (a) reaches a <see cref="string"/> equal to <see cref="CrdtSourceConfig.RootMap"/> —
/// success, entity key is that root-level item's own <c>ParentSub</c>, and the field path is every
/// <c>ParentSub</c> collected along the way, shallowest first — (b) reaches a <see cref="string"/> that
/// names some OTHER root type — not ours, the touch is dropped silently (a document may carry other
/// top-level bookkeeping this source does not project, and that is not this class's business), or
/// (c) reaches an <see cref="ID"/> this update did NOT also decode a struct for. Case (c) is
/// <see cref="CrdtUpdateInspection.Undecidable"/>: the defining item lives only in the document's
/// already-applied state (the ordinary case — an edge usually creates an entity once and edits it many
/// times afterwards, each edit its own update frame) or in a DIFFERENT frame of the same REST batch (this
/// class inspects one update's bytes at a time, exactly as <c>CrdtDocGrain.MergeAsync</c> applies them
/// one at a time — a create in frame 1 and a field edit in frame 2 of the SAME POST is still undecidable
/// when frame 2 is inspected alone). Neither is a bug; both are named in
/// <see cref="CrdtUpdateInspection.UndecidableReason"/> rather than guessed past.</para>
///
/// <para><b>Deletions.</b> A delete set entry names a <c>[clock, clock+length)</c> range for a client,
/// not a struct — there is nothing to walk from a range alone. This class resolves a deleted range only
/// when an item decoded from the SAME update overlaps it (a create-then-delete inside one offline
/// session, e.g. wave B's own live check), by walking that item exactly as above; every other deleted
/// range is undecidable for the same reason case (c) above is: the deleted content's defining chain is
/// not in this update's bytes.</para>
///
/// <para><b>Granularity actually enforced.</b> <see cref="CrdtUpdateTouch.FieldPath"/> is reported when the
/// whole chain resolves with no list-positioned (<c>ParentSub == null</c>) hop in it, purely as
/// diagnostic detail — it is not itself a grantable scope. Plan 015's scope grammar tops out at exact
/// entity NAME (plus prefix/tag); there is no field axis to plug a narrower grant into. So the
/// authorization decision <c>CrdtEndpoints</c> makes from this class's output is at ENTITY-KEY
/// granularity only (scope string <c>"{sourceName}/{entityKey}"</c>) — inspecting at the field level is
/// what makes recovering the entity key possible for nested content, but the grant itself does not (yet)
/// go any finer than the entity it is nested under.</para>
/// </summary>
public static class CrdtUpdateInspector
{
    /// <summary>Guards against a pathological/cyclic parent chain in a hostile or corrupt update —
    /// real documents this platform builds nest a handful of levels at most (an entity's own attributes,
    /// one level of nested object). 64 is generous headroom, not a real limit anyone should hit.</summary>
    private const int MaxChainDepth = 64;

    public static CrdtUpdateInspection Inspect(byte[] updateBytes, CrdtSourceConfig config)
    {
        DecodedUpdate decoded;
        try
        {
            decoded = UpdateOperations.DecodeUpdate(updateBytes);
        }
        catch (Exception ex)
        {
            // A frame CrdtDocGrain.MergeAsync would also fail to apply — refusing it here reports the
            // same fact earlier and with the same "skip it, do not abort the batch" shape.
            return new CrdtUpdateInspection { Undecidable = true, UndecidableReason = $"failed to decode: {ex.GetType().Name}: {ex.Message}" };
        }

        var rootMap = string.IsNullOrEmpty(config.RootMap) ? "root" : config.RootMap;

        var byId = new Dictionary<(long Client, long Clock), Item>();
        foreach (var s in decoded.Structs)
        {
            if (s is Item asItem)
            {
                byId[(asItem.Id.Client, asItem.Id.Clock)] = asItem;
            }
        }

        var touches = new List<CrdtUpdateTouch>();

        foreach (var s in decoded.Structs)
        {
            if (s is not Item item)
            {
                continue; // Skip/GC carry no content of interest.
            }

            var outcome = Resolve(item, byId, rootMap);
            if (outcome.Undecidable)
            {
                return new CrdtUpdateInspection
                {
                    Undecidable = true,
                    UndecidableReason = outcome.UndecidableReason,
                };
            }

            if (outcome.Touch is { } touch)
            {
                touches.Add(touch);
            }
            // outcome with neither Touch nor Undecidable = "not our root", silently not a touch.
        }

        foreach (var (client, ranges) in decoded.DeleteSet)
        {
            foreach (var range in ranges)
            {
                var found = false;
                foreach (var kvp in byId)
                {
                    var (idClient, idClock) = kvp.Key;
                    if (idClient != client)
                    {
                        continue;
                    }

                    var itemEnd = idClock + kvp.Value.Length;
                    var rangeEnd = range.Clock + range.Length;
                    if (idClock < rangeEnd && itemEnd > range.Clock)
                    {
                        // Overlaps — resolve using the item this SAME update also created, per this
                        // class's own class-doc "create-then-delete inside one frame" case.
                        found = true;
                        var outcome = Resolve(kvp.Value, byId, rootMap);
                        if (outcome.Undecidable)
                        {
                            return new CrdtUpdateInspection { Undecidable = true, UndecidableReason = outcome.UndecidableReason };
                        }

                        if (outcome.Touch is { } touch)
                        {
                            touches.Add(touch);
                        }
                    }
                }

                if (!found)
                {
                    // The deleted content's defining item is not in this update at all — it was created
                    // by an earlier frame (or already lived in the document). No way to say which entity
                    // without the document itself.
                    return new CrdtUpdateInspection
                    {
                        Undecidable = true,
                        UndecidableReason = $"delete set entry (client {client}, clock [{range.Clock},{range.Clock + range.Length})) "
                            + "deletes content this update did not itself create — its entity is not recoverable without the live document",
                    };
                }
            }
        }

        return new CrdtUpdateInspection { Touches = touches };
    }

    private readonly record struct ResolveOutcome(CrdtUpdateTouch? Touch, bool Undecidable, string? UndecidableReason);

    private static ResolveOutcome Resolve(Item item, IReadOnlyDictionary<(long, long), Item> byId, string rootMap)
    {
        var segments = new List<string>(); // deepest-first as collected; reversed before use.
        var current = item;

        for (var hop = 0; hop < MaxChainDepth; hop++)
        {
            switch (current.Parent)
            {
                case string rootName when rootName == rootMap:
                {
                    var entityKey = current.ParentSub;
                    if (entityKey is null)
                    {
                        // A root-level item with no key — the root type is list-shaped, not the YMap the
                        // config contract requires. Nothing sane to report as an entity.
                        return new ResolveOutcome(null, true, "root map entry has no key (root is not YMap-shaped)");
                    }

                    segments.Reverse();
                    return new ResolveOutcome(
                        new CrdtUpdateTouch(entityKey, segments.Count == 0 ? null : string.Join('.', segments)),
                        false,
                        null);
                }

                case string:
                    // A different root type entirely — not this source's projection.
                    return new ResolveOutcome(null, false, null);

                case ID pid:
                {
                    if (!byId.TryGetValue((pid.Client, pid.Clock), out var parentItem))
                    {
                        return new ResolveOutcome(
                            null,
                            true,
                            $"parent (client {pid.Client}, clock {pid.Clock}) is not among the structs this update decoded — "
                            + "it was created by an earlier update (or already lives in the document)");
                    }

                    if (current.ParentSub is not null)
                    {
                        segments.Add(current.ParentSub);
                    }
                    // current.ParentSub == null means this item sits in a LIST, not a MAP — its exact
                    // position cannot be recovered without materializing the array (a live-document
                    // operation), so the field path stays incomplete from here up. The entity key is
                    // still recoverable by continuing to climb, so this is not itself undecidable.

                    current = parentItem;
                    break;
                }

                default:
                    // DecodeUpdate never resolves a parent to a live AbstractType (see this class's own
                    // doc comment) — an unexpected shape is treated the same as "cannot resolve".
                    return new ResolveOutcome(null, true, $"parent has an unexpected shape: {current.Parent?.GetType().Name ?? "null"}");
            }
        }

        return new ResolveOutcome(null, true, $"parent chain did not resolve within {MaxChainDepth} hops");
    }
}
