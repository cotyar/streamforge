namespace StreamForge.Abstractions;

/// <summary>
/// Plan 009: how an update replaces a stored catalog record.
///
/// <para><b>The rule: the incoming definition IS the new record.</b> Only the fields the SERVER owns are
/// carried over from the stored one; everything else comes from the client's payload, because the client
/// is who edits it. Callers do <c>CarryServerOwnedFields(existing, incoming)</c> and then put
/// <c>incoming</c> in the list slot — they do not copy fields one at a time.</para>
///
/// <para><b>Why it is written this way.</b> Both flavors used to hand-copy a list of editable fields from
/// the incoming definition onto the stored record, which made <i>silently dropped</i> the default for any
/// field somebody forgot to add to that list. Plan 009 lost three that way — <c>Sinks</c> on both entity
/// types, and <c>JournalMaxEntries</c>, which additionally never reached the restart check that would have
/// applied it. All three were invisible: the PUT returns 200 and the value quietly reverts on the next
/// read. This shape inverts the default to <i>kept unless named</i>, and the list below is short and
/// stable while the editable surface is the part that keeps growing.</para>
///
/// <para><b>What the new failure mode is.</b> Forgetting to add a genuinely server-owned field here means
/// a client's payload can overwrite it. That is a visible, immediately-testable wrong value rather than a
/// silent reversion — which is the trade being made deliberately, not an oversight.</para>
///
/// <para>Both flavors call this (Orleans' <c>RegistryGrain</c>, Dapr's <c>CatalogStore</c>) so the rule
/// cannot drift between them, which is exactly how the two flavors ended up disagreeing about
/// <c>JournalMaxEntries</c> in the first place.</para>
/// </summary>
public static class CatalogRecordMerge
{
    /// <summary>Status/Error are server-owned even when the caller sends values for them: lifecycle
    /// belongs to start/stop, not to an edit. SourceNames is recomputed from the compile result right
    /// after this returns; it is carried anyway so a compile failure leaves the previous value standing
    /// rather than whatever the request body happened to contain.</summary>
    public static void CarryServerOwnedFields(PipelineDefinition existing, PipelineDefinition incoming, long nowMs)
        => CarryServerOwnedFields(existing, incoming, nowMs, existing.UpdatedBy);

    /// <summary>Plan 015: same rule, plus <c>UpdatedBy</c>. <c>updatedBy</c> is the authenticated caller,
    /// which is server-owned in the strongest sense — it is the one field a client must never be able to
    /// set. The 3-arg overload survives (it carries the stored value forward) so a caller that has no
    /// principal to hand — a migration, a test — is not forced to invent one.</summary>
    public static void CarryServerOwnedFields(PipelineDefinition existing, PipelineDefinition incoming, long nowMs, string updatedBy)
    {
        incoming.UpdatedBy = updatedBy;
        incoming.Id = existing.Id;
        incoming.Status = existing.Status;
        incoming.Error = existing.Error;
        incoming.CreatedBy = existing.CreatedBy;
        incoming.CreatedAtMs = existing.CreatedAtMs;
        incoming.SourceNames = existing.SourceNames;
        incoming.UpdatedAtMs = nowMs;
    }

    /// <summary>Table twin of the pipeline overload. OutputFields/StreamInputs/TableInputs/KeyFields are
    /// recomputed from the compile result right after this returns, and are carried for the same reason
    /// SourceNames is on the pipeline side. KeyFields (wishlist #18) joined this list the day it was
    /// added — it is exactly as server-owned as the three fields beside it, recomputed on the identical
    /// compile.</summary>
    public static void CarryServerOwnedFields(TableDefinition existing, TableDefinition incoming, long nowMs)
        => CarryServerOwnedFields(existing, incoming, nowMs, existing.UpdatedBy);

    /// <summary>Table twin of the 4-arg pipeline overload.</summary>
    public static void CarryServerOwnedFields(TableDefinition existing, TableDefinition incoming, long nowMs, string updatedBy)
    {
        incoming.UpdatedBy = updatedBy;
        incoming.Id = existing.Id;
        incoming.Status = existing.Status;
        incoming.Error = existing.Error;
        incoming.CreatedBy = existing.CreatedBy;
        incoming.CreatedAtMs = existing.CreatedAtMs;
        incoming.OutputFields = existing.OutputFields;
        incoming.StreamInputs = existing.StreamInputs;
        incoming.TableInputs = existing.TableInputs;
        incoming.KeyFields = existing.KeyFields;
        incoming.UpdatedAtMs = nowMs;
    }
}
