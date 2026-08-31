namespace StreamsForge.Connectors.Database;

/// <summary>One relation as pgoutput described it: the qualified name and column list a
/// <c>RelationMessage</c> carries, plus its replica identity setting — Npgsql's own enum names
/// (<c>"Default"</c> | <c>"AllColumns"</c> | <c>"IndexWithIndIsReplIdent"</c> | <c>"Nothing"</c>), passed
/// through as a string so this type does not take a dependency on <c>Npgsql.Replication</c>. <c>AllColumns</c>
/// is what an operator would call <c>REPLICA IDENTITY FULL</c> — the setting that makes a delete's old row
/// available at all (see <see cref="PgCdcSource"/>'s class doc).</summary>
public sealed record PgRelation(
    uint RelationId,
    string Namespace,
    string RelationName,
    IReadOnlyList<string> ColumnNames,
    string ReplicaIdentity)
{
    /// <summary><c>"namespace.relation"</c> — what <see cref="CdcStamp.Apply"/> stamps into
    /// <see cref="CdcStamp.TableColumn"/>.</summary>
    public string QualifiedName => $"{Namespace}.{RelationName}";
}

/// <summary>
/// <c>RelationId → PgRelation</c>, scoped to ONE replication session (i.e. one <see cref="PgCdcSource"/>
/// poll cycle — see its class doc for why a session never outlives a cycle). Pgoutput sends a
/// <c>RelationMessage</c> once per relation per session, before the first change event that references it;
/// everything downstream of that first message is positional and anonymous unless something remembers the
/// column names it carried. This is that something — pulled out of <see cref="PgCdcSource"/>'s streaming
/// loop so it is a plain, synchronous, server-free map that <c>PgTupleDecoderTests</c> can drive directly.
///
/// <para><b>An unknown relation id is a loud failure, not a silent one.</b> A change event for a relation
/// this cache has never seen a <c>RelationMessage</c> for means either pgoutput violated its own protocol
/// (a relation message is always supposed to precede its first use in a session) or this cache lost an
/// entry it should not have — either way, guessing a shape for the row for it would silently corrupt data
/// rather than surface the bug. <see cref="Get"/> throws, naming the id, so the failure lands on the
/// cycle's error status where an operator can see it, per <c>IPolledTransport.PollAsync</c>'s "throwing is
/// a normal, expected outcome" contract.</para>
/// </summary>
public sealed class PgRelationCache
{
    private readonly Dictionary<uint, PgRelation> _byId = [];

    /// <summary>Records (or replaces) what a <c>RelationMessage</c> said about one relation.</summary>
    public void Set(uint relationId, string ns, string relationName, IReadOnlyList<string> columnNames, string replicaIdentity)
        => _byId[relationId] = new PgRelation(relationId, ns, relationName, columnNames, replicaIdentity);

    /// <summary>The relation pgoutput described for <paramref name="relationId"/>. Throws
    /// <see cref="InvalidOperationException"/> naming the id when no <c>RelationMessage</c> for it has been
    /// seen this session — see the class doc for why that is the correct outcome rather than a best guess.</summary>
    public PgRelation Get(uint relationId)
        => _byId.TryGetValue(relationId, out var relation)
            ? relation
            : throw new InvalidOperationException(
                $"no RelationMessage seen yet for relation id {relationId} this replication session — " +
                "pgoutput is supposed to send one before the first change event that references a relation; " +
                "either that did not happen or this cache lost the entry, and guessing the row's shape would " +
                "silently corrupt it, so this fails the cycle instead");
}
