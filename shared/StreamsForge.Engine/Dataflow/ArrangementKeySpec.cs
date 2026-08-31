using System.Text;
using StreamsForge.Engine.Runtime;

namespace StreamsForge.Engine.Dataflow;

/// <summary>
/// Plan 003 M3 — canonical key-spec encoding for shared arrangements. An arrangement is identified by
/// (inputName, keySpec, partition): two tables that each join the SAME raw input on the SAME raw field(s)
/// must derive the IDENTICAL <c>keySpec</c> string so they attach to the SAME <see cref="ArrangementKeySpec"/>
/// rather than each building a private index. The canonical form deliberately does NOT include the join
/// alias (aliases are per-query, e.g. "t" vs "x" — irrelevant to whether it's the same physical index) or
/// the table name — only the RAW field name(s) of the input's own schema that make up the key, in order,
/// plus the partition count (since an arrangement's partitions are exchanged 1:1 with its consuming join
/// stage's partitions — see TableDataflowPlan's class doc — so a different partition count is a genuinely
/// different physical index, not a shareable one).
///
/// PUBLIC (unlike <see cref="Runtime.TableKeyEncoding"/>, which is internal): the Host side needs this to
/// (a) compute the same grain-key hash the Engine-side builder computed when it marked an edge arrangeable,
/// and (b) route a raw <see cref="EventRecord"/> to the correct arrangement partition using the exact same
/// hash function <see cref="TableDataflowPlan.PartitionOf"/> uses for a HashPartition edge — see
/// <see cref="PartitionOfRow"/>.
/// </summary>
public static class ArrangementKeySpec
{
    /// <summary>Canonical, human-readable key-spec string for a raw-field key list + partition count. Length-
    /// prefixes each field so no delimiter collision is possible even if a (theoretically reserved-word-
    /// escaped) field name contained the delimiter character itself.</summary>
    public static string Canonicalize(IReadOnlyList<string> fields, int partitionCount)
    {
        var sb = new StringBuilder();
        sb.Append("P=").Append(partitionCount).Append(';');
        foreach (var f in fields)
        {
            sb.Append(f.Length).Append(':').Append(f).Append(';');
        }
        return sb.ToString();
    }

    /// <summary>Short, deterministic (FNV-1a, not GetHashCode — must be stable across processes/restarts;
    /// see <see cref="ExchangeRouter"/>'s own doc for why) hex identity for a canonical key-spec string, for
    /// use inside an ArrangementGrain's grain key ("{inputName}:{keySpecHash}:{partition}") — keeps the
    /// grain key short and collision-free without leaking the (variable-length, human-readable) canonical
    /// form into the key namespace.</summary>
    public static string HashOf(string canonicalKeySpec)
    {
        var bytes = Encoding.UTF8.GetBytes(canonicalKeySpec);
        ulong hash = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }
        return hash.ToString("x16");
    }

    /// <summary>Partition a raw <see cref="EventRecord"/> the SAME way <see cref="TableDataflowPlan.PartitionOf"/>
    /// would for a HashPartition join edge keyed on these raw field(s) — i.e. read the raw field value(s)
    /// directly off the record (no WorkingRow/alias indirection needed since an arrangeable key is, by
    /// construction, a bare reference to the input's own field — see TableDataflowBuilder's arrangeability
    /// check), encode via TableKeyEncoding.EncodeGroupKey, and route via ExchangeRouter — guaranteeing an
    /// arrangement's partition p holds exactly the rows a consuming join stage's own partition p would have
    /// hash-partitioned to itself under the classic private-ingest path.</summary>
    public static int PartitionOfRow(IReadOnlyList<string> fields, EventRecord row, int partitionCount)
    {
        var values = new object?[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            values[i] = row.TryGetValue(fields[i], out var v) ? v : null;
        }
        var canonical = TableKeyEncoding.EncodeGroupKey(values);
        return ExchangeRouter.PartitionOf(canonical, partitionCount);
    }
}
