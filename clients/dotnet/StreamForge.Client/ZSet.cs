using System.Globalization;
using System.Text;

namespace StreamForge.Client;

/// <summary>One (row, weight) delta, or one row of a snapshot read (weight already summed
/// server-side there). A row entering a table is +1, leaving is -1; a batch can carry both.</summary>
public readonly record struct RowDelta(IReadOnlyDictionary<string, object?> Row, long Weight);

/// <summary>Stable content-based identity for rows, ported from clients/python/src/streamforge/_zset.py's
/// canonical_key/group_key_of. Not real JSON (no need to be byte-identical to any other language's
/// canonicalization -- this identity is only ever compared against itself, within one ZSet), just
/// deterministic: same content -&gt; same key, distinct content -&gt; (with overwhelming probability)
/// distinct key, independent of the row's Dictionary enumeration order.</summary>
internal static class RowIdentity
{
    public static string CanonicalKey(IReadOnlyDictionary<string, object?> row)
    {
        var sb = new StringBuilder();
        WriteCanonicalRow(sb, row);
        return sb.ToString();
    }

    /// <summary>The row's logical-identity ("group") key, or null when supersession does not
    /// apply. <paramref name="keyFields"/> is a real (possibly empty) list: <c>[]</c> is a global
    /// aggregate -- exactly one row, one constant group <c>"*"</c>. <c>null</c> means the table's
    /// key is unknown and none was given -- deliberately NOT "guess the first column" (see the
    /// design doc's "Key fields" section); this client falls back to whole-row identity instead,
    /// i.e. CanonicalKey alone, by never superseding at all. That is the safe failure mode.</summary>
    public static string? GroupKeyOf(IReadOnlyDictionary<string, object?> row, IReadOnlyList<string>? keyFields)
    {
        if (keyFields is null) return null;
        if (keyFields.Count == 0) return "*";

        var sb = new StringBuilder();
        for (var i = 0; i < keyFields.Count; i++)
        {
            if (i > 0) sb.Append('|');
            var field = keyFields[i];
            sb.Append(field).Append('=');
            if (row.TryGetValue(field, out var value)) WriteCanonicalValue(sb, value);
            else sb.Append("undefined");
        }
        return sb.ToString();
    }

    private static void WriteCanonicalRow(StringBuilder sb, IReadOnlyDictionary<string, object?> row)
    {
        sb.Append('[');
        var first = true;
        foreach (var key in row.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            if (!first) sb.Append(',');
            first = false;
            sb.Append('[');
            WriteJsonString(sb, key);
            sb.Append(',');
            WriteCanonicalValue(sb, row[key]);
            sb.Append(']');
        }
        sb.Append(']');
    }

    private static void WriteCanonicalValue(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case null:
                sb.Append("null");
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case string s:
                WriteJsonString(sb, s);
                break;
            case long l:
                sb.Append(l.ToString(CultureInfo.InvariantCulture));
                break;
            case int i:
                sb.Append(i.ToString(CultureInfo.InvariantCulture));
                break;
            case double d:
                sb.Append(d.ToString("R", CultureInfo.InvariantCulture));
                break;
            case IReadOnlyDictionary<string, object?> nested:
                WriteCanonicalRow(sb, nested);
                break;
            case System.Collections.IEnumerable list:
                sb.Append('[');
                var first = true;
                foreach (var item in list)
                {
                    if (!first) sb.Append(',');
                    first = false;
                    WriteCanonicalValue(sb, item);
                }
                sb.Append(']');
                break;
            default:
                WriteJsonString(sb, value.ToString() ?? "");
                break;
        }
    }

    private static void WriteJsonString(StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (var ch in s)
        {
            switch (ch)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (ch < 0x20) sb.Append("\\u").Append(((int)ch).ToString("x4"));
                    else sb.Append(ch);
                    break;
            }
        }
        sb.Append('"');
    }
}

/// <summary>
/// The Z-set reducer -- ported literally from clients/python/src/streamforge/_zset.py, itself a
/// port of lib/streamforge/live-table.ts (otc-terms) / web/src/hooks/useTableRows.ts. Pure state,
/// no I/O: this class never touches a socket, which is what makes it testable against the shared
/// conformance fixture (clients/conformance/zset-cases.json) with handcrafted data alone.
///
/// Hazards, and why the code below looks the way it does:
///
/// - A row's identity for retract/assert matching is the WHOLE row's content, not any one column
///   (<see cref="RowIdentity.CanonicalKey"/>). Weight is SUMMED per canonical identity across
///   every delta seen; a summed weight &lt;= 0 removes the row outright rather than going
///   negative. This is what makes retract-then-assert and assert-then-retract (arrival order is
///   not guaranteed) both converge to the same state.
///
/// - A logical key ("group", <c>keyFields</c>) can SUPERSEDE: two different canonical rows
///   sharing the same keyFields values are the same logical entity at different times (an updated
///   MTM tick, a LATEST BY row). When a new row for a group is asserted, the group's PREVIOUS
///   canonical row is deleted even though its own weight was never explicitly retracted on the
///   wire -- the retraction is implied by the new assert superseding it.
///
/// - Subscribe races the initial snapshot: deltas can arrive before, during or after the snapshot
///   read lands. <see cref="LiveTable"/> buffers everything until the snapshot is in hand, seeds
///   state from it via <see cref="Seed"/> (dropping weight&lt;=0 rows and resolving any
///   supersession the snapshot itself straddled -- a snapshot read mid-update can carry both the
///   old and new row of a group), then replays the buffered batches -- except ones the snapshot
///   has ALREADY reflected. There is no shared sequence counter between the snapshot read and the
///   delta stream (a batch's <c>seq</c> is a per-subscription counter on a completely different
///   scale -- measured ~860 vs ~15,000 at the same instant in useTableRows.ts), so "already
///   reflected" cannot be seq-based. Instead <see cref="AlreadyReflected"/> is a CONTENT
///   heuristic -- a buffered batch is skipped only when EVERY one of its retractions targets a
///   row the snapshot does not contain (i.e. the snapshot already dropped it). Replaying it
///   anyway would double-apply a retraction the snapshot already reflects, which for a plain
///   reduce-by-weight is harmless, but for a LATEST BY group it can delete the WRONG (newer) row
///   out from under the group index. Wishlist #20 (a shared epoch on both the snapshot and the
///   delta stream) is the fix that would make this exact instead of a heuristic.
/// </summary>
public sealed class ZSet
{
    private readonly IReadOnlyList<string>? _keyFields;
    private readonly Dictionary<string, (IReadOnlyDictionary<string, object?> Row, long Weight)> _map = new();
    private readonly Dictionary<string, string> _groupIndex = new();

    public ZSet(IReadOnlyList<string>? keyFields) => _keyFields = keyFields;

    /// <summary>Current live rows. Collapses to one row per group when <c>keyFields</c> is known --
    /// a defensive step mirroring live-table.ts's flushToState: <see cref="Apply"/>/<see cref="Seed"/>
    /// already maintain the one-canonical-key-per-group invariant, but a consumer must never see a
    /// group surface twice regardless.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows()
    {
        if (_keyFields is null) return _map.Values.Select(v => v.Row).ToList();

        var byGroup = new Dictionary<string, IReadOnlyDictionary<string, object?>>();
        foreach (var (row, _) in _map.Values)
        {
            var groupKey = RowIdentity.GroupKeyOf(row, _keyFields) ?? RowIdentity.CanonicalKey(row);
            byGroup[groupKey] = row;
        }
        return byGroup.Values.ToList();
    }

    /// <summary>Apply one batch of deltas in place. Returns the canonical keys newly asserted
    /// (summed weight &gt; 0 after this call) -- for on-change/flash-style tracking.</summary>
    public IReadOnlyList<string> Apply(IReadOnlyList<RowDelta> deltas)
    {
        var touched = new List<string>();
        foreach (var (row, weight) in deltas)
        {
            var key = RowIdentity.CanonicalKey(row);
            var groupKey = RowIdentity.GroupKeyOf(row, _keyFields);
            var prevWeight = _map.TryGetValue(key, out var existing) ? existing.Weight : 0;
            var nextWeight = prevWeight + weight;

            if (nextWeight <= 0)
            {
                _map.Remove(key);
                if (groupKey is not null && _groupIndex.TryGetValue(groupKey, out var gk) && gk == key)
                    _groupIndex.Remove(groupKey);
            }
            else
            {
                if (groupKey is not null)
                {
                    if (_groupIndex.TryGetValue(groupKey, out var staleKey) && staleKey != key)
                        _map.Remove(staleKey);
                    _groupIndex[groupKey] = key;
                }
                _map[key] = (row, nextWeight);
                touched.Add(key);
            }
        }
        return touched;
    }

    /// <summary>Reset and seed from a snapshot read (<c>GET /rows</c> or <c>TableService.Rows</c>).
    /// Mirrors <see cref="Apply"/>'s rules: a weight&lt;=0 row is not part of the snapshot at all,
    /// and a group keeps only its newest row -- a snapshot read mid-update can carry both sides of
    /// a supersession.</summary>
    public void Seed(IReadOnlyList<RowDelta> snapshotRows)
    {
        _map.Clear();
        _groupIndex.Clear();
        foreach (var (row, weight) in snapshotRows)
        {
            if (weight <= 0) continue;
            var key = RowIdentity.CanonicalKey(row);
            var groupKey = RowIdentity.GroupKeyOf(row, _keyFields);
            if (groupKey is not null)
            {
                if (_groupIndex.TryGetValue(groupKey, out var staleKey) && staleKey != key)
                    _map.Remove(staleKey);
                _groupIndex[groupKey] = key;
            }
            _map[key] = (row, weight);
        }
    }

    /// <summary>True when a BUFFERED batch's effect is already visible in the (just-seeded)
    /// current state -- see the class doc's "subscribe races the snapshot" hazard. A batch with no
    /// retractions is never considered reflected (an assert-only batch is always safe, and
    /// possibly necessary, to replay).</summary>
    public bool AlreadyReflected(IReadOnlyList<RowDelta> deltas)
    {
        var hasRetraction = false;
        foreach (var (row, weight) in deltas)
        {
            if (weight >= 0) continue;
            hasRetraction = true;
            var key = RowIdentity.CanonicalKey(row);
            var currentWeight = _map.TryGetValue(key, out var existing) ? existing.Weight : 0;
            if (currentWeight > 0) return false;
        }
        return hasRetraction;
    }
}
