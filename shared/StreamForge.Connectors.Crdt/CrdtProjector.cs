using System.Globalization;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Ycs;

namespace StreamForge.Connectors.Crdt;

/// <summary>
/// Plan 020 wave B — turns a <see cref="YDoc"/> into rows. This is "the projection is the dangerous
/// part" section of the plan made concrete; every choice below answers one of that section's bullets,
/// and every choice is deliberately visible here rather than folded silently into the algorithm.
///
/// <para><b>Root shape</b> (see <see cref="CrdtSourceConfig"/>'s own class doc for the full contract):
/// <see cref="CrdtSourceConfig.RootMap"/> is a <c>YMap</c> whose keys are entity keys; each value is
/// either a nested <c>YMap</c> (the entity's attributes) or a scalar (a single-column entity). A key
/// simply absent from the root map — because it was never written, or because Yjs deleted it — never
/// enumerates, which is how a <c>YMap.Delete</c> reaches <see cref="Diff"/> as a removed key with no
/// extra bookkeeping on this class's part.</para>
///
/// <para><b>Reserved-column defense.</b> <c>_ts</c>, <c>_source</c>, <c>_weight</c>, <c>_op</c> and
/// <c>_retract</c> are stamped by the platform (<c>EventRecord.Timestamp</c>/<c>.Source</c>,
/// <c>SinkStepGuard</c>'s row weight, <c>IngressRowAcceptance.RetractField</c>, and this class's own
/// tombstone op letter — see below). All five are in <see cref="ReservedColumns"/>; this sentence
/// listed only the first four until plan 020 wave E's documentation pass read it against the set. A
/// DOCUMENT-content key spelled like one of these is renamed, never passed through: strip its leading
/// underscore(s) and prepend <c>doc_</c>, so <c>_ts</c> becomes <c>doc_ts</c>, <c>_weight</c> becomes
/// <c>doc_weight</c>. Chosen over silently dropping the key (an edge's genuine field disappearing with
/// no trace is worse than it appearing under a slightly different name) and over refusing the whole
/// entity (D7's idempotence has nothing to do with a naming collision, and a twin with one oddly-named
/// column is still a usable twin). A diagnostic records exactly what was renamed to what. The rename
/// applies ONLY to names read from document CONTENT — never to <see cref="CrdtSourceConfig.KeyField"/>
/// or a scalar entity's single declared column name, both of which are operator-chosen schema, not
/// attacker-controlled document content.</para>
///
/// <para><b>Nested-key join scheme.</b> A nested <c>YMap</c>/<c>YArray</c> attribute flattens
/// recursively (plan: "v1 flattens"); the column name is the dotted path from the entity's own
/// attributes down to the leaf — <c>address</c> containing <c>city</c> becomes column
/// <c>address.city</c>; an array element's path segment is its integer index, so
/// <c>tags: ["a","b"]</c> becomes columns <c>tags.0</c>/<c>tags.1</c>. This is the accepted cost the
/// plan names: a nested element's identity (which array slot a given value used to occupy, across
/// edits) is not preserved — only ITS CURRENT dotted position is. Every dotted column must still be
/// declared in <c>fields</c> like any other column; the projector never invents schema.</para>
///
/// <para><b><c>Y.Text</c> loses formatting.</b> A <c>YText</c> value — at any depth — projects as
/// <see cref="object.ToString"/>'s plain string. Rich-text runs/attributes are discarded. This is
/// explicitly out of scope for v1 (the plan's own words); this class does not attempt to half-support
/// it (e.g. as a serialized run list) because that would be a schema this class invented rather than
/// one the plan called for.</para>
///
/// <para><b>Type drift and coercion policy.</b> Every leaf value is coerced against its declared
/// <see cref="FieldDef.Type"/> through <see cref="FieldValueCoercion.TryCoerce"/> — the one canonical
/// conversion implementation every inbound path in this codebase shares (do not re-derive the rules
/// here). On a coercion failure this class matches
/// <c>StreamForge.AppCore.Connectors.ConnectorRowCoercion</c>'s <b>Null</b> policy specifically (the
/// pre-009 lenient default, and the default of <c>SourceDefinition.OnCoercionFailure</c>): the failing
/// field is set to <c>null</c> and a diagnostic is recorded, but the row is kept. <see cref="Flatten"/>
/// has no policy parameter to consult (its signature is pinned), so there is no way to honor
/// <c>DropRow</c>/<c>RejectBatch</c> here even if a caller wanted them — matching the platform's own
/// default is the least surprising choice available, and is stated here rather than assumed.</para>
///
/// <para><b>Undeclared keys are dropped, not invented.</b> A document key — after any reserved-column
/// rename — that has no matching <see cref="FieldDef.Name"/> in <c>fields</c> is a diagnostic and is
/// left out of the row entirely; this class does not guess a type for it.</para>
///
/// <para><b>Never throws.</b> A document is untrusted input written by somebody else's edge. Each
/// entity is projected inside its own try/catch so one malformed entity cannot take the rest of the
/// document down with it; an unexpected failure becomes a diagnostic and that entity is skipped.</para>
/// </summary>
public static class CrdtProjector
{
    /// <summary>The platform's reserved row columns (see this class's own doc comment). Duplicated as
    /// string literals rather than referencing <c>StreamForge.Engine.PublicApi</c> or
    /// <c>StreamForge.AppCore.Sinks.SinkStepGuard</c> directly: this project is deliberately
    /// runtime-agnostic and Engine-free (matching <c>StreamForge.Connectors.Fix</c>'s own
    /// <c>FixRowMapper.ReservedRowColumns</c>, which duplicates the identical three names for the
    /// identical reason — see that class's doc comment).</summary>
    private static readonly HashSet<string> ReservedColumns =
        new(StringComparer.Ordinal) { "_ts", "_source", "_weight", "_op", "_retract" };

    /// <summary>
    /// The tombstone convention <see cref="Diff"/> speaks — deliberately the SAME spelling
    /// <c>StreamForge.Connectors.Database.CdcStamp</c> uses (<c>_op</c> ∈ {"c","u","d"}, <c>_weight</c>
    /// ∈ {+1,-1}), so one piece of downstream SQL covers a CDC feed and a CRDT document alike (plan 020,
    /// <see cref="CrdtSourceConfig"/>'s own class doc). Duplicated as literals here rather than taking a
    /// project reference on <c>StreamForge.Connectors.Database</c> — that project pulls a live-database
    /// driver stack (Npgsql, Microsoft.Data.SqlClient) this project has no business depending on for
    /// four string constants, and referencing it would also point the dependency arrow the wrong way (a
    /// database connector has no reason to know a CRDT document exists). <c>CrdtProjectorTests</c> pins
    /// these literals directly against what <c>CdcStamp</c> actually declares today, so a future edit to
    /// either file that breaks the spelling match fails a test instead of drifting silently.</summary>
    private const string OpColumn = "_op";
    private const string WeightColumn = "_weight";

    /// <summary>
    /// The platform's ONE real key retraction (<c>StreamForge.AppCore.Ingest.IngressRowAcceptance</c>'s
    /// <c>RetractField</c>, honoured by the Engine's <c>TableIngestOp</c>). Stamped on a tombstone in
    /// ADDITION to <see cref="OpColumn"/>/<see cref="WeightColumn"/>, and the reason is a live finding,
    /// not a precaution.
    ///
    /// <para><c>_weight = -1</c> on an inbound row is <b>just a column</b>. A database sink reads it and
    /// writes a <c>DELETE</c>; a TABLE does not — the Engine's Z-set weights are computed FROM table SQL,
    /// not carried in from ingress, so every source event is admitted as a <c>+1</c> assert. Verified
    /// live during wave B-2: deleting a document key left the table holding BOTH the original row and a
    /// second, all-null row for the same key, each at weight 1. <c>CdcEnvelope</c>'s class doc states
    /// this same limit for Debezium deletes in as many words — a CDC delete has always had it too.</para>
    ///
    /// <para><c>_retract</c> is the mechanism that does work: <c>TableIngestOp</c> honours it
    /// unconditionally, for every table shape, overriding the assert with a genuine <c>-1</c>. A
    /// <c>LATEST BY</c> table receiving one actually frees the key; other shapes fall to
    /// <c>TableReduceOp</c>'s unmatched-retraction handling, which its own doc pins as never-corrupt,
    /// at-worst-under-report. That is why all three are stamped rather than picking one: <c>_op</c> for
    /// SQL to read, <c>_weight</c> for a sink to act on, <c>_retract</c> for a table to converge.</para>
    /// </summary>
    private const string RetractColumn = "_retract";
    private const string OpCreate = "c";
    private const string OpUpdate = "u";
    private const string OpDelete = "d";

    /// <summary>Flatten the whole document into entity-key -&gt; row. See this class's doc comment for
    /// the reserved-column rename scheme, the nested-key join scheme, the <c>YText</c> handling, the
    /// coercion policy, and the undeclared-key drop rule — every one of those is exercised here.</summary>
    public static Dictionary<string, Dictionary<string, object?>> Flatten(
        YDoc doc,
        CrdtSourceConfig config,
        IReadOnlyList<FieldDef> fields,
        List<string> diagnostics)
    {
        var result = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);

        var declaredByName = new Dictionary<string, FieldDef>(StringComparer.Ordinal);
        foreach (var f in fields)
        {
            declaredByName[f.Name] = f;
        }

        var rootMapName = string.IsNullOrEmpty(config.RootMap) ? "root" : config.RootMap;
        var root = doc.GetMap(rootMapName);

        foreach (var entry in root)
        {
            var entityKey = entry.Key;

            try
            {
                var row = new Dictionary<string, object?>(StringComparer.Ordinal);

                if (entry.Value is YMap attributes)
                {
                    FlattenMap(attributes, "", entityKey, declaredByName, row, diagnostics);
                }
                else if (!TryProjectScalarEntity(entry.Value, entityKey, config, fields, row, diagnostics))
                {
                    // Diagnostic already recorded by TryProjectScalarEntity; the plan is explicit that
                    // an ambiguous scalar entity is skipped, never guessed.
                    continue;
                }

                // Authoritative and LAST: the entity's own identity always wins over anything the
                // document's content happened to carry under the same column name (e.g. a nested
                // attribute that also happens to be named "id").
                row[config.KeyField] = entityKey;
                result[entityKey] = row;
            }
            catch (Exception ex)
            {
                diagnostics.Add(
                    $"entity '{entityKey}': projection failed unexpectedly ({ex.GetType().Name}: {ex.Message}) — entity skipped");
            }
        }

        return result;
    }

    /// <summary>Rows to emit for what changed between two flattened states — plan 020 D7's idempotence
    /// property lives here: a key present in both states with an identical row emits NOTHING, so
    /// re-merging an already-delivered update batch produces zero downstream deltas.</summary>
    public static List<Dictionary<string, object?>> Diff(
        IReadOnlyDictionary<string, Dictionary<string, object?>> before,
        IReadOnlyDictionary<string, Dictionary<string, object?>> after,
        CrdtSourceConfig config)
    {
        var result = new List<Dictionary<string, object?>>();

        foreach (var (key, afterRow) in after)
        {
            if (!before.TryGetValue(key, out var beforeRow))
            {
                result.Add(StampedCopy(afterRow, OpCreate, 1L));
                continue;
            }

            if (!RowsEqual(beforeRow, afterRow))
            {
                result.Add(StampedCopy(afterRow, OpUpdate, 1L));
            }

            // else: identical in both states -> emit nothing (D7).
        }

        foreach (var key in before.Keys)
        {
            if (after.ContainsKey(key))
            {
                continue;
            }

            var beforeRow = before[key];
            var tombstone = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                // "Only" the key field, per the plan: a deletion carries no stale attribute values
                // downstream, exactly like a CdcStamp-produced DELETE row.
                [config.KeyField] = beforeRow.TryGetValue(config.KeyField, out var kv) ? kv : key,
                [OpColumn] = OpDelete,
                [WeightColumn] = -1L,
                // See RetractColumn's doc: without this the tombstone reaches a table as one more +1
                // assert and the twin silently accumulates instead of converging.
                [RetractColumn] = true,
            };
            result.Add(tombstone);
        }

        return result;
    }

    private static Dictionary<string, object?> StampedCopy(Dictionary<string, object?> row, string op, long weight)
    {
        var copy = new Dictionary<string, object?>(row, StringComparer.Ordinal)
        {
            [OpColumn] = op,
            [WeightColumn] = weight,
        };
        return copy;
    }

    private static bool RowsEqual(Dictionary<string, object?> a, Dictionary<string, object?> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach (var (key, value) in a)
        {
            if (!b.TryGetValue(key, out var other) || !Equals(value, other))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The scalar-entity case: the root map's value for this key is not a <c>YMap</c> (it is a
    /// scalar, or — an edge case the plan does not explicitly name — a top-level <c>YText</c>/<c>YArray</c>
    /// directly under the entity key rather than nested inside one). Projects to exactly one column,
    /// named by the single declared non-key field; more or fewer than one candidate is a diagnostic and
    /// the entity is skipped entirely, never guessed.</summary>
    private static bool TryProjectScalarEntity(
        object? value,
        string entityKey,
        CrdtSourceConfig config,
        IReadOnlyList<FieldDef> fields,
        Dictionary<string, object?> row,
        List<string> diagnostics)
    {
        var candidates = fields.Where(f => !string.Equals(f.Name, config.KeyField, StringComparison.Ordinal)).ToList();
        if (candidates.Count != 1)
        {
            diagnostics.Add(
                $"entity '{entityKey}': scalar value but {candidates.Count} non-key field(s) declared "
                + "(expected exactly 1 to project it into) — entity skipped");
            return false;
        }

        var field = candidates[0];
        // Callers only reach this method when the root map's value is NOT a YMap (Flatten already
        // dispatches that case). A YText still needs its plain-string projection; a YArray directly
        // under an entity key is a shape the plan does not name (it only describes "a nested YMap, or
        // a scalar") — rather than invent flattening rules for it, it is handed to FieldValueCoercion
        // like any other value and fails that coercion honestly (diagnostic, field set to null), which
        // is the same "never throws, never guesses" behavior every other unsupported shape gets here.
        var leaf = value is YText text ? text.ToString() : value;

        // A shared Y-type that is NOT a YText reaching here means the root map's value is a bare YArray
        // / YXml* sitting directly under the entity key — a shape plan 020 never names (it describes "a
        // nested YMap, or a scalar"). It must be refused HERE and not handed to FieldValueCoercion,
        // which coerces anything at all into a String field via ToString(): a bare YArray was landing in
        // the row as the literal string "Ycs.YArray", with no diagnostic. Data that looks like data and
        // is not is precisely what this class's "the projection is the dangerous part" doc exists to
        // prevent, so the shape is refused loudly instead of flattened by a rule nobody designed.
        if (leaf is AbstractType)
        {
            diagnostics.Add(
                $"entity '{entityKey}': value is a bare {leaf.GetType().Name} directly under the entity "
                + $"key, which has no defined projection (attributes belong in a nested YMap) — column "
                + $"'{field.Name}' set to null");
            row[field.Name] = null;
            return true;
        }

        if (leaf is null)
        {
            row[field.Name] = null;
        }
        else if (!FieldValueCoercion.TryCoerce(field.Type, leaf, out var coerced))
        {
            diagnostics.Add(
                $"entity '{entityKey}': scalar value of type {leaf.GetType().Name} could not be coerced "
                + $"to {field.Type} for column '{field.Name}' — field set to null");
            row[field.Name] = null;
        }
        else
        {
            row[field.Name] = coerced;
        }

        return true;
    }

    private static void FlattenMap(
        YMap map,
        string prefix,
        string entityKey,
        IReadOnlyDictionary<string, FieldDef> declaredByName,
        Dictionary<string, object?> row,
        List<string> diagnostics)
    {
        foreach (var entry in map)
        {
            var path = Join(prefix, entry.Key);
            FlattenValue(entry.Value, path, entityKey, declaredByName, row, diagnostics);
        }
    }

    private static void FlattenArray(
        YArrayBase array,
        string prefix,
        string entityKey,
        IReadOnlyDictionary<string, FieldDef> declaredByName,
        Dictionary<string, object?> row,
        List<string> diagnostics)
    {
        var index = 0;
        foreach (var element in array)
        {
            var path = Join(prefix, index.ToString(CultureInfo.InvariantCulture));
            FlattenValue(element, path, entityKey, declaredByName, row, diagnostics);
            index++;
        }
    }

    private static void FlattenValue(
        object? value,
        string path,
        string entityKey,
        IReadOnlyDictionary<string, FieldDef> declaredByName,
        Dictionary<string, object?> row,
        List<string> diagnostics)
    {
        switch (value)
        {
            case YMap nestedMap:
                FlattenMap(nestedMap, path, entityKey, declaredByName, row, diagnostics);
                break;
            case YText text:
                // YText derives from YArrayBase, so this arm MUST come before the YArrayBase arm below
                // — a plain string projection, never treated as an array of characters.
                EmitLeaf(path, text.ToString(), entityKey, declaredByName, row, diagnostics);
                break;
            case YArrayBase nestedArray:
                FlattenArray(nestedArray, path, entityKey, declaredByName, row, diagnostics);
                break;
            case AbstractType unknownSharedType:
                // Every Y-type this projector knows how to walk is handled above. One that is not — a
                // type a future Ycs bump introduces — must NOT fall through to EmitLeaf, because
                // FieldValueCoercion will happily ToString() it into a String column and the row will
                // carry the class name as if it were a value. Loud and null beats plausible and wrong.
                diagnostics.Add(
                    $"entity '{entityKey}': column '{path}' holds a {unknownSharedType.GetType().Name}, "
                    + "a shared type this projector has no projection rule for — field set to null");
                EmitLeaf(path, null, entityKey, declaredByName, row, diagnostics);
                break;
            default:
                EmitLeaf(path, value, entityKey, declaredByName, row, diagnostics);
                break;
        }
    }

    private static void EmitLeaf(
        string rawPath,
        object? value,
        string entityKey,
        IReadOnlyDictionary<string, FieldDef> declaredByName,
        Dictionary<string, object?> row,
        List<string> diagnostics)
    {
        var columnName = ApplyReservedRename(rawPath, entityKey, diagnostics);

        if (!declaredByName.TryGetValue(columnName, out var field))
        {
            diagnostics.Add(
                $"entity '{entityKey}': document key '{columnName}' is not declared in the source's "
                + "fields — dropped (the projector never invents schema)");
            return;
        }

        if (value is null)
        {
            row[columnName] = null;
            return;
        }

        if (!FieldValueCoercion.TryCoerce(field.Type, value, out var coerced))
        {
            diagnostics.Add(
                $"entity '{entityKey}': document key '{columnName}' value of type {value.GetType().Name} "
                + $"could not be coerced to {field.Type} — field set to null (Null coercion-failure policy)");
            row[columnName] = null;
            return;
        }

        row[columnName] = coerced;
    }

    /// <summary>See this class's doc comment's "Reserved-column defense" paragraph for the scheme and
    /// why it was chosen over dropping the key or refusing the entity.</summary>
    private static string ApplyReservedRename(string name, string entityKey, List<string> diagnostics)
    {
        if (!ReservedColumns.Contains(name))
        {
            return name;
        }

        var renamed = "doc_" + name.TrimStart('_');
        diagnostics.Add(
            $"entity '{entityKey}': document key '{name}' collides with a platform-reserved column and "
            + $"was renamed to '{renamed}'");
        return renamed;
    }

    private static string Join(string prefix, string segment) =>
        prefix.Length == 0 ? segment : prefix + "." + segment;
}
