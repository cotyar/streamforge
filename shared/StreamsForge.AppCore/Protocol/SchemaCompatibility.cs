using StreamsForge.Abstractions;

namespace StreamsForge.Host.Grpc.Dynamic;

/// <summary>One field whose declared type changed between two generations of a schema.
/// <paramref name="Path"/> uses <see cref="FieldNumberMap"/>'s path syntax (dot-separated raw field
/// names, "payload.user.tier"), so a caller can join a compatibility answer straight onto a field
/// number without a second walk.</summary>
public sealed record SchemaTypeChange(string Path, FieldType From, FieldType To, bool FromArray, bool ToArray)
{
    public override string ToString() =>
        $"{Path}: {Render(From, FromArray)} -> {Render(To, ToArray)}";

    private static string Render(FieldType type, bool isArray) => isArray ? $"repeated {type}" : type.ToString();
}

/// <summary>The diff between two generations of one entity's field list. Ordering inside each list is
/// the OLD schema's declaration order for <see cref="Removed"/>/<see cref="TypeChanged"/> and the NEW
/// schema's for <see cref="Added"/>, so a message built from it reads in the order the author sees.</summary>
public sealed record SchemaCompatibilityResult(
    IReadOnlyList<string> Removed,
    IReadOnlyList<SchemaTypeChange> TypeChanged,
    IReadOnlyList<string> Added)
{
    /// <summary>Nothing a generated client already holds got taken away or re-typed. Additions are
    /// always compatible — that is the entire proto3 bargain, and <see cref="FieldNumberMap.Assign"/>
    /// is what makes it true here (a new path gets a number no old path ever had).</summary>
    public bool IsCompatible => Removed.Count == 0 && TypeChanged.Count == 0;

    /// <summary>Anything changed at all, in either direction — the predicate a SchemaRevision bump asks
    /// (an ADDED field is not breaking but it IS a new shape, and a dependant pinned to the old one is
    /// entitled to know the shape moved).</summary>
    public bool ShapeChanged => Removed.Count > 0 || TypeChanged.Count > 0 || Added.Count > 0;

    /// <summary>One human-readable line per breaking change, for a 409 body or a StaleReason.</summary>
    public IReadOnlyList<string> BreakingReasons =>
    [
        .. Removed.Select(p => $"field '{p}' was removed"),
        .. TypeChanged.Select(c => $"field '{c.Path}' changed type: {Render(c.From, c.FromArray)} -> {Render(c.To, c.ToArray)}"),
    ];

    private static string Render(FieldType type, bool isArray) => isArray ? $"repeated {type}" : type.ToString();
}

/// <summary>
/// Plan 016 wave 2 — "is this new field list compatible with the old one", for the catalog gate.
///
/// <para><b>This REUSES <see cref="FieldNumberMap"/>; it deliberately does not extend it.</b> That map's
/// JSON is persisted in both flavours' registry state (<c>RegistryState.FieldNumberMaps</c> /
/// <c>CatalogState.FieldNumberMaps</c>), so adding a property to it would change a shape already on disk
/// in every deployment. Compatibility is a question you ASK about two field lists, not state you keep,
/// so it belongs in a pure sibling type — hence this file, next to the one it borrows from.</para>
///
/// <para><b>Why the removal half is free.</b> <c>Assign(newFields, existing)</c> already moves a vanished
/// path's number into <c>Reserved</c> — that is how the "never reuse a field number" guarantee works, and
/// it has been doing it since dynamic protobuf shipped. So the set of removed fields is not a new
/// computation, it is the same fact under a different name; the only genuine gap was TYPE CHANGES, which
/// the numbering machinery has no opinion about (a <c>string</c> that becomes a <c>double</c> keeps its
/// number and silently breaks every generated client that decodes it). Keeping the two derivations in
/// agreement is what makes "compatible" mean the same thing to the catalog gate and to the
/// <c>.proto</c> surface, and <c>SchemaCompatibilityAgreementTests</c> asserts exactly that.</para>
///
/// <para><b>One known asymmetry, pinned rather than papered over.</b> When a Json field that had children
/// stops being Json, its whole nested MESSAGE disappears — and <c>Assign</c> never walks a scope that no
/// longer exists, so it reserves nothing for those child paths. This type still reports them as
/// <see cref="SchemaCompatibilityResult.Removed"/>, because they genuinely are gone from the caller's
/// point of view. The divergence is therefore always in the safe direction (the gate is stricter than the
/// wire, never looser), and the agreement test states it as a theorem rather than leaving it to be
/// rediscovered: newly-reserved numbers are always a SUBSET of Removed, and are EQUAL to it whenever
/// every removed path's parent scope survives.</para>
///
/// <para>Pure. No Orleans, Dapr or ASP.NET types; no I/O; no state.</para>
/// </summary>
public static class SchemaCompatibility
{
    /// <summary>Compares two generations of one entity's field list, recursively, using the same walk
    /// <see cref="FieldNumberMap.Assign"/> performs — a Json field descends into its children, anything
    /// else is a leaf — so the two always talk about the same set of paths.</summary>
    public static SchemaCompatibilityResult Compare(
        IReadOnlyList<FieldDef>? oldFields,
        IReadOnlyList<FieldDef>? newFields)
    {
        var before = Flatten(oldFields ?? []);
        var after = Flatten(newFields ?? []);

        var removed = new List<string>();
        var typeChanged = new List<SchemaTypeChange>();
        foreach (var (path, oldDef) in before)
        {
            if (!after.TryGetValue(path, out var newDef))
            {
                removed.Add(path);
                continue;
            }

            if (oldDef.Type != newDef.Type || oldDef.IsArray != newDef.IsArray)
            {
                typeChanged.Add(new SchemaTypeChange(path, oldDef.Type, newDef.Type, oldDef.IsArray, newDef.IsArray));
            }
        }

        var added = after.Keys.Where(p => !before.ContainsKey(p)).ToList();
        return new SchemaCompatibilityResult(removed, typeChanged, added);
    }

    /// <summary>The SchemaRevision predicate in one call: did the FIELD SHAPE move? Deliberately not
    /// "did the definition change" — an <c>eventsPerSecond</c> edit must not invalidate a downstream pin,
    /// and that split is the whole reason there are two counters.</summary>
    public static bool ShapeChanged(IReadOnlyList<FieldDef>? oldFields, IReadOnlyList<FieldDef>? newFields) =>
        Compare(oldFields, newFields).ShapeChanged;

    /// <summary>path -&gt; the field at that path, in declaration order (insertion-ordered because a
    /// Dictionary here is only ever built once and never mutated after — the ordered enumeration callers
    /// rely on comes from <see cref="Flatten"/>'s own recursion order, which IS declaration order).
    /// A duplicate name within one scope resolves last-wins, matching what <c>Assign</c>'s
    /// <c>newActive[path] = number</c> does with the same input.</summary>
    private static Dictionary<string, FieldDef> Flatten(IReadOnlyList<FieldDef> fields)
    {
        var result = new Dictionary<string, FieldDef>(StringComparer.Ordinal);
        Walk("", fields);
        return result;

        void Walk(string scope, IReadOnlyList<FieldDef> defs)
        {
            foreach (var f in defs)
            {
                var path = FieldNumberMap.ChildPath(scope, f.Name);
                result[path] = f;

                // Exactly Assign's descent condition. If these two ever disagree the paths they talk
                // about stop lining up and the agreement test fails, which is the point of it.
                if (f.Type == FieldType.Json && f.Children is { Count: > 0 })
                {
                    Walk(path, f.Children);
                }
            }
        }
    }
}
