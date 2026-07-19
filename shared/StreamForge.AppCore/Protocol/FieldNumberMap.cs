using StreamForge.Abstractions;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// Persisted field-numbering state for one dynamic entity, giving proto3 field numbers stability
/// across schema edits — the backbone that lets typed clients (generated from a downloaded .proto)
/// keep working after a column is added/removed/reordered, and lets DescriptorFactory emit correct
/// <c>reserved</c> statements for numbers that must never be reused.
///
/// <para><b>Path syntax</b>: dot-separated, using the raw (non-PascalCased) <see cref="FieldDef.Name"/>
/// values from the schema root down to the field in question, e.g. "amount" for a top-level field,
/// "payload.user.tier" for a field three levels deep inside nested Json fields.</para>
///
/// <para><b>Scoping</b>: protobuf field numbers are unique only within their own message, not
/// file-wide. Each Json field that declares <see cref="FieldDef.Children"/> becomes its own nested
/// message with its own 1..N numbering space; we key that space by the *scope*, which is simply the
/// path of the Json field that produced it ("" for the entity's root message, "payload.user" for the
/// message nested at that path).</para>
///
/// <para><b>Persistence shape</b>: rather than a bare {path → number} dict, this type carries BOTH the
/// active numbers and a per-scope set of historically-reserved (retired) numbers. That second half is
/// required for the "never reuse a number" guarantee: once a field is removed its path drops out of
/// <see cref="Active"/>, so a later re-add of the *same* path/name would, with only the active map to
/// consult, be indistinguishable from a field that was never assigned a number and so incorrectly
/// reuse the value it used to have. Carrying the reserved numbers forward (they are never removed,
/// only ever added to) makes the per-scope high-water mark computable from Active ∪ Reserved at every
/// call, regardless of how many times a field has been added/removed/re-added. Callers persist the
/// whole <see cref="FieldNumberMap"/> (e.g. as JSON) and pass it back in as <c>existing</c> next time
/// they regenerate a schema.</para>
/// </summary>
public sealed class FieldNumberMap
{
    /// <summary>path → field number, for fields that exist in the schema this map was computed for.</summary>
    public Dictionary<string, int> Active { get; init; } = [];

    /// <summary>message-scope → historically-assigned numbers no longer in use in that scope (fields
    /// that were removed at some point). Accumulates forever so a number is never reused, even across
    /// repeated add/remove/re-add of the same field name. Sorted ascending, deduplicated.</summary>
    public Dictionary<string, List<int>> Reserved { get; init; } = [];

    public static FieldNumberMap Empty => new();

    /// <summary>The message-scope a path belongs to: everything up to (not including) the last
    /// dot-separated segment, or "" for a top-level (root-message) path.</summary>
    public static string ParentScope(string path)
    {
        var lastDot = path.LastIndexOf('.');
        return lastDot < 0 ? "" : path[..lastDot];
    }

    public static string ChildPath(string scope, string name) => scope.Length == 0 ? name : scope + "." + name;

    /// <summary>
    /// Computes field numbers for <paramref name="fields"/> (recursively, one number-space per
    /// message scope): a path present in <paramref name="existing"/>.Active keeps its number; a new
    /// path gets max(everything ever assigned in its scope) + 1; a path present in
    /// <paramref name="existing"/> but absent from the current schema has its number moved into
    /// <see cref="Reserved"/> for that scope. With no <paramref name="existing"/> map (or an empty
    /// one) every scope numbers its fields sequentially 1..N in declaration order.
    /// Pure function — does not mutate <paramref name="existing"/>. Returns a new map to persist.
    /// </summary>
    public static FieldNumberMap Assign(IReadOnlyList<FieldDef> fields, FieldNumberMap? existing = null)
    {
        existing ??= Empty;

        var newActive = new Dictionary<string, int>();
        // Reserved numbers persist forever: seed from existing, then this call may add more.
        var newReserved = new Dictionary<string, List<int>>();
        foreach (var (scope, nums) in existing.Reserved)
        {
            newReserved[scope] = [.. nums];
        }

        void Walk(string scope, IReadOnlyList<FieldDef> defs)
        {
            // High-water mark for this scope = max over everything ever assigned here: fields
            // still active from a prior generation, plus numbers already reserved in this scope.
            var priorNumbers = existing.Active
                .Where(kv => ParentScope(kv.Key) == scope)
                .Select(kv => kv.Value)
                .Concat(newReserved.TryGetValue(scope, out var reservedSoFar) ? reservedSoFar : []);
            var nextNumber = priorNumbers.Any() ? priorNumbers.Max() + 1 : 1;

            var currentPaths = new HashSet<string>();
            foreach (var f in defs)
            {
                var path = ChildPath(scope, f.Name);
                currentPaths.Add(path);

                var number = existing.Active.TryGetValue(path, out var existingNumber) ? existingNumber : nextNumber++;
                newActive[path] = number;

                if (f.Type == FieldType.Json && f.Children is { Count: > 0 })
                {
                    Walk(path, f.Children);
                }
            }

            // Paths that existed in this scope before but are gone now: retire their numbers.
            var removed = existing.Active
                .Where(kv => ParentScope(kv.Key) == scope && !currentPaths.Contains(kv.Key));
            foreach (var (path, number) in removed)
            {
                if (!newReserved.TryGetValue(scope, out var list))
                {
                    list = [];
                    newReserved[scope] = list;
                }
                if (!list.Contains(number))
                {
                    list.Add(number);
                }
            }
        }

        Walk("", fields);

        foreach (var list in newReserved.Values)
        {
            list.Sort();
        }

        return new FieldNumberMap { Active = newActive, Reserved = newReserved };
    }
}
