using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 2 — <see cref="SchemaCompatibility"/>, and above all THE AGREEMENT TEST.
///
/// <para>The compatibility gate and the <c>.proto</c> surface must mean the same thing by "this field is
/// gone". They are computed by two different pieces of code — <see cref="SchemaCompatibility.Compare"/>
/// for the gate, <see cref="FieldNumberMap.Assign"/>'s <c>Reserved</c> growth for the wire — and if they
/// can ever disagree, the feature is worse than nothing: a schema edit would be waved through as
/// compatible while the generated client's decoder loses a field, or refused as breaking while the wire
/// says it is fine. So the relationship is asserted as a theorem over a table of shapes, not spot-checked
/// on one example.</para>
///
/// <para>The theorem has two halves, because there is exactly one shape where the two CANNOT line up and
/// pretending otherwise would be the lie this test exists to prevent:</para>
///
/// <list type="number">
/// <item><b>Always: newly-reserved ⊆ Removed.</b> Everything the wire retires, the gate reports. This is
/// the half that matters for safety — the gate is never LOOSER than the wire.</item>
/// <item><b>Equality whenever every removed path's message scope survives.</b> When a Json field with
/// children stops being Json, its whole nested MESSAGE disappears; <c>Assign</c> never walks a scope that
/// no longer exists, so it reserves nothing for those child paths, while the gate still reports them as
/// removed (they are gone, from the caller's point of view). The divergence is therefore always in the
/// safe direction, and it is stated here rather than left to be rediscovered.</item>
/// </list>
/// </summary>
public class SchemaCompatibilityTests
{
    private static FieldDef Json(string name, List<FieldDef> children) =>
        new(name, FieldType.Json, children);

    // ---------------------------------------------------------------------------------------------
    // The agreement theorem.
    // ---------------------------------------------------------------------------------------------

    public static TheoryData<string, List<FieldDef>, List<FieldDef>> SchemaEvolutions() => new()
    {
        {
            "nothing changed",
            [new("a", FieldType.String), new("b", FieldType.Double)],
            [new("a", FieldType.String), new("b", FieldType.Double)]
        },
        {
            "field added",
            [new("a", FieldType.String)],
            [new("a", FieldType.String), new("b", FieldType.Long)]
        },
        {
            "field removed from the middle",
            [new("a", FieldType.String), new("b", FieldType.Double), new("c", FieldType.Long)],
            [new("a", FieldType.String), new("c", FieldType.Long)]
        },
        {
            "every field removed",
            [new("a", FieldType.String), new("b", FieldType.Double)],
            []
        },
        {
            "renamed = removed + added",
            [new("qty", FieldType.Long)],
            [new("quantity", FieldType.Long)]
        },
        {
            "reordered only",
            [new("a", FieldType.String), new("b", FieldType.Double)],
            [new("b", FieldType.Double), new("a", FieldType.String)]
        },
        {
            "type changed in place",
            [new("a", FieldType.String)],
            [new("a", FieldType.Double)]
        },
        {
            "scalar became repeated",
            [new("a", FieldType.String)],
            [new("a", FieldType.String, null, IsArray: true)]
        },
        {
            "nested child removed, parent survives",
            [Json("payload", [new("u", FieldType.String), new("v", FieldType.Long)])],
            [Json("payload", [new("u", FieldType.String)])]
        },
        {
            "nested child added, parent survives",
            [Json("payload", [new("u", FieldType.String)])],
            [Json("payload", [new("u", FieldType.String), new("v", FieldType.Long)])]
        },
        {
            "the whole nested parent removed",
            [Json("payload", [new("u", FieldType.String), new("v", FieldType.Long)]), new("x", FieldType.Long)],
            [new("x", FieldType.Long)]
        },
        {
            "a Json parent stops being Json — the one divergent shape",
            [Json("payload", [new("u", FieldType.String)])],
            [new("payload", FieldType.String)]
        },
        {
            "three levels deep, leaf removed",
            [Json("a", [Json("b", [new("c", FieldType.String), new("d", FieldType.Long)])])],
            [Json("a", [Json("b", [new("c", FieldType.String)])])]
        },
    };

    [Theory]
    [MemberData(nameof(SchemaEvolutions))]
    public void EveryNumberTheWireRetiresIsAFieldTheGateCallsRemoved(
        string _, List<FieldDef> before, List<FieldDef> after)
    {
        var removed = SchemaCompatibility.Compare(before, after).Removed.ToHashSet(StringComparer.Ordinal);
        var retired = NewlyReservedPaths(before, after);

        // Half 1 of the theorem: the gate is never looser than the wire.
        Assert.Subset(removed, retired);
    }

    [Theory]
    [MemberData(nameof(SchemaEvolutions))]
    public void TheTwoAgreeExactlyWheneverEveryRemovedPathsMessageScopeSurvives(
        string _, List<FieldDef> before, List<FieldDef> after)
    {
        var removed = SchemaCompatibility.Compare(before, after).Removed.ToHashSet(StringComparer.Ordinal);
        var survivingScopes = WalkedScopes(after);

        if (!removed.All(p => survivingScopes.Contains(FieldNumberMap.ParentScope(p))))
        {
            // The documented divergence: a vanished message scope has no numbers to reserve. Half 1
            // above still holds for this case, and that is the half that keeps the gate safe.
            return;
        }

        // Half 2 of the theorem: where they can line up, they line up exactly.
        Assert.Equal(removed.OrderBy(x => x, StringComparer.Ordinal), NewlyReservedPaths(before, after).OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public void TheDivergentShapeIsExactlyTheOneDocumented()
    {
        // Pinned as its own test so "the divergence is in the SAFE direction" is an assertion rather
        // than a claim in a doc comment: payload.u is reported removed by the gate and reserved by
        // nobody, because the message it lived in no longer exists.
        List<FieldDef> before = [Json("payload", [new("u", FieldType.String)])];
        List<FieldDef> after = [new("payload", FieldType.String)];

        var result = SchemaCompatibility.Compare(before, after);

        Assert.Equal(["payload.u"], result.Removed);
        Assert.Empty(NewlyReservedPaths(before, after));
        Assert.False(result.IsCompatible);
    }

    /// <summary>The paths whose numbers <see cref="FieldNumberMap.Assign"/> moved into
    /// <c>Reserved</c> on this exact transition, resolved back through the OLD active map — i.e. "what
    /// the wire says was retired", expressed in the gate's vocabulary so the two are comparable at all.</summary>
    private static HashSet<string> NewlyReservedPaths(List<FieldDef> before, List<FieldDef> after)
    {
        var oldMap = FieldNumberMap.Assign(before);
        var newMap = FieldNumberMap.Assign(after, oldMap);

        var paths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (scope, numbers) in newMap.Reserved)
        {
            var alreadyReserved = oldMap.Reserved.TryGetValue(scope, out var prior) ? prior : [];
            foreach (var number in numbers.Where(n => !alreadyReserved.Contains(n)))
            {
                paths.Add(oldMap.Active
                    .First(kv => FieldNumberMap.ParentScope(kv.Key) == scope && kv.Value == number)
                    .Key);
            }
        }

        return paths;
    }

    /// <summary>The message scopes <c>Assign</c> would walk for a schema — the root plus every Json
    /// field that has children. A removed path whose scope is NOT in here is one the wire has no place
    /// to reserve a number in.</summary>
    private static HashSet<string> WalkedScopes(List<FieldDef> fields)
    {
        var scopes = new HashSet<string>(StringComparer.Ordinal) { "" };
        void Walk(string scope, IReadOnlyList<FieldDef> defs)
        {
            foreach (var f in defs)
            {
                if (f.Type == FieldType.Json && f.Children is { Count: > 0 })
                {
                    var path = FieldNumberMap.ChildPath(scope, f.Name);
                    scopes.Add(path);
                    Walk(path, f.Children);
                }
            }
        }

        Walk("", fields);
        return scopes;
    }

    // ---------------------------------------------------------------------------------------------
    // The half FieldNumberMap has no opinion about: type changes.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void ATypeChangeIsBreakingEvenThoughTheFieldNumberIsUntouched()
    {
        // The entire reason this type exists. `amount` keeps number 1 across the edit, so the numbering
        // machinery sees nothing wrong — while every generated client that decodes it as a string now
        // misreads a double. Silent, on the wire, at runtime.
        List<FieldDef> before = [new("amount", FieldType.String)];
        List<FieldDef> after = [new("amount", FieldType.Double)];

        Assert.Equal(1, FieldNumberMap.Assign(before).Active["amount"]);
        Assert.Equal(1, FieldNumberMap.Assign(after, FieldNumberMap.Assign(before)).Active["amount"]);

        var result = SchemaCompatibility.Compare(before, after);
        Assert.False(result.IsCompatible);
        Assert.Equal("amount", Assert.Single(result.TypeChanged).Path);
        Assert.Contains("amount", Assert.Single(result.BreakingReasons));
    }

    [Fact]
    public void ScalarToRepeatedCountsAsATypeChange()
    {
        var result = SchemaCompatibility.Compare(
            [new("tags", FieldType.String)],
            [new("tags", FieldType.String, null, IsArray: true)]);

        Assert.False(result.IsCompatible);
        Assert.Single(result.TypeChanged);
    }

    [Fact]
    public void AddingAFieldIsCompatibleButStillCountsAsAShapeChange()
    {
        var result = SchemaCompatibility.Compare(
            [new("a", FieldType.String)],
            [new("a", FieldType.String), new("b", FieldType.Long)]);

        Assert.True(result.IsCompatible);   // nothing a client already holds was taken away
        Assert.True(result.ShapeChanged);   // …but a pin against the old shape is entitled to know
        Assert.Equal(["b"], result.Added);
    }

    [Fact]
    public void ReorderingFieldsIsNotAShapeChange()
    {
        // Order is not part of a proto message's identity — the field NUMBERS are, and Assign keeps
        // those stable across a reorder. A SchemaRevision that moved on a cosmetic reorder would be
        // exactly the pin-that-fires-constantly the two-counter split exists to avoid.
        Assert.False(SchemaCompatibility.ShapeChanged(
            [new("a", FieldType.String), new("b", FieldType.Double)],
            [new("b", FieldType.Double), new("a", FieldType.String)]));
    }

    [Fact]
    public void NullAndEmptyFieldListsAreTheSameThing()
    {
        Assert.False(SchemaCompatibility.ShapeChanged(null, []));
        Assert.True(SchemaCompatibility.ShapeChanged(null, [new("a", FieldType.String)]));
        Assert.Equal(["a"], SchemaCompatibility.Compare([new("a", FieldType.String)], null).Removed);
    }
}
