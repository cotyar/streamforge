using System.Reflection;
using Orleans;
using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 0 — the guard that should have existed before plan 015 added a field.
///
/// <para><b>What went wrong.</b> `TableDefinition.UpdatedBy` was added at <c>[Id(26)]</c>, which
/// <c>RetentionMaxRows</c> had held since plan 011 C2 — further down a 30-property class, past where a
/// "what is the next free number" glance reached. Orleans' generated codec kept the first declaration and
/// dropped the second, so <c>UpdatedBy</c> round-tripped as EMPTY through every grain call and every
/// persisted snapshot. It never worked on the Orleans flavour at all, while Dapr — which serializes state
/// as JSON by property name — was unaffected, so the two flavours silently disagreed. Nothing caught it:
/// the REST-level tests compare in-memory objects or JSON, never the wire.</para>
///
/// <para><b>Why a test and not a convention.</b> "Field numbers are forever" is the repo's rule 5 and it
/// was followed in spirit — somebody did look for the next free number. The failure was that a human
/// reads a class top-down and stops looking, while a serializer does not. This asserts the property
/// mechanically over every <c>[GenerateSerializer]</c> type in the contracts assembly, so the answer no
/// longer depends on how far down the file anyone scrolled.</para>
///
/// <para>In AppCore.Tests deliberately: that project is in BOTH solutions, so a duplicate fails the Dapr
/// build too — and Dapr is precisely the flavour whose own serialization would not have noticed.</para>
/// </summary>
public class ContractFieldNumberTests
{
    public static TheoryData<Type> SerializableContracts()
    {
        var data = new TheoryData<Type>();
        foreach (var type in typeof(TableDefinition).Assembly.GetTypes())
        {
            if (type.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            {
                data.Add(type);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(SerializableContracts))]
    public void NoTwoMembersShareAFieldNumber(Type type)
    {
        // Fields as well as properties: [Id] is legal on both, and a mixed collision is the same bug.
        var numbered = type
            .GetMembers(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Select(m => (Member: m, Id: m.GetCustomAttribute<IdAttribute>()))
            .Where(x => x.Id is not null)
            .ToList();

        var collisions = numbered
            .GroupBy(x => x.Id!.Id)
            .Where(g => g.Count() > 1)
            .Select(g => $"[Id({g.Key})] is on {string.Join(" and ", g.Select(x => x.Member.Name))}")
            .ToList();

        Assert.True(
            collisions.Count == 0,
            $"{type.Name} reuses field numbers, which silently drops members on the wire: "
            + string.Join("; ", collisions));
    }

    /// <summary>The three catalog types are the ones that keep growing, so their numbering is asserted to
    /// be gap-free as well. A gap is not a bug — a retired field's number must stay retired — but an
    /// UNINTENDED gap means somebody skipped a number they thought was taken, which is the same misread
    /// that produces a collision. If a number is ever deliberately retired, add it here with the reason.
    /// </summary>
    [Theory]
    [InlineData(typeof(SourceDefinition))]
    [InlineData(typeof(PipelineDefinition))]
    [InlineData(typeof(TableDefinition))]
    public void TheCatalogTypesNumberContiguouslyFromZero(Type type)
    {
        var ids = type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Select(p => p.GetCustomAttribute<IdAttribute>())
            .Where(a => a is not null)
            .Select(a => (int)a!.Id)
            .OrderBy(n => n)
            .ToList();

        Assert.NotEmpty(ids);
        Assert.Equal(Enumerable.Range(0, ids.Count).ToList(), ids);
    }
}
