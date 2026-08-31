using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

public class FieldNumberMapTests
{
    [Fact]
    public void No_existing_map_numbers_sequentially_in_declaration_order()
    {
        var fields = new List<FieldDef>
        {
            new("c", FieldType.String),
            new("a", FieldType.Double),
            new("b", FieldType.Long),
        };

        var map = FieldNumberMap.Assign(fields);

        Assert.Equal(1, map.Active["c"]);
        Assert.Equal(2, map.Active["a"]);
        Assert.Equal(3, map.Active["b"]);
        Assert.Empty(map.Reserved);
    }

    [Fact]
    public void Adding_a_field_keeps_old_numbers_stable_and_assigns_max_plus_one()
    {
        var v1 = new List<FieldDef> { new("symbol", FieldType.String), new("price", FieldType.Double) };
        var gen1 = FieldNumberMap.Assign(v1);

        var v2 = new List<FieldDef> { new("symbol", FieldType.String), new("price", FieldType.Double), new("qty", FieldType.Long) };
        var gen2 = FieldNumberMap.Assign(v2, gen1);

        Assert.Equal(gen1.Active["symbol"], gen2.Active["symbol"]);
        Assert.Equal(gen1.Active["price"], gen2.Active["price"]);
        Assert.Equal(3, gen2.Active["qty"]); // max(1,2) + 1
    }

    [Fact]
    public void Removing_a_field_reserves_its_number_and_drops_it_from_active()
    {
        var v1 = new List<FieldDef> { new("symbol", FieldType.String), new("price", FieldType.Double), new("qty", FieldType.Long) };
        var gen1 = FieldNumberMap.Assign(v1);
        var qtyNumber = gen1.Active["qty"];

        var v2 = new List<FieldDef> { new("symbol", FieldType.String), new("price", FieldType.Double) };
        var gen2 = FieldNumberMap.Assign(v2, gen1);

        Assert.False(gen2.Active.ContainsKey("qty"));
        Assert.True(gen2.Reserved.TryGetValue("", out var reserved));
        Assert.Contains(qtyNumber, reserved!);
    }

    [Fact]
    public void Re_adding_the_same_field_name_after_removal_gets_a_brand_new_number_never_the_old_one()
    {
        var v1 = new List<FieldDef> { new("symbol", FieldType.String), new("qty", FieldType.Long) };
        var gen1 = FieldNumberMap.Assign(v1);
        var originalQtyNumber = gen1.Active["qty"];

        // v2: qty removed.
        var v2 = new List<FieldDef> { new("symbol", FieldType.String) };
        var gen2 = FieldNumberMap.Assign(v2, gen1);

        // v3: qty re-added (same name).
        var v3 = new List<FieldDef> { new("symbol", FieldType.String), new("qty", FieldType.Long) };
        var gen3 = FieldNumberMap.Assign(v3, gen2);

        var newQtyNumber = gen3.Active["qty"];
        Assert.NotEqual(originalQtyNumber, newQtyNumber);
        // The old number stays reserved forever.
        Assert.Contains(originalQtyNumber, gen3.Reserved[""]);
    }

    [Fact]
    public void Repeated_add_remove_cycles_never_reuse_any_previously_assigned_number()
    {
        var used = new HashSet<int>();
        FieldNumberMap? map = null;

        for (var i = 0; i < 5; i++)
        {
            var withField = FieldNumberMap.Assign([new FieldDef("flag", FieldType.Bool)], map);
            var num = withField.Active["flag"];
            Assert.DoesNotContain(num, used); // never reused across cycles
            used.Add(num);

            map = FieldNumberMap.Assign([], withField); // remove it again
        }
    }

    [Fact]
    public void Nested_json_scope_has_its_own_independent_number_space()
    {
        var fields = new List<FieldDef>
        {
            new("symbol", FieldType.String), // root #1
            new("payload", FieldType.Json, Children: // root #2
            [
                new FieldDef("id", FieldType.String),   // scope "payload" #1
                new FieldDef("tier", FieldType.String),  // scope "payload" #2
            ]),
        };

        var map = FieldNumberMap.Assign(fields);

        Assert.Equal(1, map.Active["symbol"]);
        Assert.Equal(2, map.Active["payload"]);
        Assert.Equal(1, map.Active["payload.id"]);
        Assert.Equal(2, map.Active["payload.tier"]);
    }

    [Fact]
    public void Removing_a_nested_field_reserves_it_in_the_nested_scope_not_the_root_scope()
    {
        var v1 = new List<FieldDef>
        {
            new("payload", FieldType.Json, Children:
            [
                new FieldDef("id", FieldType.String),
                new FieldDef("tier", FieldType.String),
            ]),
        };
        var gen1 = FieldNumberMap.Assign(v1);

        var v2 = new List<FieldDef>
        {
            new("payload", FieldType.Json, Children:
            [
                new FieldDef("id", FieldType.String),
            ]),
        };
        var gen2 = FieldNumberMap.Assign(v2, gen1);

        Assert.True(gen2.Reserved.TryGetValue("payload", out var payloadReserved));
        Assert.Contains(2, payloadReserved!); // "tier" was #2 within the "payload" scope
        Assert.False(gen2.Reserved.ContainsKey("")); // root scope untouched
    }
}
