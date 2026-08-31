using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 C2: <see cref="ConnectorRowCoercion.Apply"/> — declared-type coercion over
/// already-extracted connector rows, per <see cref="CoercionFailurePolicy"/>. Uses
/// <see cref="FieldValueCoercion"/>'s own documented accept/reject rules (see
/// orleans/tests/StreamsForge.Engine.Tests/FieldValueCoercionTests.cs) as ground truth for what
/// counts as a failure.</summary>
public class ConnectorRowCoercionTests
{
    private static readonly List<FieldDef> ScalarFields =
    [
        new("id", FieldType.String),
        new("price", FieldType.Double),
    ];

    // ------------------------------------------------------------------
    // Absent / null fields — never touched, never counted.
    // ------------------------------------------------------------------

    [Fact]
    public void Apply_A_field_absent_from_the_row_is_left_alone()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1" } }; // no "price" key at all

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(0, result.FailureCount);
        Assert.False(result.BatchRejected);
        Assert.Single(result.Rows);
        Assert.False(result.Rows[0].ContainsKey("price"));
    }

    [Fact]
    public void Apply_A_null_field_value_is_left_alone()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1", ["price"] = null } };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(0, result.FailureCount);
        Assert.Null(result.Rows[0]["price"]);
    }

    [Fact]
    public void Apply_A_value_that_coerces_cleanly_is_replaced_with_the_coerced_value()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1", ["price"] = "3.5" } };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(0, result.FailureCount);
        Assert.Equal(3.5, result.Rows[0]["price"]);
    }

    [Fact]
    public void Apply_Undeclared_keys_are_never_touched()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1", ["_source"] = "s", ["_ts"] = 123L } };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal("s", result.Rows[0]["_source"]);
        Assert.Equal(123L, result.Rows[0]["_ts"]);
    }

    // ------------------------------------------------------------------
    // Null policy (default, pre-009 lenient behavior).
    // ------------------------------------------------------------------

    [Fact]
    public void Null_policy_Nulls_the_failing_field_and_keeps_the_rest_of_the_row()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1", ["price"] = "not-a-number" } };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(1, result.FailureCount);
        Assert.False(result.BatchRejected);
        Assert.Single(result.Rows);
        Assert.Null(result.Rows[0]["price"]);
        Assert.Equal("a1", result.Rows[0]["id"]); // rest of the row survives
    }

    [Fact]
    public void Null_policy_Counts_a_failure_per_field_not_per_row()
    {
        var manyFields = new List<FieldDef> { new("a", FieldType.Double), new("b", FieldType.Double) };
        var rows = new List<Dictionary<string, object?>> { new() { ["a"] = "bad", ["b"] = "also-bad" } };

        var result = ConnectorRowCoercion.Apply(manyFields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(2, result.FailureCount);
        Assert.Single(result.Rows);
    }

    // ------------------------------------------------------------------
    // DropRow policy.
    // ------------------------------------------------------------------

    [Fact]
    public void DropRow_policy_Drops_only_the_failing_row_keeping_good_rows()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["id"] = "a1", ["price"] = "not-a-number" },
            new() { ["id"] = "a2", ["price"] = 200.0 },
        };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.DropRow);

        Assert.Equal(1, result.FailureCount);
        Assert.False(result.BatchRejected);
        Assert.Single(result.Rows);
        Assert.Equal("a2", result.Rows[0]["id"]);
    }

    [Fact]
    public void DropRow_policy_A_clean_row_is_unaffected()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["id"] = "a1", ["price"] = 1.5 } };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.DropRow);

        Assert.Equal(0, result.FailureCount);
        Assert.Single(result.Rows);
    }

    // ------------------------------------------------------------------
    // RejectBatch policy.
    // ------------------------------------------------------------------

    [Fact]
    public void RejectBatch_policy_Rejects_the_whole_batch_on_the_first_failure()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["id"] = "a1", ["price"] = 100.0 }, // clean
            new() { ["id"] = "a2", ["price"] = "bad" }, // fails
            new() { ["id"] = "a3", ["price"] = 300.0 }, // never examined — batch already rejected
        };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.RejectBatch);

        Assert.True(result.BatchRejected);
        Assert.Empty(result.Rows); // nothing left behind
        Assert.Equal(1, result.FailureCount);
        Assert.Contains("price", result.RejectReason);
    }

    [Fact]
    public void RejectBatch_policy_A_fully_clean_batch_is_not_rejected()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["id"] = "a1", ["price"] = 100.0 },
            new() { ["id"] = "a2", ["price"] = 200.0 },
        };

        var result = ConnectorRowCoercion.Apply(ScalarFields, rows, CoercionFailurePolicy.RejectBatch);

        Assert.False(result.BatchRejected);
        Assert.Equal(0, result.FailureCount);
        Assert.Equal(2, result.Rows.Count);
    }

    // ------------------------------------------------------------------
    // Array fields (element-by-element, mirrors IngressRowAcceptance.TryCoerceField).
    // ------------------------------------------------------------------

    [Fact]
    public void Array_field_Coerces_every_element()
    {
        var fields = new List<FieldDef> { new("tags", FieldType.Double, IsArray: true) };
        var rows = new List<Dictionary<string, object?>> { new() { ["tags"] = new List<object?> { "1.5", "2.5" } } };

        var result = ConnectorRowCoercion.Apply(fields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(0, result.FailureCount);
        Assert.Equal(new List<object?> { 1.5, 2.5 }, result.Rows[0]["tags"]);
    }

    [Fact]
    public void Array_field_Skips_null_elements_without_failing()
    {
        var fields = new List<FieldDef> { new("tags", FieldType.Double, IsArray: true) };
        var rows = new List<Dictionary<string, object?>> { new() { ["tags"] = new List<object?> { "1.5", null, "2.5" } } };

        var result = ConnectorRowCoercion.Apply(fields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(0, result.FailureCount);
        Assert.Equal(new List<object?> { 1.5, 2.5 }, result.Rows[0]["tags"]);
    }

    [Fact]
    public void Array_field_One_bad_element_fails_the_whole_field()
    {
        var fields = new List<FieldDef> { new("tags", FieldType.Double, IsArray: true) };
        var rows = new List<Dictionary<string, object?>> { new() { ["tags"] = new List<object?> { "1.5", "not-a-number" } } };

        var result = ConnectorRowCoercion.Apply(fields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(1, result.FailureCount);
        Assert.Null(result.Rows[0]["tags"]);
    }

    [Fact]
    public void Array_field_A_non_enumerable_value_fails()
    {
        var fields = new List<FieldDef> { new("tags", FieldType.Double, IsArray: true) };
        var rows = new List<Dictionary<string, object?>> { new() { ["tags"] = 42L } };

        var result = ConnectorRowCoercion.Apply(fields, rows, CoercionFailurePolicy.Null);

        Assert.Equal(1, result.FailureCount);
        Assert.Null(result.Rows[0]["tags"]);
    }
}
