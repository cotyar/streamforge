using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Plan 008 W4: FieldValueCoercion.TryCoerce, extracted from ProtoWireEncoder so proto
/// encoding, connector mapping, and client-push ingest agree on what each FieldType accepts.</summary>
public class FieldValueCoercionTests
{
    [Fact]
    public void String_passes_a_string_through_unchanged()
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.String, "hello", out var coerced));
        Assert.Equal("hello", coerced);
    }

    [Theory]
    [InlineData(42L, "42")]
    [InlineData(3.5, "3.5")]
    public void String_coerces_a_number_via_ToString(object input, string expected)
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.String, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void String_coerces_a_bool_as_lowercase_literal(bool input, string expected)
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.String, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Theory]
    [InlineData(3.5)]
    [InlineData(3L)]
    [InlineData(3)]
    [InlineData(true)]
    public void Double_coercion_accepts_numeric_and_bool_types(object input)
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.Double, input, out var coerced));
        Assert.IsType<double>(coerced);
    }

    [Fact]
    public void Double_coercion_accepts_a_numeric_string()
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.Double, "3.5", out var coerced));
        Assert.Equal(3.5, coerced);
    }

    [Fact]
    public void Double_coercion_rejects_a_non_numeric_string()
    {
        Assert.False(FieldValueCoercion.TryCoerce(FieldType.Double, "not-a-number", out var coerced));
        Assert.Null(coerced);
    }

    [Fact]
    public void Double_coercion_rejects_an_unsupported_type()
    {
        Assert.False(FieldValueCoercion.TryCoerce(FieldType.Double, new object(), out _));
    }

    [Theory]
    [InlineData(FieldType.Long)]
    [InlineData(FieldType.Timestamp)]
    public void Long_and_Timestamp_share_identical_coercion(FieldType type)
    {
        Assert.True(FieldValueCoercion.TryCoerce(type, "42", out var fromString));
        Assert.Equal(42L, fromString);

        Assert.True(FieldValueCoercion.TryCoerce(type, 3.9, out var fromDouble));
        Assert.Equal(3L, fromDouble); // truncates, does not round

        Assert.True(FieldValueCoercion.TryCoerce(type, true, out var fromBool));
        Assert.Equal(1L, fromBool);

        Assert.False(FieldValueCoercion.TryCoerce(type, "not-a-number", out _));
        Assert.False(FieldValueCoercion.TryCoerce(type, new object(), out _));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("", false)]
    [InlineData("0", false)]
    [InlineData("anything-else", true)]
    public void Bool_coercion_from_string(string input, bool expected)
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.Bool, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Theory]
    [InlineData(1L, true)]
    [InlineData(0L, false)]
    [InlineData(2.5, true)]
    [InlineData(0.0, false)]
    public void Bool_coercion_from_number(object input, bool expected)
    {
        Assert.True(FieldValueCoercion.TryCoerce(FieldType.Bool, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Fact]
    public void Bool_coercion_rejects_an_unsupported_type()
    {
        Assert.False(FieldValueCoercion.TryCoerce(FieldType.Bool, new object(), out _));
    }

    [Fact]
    public void Json_coercion_always_passes_the_value_through_unchanged()
    {
        var dict = new Dictionary<string, object?> { ["a"] = 1L };

        Assert.True(FieldValueCoercion.TryCoerce(FieldType.Json, dict, out var coerced));

        Assert.Same(dict, coerced);
    }
}
