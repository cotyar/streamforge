using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Wishlist "explicit key retraction through ingest": <see cref="IngressRowAcceptance"/>'s
/// fourth reserved key, "_retract" — coercion and survival through <see cref="IngressRowAcceptance.Accept"/>
/// only. Whether a retraction is actually MEANINGFUL for the source it targets (i.e. whether every
/// running consumer is a LATEST BY table) is a separate, catalog-aware concern covered by
/// RetractConsumerValidationTests.cs — this file only proves the row-shaping half every ingest
/// transport shares (see IngressRowAcceptance's own class doc on why that split exists).</summary>
public class RetractIngestRowAcceptanceTests
{
    private static readonly List<FieldDef> Fields =
    [
        new("order_id", FieldType.String),
        new("stage", FieldType.String),
    ];

    [Fact]
    public void Retract_true_is_coerced_and_carried_into_the_accepted_row()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = true };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(true, result.Row!["_retract"]);
    }

    [Fact]
    public void Retract_absent_leaves_no_trace_on_the_accepted_row()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1" };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.False(result.Row!.ContainsKey("_retract"));
    }

    [Fact]
    public void Retract_is_never_treated_as_an_unknown_field()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = true };

        // rejectUnknownFields: true would fail the row on any key that isn't a declared field, "_ts" or
        // "_source" — "_retract" must be recognized as a fifth safe key alongside those two, exactly
        // like a genuine reserved field, not something a strict source can reject sight-unseen.
        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: true, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(0, result.UnknownFieldsDropped);
    }

    [Fact]
    public void Retract_false_is_coerced_and_carried_through_as_false()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = false };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(false, result.Row!["_retract"]);
    }

    /// <summary>Strings coerce leniently here (FieldValueConversion.TryToBool: bool.TryParse first,
    /// then "nonzero/non-empty is true" — matches every other Bool-typed declared field, not a rule
    /// special-cased for "_retract"), so a genuinely uncoercible value has to be a structural one — a
    /// JSON array/object survives JsonValueNormalizer as a List/Dictionary, which TryToBool's default
    /// arm rejects outright.</summary>
    [Fact]
    public void Retract_that_cannot_be_coerced_to_bool_fails_the_row()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = new List<object?> { 1L, 2L } };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.False(result.Accepted);
        Assert.Contains("_retract", result.Error);
        Assert.Null(result.Row);
    }

    /// <summary>The lenient string rule from the doc above, pinned as its own case: a string that
    /// ISN'T "true"/"false"/""/"0" still coerces (to true), it does not fail the row.</summary>
    [Fact]
    public void Retract_as_a_non_empty_non_zero_string_coerces_to_true()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = "yes" };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(true, result.Row!["_retract"]);
    }

    [Fact]
    public void Retract_null_is_treated_as_absent_not_as_a_coercion_failure()
    {
        var raw = new Dictionary<string, object?> { ["order_id"] = "O1", ["_retract"] = null };

        var result = IngressRowAcceptance.Accept(Fields, "order_events", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.False(result.Row!.ContainsKey("_retract"));
    }
}
