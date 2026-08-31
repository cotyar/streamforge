using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 C2: <see cref="ConnectorPollCycle"/>'s declared-type coercion integration —
/// <see cref="ConnectorRowCoercion"/> runs inside <see cref="ConnectorPollCycle"/>'s private Emit
/// (exercised here through the public ExecuteUrl/ExecuteNatsMessage entry points), BEFORE dedup, so a
/// RejectBatch rejection surfaces as a <see cref="PollCycleResult.Error"/> exactly like a malformed
/// response body would, and Null/DropRow failures are reflected in
/// <see cref="PollCycleResult.CoercionFailures"/>.</summary>
public class ConnectorPollCycleCoercionTests
{
    private static SourceDefinition UrlSource(CoercionFailurePolicy policy) => new()
    {
        Name = "s1",
        Kind = SourceKinds.Url,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("price", FieldType.Double)],
        OnCoercionFailure = policy,
        Connector = new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://example.invalid/data" },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                ],
            },
        },
    };

    [Fact]
    public void ExecuteUrl_Default_Null_policy_coerces_a_stringly_typed_number_and_emits_the_row()
    {
        var def = UrlSource(CoercionFailurePolicy.Null);
        var body = """[{"id":"a1","price":"100.5"}]""";

        var result = ConnectorPollCycle.ExecuteUrl(def, body, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(0, result.CoercionFailures);
        Assert.Single(result.Rows);
        Assert.Equal(100.5, result.Rows[0]["price"]);
    }

    [Fact]
    public void ExecuteUrl_Null_policy_nulls_an_uncoercible_field_counts_the_failure_and_still_emits_the_row()
    {
        var def = UrlSource(CoercionFailurePolicy.Null);
        var body = """[{"id":"a1","price":"not-a-number"}]""";

        var result = ConnectorPollCycle.ExecuteUrl(def, body, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(1, result.CoercionFailures);
        Assert.Single(result.Rows);
        Assert.Null(result.Rows[0]["price"]);
        Assert.Equal("a1", result.Rows[0]["id"]);
        // Still stamped/emitted like any other row.
        Assert.Equal("s1", result.Rows[0]["_source"]);
    }

    [Fact]
    public void ExecuteUrl_DropRow_policy_drops_the_bad_row_but_keeps_good_ones()
    {
        var def = UrlSource(CoercionFailurePolicy.DropRow);
        var body = """[{"id":"a1","price":"bad"},{"id":"a2","price":200.5}]""";

        var result = ConnectorPollCycle.ExecuteUrl(def, body, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(1, result.CoercionFailures);
        Assert.Single(result.Rows);
        Assert.Equal("a2", result.Rows[0]["id"]);
    }

    [Fact]
    public void ExecuteUrl_RejectBatch_policy_surfaces_as_a_cycle_Error_with_nothing_emitted()
    {
        var def = UrlSource(CoercionFailurePolicy.RejectBatch);
        var body = """[{"id":"a1","price":100.5},{"id":"a2","price":"bad"}]""";

        var result = ConnectorPollCycle.ExecuteUrl(def, body, new DedupTracker(), 1000);

        Assert.NotNull(result.Error);
        Assert.Contains("coercion rejected batch", result.Error);
        Assert.Empty(result.Rows); // coerce-before-admission: nothing left behind
    }

    [Fact]
    public void Coercion_runs_before_dedup_so_a_rejected_batch_never_consumes_a_dedup_slot()
    {
        var def = UrlSource(CoercionFailurePolicy.RejectBatch);
        def.Connector!.Mapping!.DedupKeyField = "id";
        var dedup = new DedupTracker();
        var body = """[{"id":"a1","price":"bad"}]""";

        ConnectorPollCycle.ExecuteUrl(def, body, dedup, 1000);

        Assert.Equal(0, dedup.Count); // the dedup key was never recorded
    }

    [Fact]
    public void ExecuteNatsMessage_runs_the_same_coercion_pipeline_as_a_polled_body()
    {
        var def = UrlSource(CoercionFailurePolicy.Null);
        var payload = """{"id":"a1","price":"3.5"}""";

        var result = ConnectorPollCycle.ExecuteNatsMessage(def, "json", payload, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(0, result.CoercionFailures);
        Assert.Single(result.Rows);
        Assert.Equal(3.5, result.Rows[0]["price"]);
    }

    [Fact]
    public void ExecuteNatsMessage_RejectBatch_leaves_nothing_behind_for_a_single_bad_message()
    {
        var def = UrlSource(CoercionFailurePolicy.RejectBatch);
        var payload = """{"id":"a1","price":"bad"}""";

        var result = ConnectorPollCycle.ExecuteNatsMessage(def, "json", payload, new DedupTracker(), 1000);

        Assert.NotNull(result.Error);
        Assert.Empty(result.Rows);
    }
}
