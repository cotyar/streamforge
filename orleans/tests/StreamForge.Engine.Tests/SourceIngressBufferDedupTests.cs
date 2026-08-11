using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Plan 009 A1.1: row-level dedup (<see cref="SourceIngressBuffer.FilterRowLevelDuplicates"/>)
/// — reuses <c>DedupTracker</c> per the plan brief. Ordering under test: dedup runs AFTER coercion,
/// BEFORE admission, so these tests call <c>FilterRowLevelDuplicates</c> directly on already-coerced
/// rows (mirroring what OrleansIngressFacade/DaprIngressFacade actually do) rather than going through
/// a whole facade.</summary>
public class SourceIngressBufferDedupTests
{
    private static Dictionary<string, object?> Row(string id, double price) => new() { ["id"] = id, ["price"] = price };

    private static SourceIngressBuffer MakeBuffer(string? dedupKeyField, int dedupWindow = 0) =>
        new("test-source",
            new IngestConfig { Policy = IngressOverflowPolicy.Reject, CapacityRows = 1000, MaxBatchRows = 1000, DedupKeyField = dedupKeyField, DedupWindow = dedupWindow },
            "fp", (_, _) => Task.CompletedTask);

    [Fact]
    public void No_DedupKeyField_configured_keeps_every_row_and_reports_zero_duplicates()
    {
        var buffer = MakeBuffer(dedupKeyField: null);
        var rows = new List<Dictionary<string, object?>> { Row("a", 1), Row("a", 1), Row("b", 2) };

        var result = buffer.FilterRowLevelDuplicates(rows);

        Assert.Equal(3, result.Kept.Count);
        Assert.Equal(0, result.DuplicateCount);
        Assert.Equal(0, buffer.GetStatus().TotalDuplicate);
    }

    [Fact]
    public void Repeated_key_values_are_deduped_and_counted()
    {
        var buffer = MakeBuffer(dedupKeyField: "id");
        var rows = new List<Dictionary<string, object?>> { Row("a", 1), Row("a", 2), Row("b", 3), Row("a", 4) };

        var result = buffer.FilterRowLevelDuplicates(rows);

        Assert.Equal(2, result.Kept.Count); // first "a" and the "b"
        Assert.Equal(2, result.DuplicateCount); // the two later "a"s
        Assert.Equal(["a", "b"], result.Kept.Select(r => (string)r["id"]!));
        Assert.Equal(2, buffer.GetStatus().TotalDuplicate);
    }

    [Fact]
    public void Dedup_state_carries_across_separate_FilterRowLevelDuplicates_calls()
    {
        var buffer = MakeBuffer(dedupKeyField: "id");

        var first = buffer.FilterRowLevelDuplicates([Row("a", 1)]);
        var second = buffer.FilterRowLevelDuplicates([Row("a", 2)]); // same key, later "batch"

        Assert.Single(first.Kept);
        Assert.Equal(0, first.DuplicateCount);
        Assert.Empty(second.Kept);
        Assert.Equal(1, second.DuplicateCount);
    }

    [Fact]
    public void A_row_missing_the_configured_key_field_is_kept_not_deduped()
    {
        var buffer = MakeBuffer(dedupKeyField: "id");
        var rows = new List<Dictionary<string, object?>> { new() { ["price"] = 1.0 } }; // no "id"

        var result = buffer.FilterRowLevelDuplicates(rows);

        Assert.Single(result.Kept);
        Assert.Equal(0, result.DuplicateCount);
    }

    [Fact]
    public void Custom_DedupWindow_bounds_the_underlying_tracker()
    {
        var buffer = MakeBuffer(dedupKeyField: "id", dedupWindow: 2);

        buffer.FilterRowLevelDuplicates([Row("a", 1)]);
        buffer.FilterRowLevelDuplicates([Row("b", 1)]);
        buffer.FilterRowLevelDuplicates([Row("c", 1)]); // evicts "a" out of the 2-key window

        var replay = buffer.FilterRowLevelDuplicates([Row("a", 1)]); // "a" was forgotten — not a duplicate anymore

        Assert.Single(replay.Kept);
        Assert.Equal(0, replay.DuplicateCount);
    }

    [Fact]
    public async Task Accepted_plus_Dropped_plus_Invalid_plus_Duplicate_accounts_for_every_row_in_the_request()
    {
        // Mirrors the facade's actual order: coerce (simulated here — every row is already "coerced"),
        // filter row-level dupes, THEN admit what's left. This is the invariant plan 009 A1's brief
        // calls out explicitly: "Accepted + Dropped + Invalid + Duplicate must account for every row
        // in the request — assert that in a test."
        var buffer = MakeBuffer(dedupKeyField: "id");
        const int invalidCount = 2; // rows that failed coercion before ever reaching the buffer at all
        var incoming = new List<Dictionary<string, object?>>
        {
            Row("a", 1), Row("a", 2), Row("b", 3), Row("c", 4), Row("c", 5),
        };
        const int totalRequestRows = 5 /* incoming */ + invalidCount;

        var dedup = buffer.FilterRowLevelDuplicates(incoming);
        var pushResult = await buffer.PushAsync(dedup.Kept);
        buffer.RecordInvalid(invalidCount);
        pushResult.Invalid = invalidCount;
        pushResult.Duplicate = dedup.DuplicateCount;

        Assert.Equal(totalRequestRows, pushResult.Accepted + pushResult.Dropped + pushResult.Invalid + pushResult.Duplicate);
        Assert.Equal(3, pushResult.Accepted); // "a" (first), "b", "c" (first)
        Assert.Equal(0, pushResult.Dropped);
        Assert.Equal(invalidCount, pushResult.Invalid);
        Assert.Equal(2, pushResult.Duplicate); // second "a", second "c"
    }
}
