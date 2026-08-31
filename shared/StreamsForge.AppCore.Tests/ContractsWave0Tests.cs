using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>Plan 015 wave 0's own two pieces of logic. Small, but both are the kind that fails silently:
/// a merge that forgets a field reverts it on the next read (the exact failure CatalogRecordMerge exists
/// to prevent), and a day key computed in local time splits or merges audit shards on a redeploy.</summary>
public class ContractsWave0Tests
{
    [Fact]
    public void TheFourArgMergeStampsUpdatedByAndStillCarriesEveryServerOwnedField()
    {
        var existing = new PipelineDefinition
        {
            Id = "p1", CreatedBy = "alice", CreatedAtMs = 100, Status = PipelineStatus.Running,
            Error = "boom", SourceNames = ["trades"], UpdatedBy = "bob",
        };
        var incoming = new PipelineDefinition
        {
            Id = "forged", CreatedBy = "mallory", CreatedAtMs = 999, Status = PipelineStatus.Stopped,
            Error = null, SourceNames = [], UpdatedBy = "mallory", Sql = "SELECT 1",
        };

        CatalogRecordMerge.CarryServerOwnedFields(existing, incoming, nowMs: 500, updatedBy: "carol");

        Assert.Equal("carol", incoming.UpdatedBy);
        Assert.Equal("p1", incoming.Id);
        Assert.Equal("alice", incoming.CreatedBy);
        Assert.Equal(100, incoming.CreatedAtMs);
        Assert.Equal(PipelineStatus.Running, incoming.Status);
        Assert.Equal("boom", incoming.Error);
        Assert.Equal(["trades"], incoming.SourceNames);
        Assert.Equal(500, incoming.UpdatedAtMs);
        Assert.Equal("SELECT 1", incoming.Sql);   // client-owned, untouched
    }

    [Fact]
    public void TheThreeArgMergeCarriesTheStoredUpdatedByRatherThanTheClientsClaim()
    {
        var existing = new TableDefinition { Id = "t1", UpdatedBy = "alice" };
        var incoming = new TableDefinition { Id = "forged", UpdatedBy = "mallory" };

        CatalogRecordMerge.CarryServerOwnedFields(existing, incoming, nowMs: 7);

        Assert.Equal("alice", incoming.UpdatedBy);
    }

    [Fact]
    public void TheAuditDayKeyIsUtc()
    {
        // 2026-08-19T23:30Z — an evening UTC that is already the 20th in half the world's timezones and
        // still the 19th in the other half. The key must not depend on which half the host sits in.
        var atMs = new DateTimeOffset(2026, 8, 19, 23, 30, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();

        Assert.Equal("audit:20260819", StreamConstants.AuditKeyFor(atMs));
    }
}
