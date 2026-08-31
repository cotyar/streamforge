using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>Plan 008 W4: exhaustive coverage of IngressAdmission.Decide — every
/// IngressOverflowPolicy x under/at/over capacity x oversized-batch, pure and zero-setup.</summary>
public class IngressAdmissionTests
{
    private static IngestConfig Cfg(IngressOverflowPolicy policy, int capacity = 100, int maxBatch = 50)
        => new() { Policy = policy, CapacityRows = capacity, MaxBatchRows = maxBatch };

    [Theory]
    [InlineData(IngressOverflowPolicy.Reject)]
    [InlineData(IngressOverflowPolicy.Block)]
    [InlineData(IngressOverflowPolicy.DropNewest)]
    [InlineData(IngressOverflowPolicy.DropOldest)]
    [InlineData(IngressOverflowPolicy.Inline)]
    public void Under_capacity_admits_the_whole_batch_for_every_policy(IngressOverflowPolicy policy)
    {
        var decision = IngressAdmission.Decide(depth: 10, batchSize: 20, Cfg(policy), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(20, decision.Admit);
        Assert.Equal(0, decision.Drop);
        Assert.Equal(0, decision.Evict);
    }

    [Theory]
    [InlineData(IngressOverflowPolicy.Reject)]
    [InlineData(IngressOverflowPolicy.Block)]
    [InlineData(IngressOverflowPolicy.DropNewest)]
    [InlineData(IngressOverflowPolicy.DropOldest)]
    public void At_capacity_exactly_admits_the_whole_batch(IngressOverflowPolicy policy)
    {
        // depth 90, capacity 100 -> exactly 10 free; a batch of 10 fits exactly, no overflow branch.
        var decision = IngressAdmission.Decide(depth: 90, batchSize: 10, Cfg(policy), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(10, decision.Admit);
        Assert.Equal(0, decision.Drop);
    }

    [Fact]
    public void Inline_ignores_depth_and_capacity_entirely()
    {
        var decision = IngressAdmission.Decide(depth: 999_999, batchSize: 10, Cfg(IngressOverflowPolicy.Inline, capacity: 5), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(10, decision.Admit);
        Assert.Equal(0, decision.Drop);
    }

    [Fact]
    public void Inline_still_enforces_MaxBatchRows()
    {
        var decision = IngressAdmission.Decide(depth: 0, batchSize: 60, Cfg(IngressOverflowPolicy.Inline, capacity: 5, maxBatch: 50), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.TooLarge, decision.Kind);
        Assert.Equal(0, decision.Admit);
    }

    [Fact]
    public void Over_capacity_reject_policy_rejects_the_whole_batch_never_a_partial_admit()
    {
        // depth 95, capacity 100 -> 5 free; batch of 10 -> 5 deficit.
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 10, Cfg(IngressOverflowPolicy.Reject), drainRowsPerMs: 0.5);

        Assert.Equal(IngressAdmission.AdmissionKind.Reject, decision.Kind);
        Assert.Equal(0, decision.Admit);
        Assert.Equal(0, decision.Drop);
        Assert.True(decision.RetryAfterMs > 0);
    }

    [Fact]
    public void Over_capacity_block_policy_waits_never_admits_partially()
    {
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 10, Cfg(IngressOverflowPolicy.Block), drainRowsPerMs: 0.5);

        Assert.Equal(IngressAdmission.AdmissionKind.Wait, decision.Kind);
        Assert.Equal(0, decision.Admit);
    }

    [Fact]
    public void Over_capacity_drop_newest_admits_what_fits_and_reports_the_drop()
    {
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 10, Cfg(IngressOverflowPolicy.DropNewest), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(5, decision.Admit);
        Assert.Equal(5, decision.Drop);
        Assert.Equal(0, decision.Evict);
    }

    [Fact]
    public void Over_capacity_drop_oldest_admits_the_whole_batch_and_evicts_from_the_head()
    {
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 10, Cfg(IngressOverflowPolicy.DropOldest), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(10, decision.Admit);
        Assert.Equal(5, decision.Drop);
        Assert.Equal(5, decision.Evict);
    }

    [Theory]
    [InlineData(IngressOverflowPolicy.Reject)]
    [InlineData(IngressOverflowPolicy.Block)]
    [InlineData(IngressOverflowPolicy.DropNewest)]
    [InlineData(IngressOverflowPolicy.DropOldest)]
    public void Batch_exceeding_capacity_is_always_TooLarge_never_a_partial_admit(IngressOverflowPolicy policy)
    {
        // Even against an EMPTY buffer, a batch bigger than total capacity can never fully fit.
        var decision = IngressAdmission.Decide(depth: 0, batchSize: 150, Cfg(policy, capacity: 100, maxBatch: 1000), drainRowsPerMs: 0.5);

        Assert.Equal(IngressAdmission.AdmissionKind.TooLarge, decision.Kind);
        Assert.Equal(0, decision.Admit);
        Assert.Equal(0, decision.Drop);
    }

    [Theory]
    [InlineData(IngressOverflowPolicy.Reject)]
    [InlineData(IngressOverflowPolicy.Block)]
    [InlineData(IngressOverflowPolicy.DropNewest)]
    [InlineData(IngressOverflowPolicy.DropOldest)]
    public void Batch_exceeding_MaxBatchRows_is_TooLarge_even_when_capacity_would_allow_it(IngressOverflowPolicy policy)
    {
        var decision = IngressAdmission.Decide(depth: 0, batchSize: 60, Cfg(policy, capacity: 1000, maxBatch: 50), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.TooLarge, decision.Kind);
    }

    [Fact]
    public void A_batch_larger_than_capacity_under_Block_is_TooLarge_not_Wait()
    {
        // "A batch larger than capacity must reject immediately rather than burn the timeout."
        var decision = IngressAdmission.Decide(depth: 0, batchSize: 200, Cfg(IngressOverflowPolicy.Block, capacity: 100, maxBatch: 1000), drainRowsPerMs: 1);

        Assert.Equal(IngressAdmission.AdmissionKind.TooLarge, decision.Kind);
    }

    [Fact]
    public void Empty_batch_is_a_trivial_admit_for_any_policy()
    {
        var decision = IngressAdmission.Decide(depth: 0, batchSize: 0, Cfg(IngressOverflowPolicy.Reject), drainRowsPerMs: 0);

        Assert.Equal(IngressAdmission.AdmissionKind.Admit, decision.Kind);
        Assert.Equal(0, decision.Admit);
    }

    [Fact]
    public void RetryAfterMs_is_derived_from_the_observed_drain_rate()
    {
        // depth 95 / capacity 100 -> 5 free; batch of 15 -> 10 deficit; 2 rows/ms -> 5ms.
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 15, Cfg(IngressOverflowPolicy.Reject), drainRowsPerMs: 2.0);

        Assert.Equal(5, decision.RetryAfterMs);
    }

    [Fact]
    public void RetryAfterMs_falls_back_to_a_default_when_the_drain_rate_is_unknown()
    {
        var decision = IngressAdmission.Decide(depth: 95, batchSize: 15, Cfg(IngressOverflowPolicy.Reject), drainRowsPerMs: 0);

        Assert.True(decision.RetryAfterMs > 0);
    }

    [Fact]
    public void MaxBlockWaitMs_server_cap_is_thirty_seconds()
    {
        Assert.Equal(30_000, IngressAdmission.MaxBlockWaitMs);
    }
}
