using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 025 G2 — <see cref="EntityStreamFanout"/>, the per-entity index that lets the shared gRPC
/// streaming services (<c>shared/StreamsForge.Api/Grpc/**</c>) run on a flavor whose streaming spine is
/// five fixed pub/sub topics rather than a stream per entity.
///
/// <para>Everything here is pure: an envelope in, a handler list out. There is no sidecar, no HTTP and
/// no gRPC — the class's whole job is the routing decision (does this envelope's key match a
/// subscription?) and the lifecycle around it, and those are exactly what a wrong answer would break
/// silently in production: a gRPC subscriber that receives nothing looks identical to an idle
/// entity.</para>
/// </summary>
public class EntityStreamFanoutTests
{
    private static SourceEventsEnvelope Source(string key, params Dictionary<string, object?>[] events) =>
        new() { Source = key, Events = [.. events] };

    private static TableDeltaEnvelope Table(string key, long seq = 1) =>
        new() { Table = key, Seq = seq, Deltas = [new TableDeltaDto { Row = new() { ["x"] = 1L }, Weight = 1 }] };

    private static PipelineResultsEnvelope Pipeline(string key) =>
        new() { PipelineId = key, Results = [new ResultEnvelope { PipelineId = key, Seq = 7, Row = new() { ["y"] = 2L } }] };

    [Fact]
    public async Task A_source_subscription_receives_one_callback_per_event_with_the_ts_extracted()
    {
        var fanout = new EntityStreamFanout();
        List<(IReadOnlyDictionary<string, object?> Row, long Ts)> seen = [];

        await fanout.SubscribeSourceAsync("", "trades", (row, ts) =>
        {
            seen.Add((row, ts));
            return Task.CompletedTask;
        });

        await fanout.OnSourceEventsAsync(Source(
            "trades",
            new Dictionary<string, object?> { ["symbol"] = "AAPL", ["_ts"] = 1700000000123L },
            new Dictionary<string, object?> { ["symbol"] = "MSFT", ["_ts"] = 1700000000456L }));

        // One callback PER EVENT, not per envelope: a gRPC SourceEvent frame carries a single row, the
        // same way the Orleans stream item does.
        Assert.Equal(2, seen.Count);
        Assert.Equal("AAPL", seen[0].Row["symbol"]);
        Assert.Equal(1700000000123L, seen[0].Ts);
        Assert.Equal(1700000000456L, seen[1].Ts);
    }

    [Fact]
    public async Task A_source_event_with_no_ts_reports_zero_rather_than_throwing()
    {
        var fanout = new EntityStreamFanout();
        long? ts = null;
        await fanout.SubscribeSourceAsync("", "trades", (_, t) => { ts = t; return Task.CompletedTask; });

        await fanout.OnSourceEventsAsync(Source("trades", new Dictionary<string, object?> { ["symbol"] = "AAPL" }));

        // Mirrors EventRecord.Timestamp's own "0 when absent or not a long" rule — a source that never
        // stamps _ts must still stream, it just has no timestamp to report.
        Assert.Equal(0L, ts);
    }

    [Fact]
    public async Task Keys_are_environment_qualified_the_way_the_envelopes_are()
    {
        var fanout = new EntityStreamFanout();
        var defaultEnv = 0;
        var staging = 0;

        await fanout.SubscribeSourceAsync("", "trades", (_, _) => { defaultEnv++; return Task.CompletedTask; });
        await fanout.SubscribeSourceAsync("staging", "trades", (_, _) => { staging++; return Task.CompletedTask; });

        // The publishing actor's own id IS the qualified key — "trades" in the default environment
        // (EnvKeys.Qualify("", k) == k) and "staging.trades" in staging.
        await fanout.OnSourceEventsAsync(Source("trades", []));
        await fanout.OnSourceEventsAsync(Source("trades", new Dictionary<string, object?> { ["a"] = 1L }));
        await fanout.OnSourceEventsAsync(Source("staging.trades", new Dictionary<string, object?> { ["a"] = 1L }));

        Assert.Equal(1, defaultEnv);
        Assert.Equal(1, staging);
    }

    [Fact]
    public async Task Two_subscriptions_to_one_table_both_receive_and_a_different_table_receives_nothing()
    {
        var fanout = new EntityStreamFanout();
        var first = 0;
        var second = 0;
        var other = 0;

        await fanout.SubscribeTableAsync("", "positions", _ => { first++; return Task.CompletedTask; });
        await fanout.SubscribeTableAsync("", "positions", _ => { second++; return Task.CompletedTask; });
        await fanout.SubscribeTableAsync("", "orders", _ => { other++; return Task.CompletedTask; });

        await fanout.OnTableDeltaAsync(Table("positions"));

        Assert.Equal(1, first);
        Assert.Equal(1, second);
        Assert.Equal(0, other);
    }

    [Fact]
    public async Task Disposing_one_handle_removes_exactly_that_subscription()
    {
        var fanout = new EntityStreamFanout();
        var kept = 0;
        var dropped = 0;

        await fanout.SubscribeTableAsync("", "positions", _ => { kept++; return Task.CompletedTask; });
        var handle = await fanout.SubscribeTableAsync("", "positions", _ => { dropped++; return Task.CompletedTask; });

        await fanout.OnTableDeltaAsync(Table("positions"));
        await handle.DisposeAsync();
        await fanout.OnTableDeltaAsync(Table("positions"));

        Assert.Equal(2, kept);
        Assert.Equal(1, dropped);
    }

    [Fact]
    public async Task Disposing_the_same_handle_twice_is_harmless()
    {
        var fanout = new EntityStreamFanout();
        var handle = await fanout.SubscribeTableAsync("", "positions", _ => Task.CompletedTask);

        await handle.DisposeAsync();
        await handle.DisposeAsync();

        // Nothing left to deliver to, and no throw: a gRPC call that is cancelled while already
        // unwinding must not turn a client disconnect into a server error.
        await fanout.OnTableDeltaAsync(Table("positions"));
    }

    [Fact]
    public async Task A_throwing_handler_does_not_stop_the_others_and_does_not_escape()
    {
        var fanout = new EntityStreamFanout();
        var after = 0;

        await fanout.SubscribeTableAsync("", "positions", _ => throw new InvalidOperationException("client vanished"));
        await fanout.SubscribeTableAsync("", "positions", _ => { after++; return Task.CompletedTask; });

        // If this threw, the sf-table-delta topic endpoint would return 500 and Dapr would redeliver the
        // message to EVERY subscriber of the topic — including the SignalR bridge and the NATS sink.
        // Containment here is what keeps one disconnected gRPC client from doing that.
        await fanout.OnTableDeltaAsync(Table("positions"));

        Assert.Equal(1, after);
    }

    [Fact]
    public async Task Pipeline_results_reach_the_subscriber_for_that_pipeline_id_only()
    {
        var fanout = new EntityStreamFanout();
        List<ResultEnvelope> seen = [];

        await fanout.SubscribePipelineAsync("", "p-1", rows => { seen.AddRange(rows); return Task.CompletedTask; });

        await fanout.OnPipelineResultsAsync(Pipeline("p-2"));
        await fanout.OnPipelineResultsAsync(Pipeline("p-1"));

        Assert.Single(seen);
        Assert.Equal(7, seen[0].Seq);
    }

    [Fact]
    public async Task An_envelope_for_a_key_nobody_subscribed_to_is_a_no_op()
    {
        var fanout = new EntityStreamFanout();

        // The overwhelmingly common case on this flavor: the fixed topics carry every entity's traffic,
        // and a host with no gRPC subscriber at all still receives all of it.
        await fanout.OnSourceEventsAsync(Source("trades", new Dictionary<string, object?> { ["a"] = 1L }));
        await fanout.OnTableDeltaAsync(Table("positions"));
        await fanout.OnPipelineResultsAsync(Pipeline("p-1"));
    }

    [Fact]
    public async Task It_is_registered_as_all_three_seams_the_host_wires_it_up_as()
    {
        var fanout = new EntityStreamFanout();

        // StreamingRuntimeSetup forwards ISourceEventsSink/ITableDeltaSink and IEntityStreamFacade to
        // ONE instance; if any of these stopped being implemented the registration would still compile
        // as a cast in DI and fail only at resolve time, inside a running host.
        Assert.IsAssignableFrom<ISourceEventsSink>(fanout);
        Assert.IsAssignableFrom<ITableDeltaSink>(fanout);
        Assert.IsAssignableFrom<IEntityStreamFacade>(fanout);
        await Task.CompletedTask;
    }
}
