using System.Runtime.CompilerServices;
using System.Text;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Nats;
using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 B1: <see cref="NatsSubscriberCore"/> — the reconnecting subscribe loop behind a
/// <c>nats</c>-kind source. There is no NATS server in this sandbox (verified once — see the plan's own
/// note), so every test here drives the loop through a fake <see cref="INatsMessageSource"/>: the seam
/// that keeps message→row mapping, queue-group/subject plumbing, coercion-failure reporting, ack
/// behavior, and reconnect/backoff testable without a live broker.
///
/// <para>Every test cancels its own <see cref="CancellationTokenSource"/> from INSIDE the callback that
/// observed what it needed (never a fixed wall-clock wait) — <see cref="FakeNatsMessageSource"/> blocks
/// (respecting the cancellation token) after its fixed message list is exhausted rather than completing
/// cleanly, specifically so a test's assertions see each message processed EXACTLY once: a clean end
/// would otherwise trigger NatsSubscriberCore's documented "immediate no-backoff reconnect", which would
/// re-yield the same fixed list forever until an external timeout fired.</para></summary>
public class NatsSubscriberCoreTests
{
    private static SourceDefinition NatsSource(
        CoercionFailurePolicy policy = CoercionFailurePolicy.Null, string? dedupKeyField = null) => new()
    {
        Name = "s1",
        Kind = SourceKinds.Nats,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("price", FieldType.Double)],
        OnCoercionFailure = policy,
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "nats://localhost:4222", Subject = "trades.>", QueueGroup = "workers", Format = "json" },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                DedupKeyField = dedupKeyField,
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                ],
            },
        },
    };

    private sealed class FakeNatsMessageSource : INatsMessageSource
    {
        public List<(byte[] Payload, bool Jetstream)> Messages { get; init; } = [];
        public Exception? ThrowOnSubscribe { get; init; }
        public NatsSubConfig? ObservedConfig { get; private set; }
        public List<string> Acked { get; } = [];
        public bool Disposed { get; private set; }
        public int SubscribeCallCount { get; private set; }

        public async IAsyncEnumerable<NatsInboundMessage> SubscribeAsync(NatsSubConfig config, [EnumeratorCancellation] CancellationToken ct)
        {
            SubscribeCallCount++;
            ObservedConfig = config;
            if (ThrowOnSubscribe is not null)
            {
                throw ThrowOnSubscribe;
            }

            var i = 0;
            foreach (var (payload, jetstream) in Messages)
            {
                var id = $"msg{i++}";
                yield return new NatsInboundMessage(config.Subject, payload, jetstream ? () => { Acked.Add(id); return Task.CompletedTask; } : null);
            }

            // Block "connected and idle" rather than completing cleanly — see the class doc for why.
            await Task.Delay(Timeout.Infinite, ct);
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>A fake whose FIRST subscribe call completes cleanly (no messages, no block) and whose
    /// SECOND+ call blocks — isolates "clean end reconnects immediately" without looping forever.</summary>
    private sealed class CleanEndOnceThenBlockSource : INatsMessageSource
    {
        public int SubscribeCallCount { get; private set; }

        public async IAsyncEnumerable<NatsInboundMessage> SubscribeAsync(NatsSubConfig config, [EnumeratorCancellation] CancellationToken ct)
        {
            SubscribeCallCount++;
            if (SubscribeCallCount == 1)
            {
                yield break; // clean end — no messages, no exception
            }

            await Task.Delay(Timeout.Infinite, ct);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static byte[] Json(string s) => Encoding.UTF8.GetBytes(s);

    // ------------------------------------------------------------------
    // Message → row mapping (the shared format/mapping path, not a second extraction path).
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_Maps_a_message_payload_to_a_row_via_the_shared_mapping_path()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("""{"id":"a1","price":1.5}"""), false)] };
        var rows = new List<Dictionary<string, object?>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(),
            onRows: (r, _) => { rows.AddRange(r); cts.Cancel(); return Task.CompletedTask; },
            onStatus: (_, _) => { },
            sourceFactory: () => fake);

        await core.RunAsync(cts.Token);

        var row = Assert.Single(rows);
        Assert.Equal("a1", row["id"]);
        Assert.Equal(1.5, row["price"]);
        Assert.Equal("s1", row["_source"]);
    }

    [Fact]
    public async Task RunAsync_Passes_subject_queueGroup_and_format_through_to_the_message_source()
    {
        var fake = new FakeNatsMessageSource();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, _) => { if (s == "ok") cts.Cancel(); }, () => fake);

        await core.RunAsync(cts.Token);

        Assert.NotNull(fake.ObservedConfig);
        Assert.Equal("trades.>", fake.ObservedConfig!.Subject);
        Assert.Equal("workers", fake.ObservedConfig.QueueGroup);
        Assert.Equal("json", fake.ObservedConfig.Format);
    }

    [Fact]
    public async Task RunAsync_Ndjson_message_with_multiple_lines_extracts_multiple_rows()
    {
        var def = NatsSource();
        def.Connector!.Nats!.Format = "ndjson";
        var fake = new FakeNatsMessageSource { Messages = [(Json("{\"id\":\"a1\",\"price\":1}\n{\"id\":\"a2\",\"price\":2}\n"), false)] };
        var rows = new List<Dictionary<string, object?>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(def, new DedupTracker(), (r, _) => { rows.AddRange(r); cts.Cancel(); return Task.CompletedTask; }, (_, _) => { }, () => fake);

        await core.RunAsync(cts.Token);

        Assert.Equal(2, rows.Count);
    }

    // ------------------------------------------------------------------
    // Coercion (plan 009 C2) — Null/DropRow/RejectBatch.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_Null_policy_coercion_failure_still_emits_the_row_and_reports_a_status_note()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("""{"id":"a1","price":"bad"}"""), false)] };
        var rows = new List<Dictionary<string, object?>>();
        var statuses = new List<(string Status, string? Error)>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(CoercionFailurePolicy.Null), new DedupTracker(),
            (r, _) => { rows.AddRange(r); return Task.CompletedTask; },
            (s, e) =>
            {
                statuses.Add((s, e));
                if (e is not null && e.Contains("coercion failure"))
                {
                    cts.Cancel();
                }
            },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Single(rows);
        Assert.Null(rows[0]["price"]);
        Assert.Contains(statuses, s => s.Status == "ok" && s.Error != null && s.Error.Contains("coercion failure"));
    }

    [Fact]
    public async Task RunAsync_RejectBatch_policy_drops_the_message_reports_error_and_does_not_call_onRows()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("""{"id":"a1","price":"bad"}"""), false)] };
        var onRowsCalled = false;
        var statuses = new List<(string Status, string? Error)>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(CoercionFailurePolicy.RejectBatch), new DedupTracker(),
            (_, _) => { onRowsCalled = true; return Task.CompletedTask; },
            (s, e) =>
            {
                statuses.Add((s, e));
                if (e is not null && e.Contains("coercion rejected batch"))
                {
                    cts.Cancel();
                }
            },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.False(onRowsCalled);
        Assert.Contains(statuses, s => s.Status == "error" && s.Error != null && s.Error.Contains("coercion rejected batch"));
    }

    // ------------------------------------------------------------------
    // JetStream ack behavior.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_Acks_a_JetStream_message_after_a_clean_outcome()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("""{"id":"a1","price":1.5}"""), true)] };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(NatsSource(), new DedupTracker(), (_, _) => { cts.Cancel(); return Task.CompletedTask; }, (_, _) => { }, () => fake);

        await core.RunAsync(cts.Token);

        Assert.Single(fake.Acked);
    }

    [Fact]
    public async Task RunAsync_Does_not_ack_a_JetStream_message_a_RejectBatch_policy_rejected()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("""{"id":"a1","price":"bad"}"""), true)] };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(CoercionFailurePolicy.RejectBatch), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, e) => { if (s == "error") cts.Cancel(); },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Empty(fake.Acked); // left unacked -> the JetStream consumer will redeliver it
    }

    [Fact]
    public async Task RunAsync_Does_not_ack_a_message_that_failed_to_parse()
    {
        var fake = new FakeNatsMessageSource { Messages = [(Json("not-json-at-all"), true)] };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, e) => { if (s == "error") cts.Cancel(); },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Empty(fake.Acked);
    }

    // ------------------------------------------------------------------
    // Dedup (MappingSpec.DedupKeyField) across messages.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_Dedups_repeated_keys_across_messages_using_the_shared_tracker()
    {
        var fake = new FakeNatsMessageSource
        {
            Messages =
            [
                (Json("""{"id":"a1","price":1.0}"""), false),
                (Json("""{"id":"a1","price":2.0}"""), false), // duplicate id -> suppressed
                (Json("""{"id":"a2","price":3.0}"""), false),
            ],
        };
        var rows = new List<Dictionary<string, object?>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(dedupKeyField: "id"), new DedupTracker(),
            (r, _) =>
            {
                rows.AddRange(r);
                if (rows.Count >= 2)
                {
                    cts.Cancel();
                }
                return Task.CompletedTask;
            },
            (_, _) => { },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => (string?)r["id"] == "a1");
        Assert.Contains(rows, r => (string?)r["id"] == "a2");
    }

    // ------------------------------------------------------------------
    // Reconnect / backoff.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_Reports_connecting_then_ok_before_any_message()
    {
        var fake = new FakeNatsMessageSource();
        var statuses = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, _) => { statuses.Add(s); if (s == "ok") cts.Cancel(); },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Equal("connecting", statuses[0]);
        Assert.Contains("ok", statuses);
    }

    [Fact]
    public async Task RunAsync_A_thrown_connect_failure_reports_error_and_disposes_the_source()
    {
        var fake = new FakeNatsMessageSource { ThrowOnSubscribe = new InvalidOperationException("dial refused") };
        var statuses = new List<(string Status, string? Error)>();
        using var cts = new CancellationTokenSource();
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, e) =>
            {
                statuses.Add((s, e));
                if (s == "error")
                {
                    cts.Cancel(); // one failed attempt is enough — don't wait out the 30s backoff
                }
            },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.Contains(statuses, s => s.Status == "error" && s.Error != null && s.Error.Contains("dial refused"));
        Assert.True(fake.Disposed);
        Assert.Equal(1, fake.SubscribeCallCount);
    }

    [Fact]
    public async Task RunAsync_Exits_promptly_when_cancelled_before_any_connect_attempt()
    {
        var fake = new FakeNatsMessageSource();
        var core = new NatsSubscriberCore(NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask, (_, _) => { }, () => fake);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var task = core.RunAsync(cts.Token);
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5))) == task;

        Assert.True(completed, "RunAsync did not return promptly for an already-cancelled token");
        Assert.Equal(0, fake.SubscribeCallCount); // never even tried to connect
    }

    [Fact]
    public async Task RunAsync_A_clean_end_of_subscription_reconnects_immediately_with_no_backoff()
    {
        var fake = new CleanEndOnceThenBlockSource();
        var connectingCount = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var core = new NatsSubscriberCore(
            NatsSource(), new DedupTracker(), (_, _) => Task.CompletedTask,
            (s, _) =>
            {
                if (s == "connecting")
                {
                    connectingCount++;
                    if (connectingCount >= 2)
                    {
                        cts.Cancel();
                    }
                }
            },
            () => fake);

        await core.RunAsync(cts.Token);

        Assert.True(fake.SubscribeCallCount >= 2, "expected a reconnect after the first clean end");
    }
}
