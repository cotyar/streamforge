using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Streaming;
using StreamForge.Engine;
using StreamForge.Host.Generators;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: Dapr counterpart of Orleans' <c>GeneratorGrain</c>
/// (orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs) — one actor per source (actor type
/// "GeneratorActor", key = the source's name), timer-driven synthetic event publisher built on the same
/// <see cref="MarketDataProfiles"/> profiles.
///
/// <para><b>Batched, not per-event (decision D-E: "generator ticks batch events at ≤20 Hz").</b> Orleans'
/// GeneratorGrain fires a grain timer once per synthetic event (down to 1ms for a high-EPS source) and
/// pushes one item onto an in-process Orleans stream — cheap, because it never leaves the silo. On Dapr,
/// every actor timer tick is a sidecar HTTP round-trip (the Dapr runtime calls back into this process
/// over the sidecar to fire it), and every <see cref="DaprClient.PublishEventAsync{TData}"/> call is a
/// SECOND sidecar hop. A 1ms-per-event cadence would be dozens of pointless sidecar round-trips a second
/// per source for no user-visible benefit. Instead this actor ticks on a fixed
/// <see cref="TickPeriod"/> (200ms, comfortably inside the ≤20Hz/50ms-minimum budget) and each tick
/// publishes <c>round(EventsPerSecond × elapsed)</c> events — computed by the pure, unit-tested
/// <see cref="GeneratorBatching.NextBatchCount"/> — as ONE <see cref="SourceEventsEnvelope"/> batch, using
/// actual measured wall-clock elapsed time (not the nominal period) so scheduling jitter doesn't bias the
/// long-run rate, and carrying the fractional remainder forward tick-to-tick so a low-EPS source (e.g.
/// 3/sec at a 200ms period — 0.6 events/tick) still converges on its configured rate over time instead of
/// rounding down to zero forever.</para>
///
/// <para><b>Acyclic by construction — never calls back into the registry.</b> This actor never resolves
/// <c>ICatalogFacade</c>, an <c>IRegistryActor</c> proxy, or any other actor: everything it needs (profile,
/// EventsPerSecond, field schema) arrives once as the <see cref="StartAsync"/> parameter and is cached
/// for the lifetime of the activation. It only ever talks OUTWARD, to Dapr pub/sub. This is what keeps
/// the RegistryActor → <see cref="Lifecycle.DaprLifecycleOrchestrator"/> → GeneratorActor call chain
/// acyclic (see dapr/ARCHITECTURE.md's reentrancy decision and <see cref="Lifecycle.DaprLifecycleOrchestrator"/>'s
/// own class doc): there is no path from here back to RegistryActor, so nothing calling into this actor
/// can ever deadlock waiting on a cycle.</para>
///
/// <para><b>State: the definition and running flag ARE persisted</b> — a deliberate difference from
/// Orleans' GeneratorGrain, which keeps its <c>_def</c>/timer purely in memory and relies on
/// GeneratorSupervisorService's periodic ping to prevent/repair grain eviction (a ping that, as written,
/// does not actually re-arm anything after a real eviction — see that grain's PingAsync). Dapr actors
/// idle-deactivate by default, and a registered timer does NOT survive deactivation/reactivation
/// (D-E: "actor timers die with deactivation/restart"). Persisting <see cref="GeneratorActorState"/> lets
/// <see cref="OnActivateAsync"/> immediately re-arm the timer from the last known-good definition on ANY
/// reactivation — not just ones <see cref="Services.GeneratorSupervisorService"/> happens to trigger —
/// so a source that was Running keeps generating across an idle-deactivate/reactivate cycle without
/// waiting up to one sweep interval. The supervisor remains the safety net for the one case self-healing
/// can't cover: a newly-enabled source whose actor has never been activated at all yet.</para>
///
/// <para><b>Wishlist #9(b): also the loopback target.</b> Independent of <see cref="_running"/>/tick
/// cadence, StartAsync attaches this activation to <c>LoopbackHub</c> (shared/StreamForge.AppCore/
/// Generators/LoopbackHub.cs — read its class doc for the whole design, including the explicit
/// "what happens on an unbounded cycle" argument) and arms a SECOND, always-on timer
/// (<see cref="LoopbackDrainTimerName"/>) that drains whatever a <c>LoopbackSinkClient</c> has written for
/// this source and republishes it via the same two pub/sub topics <see cref="TickAsync"/> already uses.
/// Whether this attachment/timer survives deactivation follows <see cref="_started"/> — a NEW persisted
/// flag distinct from <see cref="_running"/> (which means "the TICK timer should be armed", already false
/// for e.g. every scenario-profile source by convention) — re-armed by <see cref="OnActivateAsync"/>
/// exactly like the tick timer is.</para>
/// </summary>
public sealed class GeneratorActor(ActorHost host, DaprClient daprClient, ILogger<GeneratorActor> logger)
    : Actor(host), IGeneratorActor
{
    private const string StateName = "generator";
    private const string TimerName = "generator-tick";
    private const string EgressTopicPrefix = "sf-source-";

    /// <summary>Wishlist #9(b)'s drain timer name — see this class's doc comment.</summary>
    private const string LoopbackDrainTimerName = "generator-loopback-drain";

    /// <summary>Fixed batch cadence for every generator, regardless of its configured EventsPerSecond —
    /// well inside decision D-E's "≤20Hz" budget (5x margin) and comfortably above the "never per-event,
    /// 50ms minimum" floor from the wave brief.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(200);

    /// <summary>Wishlist #9(b): the loopback drain cadence. Still a sidecar round-trip per fire (same
    /// D-E reasoning as <see cref="TickPeriod"/> applies to ANY Dapr actor timer), so this does not drop
    /// to Orleans' 20ms — 100ms keeps a source's own sidecar timer traffic bounded while still giving the
    /// "scenario clock" use case (wishlist #9's own framing: step t+1 should start soon after step t's
    /// downstream table settles) a sub-tick-period turnaround.</summary>
    private static readonly TimeSpan LoopbackDrainPeriod = TimeSpan.FromMilliseconds(100);

    /// <summary>Caps how many rows one drain tick will publish (chunked further by
    /// <see cref="RunBatchChunkSize"/> on the way out) — a burst, or a tight unbounded cycle (see
    /// <c>LoopbackHub</c>'s doc comment on that case), cannot hold one timer fire indefinitely; anything
    /// left over is picked up on the next drain tick, not lost.</summary>
    private const int LoopbackDrainBatchCap = 2000;

    /// <summary>Wishlist #8's RunAsync chunk size — same "batched, not per-event" reasoning as
    /// <see cref="TickPeriod"/> above, sized for a one-off large batch rather than a steady tick rate.</summary>
    private const int RunBatchChunkSize = 500;

    private SourceDefinition? _def;
    private bool _running;
    private bool _timerArmed;
    private double _carry;
    private DateTimeOffset _lastTickAt;

    /// <summary>Wishlist #9(b): persisted separately from <see cref="_running"/> — "this source has been
    /// StartAsync'd and not yet StopAsync'd", independent of whether its TICK timer happens to be armed
    /// (a scenario-profile / run-on-demand-only source is never <see cref="_running"/> but must still be
    /// a reachable loopback target the whole time it is started). See this class's class doc.</summary>
    private bool _started;

    private bool _loopbackDrainTimerArmed;

    /// <summary>Wishlist #9(b): per-RunId continuation state for <c>request.Step</c> — see
    /// <c>ScenarioRunState</c>'s doc comment for its in-memory-only lifecycle. NOT persisted (unlike
    /// <see cref="_def"/>/<see cref="_running"/>): a step sequence does not survive this actor's
    /// deactivation, a documented limitation (System.Random carries no publicly extractable/round-
    /// trippable state to persist it faithfully). Cleared on every StartAsync/StopAsync.</summary>
    private readonly Dictionary<string, ScenarioRunState> _runStates = new(StringComparer.Ordinal);

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<GeneratorActorState>(StateName);
        if (existing.HasValue)
        {
            _def = existing.Value.Def;
            _running = existing.Value.Running;
            _started = existing.Value.Started;
        }

        // Wishlist #9(b): re-attach + re-arm the loopback drain independent of _running — see this
        // class's doc comment for why _started (not _running) gates it.
        if (_started)
        {
            LoopbackHub.Attach(Id.GetId());
            await ArmLoopbackDrainTimerAsync();
        }

        // Self-heal: a fresh activation never has a timer registered yet (Dapr timers don't survive
        // deactivation) — if the last StartAsync we persisted left this generator Running, re-arm
        // immediately instead of waiting for GeneratorSupervisorService's next sweep.
        if (_running && _def is { EventsPerSecond: > 0 })
        {
            await ArmTimerAsync();
        }
    }

    public async Task StartAsync(SourceDefinition def)
    {
        await DisarmTimerIfArmedAsync();
        await DisarmLoopbackDrainTimerIfArmedAsync();
        _runStates.Clear();

        _def = def;
        _running = def.EventsPerSecond > 0;
        _started = true;
        await SaveAsync();

        // Wishlist #9(b): attach + arm the drain loop regardless of EventsPerSecond — see this class's
        // doc comment for why _started, not _running, gates it.
        LoopbackHub.Attach(Id.GetId());
        await ArmLoopbackDrainTimerAsync();

        if (_running)
        {
            await ArmTimerAsync();
        }
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();
        await DisarmLoopbackDrainTimerIfArmedAsync();
        // Detach LAST: any LoopbackSinkClient still writing after this returns false, reported as a
        // failure by the caller — see LoopbackHub.Detach's doc comment.
        LoopbackHub.Detach(Id.GetId());
        _runStates.Clear();

        _running = false;
        _started = false;
        await SaveAsync();
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_running);

    /// <summary>Wishlist #8 (whole-batch) and #9(b) (<c>request.Step</c> — see
    /// <see cref="ScenarioRunRequest.Step"/>'s doc comment for the full stepping contract) — Dapr mirror
    /// of Orleans' <c>GeneratorGrain.RunAsync</c> (orleans/src/StreamForge.Host/Grains/GeneratorGrain.cs).
    /// Delegates the whole spec/request-validation + deterministic row-math decision to the pure, TOTAL
    /// <c>ScenarioGenerator</c> (shared/StreamForge.AppCore/Generators/ScenarioGenerator.cs — the exact
    /// same calls Orleans' GeneratorGrain makes, so both flavors produce byte-identical rows for the same
    /// seed/RunId, step-by-step or whole) and, only when rows come back, publishes them on this actor's
    /// two existing topics via the same batched-envelope shape <see cref="TickAsync"/> uses (see this
    /// class's "batched, not per-event" decision D-E doc — a run-on-demand batch is exactly the
    /// unbounded-tick-count case that doc warns against, so it is chunked into <see cref="RunBatchChunkSize"/>-
    /// row envelopes rather than either one huge envelope or one sidecar round-trip per row).
    ///
    /// <para><b>Step mode</b> (<c>request.Step == true</c>): mirrors GeneratorGrain.RunAsync's step branch
    /// exactly — the FIRST step call for a RunId calls <c>ScenarioGenerator.BeginRun</c> and caches the
    /// state in <see cref="_runStates"/> (see that field's doc comment for why it is NOT persisted, unlike
    /// <see cref="_def"/>); every later step call for the same RunId reuses it and ignores that call's
    /// Seed/Overrides. Each call emits one day; past the end of the run it is Accepted with 0 rows, never
    /// an error.</para>
    ///
    /// <para>Reachable via <c>ActorProxy.Create&lt;IGeneratorActor&gt;</c> — <see cref="IGeneratorActor.RunAsync"/>
    /// already declares this method and <c>DaprFacades.RunSourceAsync</c> already forwards to it, so
    /// there is no separate wiring gap left to close for either whole-batch or step mode.</para>
    /// </summary>
    public async Task<ScenarioRunResult> RunAsync(ScenarioRunRequest request)
    {
        if (_def is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        List<ScenarioRow> rows;
        if (request.Step)
        {
            if (!_runStates.TryGetValue(request.RunId, out var state))
            {
                if (!ScenarioGenerator.BeginRun(_def, request, out state, out var failure))
                {
                    return failure!;
                }

                _runStates[request.RunId] = state!;
            }

            rows = ScenarioGenerator.GenerateDay(state!, nowMs);
            if (rows.Count == 0 && state!.IsComplete)
            {
                // Wishlist #9(b): stepping past the end of a run is a no-op, not an error.
                return new ScenarioRunResult { Outcome = ScenarioRunOutcome.Accepted, Accepted = 0, Rows = [] };
            }
        }
        else
        {
            var result = ScenarioGenerator.GenerateBatch(_def, request, nowMs);
            if (result.Outcome != ScenarioRunOutcome.Accepted)
            {
                return result;
            }

            rows = result.Rows;
        }

        foreach (var chunk in rows.Chunk(RunBatchChunkSize))
        {
            var events = chunk.Select(row => (Dictionary<string, object?>)ScenarioGenerator.ToEventRecord(row, _def.Name)).ToList();
            var envelope = new SourceEventsEnvelope { Source = _def.Name, Events = events };
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + _def.Name, envelope);
        }

        return new ScenarioRunResult { Outcome = ScenarioRunOutcome.Accepted, Accepted = rows.Count, Rows = rows };
    }

    /// <summary>Named "SaveAsync", not "SaveStateAsync", to avoid hiding <see cref="Actor.SaveStateAsync"/>
    /// (that base member flushes StateManager's buffered changes; this project instead writes state
    /// immediately via <c>StateManager.SetStateAsync</c>, same convention as
    /// <see cref="RegistryActor"/>'s own private <c>SaveAsync</c>).</summary>
    private Task SaveAsync() => StateManager.SetStateAsync(
        StateName, new GeneratorActorState { Def = _def, Running = _running, Started = _started });

    private async Task ArmTimerAsync()
    {
        _lastTickAt = DateTimeOffset.UtcNow;
        _carry = 0;
        await RegisterTimerAsync(TimerName, nameof(TickAsync), null, TickPeriod, TickPeriod);
        _timerArmed = true;
    }

    private async Task DisarmTimerIfArmedAsync()
    {
        if (!_timerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(TimerName);
        _timerArmed = false;
    }

    private async Task ArmLoopbackDrainTimerAsync()
    {
        await RegisterTimerAsync(LoopbackDrainTimerName, nameof(DrainLoopbackAsync), null, LoopbackDrainPeriod, LoopbackDrainPeriod);
        _loopbackDrainTimerArmed = true;
    }

    private async Task DisarmLoopbackDrainTimerIfArmedAsync()
    {
        if (!_loopbackDrainTimerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(LoopbackDrainTimerName);
        _loopbackDrainTimerArmed = false;
    }

    /// <summary>Wishlist #9(b): the loopback drain tick — see this class's doc comment and
    /// <c>LoopbackHub</c>'s for the full design, in particular why this MUST be a scheduled timer callback
    /// and never a continuation chained off a <c>LoopbackSinkClient</c> write (that is what keeps a
    /// feedback cycle from overflowing the stack or deadlocking). Same publish shape as
    /// <see cref="TickAsync"/> — chunked, both topics, swallow-and-log on a transient sidecar failure so a
    /// hiccup doesn't tear down the timer.
    ///
    /// <para><b>KNOWN GAP, stated rather than left to be found</b> (same tradeoff Orleans'
    /// <c>GeneratorGrain.DrainLoopbackAsync</c> documents): this path does NOT run
    /// <see cref="SourceDefinition.OnCoercionFailure"/> field-type coercion against the drained row — it
    /// republishes exactly what the sink handed <c>LoopbackHub</c>, on the theory that a loopback row
    /// already comes from a live table's own already-typed output.</para></summary>
    private async Task DrainLoopbackAsync()
    {
        if (_def is null)
        {
            return;
        }

        var rows = LoopbackHub.Drain(Id.GetId(), LoopbackDrainBatchCap);
        if (rows.Count == 0)
        {
            return;
        }

        // Fresh _source/_ts on arrival — a row drained here is a NEW event at THIS source, same as
        // ScenarioGenerator/MarketDataProfiles rows are, regardless of whatever stamped values (if any)
        // the upstream table's own row happened to carry.
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var row in rows)
        {
            row[EventRecord.SourceField] = _def.Name;
            row[EventRecord.TimestampField] = nowMs;
        }

        try
        {
            foreach (var chunk in rows.Chunk(RunBatchChunkSize))
            {
                var envelope = new SourceEventsEnvelope { Source = _def.Name, Events = chunk.ToList() };
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + _def.Name, envelope);
            }
        }
        catch (Exception ex)
        {
            // Same resilience reasoning as TickAsync's own catch: a transient sidecar hiccup must not
            // tear down this timer. Rows already dequeued from LoopbackHub are NOT requeued on failure —
            // same at-most-once, fire-and-forget ceiling every sink in this codebase already has.
            logger.LogWarning(ex, "GeneratorActor[{Source}]: failed to publish a loopback-drained batch of {Count} event(s) — will retry next drain tick (rows already dequeued are not requeued).", _def.Name, rows.Count);
        }
    }

    /// <summary>Timer callback (registered by name — see <see cref="ArmTimerAsync"/>; Dapr fires this via
    /// a sidecar round-trip on <see cref="TickPeriod"/>). Zero-parameter: this actor needs no per-tick
    /// argument, everything comes from <see cref="_def"/> cached at <see cref="StartAsync"/>/
    /// <see cref="OnActivateAsync"/> time.</summary>
    private async Task TickAsync()
    {
        if (_def is null || !_running)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var elapsed = now - _lastTickAt;
        _lastTickAt = now;

        var count = GeneratorBatching.NextBatchCount(_def.EventsPerSecond, elapsed, ref _carry);
        if (count <= 0)
        {
            return;
        }

        var events = new List<Dictionary<string, object?>>(count);
        for (var i = 0; i < count; i++)
        {
            // MarketDataProfiles.GenerateEvent returns an EventRecord (a Dictionary<string, object?>
            // subclass) — already plain CLR values, no JsonElement in sight (this is the publish SIDE;
            // JsonValueNormalizer runs at every topic's INGRESS, i.e. in the subscriber, per decision D-D).
            events.Add(MarketDataProfiles.GenerateEvent(_def));
        }

        var envelope = new SourceEventsEnvelope { Source = _def.Name, Events = events };

        try
        {
            // sf-sources: the router (Streaming/StreamingRuntimeSetup.cs) fans this out in-host.
            // sf-source-{name}: publish-only egress copy for polyglot subscribers — nothing in this
            // process subscribes to it (see Streaming/Sinks.cs's class doc); both topics ride the same
            // "pubsub" component.
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + _def.Name, envelope);
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not tear down the timer (that would silently stop this
            // source generating until GeneratorSupervisorService's next ~15s sweep re-arms it) — drop
            // this tick's batch and let the next tick try again with a fresh elapsed/carry baseline.
            logger.LogWarning(ex, "GeneratorActor[{Source}]: failed to publish a batch of {Count} event(s) — will retry next tick.", _def.Name, count);
        }
    }
}

/// <summary>Persisted shape of a GeneratorActor's state — see that class's doc comment for why the
/// definition and running flag are persisted at all (self-healing across deactivation/reactivation).
/// Plain get/set properties (same style as <see cref="Catalog.CatalogState"/>) for a clean
/// System.Text.Json round trip through Dapr's actor state store.</summary>
public sealed class GeneratorActorState
{
    public SourceDefinition? Def { get; set; }

    public bool Running { get; set; }

    /// <summary>Wishlist #9(b), additive: "StartAsync has been called and StopAsync has not been called
    /// since" — see <see cref="GeneratorActor"/>'s <c>_started</c> field doc comment for why this is
    /// separate from <see cref="Running"/>. Missing on an OLDER persisted state (pre-#9(b)) deserializes
    /// to false, same as any other new bool property — a generator activated once before this change
    /// shipped simply re-attaches to the loopback hub on its next StartAsync, not before.</summary>
    public bool Started { get; set; }
}

/// <summary>
/// Pure EPS×Δt batching math, extracted from <see cref="GeneratorActor"/> specifically so it can be unit
/// tested without any actor/timer/Dapr-sidecar machinery (see dapr/tests/StreamForge.Dapr.Tests/
/// GeneratorBatchingTests.cs). Given a target events-per-second rate and the wall-clock time elapsed
/// since the previous tick, returns how many whole events to emit THIS tick, carrying the fractional
/// remainder forward across calls via <paramref name="carry"/> so the long-run average rate converges on
/// exactly <paramref name="eventsPerSecond"/> — simply flooring each tick's <c>eventsPerSecond × elapsed</c>
/// without carrying the remainder would systematically under-deliver (worst case: a source configured
/// below one event per tick would emit nothing, ever).
/// </summary>
public static class GeneratorBatching
{
    public static int NextBatchCount(double eventsPerSecond, TimeSpan elapsed, ref double carry)
    {
        if (eventsPerSecond <= 0 || elapsed <= TimeSpan.Zero)
        {
            return 0;
        }

        var exact = eventsPerSecond * elapsed.TotalSeconds + carry;
        var count = (int)Math.Floor(exact);
        carry = exact - count;
        return count;
    }
}
