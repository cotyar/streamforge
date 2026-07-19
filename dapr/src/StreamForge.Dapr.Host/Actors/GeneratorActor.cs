using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Streaming;
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
/// </summary>
public sealed class GeneratorActor(ActorHost host, DaprClient daprClient, ILogger<GeneratorActor> logger)
    : Actor(host), IGeneratorActor
{
    private const string StateName = "generator";
    private const string TimerName = "generator-tick";
    private const string EgressTopicPrefix = "sf-source-";

    /// <summary>Fixed batch cadence for every generator, regardless of its configured EventsPerSecond —
    /// well inside decision D-E's "≤20Hz" budget (5x margin) and comfortably above the "never per-event,
    /// 50ms minimum" floor from the wave brief.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(200);

    private SourceDefinition? _def;
    private bool _running;
    private bool _timerArmed;
    private double _carry;
    private DateTimeOffset _lastTickAt;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<GeneratorActorState>(StateName);
        if (existing.HasValue)
        {
            _def = existing.Value.Def;
            _running = existing.Value.Running;
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

        _def = def;
        _running = def.EventsPerSecond > 0;
        await SaveAsync();

        if (_running)
        {
            await ArmTimerAsync();
        }
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();
        _running = false;
        await SaveAsync();
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_running);

    /// <summary>Named "SaveAsync", not "SaveStateAsync", to avoid hiding <see cref="Actor.SaveStateAsync"/>
    /// (that base member flushes StateManager's buffered changes; this project instead writes state
    /// immediately via <c>StateManager.SetStateAsync</c>, same convention as
    /// <see cref="RegistryActor"/>'s own private <c>SaveAsync</c>).</summary>
    private Task SaveAsync() => StateManager.SetStateAsync(StateName, new GeneratorActorState { Def = _def, Running = _running });

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
