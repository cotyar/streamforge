using Dapr.Actors;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: actor-invocation surface for one source's synthetic-event
/// generator — actor type "GeneratorActor" (see <see cref="GeneratorActor"/>), key = the source's
/// <see cref="SourceDefinition.Name"/>. Dapr counterpart of Orleans' <c>IGeneratorGrain</c>
/// (orleans/src/StreamsForge.Host/Grains/GeneratorGrain.cs); no <c>PingAsync</c> here — see
/// <see cref="GeneratorActor"/>'s class doc for why this flavor self-heals from persisted state instead
/// of relying on a keep-alive ping.
/// </summary>
public interface IGeneratorActor : IActor
{
    /// <summary>Wishlist #8's run-on-demand: generate a scenario batch and publish it, once. Declared
    /// here and not only on the actor class because <c>ActorProxy.Create&lt;IGeneratorActor&gt;</c> can only
    /// dispatch what the interface declares — without this line the implementation exists and is
    /// unreachable, which is precisely how the REST endpoint ended up unable to publish anything.</summary>
    Task<ScenarioRunResult> RunAsync(ScenarioRunRequest request);

    /// <summary>(Re)starts this source's generator with the given definition, replacing any timer from a
    /// previous call. A definition with <c>EventsPerSecond &lt;= 0</c> is accepted but registers no
    /// timer (mirrors Orleans' <c>GeneratorGrain.StartAsync</c>). Idempotent — safe to call repeatedly
    /// with the current definition; this is exactly what <see cref="Services.GeneratorSupervisorService"/>'s
    /// periodic sweep does.</summary>
    Task StartAsync(SourceDefinition def);

    /// <summary>Stops the generator (unregisters its timer, if one is armed). Idempotent.</summary>
    Task StopAsync();

    /// <summary>True if this actor currently has a live timer armed (backed by persisted state — see
    /// <see cref="GeneratorActor"/>'s class doc). Not currently read by any endpoint (the catalog's own
    /// <c>SourceDefinition.Enabled</c> is the REST-visible truth) but exposed for diagnostics/tests.</summary>
    Task<bool> IsRunningAsync();
}
