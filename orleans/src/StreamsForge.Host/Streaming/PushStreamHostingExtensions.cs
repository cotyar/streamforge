using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace StreamsForge.Host.Streaming;

/// <summary>Registers the push transport (<see cref="PushStreamProvider"/>) under a stream provider name,
/// as a drop-in replacement for <c>siloBuilder.AddMemoryStreams(name)</c>.</summary>
public static class PushStreamHostingExtensions
{
    /// <summary>
    /// Registers a keyed <see cref="IStreamProvider"/> named <paramref name="name"/> backed by a single
    /// process-wide <see cref="PushStreamBus"/>.
    ///
    /// Registration lives on the SILO's service collection, which in this co-hosted process is also the
    /// web app's container — so <c>client.GetStreamProvider(name)</c> (StreamBridgeService, the gRPC
    /// streaming services) resolves the very same provider instance grains resolve via
    /// <c>this.GetStreamProvider(name)</c>. That shared container is exactly why an in-process push bus is
    /// a legitimate transport for this flavor (single-silo localhost clustering is a documented constraint
    /// — see orleans/ARCHITECTURE.md); it is also why the provider is NOT registered on a separate external
    /// client builder, which would get its own container and therefore its own, disconnected bus.
    /// </summary>
    public static ISiloBuilder AddPushStreams(this ISiloBuilder builder, string name, int capacity)
    {
        builder.Services.TryAddSingleton(sp => new PushStreamBus(
            capacity,
            sp.GetService<ILoggerFactory>()?.CreateLogger(typeof(PushStreamBus).FullName!),
            sp.GetService<DeepCopier>()));

        builder.Services.AddKeyedSingleton<IStreamProvider>(name, (sp, key) => new PushStreamProvider(
            (string)key!,
            sp.GetRequiredService<PushStreamBus>(),
            sp.GetService<IGrainContextAccessor>()));

        return builder;
    }
}
