using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Generators;

namespace StreamForge.Host.Grains;

/// <summary>Key = source name. Publishes one synthetic event per tick on a grain timer.</summary>
public sealed class GeneratorGrain : Grain, IGeneratorGrain
{
    private SourceDefinition? _def;
    private IGrainTimer? _timer;

    public Task StartAsync(SourceDefinition def)
    {
        _def = def;
        _timer?.Dispose();
        _timer = null;

        if (def.EventsPerSecond <= 0)
        {
            return Task.CompletedTask;
        }

        var intervalMs = Math.Clamp(1000.0 / def.EventsPerSecond, 1, 10_000);
        var period = TimeSpan.FromMilliseconds(intervalMs);
        _timer = this.RegisterGrainTimer(TickAsync, period, period);
        return Task.CompletedTask;
    }

    public Task StopAsync()
    {
        _timer?.Dispose();
        _timer = null;
        return Task.CompletedTask;
    }

    public Task PingAsync() => Task.CompletedTask;

    private async Task TickAsync()
    {
        if (_def is null)
        {
            return;
        }

        var evt = MarketDataProfiles.GenerateEvent(_def.GeneratorProfile, _def.Name);
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, _def.Name));
        await stream.OnNextAsync(evt);
    }
}
