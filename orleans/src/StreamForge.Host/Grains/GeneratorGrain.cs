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

    /// <summary>Wishlist #8 — see IGeneratorGrain.RunAsync's doc comment for the contract. NotFound when
    /// this activation has never been StartAsync'd (no _def on file yet); otherwise delegates the whole
    /// spec/request-validation + row-math decision to the pure, TOTAL
    /// <see cref="MarketDataProfiles"/>-sibling <c>ScenarioGenerator.GenerateBatch</c> and — only for
    /// <see cref="ScenarioRunOutcome.Accepted"/> — publishes every row.
    ///
    /// <para><b>"Honouring MaxBatchRows/backpressure" (wishlist wording), as implemented here.</b>
    /// MaxBatchRows is enforced as a hard config-validation cap (ScenarioSpec.MaxBatchRows / Validate) —
    /// a run either emits the WHOLE batch or none of it, never a partial one, which is the same "never a
    /// partial admit" shape <see cref="IngestConfig.MaxBatchRows"/> already uses for push ingress
    /// (IngestModels.cs's header comment). Backpressure: rows are published ONE AT A TIME with `await
    /// stream.OnNextAsync(...)` in a loop, exactly like TickAsync below — Orleans' stream provider applies
    /// its own admission/queueing under that await, so a slow consumer genuinely holds this loop up rather
    /// than this method firing N*K*D publishes without ever yielding for one. This is a narrower claim
    /// than IngestConfig's own buffer+overflow-policy machinery (IngestModels.cs's header note on why
    /// there is no true end-to-end backpressure in this architecture); a shared, observable admission
    /// buffer for run-on-demand batches — mirroring SourceIngressBuffer — is out of scope for this
    /// change (would require extending the Ingest facade seam, which is intentionally untouched here).</para>
    /// </summary>
    public async Task<ScenarioRunResult> RunAsync(ScenarioRunRequest request)
    {
        if (_def is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        var result = ScenarioGenerator.GenerateBatch(_def, request, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        if (result.Outcome != ScenarioRunOutcome.Accepted)
        {
            return result;
        }

        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, _def.Name));
        foreach (var row in result.Rows)
        {
            await stream.OnNextAsync(ScenarioGenerator.ToEventRecord(row, _def.Name));
        }

        return result;
    }

    private async Task TickAsync()
    {
        if (_def is null)
        {
            return;
        }

        var evt = MarketDataProfiles.GenerateEvent(_def);
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, _def.Name));
        await stream.OnNextAsync(evt);
    }
}
