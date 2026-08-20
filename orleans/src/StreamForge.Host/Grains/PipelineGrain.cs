using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Engine;
using StreamForge.Host.Facades;

namespace StreamForge.Host.Grains;

/// <summary>Key = pipeline id. One activation per running pipeline. Subscribes to its SQL's source
/// streams, feeds events through a <see cref="PipelineExecutor"/>, and publishes emitted rows +
/// periodic metrics back onto Orleans streams for <see cref="Services.StreamBridgeService"/> to relay.</summary>
public sealed class PipelineGrain : Grain, IPipelineGrain
{
    private const int RecentResultsCapacity = 100;
    private const int MetricsEveryNTicks = 4; // 4 * 500ms ≈ 2s

    private PipelineDefinition? _def;
    private PipelineStatus _status = PipelineStatus.Stopped;
    private PipelineExecutor? _executor;
    private IGrainTimer? _timer;
    private readonly List<StreamSubscriptionHandle<EventRecord>> _subscriptions = [];
    private readonly List<ResultEnvelope> _recentResults = [];

    private long _seq;
    private int _tickCount;
    private long _totalEventsIn;
    private long _totalRowsOut;
    private long _lastEventTsMs;
    private long _eventsInAtLastMetricsTick;
    private long _rowsOutAtLastMetricsTick;
    private DateTimeOffset _lastMetricsTickAt;
    private double _lastEventsInPerSec;
    private double _lastRowsOutPerSec;

    public async Task StartAsync(PipelineDefinition def)
    {
        await StopAsync();

        _def = def;

        // Plan 021 D5 — a grain acting on an entity it was just handed reads that entity's OWN Environment
        // field rather than any ambient: def.Environment was stamped by the registry that created it and
        // never edited afterwards, so it is the durable answer to "which catalog does this pipeline belong
        // to" even though IPipelineGrain itself stays keyed by GUID (D3's one exception), not by environment.
        var registry = GrainFactory.RegistryFor(def.Environment);
        var sources = await registry.GetSourcesAsync();
        var schemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.Compile(def.Sql, schemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            throw new InvalidOperationException(message);
        }

        _executor = compileResult.Plan.CreateExecutor();
        _status = PipelineStatus.Running;

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        foreach (var sourceName in compileResult.SourceNames.Distinct())
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, EnvKeys.Qualify(def.Environment, sourceName)));
            var handle = await stream.SubscribeAsync((evt, _) => OnSourceEventAsync(sourceName, evt));
            _subscriptions.Add(handle);
        }

        var now = DateTimeOffset.UtcNow;
        _lastMetricsTickAt = now;
        _timer = this.RegisterGrainTimer(OnTimerTickAsync, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500));

        // Keep this activation alive for as long as the pipeline is running — a grain with no
        // pending calls would otherwise be collected by idle-activation GC despite the live
        // stream subscriptions and timer.
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task StopAsync()
    {
        _status = PipelineStatus.Stopped;

        _timer?.Dispose();
        _timer = null;

        foreach (var handle in _subscriptions)
        {
            try
            {
                await handle.UnsubscribeAsync();
            }
            catch
            {
                // best-effort; the subscription's silo-side state is torn down regardless
            }
        }
        _subscriptions.Clear();

        _executor = null;

        // Cancel the earlier keep-alive; TimeSpan.Zero restores normal idle-activation GC.
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit)
    {
        var take = Math.Max(0, Math.Min(limit, _recentResults.Count));
        var start = _recentResults.Count - take;
        return Task.FromResult(_recentResults.GetRange(start, take));
    }

    public Task<PipelineMetrics> GetMetricsAsync() => Task.FromResult(new PipelineMetrics
    {
        PipelineId = _def?.Id ?? this.GetPrimaryKeyString(),
        Status = _status,
        EventsInPerSec = _lastEventsInPerSec,
        RowsOutPerSec = _lastRowsOutPerSec,
        TotalEventsIn = _totalEventsIn,
        TotalRowsOut = _totalRowsOut,
        WindowsClosed = 0,
        LastEventTsMs = _lastEventTsMs,
    });

    private async Task OnSourceEventAsync(string sourceName, EventRecord evt)
    {
        if (_executor is null)
        {
            return;
        }

        _totalEventsIn++;
        _lastEventTsMs = evt.Timestamp;

        var rows = _executor.OnEvent(sourceName, evt);
        if (rows.Count > 0)
        {
            await PublishRowsAsync(rows);
        }
    }

    private async Task OnTimerTickAsync()
    {
        if (_executor is null)
        {
            return;
        }

        var rows = _executor.AdvanceWatermark(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        if (rows.Count > 0)
        {
            await PublishRowsAsync(rows);
        }

        _tickCount++;
        if (_tickCount % MetricsEveryNTicks == 0)
        {
            await PublishMetricsAsync();
        }
    }

    private async Task PublishRowsAsync(IReadOnlyList<EventRecord> rows)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = new List<ResultEnvelope>(rows.Count);

        foreach (var row in rows)
        {
            _seq++;
            _totalRowsOut++;

            var envelope = new ResultEnvelope
            {
                PipelineId = _def!.Id,
                Seq = _seq,
                TimestampMs = row.Timestamp != 0 ? row.Timestamp : nowMs,
                Row = new Dictionary<string, object?>(row),
            };

            batch.Add(envelope);
            _recentResults.Add(envelope);
        }

        if (_recentResults.Count > RecentResultsCapacity)
        {
            _recentResults.RemoveRange(0, _recentResults.Count - RecentResultsCapacity);
        }

        // Plan 021 D6 — self-publish onto THIS pipeline's own output stream: this.GetPrimaryKeyString() is
        // already the D3-qualified key (IPipelineGrain is qualified uniformly like every other name/id-keyed
        // grain kind), so it is correct here without re-deriving anything from _def.
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, this.GetPrimaryKeyString()));
        await stream.OnNextAsync(batch);
    }

    private async Task PublishMetricsAsync()
    {
        var now = DateTimeOffset.UtcNow;
        var elapsedSec = Math.Max(0.001, (now - _lastMetricsTickAt).TotalSeconds);

        _lastEventsInPerSec = (_totalEventsIn - _eventsInAtLastMetricsTick) / elapsedSec;
        _lastRowsOutPerSec = (_totalRowsOut - _rowsOutAtLastMetricsTick) / elapsedSec;

        _eventsInAtLastMetricsTick = _totalEventsIn;
        _rowsOutAtLastMetricsTick = _totalRowsOut;
        _lastMetricsTickAt = now;

        var metrics = await GetMetricsAsync();

        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<PipelineMetrics>(StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.MetricsKey));
        await stream.OnNextAsync(metrics);
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
