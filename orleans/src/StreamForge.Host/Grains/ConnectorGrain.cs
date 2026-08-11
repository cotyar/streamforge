using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Transports;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Connectors.Scheduling;
using StreamForge.Engine;

namespace StreamForge.Host.Grains;

public sealed class ConnectorGrainState
{
    public SourceDefinition? Def { get; set; }
    public bool Running { get; set; }
    public List<string> DedupKeys { get; set; } = [];
    public Dictionary<string, long> Ledger { get; set; } = [];
    public int ConsecutiveFailures { get; set; }
    public long? LastRunMs { get; set; }
    public long? NextRunMs { get; set; }
    /// <summary>"never" | "ok" | "error" | "connecting" (gRPC/NATS-kind only, transient — see
    /// GrpcSubscriberCore/NatsSubscriberCore's onStatus contract).</summary>
    public string LastStatus { get; set; } = "never";
    public string? LastError { get; set; }
    public long EventsEmittedTotal { get; set; }
    /// <summary>Plan 009 C2: cumulative field coercion failures — see ConnectorRuntimeStatus's own doc.</summary>
    public long CoercionFailuresTotal { get; set; }
    public int LastBatchCount { get; set; }
}

/// <summary>Grain-internal companion to <see cref="IConnectorGrain"/>: the gRPC subscriber's onStatus
/// callback (plan 006 D-G, GrpcSubscriberCore) runs off this grain's turn — a background Task, not a
/// grain-context thread — and must not touch grain state directly, exactly like the onRows callback
/// that reaches <see cref="IConnectorGrain.EmitRowsAsync"/> via a captured self-reference. Status
/// transitions ("connecting"/"ok"/"error") route back the same way, through this second self-reference,
/// so both callbacks re-enter the grain's normal single-turn message queue instead of racing it.
/// Deliberately NOT folded into IConnectorGrain itself (that interface's public surface is pinned by
/// plan 006 W3A) — this is purely an internal wiring detail of ConnectorGrain's gRPC path, so it lives
/// here rather than in the frozen GrainInterfaces.cs.</summary>
public interface IConnectorStatusSink : IGrainWithStringKey
{
    Task ReportStatusAsync(string status, string? error);

    /// <summary>Plan 009 C2: accumulate coercion failures WITHOUT touching status or LastError. Separate
    /// from ReportStatusAsync on purpose — the two would otherwise race for LastError and the counting
    /// call, arriving second, would erase the very note the status call had just written.</summary>
    Task ReportCoercionFailuresAsync(int count);
}

/// <summary>Key = source name. The Orleans driver for connector-kind sources (plan 006 W3A) — mirrors
/// GeneratorGrain's shape (StartAsync/StopAsync/PingAsync on a grain timer) but composes the shared,
/// pure AppCore.Connectors cores (ConnectorPollCycle, ScheduleCalc/BackoffPolicy, DedupTracker/
/// FileLedger, GrpcSubscriberCore) instead of MarketDataProfiles. RegistryGrain dispatches "generator"
/// -kind sources to IGeneratorGrain and everything else here (SourceKinds.Url/File/Folder/Grpc) — this
/// grain never sees Kind == "generator".
///
/// url/file/folder: a ONE-SHOT grain timer (dueTime = next-run delay, period = Infinite; each fire runs
/// one cycle then re-arms) — cron correctness requires recomputing the next occurrence after every fire
/// rather than a fixed period. grpc: a persistent background subscription (GrpcSubscriberCore.RunAsync)
/// started on StartAsync and cancelled on StopAsync; its Schedule is ignored (D-B).
///
/// Emission goes through the same door GeneratorGrain uses: one EventRecord per row onto
/// (StreamConstants.SourcesNamespace, sourceName) — pipelines/tables/SignalR/SPA all work unchanged
/// for a connector-kind source's events.
///
/// State ([PersistentState("connector", ...)]) is written after every completed cycle (poll rates are
/// low — D-E's 1s floor) so status/dedup/ledger survive a silo recycle; OnActivateAsync self-resumes
/// (re-arms the timer / restarts the subscriber) when persisted Running is true, mirroring
/// RegistryGrain.EnsureInitializedAsync's boot-resume for generators/pipelines/tables.</summary>
public sealed class ConnectorGrain(
    [PersistentState("connector", StreamConstants.StorageName)] IPersistentState<ConnectorGrainState> state)
    : Grain, IConnectorGrain, IConnectorStatusSink
{
    /// <summary>D-D fallback schedule for a connector with no explicit ScheduleSpec.</summary>
    private static readonly ScheduleSpec DefaultSchedule = new() { IntervalMs = 30_000 };

    /// <summary>10 MB response cap (design D-D/W3A pin) — checked against Content-Length up front, and
    /// again against the actually-read body as a belt-and-braces fallback for chunked responses that
    /// carry no Content-Length header at all.</summary>
    private const long MaxUrlResponseBytes = 10 * 1024 * 1024;

    private static readonly HttpClient SharedHttpClient = new() { Timeout = TimeSpan.FromSeconds(30) };

    private IGrainTimer? _timer;
    private CancellationTokenSource? _grpcCts;
    private Task? _grpcTask;
    private CancellationTokenSource? _transportCts;
    private Task? _transportTask;
    private DedupTracker? _dedup;
    private FileLedger? _ledger;

    // Emission-counter persist throttle for the gRPC/NATS path (EmitRowsAsync can fire once per remote
    // frame/message — far more often than a poll cycle's natural "persist once per cycle" cadence):
    // persist at most every 50 batches or 5 seconds, whichever comes first (design pin).
    private int _batchesSincePersist;
    private DateTime _lastPersistUtc = DateTime.MinValue;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.Running && state.State.Def is not null)
        {
            EnsureTrackers();
            ArmForKind(state.State.Def, persistNextRun: false);
        }
        return base.OnActivateAsync(cancellationToken);
    }

    public async Task StartAsync(SourceDefinition def)
    {
        state.State.Def = def;
        state.State.Running = true;
        EnsureTrackers();

        _timer?.Dispose();
        _timer = null;
        CancelGrpc();
        CancelTransport();

        ArmForKind(def, persistNextRun: true);
        await state.WriteStateAsync();

        if (def.Kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder)
        {
            ArmTimer(NextRunDelay(def));
        }
    }

    public async Task StopAsync()
    {
        state.State.Running = false;
        _timer?.Dispose();
        _timer = null;
        CancelGrpc();
        CancelTransport();
        await state.WriteStateAsync();
    }

    public Task PingAsync() => Task.CompletedTask;

    public Task<ConnectorRuntimeStatus> GetStatusAsync() => Task.FromResult(new ConnectorRuntimeStatus
    {
        SourceName = state.State.Def?.Name ?? this.GetPrimaryKeyString(),
        NextRunMs = state.State.NextRunMs,
        LastRunMs = state.State.LastRunMs,
        LastStatus = state.State.LastStatus,
        LastError = state.State.LastError,
        ConsecutiveFailures = state.State.ConsecutiveFailures,
        EventsEmittedTotal = state.State.EventsEmittedTotal,
        CoercionFailuresTotal = state.State.CoercionFailuresTotal,
        LastBatchCount = state.State.LastBatchCount,
    });

    /// <summary>gRPC onRows callback entry (reached via a captured self-reference — see
    /// IConnectorStatusSink's class doc). Decoded rows carry no _source stamp (unlike poll-cycle rows,
    /// which ConnectorPollCycle already stamps) so it is applied here; _ts is stamped only if absent
    /// (a table-kind remote's decoded delta rows may already carry one from the source schema).</summary>
    public async Task EmitRowsAsync(List<Dictionary<string, object?>> rows, long remoteSeq)
    {
        var def = state.State.Def;
        if (def is null)
        {
            return;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (rows.Count > 0)
        {
            var stream = this.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, def.Name));
            foreach (var row in rows)
            {
                row.TryAdd("_source", def.Name);
                row.TryAdd("_ts", nowMs);
                await stream.OnNextAsync(new EventRecord(row));
            }

            state.State.EventsEmittedTotal += rows.Count;
            state.State.LastBatchCount = rows.Count;
        }

        state.State.LastRunMs = nowMs;
        state.State.LastStatus = "ok";
        state.State.LastError = null;
        state.State.ConsecutiveFailures = 0;

        _batchesSincePersist++;
        var elapsed = DateTime.UtcNow - _lastPersistUtc;
        if (_batchesSincePersist >= 50 || elapsed >= TimeSpan.FromSeconds(5))
        {
            // Plan 009 B1: the nats path's MappingSpec.DedupKeyField dedup runs on _dedup (via
            // ConnectorPollCycle.Emit inside NatsSubscriberCore) — persist it on the same throttle as
            // everything else here so it survives a silo recycle. Harmless no-op for the grpc path,
            // which never touches _dedup.
            if (_dedup is not null)
            {
                state.State.DedupKeys = _dedup.ToPersistable();
            }

            await state.WriteStateAsync();
            _batchesSincePersist = 0;
            _lastPersistUtc = DateTime.UtcNow;
        }
    }

    /// <summary>gRPC onStatus callback entry (reached via a captured self-reference — see this
    /// interface's class doc on IConnectorStatusSink). "connecting"/"ok" carry no ConsecutiveFailures
    /// change on their own beyond what EmitRowsAsync already does for "ok"; "error" increments it so
    /// GetStatusAsync reflects a struggling subscription even when no rows ever arrive to drive
    /// EmitRowsAsync. Always persists immediately — status transitions are inherently low-frequency
    /// (once per reconnect attempt), unlike EmitRowsAsync's per-frame throttling.</summary>
    public async Task ReportStatusAsync(string status, string? error)
    {
        if (state.State.Def is null)
        {
            return;
        }

        state.State.LastStatus = status;
        state.State.LastError = error;
        if (string.Equals(status, "error", StringComparison.Ordinal))
        {
            state.State.ConsecutiveFailures++;
        }
        else if (string.Equals(status, "ok", StringComparison.Ordinal))
        {
            state.State.ConsecutiveFailures = 0;
        }

        await state.WriteStateAsync();
    }

    /// <summary>Plan 009 C2: the queryable half of "counted and surfaced". The subscriber kinds (grpc,
    /// nats) reach ConnectorRuntimeStatus only through the status channel, so without this their
    /// coercion failures would exist as a LastError string and nowhere countable, while the polled kinds
    /// — which report through EmitRowsAsync's cycle handler — had a real counter. A counter that is
    /// accurate for some source kinds and silently zero for others is worse than no counter.</summary>
    public async Task ReportCoercionFailuresAsync(int count)
    {
        if (count <= 0 || state.State.Def is null)
        {
            return;
        }

        state.State.CoercionFailuresTotal += count;
        await state.WriteStateAsync();
    }

    // ------------------------------------------------------------------
    // Arming
    // ------------------------------------------------------------------

    /// <summary>Common StartAsync/OnActivateAsync-resume logic: for url/file/folder computes and stores
    /// NextRunMs (persisted by the caller); for grpc launches the background subscriber. Does NOT arm
    /// the grain timer itself for the url/file/folder branch when <paramref name="persistNextRun"/> is
    /// false — OnActivateAsync's resume path arms directly off the already-persisted NextRunMs via
    /// <see cref="NextRunDelay"/> after this returns, so a resumed activation doesn't recompute a
    /// (possibly different, now-past) schedule twice.</summary>
    private void ArmForKind(SourceDefinition def, bool persistNextRun)
    {
        if (def.Kind == SourceKinds.Grpc)
        {
            StartGrpcSubscriber(def);
            return;
        }

        // Plan 010: any registered message transport, not a hardcoded nats branch.
        if (InboundTransports.Find(def.Kind) is { } transport)
        {
            StartTransportSubscriber(def, transport);
            return;
        }

        if (def.Kind is SourceKinds.Url or SourceKinds.File or SourceKinds.Folder)
        {
            if (persistNextRun)
            {
                var schedule = def.Connector?.Schedule ?? DefaultSchedule;
                var nowUtc = DateTimeOffset.UtcNow;
                var nextRun = BackoffPolicy.NextRun(schedule, nowUtc, state.State.ConsecutiveFailures);
                state.State.NextRunMs = nextRun?.ToUnixTimeMilliseconds();
            }
            else
            {
                ArmTimer(NextRunDelay(def));
            }
            return;
        }

        // Kind == "generator" (or an unrecognized value) never reaches ConnectorGrain in normal
        // operation — RegistryGrain dispatches those to IGeneratorGrain instead. Defensive no-op.
    }

    /// <summary>Delay until the persisted NextRunMs (falling back to a 30 s retry if unset/invalid —
    /// e.g. a schedule that failed ScheduleCalc.Validate at some point).</summary>
    private TimeSpan NextRunDelay(SourceDefinition def)
    {
        if (state.State.NextRunMs is long nextMs)
        {
            var delay = DateTimeOffset.FromUnixTimeMilliseconds(nextMs) - DateTimeOffset.UtcNow;
            return delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        }
        return TimeSpan.FromSeconds(30);
    }

    private void ArmTimer(TimeSpan dueTime)
    {
        _timer?.Dispose();
        _timer = this.RegisterGrainTimer(RunCycleAsync, dueTime, Timeout.InfiniteTimeSpan);
    }

    private void CancelGrpc()
    {
        if (_grpcCts is null)
        {
            return;
        }
        _grpcCts.Cancel();
        _grpcCts.Dispose();
        _grpcCts = null;
        _grpcTask = null;
    }

    private void StartGrpcSubscriber(SourceDefinition def)
    {
        var cfg = def.Connector?.Grpc
            ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'grpc' but no grpc config");

        _grpcCts?.Cancel();
        _grpcCts?.Dispose();
        _grpcCts = new CancellationTokenSource();
        var ct = _grpcCts.Token;

        // Self-references: the subscriber's callbacks run off this grain's turn (a background Task, not
        // grain-context) — see IConnectorStatusSink's class doc for why both callbacks route through a
        // captured grain reference (a normal, safely-serialized grain call) instead of touching `state`
        // directly.
        var self = this.AsReference<IConnectorGrain>();
        var statusSink = this.AsReference<IConnectorStatusSink>();

        var core = new GrpcSubscriberCore(
            cfg,
            (rows, seq) => HandleDecodedRowsAsync(def, self, statusSink, rows, seq),
            (status, error) => _ = SafeReportStatusAsync(statusSink, status, error));

        _grpcTask = Task.Run(async () =>
        {
            try
            {
                await core.RunAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Expected on StopAsync.
            }
            catch
            {
                // Best-effort — GrpcSubscriberCore.RunAsync already catches and status-reports every
                // failure it can attribute to a specific reconnect attempt; anything escaping here is
                // unexpected but must not crash the background task's thread.
            }
        }, CancellationToken.None);
    }

    /// <summary>Plan 009 C2: gRPC's onRows callback entry point. Unlike NATS (whose rows are already
    /// coerced — <see cref="ConnectorPollCycle.Emit"/> runs inside <see cref="NatsSubscriberCore"/> via
    /// the shared mapping path), a gRPC-decoded row never passes through
    /// <see cref="ConnectorPollCycle"/> at all, so coercion happens HERE, once per callback, before the
    /// rows ever reach <see cref="IConnectorGrain.EmitRowsAsync"/> — "coerce before admission" applies
    /// here exactly as it does for the poll-cycle kinds. A RejectBatch rejection never calls
    /// EmitRowsAsync at all (nothing left behind); Null/DropRow's non-zero failure count is reported
    /// AFTER emission (never before — see <see cref="EmitRowsAsync"/>'s own unconditional
    /// <c>LastError = null</c> on a successful batch, which would otherwise clobber this note if the
    /// order were reversed).</summary>
    private static async Task HandleDecodedRowsAsync(
        SourceDefinition def, IConnectorGrain self, IConnectorStatusSink statusSink,
        IReadOnlyList<Dictionary<string, object?>> rows, long seq)
    {
        var coercion = ConnectorRowCoercion.Apply(def.Fields, rows.ToList(), def.OnCoercionFailure);
        if (coercion.BatchRejected)
        {
            await SafeReportStatusAsync(statusSink, "error", $"coercion rejected batch: {coercion.RejectReason}");
            return;
        }

        await self.EmitRowsAsync(coercion.Rows, seq);

        if (coercion.FailureCount > 0)
        {
            await SafeReportStatusAsync(
                statusSink, "ok", $"{coercion.FailureCount} field coercion failure(s) this batch; policy={def.OnCoercionFailure}");
            try { await statusSink.ReportCoercionFailuresAsync(coercion.FailureCount); } catch { /* best-effort, same as the status report above */ }
        }
    }

    /// <summary>Plan 010: the driver half of a message-transport source, for ANY registered transport —
    /// previously a nats-shaped copy of this method. <see cref="SubscriberCore"/> already runs rows through
    /// ConnectorPollCycle (parse/extract/coerce/dedup/stamp) internally, so — unlike gRPC above — the onRows
    /// callback here just forwards already-finished rows straight to EmitRowsAsync (its own "_source"/"_ts"
    /// TryAdd stamping is a harmless no-op on rows that already carry both).</summary>
    private void StartTransportSubscriber(SourceDefinition def, IInboundTransport transport)
    {
        _transportCts?.Cancel();
        _transportCts?.Dispose();
        _transportCts = new CancellationTokenSource();
        var ct = _transportCts.Token;

        var self = this.AsReference<IConnectorGrain>();
        var statusSink = this.AsReference<IConnectorStatusSink>();

        var core = new SubscriberCore(
            def, transport, _dedup!,
            (rows, seq) => self.EmitRowsAsync(rows.ToList(), seq),
            (status, error) => _ = SafeReportStatusAsync(statusSink, status, error),
            onCoercionFailures: n => _ = statusSink.ReportCoercionFailuresAsync(n));

        _transportTask = Task.Run(async () =>
        {
            try
            {
                await core.RunAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Expected on StopAsync.
            }
            catch
            {
                // Best-effort — SubscriberCore.RunAsync already catches and status-reports every failure it
                // can attribute to a specific reconnect attempt; anything escaping here is unexpected but
                // must not crash the background task's thread.
            }
        }, CancellationToken.None);
    }

    private void CancelTransport()
    {
        if (_transportCts is null)
        {
            return;
        }
        _transportCts.Cancel();
        _transportCts.Dispose();
        _transportCts = null;
        _transportTask = null;
    }

    private static async Task SafeReportStatusAsync(IConnectorStatusSink sink, string status, string? error)
    {
        try
        {
            await sink.ReportStatusAsync(status, error);
        }
        catch
        {
            // Best-effort — a lost status update self-heals on the next transition (or the next
            // EmitRowsAsync call, for "ok").
        }
    }

    private void EnsureTrackers()
    {
        _dedup = new DedupTracker(state.State.DedupKeys);
        _ledger = new FileLedger(state.State.Ledger);
    }

    // ------------------------------------------------------------------
    // Poll cycle (url/file/folder only — grpc is a persistent subscription, not a poll)
    // ------------------------------------------------------------------

    private async Task RunCycleAsync()
    {
        var def = state.State.Def;
        if (def is null || !state.State.Running)
        {
            return;
        }

        EnsureTrackers();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        PollCycleResult result;
        try
        {
            result = def.Kind switch
            {
                SourceKinds.Url => await ExecuteUrlCycleAsync(def, nowMs),
                SourceKinds.File => ConnectorPollCycle.ExecuteFile(def, _ledger!, _dedup!, nowMs),
                SourceKinds.Folder => ConnectorPollCycle.ExecuteFolder(def, _ledger!, _dedup!, nowMs),
                _ => new PollCycleResult([], null),
            };
        }
        catch (Exception ex)
        {
            result = new PollCycleResult([], $"{ex.GetType().Name}: {ex.Message}");
        }

        // Driver-level emit policy (design pin, W3A): a cycle either succeeds (emit everything it
        // produced, reset the failure streak) or fails (emit NOTHING this cycle, even if the cycle core
        // partially succeeded — e.g. ExecuteFolder having parsed some files before hitting one it
        // couldn't). The already-updated ledger/dedup state (below) still means a partially-successful
        // folder cycle won't re-parse the files it DID get through next time — only this cycle's rows
        // are dropped, not re-queued.
        if (result.Error is null)
        {
            if (result.Rows.Count > 0)
            {
                var stream = this.GetStreamProvider(StreamConstants.ProviderName)
                    .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, def.Name));
                foreach (var row in result.Rows)
                {
                    await stream.OnNextAsync(new EventRecord(row));
                }
            }
            state.State.ConsecutiveFailures = 0;
            state.State.LastStatus = "ok";
            // Plan 009 C2: a coercion failure under Null/DropRow never produces a non-null Error above
            // (only RejectBatch does, via ConnectorPollCycle.Emit). It is counted in
            // CoercionFailuresTotal — the queryable channel — and ALSO restated in LastError so an
            // operator reading the status line sees it without knowing to look at a counter.
            state.State.CoercionFailuresTotal += result.CoercionFailures;
            state.State.LastError = result.CoercionFailures > 0
                ? $"{result.CoercionFailures} field coercion failure(s) this cycle; policy={def.OnCoercionFailure}"
                : null;
        }
        else
        {
            state.State.ConsecutiveFailures++;
            state.State.LastStatus = "error";
            state.State.LastError = result.Error;
        }

        state.State.LastRunMs = nowMs;
        state.State.LastBatchCount = result.Error is null ? result.Rows.Count : 0;
        state.State.EventsEmittedTotal += state.State.LastBatchCount;
        state.State.DedupKeys = _dedup!.ToPersistable();
        state.State.Ledger = _ledger!.ToPersistable();

        var schedule = def.Connector?.Schedule ?? DefaultSchedule;
        var nowUtc = DateTimeOffset.UtcNow;
        var nextRun = BackoffPolicy.NextRun(schedule, nowUtc, state.State.ConsecutiveFailures);
        state.State.NextRunMs = nextRun?.ToUnixTimeMilliseconds();

        await state.WriteStateAsync();

        if (state.State.Running)
        {
            var delay = nextRun.HasValue ? nextRun.Value - nowUtc : TimeSpan.FromSeconds(30);
            ArmTimer(delay > TimeSpan.Zero ? delay : TimeSpan.Zero);
        }
    }

    private async Task<PollCycleResult> ExecuteUrlCycleAsync(SourceDefinition def, long nowMs)
    {
        var cfg = def.Connector?.Url
            ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'url' but no url config");

        using var request = new HttpRequestMessage(HttpMethod.Get, cfg.Url);
        foreach (var (name, value) in cfg.Headers)
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        using var response = await SharedHttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        if (response.Content.Headers.ContentLength is long contentLength && contentLength > MaxUrlResponseBytes)
        {
            return new PollCycleResult([], $"response too large ({contentLength} bytes, cap {MaxUrlResponseBytes})");
        }
        if (!response.IsSuccessStatusCode)
        {
            return new PollCycleResult([], $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
        }

        var body = await response.Content.ReadAsStringAsync();
        if (body.Length > MaxUrlResponseBytes)
        {
            return new PollCycleResult([], $"response too large ({body.Length} bytes, cap {MaxUrlResponseBytes})");
        }

        return ConnectorPollCycle.ExecuteUrl(def, body, _dedup!, nowMs);
    }
}
