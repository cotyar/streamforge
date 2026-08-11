using System.Text;
using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Transports;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Connectors.Scheduling;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: Dapr counterpart of the Orleans flavor's <c>ConnectorGrain</c>
/// (W3-A) — one actor per connector-kind source (actor type "ConnectorActor", key = the source's name),
/// driving the pure, framework-free connector core in shared/StreamForge.AppCore/Connectors/
/// (<see cref="ConnectorPollCycle"/>, <see cref="DedupTracker"/>, <see cref="FileLedger"/>,
/// <see cref="BackoffPolicy"/>/<see cref="ScheduleCalc"/>, <see cref="GrpcSubscriberCore"/>) exactly like
/// <see cref="GeneratorActor"/> drives <c>MarketDataProfiles</c>.
///
/// <para><b>Two very different run shapes, one actor:</b> url/file/folder kinds are a scheduled POLL —
/// a ONE-SHOT actor timer, re-armed after every fire at the next due time computed by
/// <see cref="BackoffPolicy.NextRun"/> (D-E). Unlike <see cref="GeneratorActor"/>'s fixed 200ms
/// periodic timer, poll rates are low (schedules are seconds-to-hours), so re-arming per fire rather
/// than running a fixed-period timer avoids drifting the schedule and lets backoff push the next fire
/// arbitrarily far out. The grpc kind is a PERSISTENT SUBSCRIPTION — no timer at all; a background
/// <see cref="Task"/> running <see cref="GrpcSubscriberCore.RunAsync"/> for the actor's lifetime,
/// reconnecting forever with its own internal backoff (see that class's doc comment for why its backoff
/// is a local copy of the same D-E formula rather than a shared call).</para>
///
/// <para><b>State is fully persisted</b> (state name "connector") — Def, Running, the dedup/ledger
/// trackers' persistable snapshots, and every <see cref="ConnectorRuntimeStatus"/> field — because,
/// exactly like <see cref="GeneratorActor"/>, Dapr actor timers do NOT survive deactivation/
/// reactivation. <see cref="OnActivateAsync"/> self-resumes from persisted state: re-arms the poll timer
/// (computing a fresh due time from the persisted <c>NextRunMs</c>, clamped to "now" if it's already in
/// the past) for url/file/folder kinds, or relaunches the background subscriber task for the grpc kind.
/// <see cref="Services.GeneratorSupervisorService"/>'s sweep remains the safety net for a connector-kind
/// source whose actor has never been activated at all (see that class's updated doc comment).</para>
///
/// <para><b>Acyclic by construction</b> (same discipline as <see cref="GeneratorActor"/>/
/// <see cref="PipelineActor"/> — see dapr/ARCHITECTURE.md's reentrancy decision): this actor never
/// resolves <see cref="ICatalogFacade"/>, an <c>IRegistryActor</c> proxy, or any other actor from inside
/// one of its own turns. The ONE exception — and it is not a cycle — is the grpc kind's background
/// subscriber task calling back into THIS SAME actor type via a fresh <see cref="IConnectorActor"/>
/// proxy (<see cref="RecordSubscriberBatchAsync"/>): that call happens on an independent background
/// thread, not from inside an in-flight actor turn, so from the Dapr runtime's perspective it is an
/// ordinary new inbound invocation — no different from any other client calling this actor — not a
/// reentrant self-call. <see cref="IConnectorActor.RecordSubscriberBatchAsync"/>'s doc comment covers the
/// same point from the interface side.</para>
///
/// <para><b>URL fetch:</b> a single shared static <see cref="HttpClient"/> (30s timeout), request
/// headers applied verbatim from <see cref="UrlPollConfig.Headers"/> (already secrets-lite masked/
/// unmasked upstream — this actor just uses whatever value it was given), response body capped at 10 MB
/// (<see cref="MaxUrlResponseBytes"/>) to bound memory for a misbehaving/huge endpoint. File/folder I/O
/// is plain BCL file access via <see cref="ConnectorPollCycle.ExecuteFile"/>/
/// <see cref="ConnectorPollCycle.ExecuteFolder"/> — no injected delegate needed on this side (those
/// methods already take the concrete <see cref="FileLedger"/>/<see cref="DedupTracker"/> instances).</para>
/// </summary>
public sealed class ConnectorActor(ActorHost host, DaprClient daprClient, ILogger<ConnectorActor> logger)
    : Actor(host), IConnectorActor
{
    private const string StateName = "connector";
    private const string TimerName = "connector-tick";

    /// <summary>Publish-only polyglot egress copy, same convention as <see cref="GeneratorActor"/>'s own
    /// private constant of the same name/value — duplicated rather than shared because the two actor
    /// types were built in concurrent waves with disjoint file ownership (see plan 006 W3's file
    /// ownership split); both must agree on the literal, which "sf-source-{name}" pins.</summary>
    private const string EgressTopicPrefix = "sf-source-";

    /// <summary>D-D-adjacent memory-safety cap for a single URL poll's response body (not itself a plan
    /// decision — a defensive ceiling so a misconfigured/huge endpoint can't balloon this actor's memory).</summary>
    private const long MaxUrlResponseBytes = 10 * 1024 * 1024;

    private static readonly HttpClient SharedHttpClient = new() { Timeout = TimeSpan.FromSeconds(30) };

    private ConnectorActorState _state = new();
    private bool _timerArmed;
    private CancellationTokenSource? _grpcCts;
    private CancellationTokenSource? _transportCts;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<ConnectorActorState>(StateName);
        if (existing.HasValue)
        {
            _state = existing.Value;
        }

        if (_state is { Running: true, Def: not null })
        {
            if (_state.Def.Kind == SourceKinds.Grpc)
            {
                StartGrpcSubscriber(_state.Def);
            }
            else if (InboundTransports.Find(_state.Def.Kind) is { } transport)
            {
                StartTransportSubscriber(_state.Def, transport);
            }
            else
            {
                await ArmTimerFromPersistedNextRunAsync();
            }
        }
    }

    public async Task StartAsync(SourceDefinition def)
    {
        await DisarmTimerIfArmedAsync();
        StopGrpcSubscriberIfRunning();
        StopTransportSubscriberIfRunning();

        _state.Def = def;
        _state.Running = def.Enabled;
        await SaveAsync();

        if (!_state.Running)
        {
            return;
        }

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

        // Fresh start: schedule the first run from "now" at the current (persisted) failure streak —
        // StartAsync replaces the previous run wholesale, same contract as GeneratorActor.StartAsync, so
        // a config edit (CatalogStore.UpsertSourceAsync calls this on every upsert) gets an immediate
        // reschedule off the new definition's schedule.
        var nowUtc = DateTimeOffset.UtcNow;
        var next = BackoffPolicy.NextRun(ConnectorBookkeeping.EffectiveSchedule(def), nowUtc, _state.ConsecutiveFailures);
        _state.NextRunMs = next?.ToUnixTimeMilliseconds();
        await SaveAsync();
        await ArmTimerAsync(DueFrom(next, nowUtc));
    }

    public async Task StopAsync()
    {
        await DisarmTimerIfArmedAsync();
        StopGrpcSubscriberIfRunning();
        StopTransportSubscriberIfRunning();
        _state.Running = false;
        await SaveAsync();
    }

    public Task<bool> IsRunningAsync() => Task.FromResult(_state.Running);

    public Task<ConnectorRuntimeStatus> GetStatusAsync() => Task.FromResult(new ConnectorRuntimeStatus
    {
        SourceName = Id.GetId(),
        NextRunMs = _state.NextRunMs,
        LastRunMs = _state.LastRunMs,
        LastStatus = _state.LastStatus,
        LastError = _state.LastError,
        ConsecutiveFailures = _state.ConsecutiveFailures,
        EventsEmittedTotal = _state.EventsEmittedTotal,
        CoercionFailuresTotal = _state.CoercionFailuresTotal,
        LastBatchCount = _state.LastBatchCount,
    });

    public async Task RecordSubscriberBatchAsync(
        int rowCount, string status, string? error, List<string>? dedupKeys = null, int coercionFailures = 0)
    {
        ConnectorBookkeeping.ApplySubscriberBatch(_state, rowCount, status, error, dedupKeys);
        // Plan 009 C2: the queryable half of "counted and surfaced". The subscriber kinds (grpc, nats)
        // never pass through the poll-cycle handler that counts for the polled kinds, so without this
        // their coercion failures would live only as a LastError string. A counter that is accurate for
        // some source kinds and silently zero for others is worse than no counter.
        _state.CoercionFailuresTotal += Math.Max(0, coercionFailures);
        await SaveAsync();
    }

    /// <summary>Named "SaveAsync", not "SaveStateAsync" — same rationale as
    /// <see cref="GeneratorActor"/>'s own private method of the same name (avoids hiding
    /// <see cref="Actor.SaveStateAsync"/>; this project writes state immediately via
    /// <c>StateManager.SetStateAsync</c> instead of buffering).</summary>
    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _state);

    // ------------------------------------------------------------------
    // Timer plumbing (url/file/folder kinds) — ONE-SHOT, re-armed every fire (unlike GeneratorActor's
    // fixed-period timer). Dapr's actor timer treats period == -1ms (Timeout.InfiniteTimeSpan) as "no
    // periodic signaling" — i.e. a genuine one-shot fire at dueTime (confirmed against the Dapr.Actors
    // 1.18.4 XML docs for Actor.RegisterTimerAsync).
    // ------------------------------------------------------------------

    private async Task ArmTimerAsync(TimeSpan due)
    {
        await RegisterTimerAsync(TimerName, nameof(TickAsync), null, due, Timeout.InfiniteTimeSpan);
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

    /// <summary>Self-heal path from <see cref="OnActivateAsync"/>: re-arms from the persisted
    /// <see cref="ConnectorActorState.NextRunMs"/> rather than recomputing a fresh schedule — a
    /// reactivation is not a new "start", so the due time this source was already committed to (however
    /// overdue) is honored, firing immediately (due = 0) if it has already passed.</summary>
    private async Task ArmTimerFromPersistedNextRunAsync()
    {
        var nowUtc = DateTimeOffset.UtcNow;
        var next = _state.NextRunMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(_state.NextRunMs.Value) : (DateTimeOffset?)null;
        await ArmTimerAsync(DueFrom(next, nowUtc));
    }

    private static TimeSpan DueFrom(DateTimeOffset? next, DateTimeOffset nowUtc)
    {
        if (next is null)
        {
            // Schedule failed to compute (e.g. an invalid spec that slipped past source-create
            // validation) — fall back to the D-E base backoff delay rather than never firing again.
            return TimeSpan.FromSeconds(30);
        }

        var due = next.Value - nowUtc;
        return due < TimeSpan.Zero ? TimeSpan.Zero : due;
    }

    /// <summary>One-shot timer callback: runs exactly one <see cref="ConnectorPollCycle"/> for this
    /// source's kind, persists the resulting bookkeeping (<see cref="ConnectorBookkeeping.ApplyPollResult"/>),
    /// publishes any emitted rows, then re-arms itself at the freshly computed next-due time — mirroring
    /// the Orleans twin's identical cycle shape.</summary>
    private async Task TickAsync()
    {
        _timerArmed = false; // this fire consumed the one-shot registration; ArmTimerAsync below re-registers it

        var def = _state.Def;
        if (def is null || !_state.Running)
        {
            return;
        }

        var nowUtc = DateTimeOffset.UtcNow;
        var nowMs = nowUtc.ToUnixTimeMilliseconds();
        var dedup = new DedupTracker(_state.DedupKeys);
        var ledger = new FileLedger(_state.Ledger);

        PollCycleResult result;
        try
        {
            result = def.Kind switch
            {
                SourceKinds.Url => await ExecuteUrlKindAsync(def, dedup, nowMs),
                SourceKinds.File => ConnectorPollCycle.ExecuteFile(def, ledger, dedup, nowMs),
                SourceKinds.Folder => ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, nowMs),
                _ => new PollCycleResult([], $"unsupported connector kind '{def.Kind}' for a scheduled poll cycle"),
            };
        }
        catch (Exception ex)
        {
            // Defensive last resort: ConnectorPollCycle/ExecuteUrlKindAsync already catch their own I/O
            // exceptions into PollCycleResult.Error. An unexpected exception here (e.g. a bug elsewhere
            // in the mapping/format core) must still leave the timer re-armed at the next backoff slot —
            // same "never let a tick's exception silently kill the timer" rule GeneratorActor.TickAsync
            // documents for its own publish try/catch.
            logger.LogWarning(ex, "ConnectorActor[{Source}]: unhandled exception during poll cycle.", def.Name);
            result = new PollCycleResult([], $"{ex.GetType().Name}: {ex.Message}");
        }

        _state.DedupKeys = dedup.ToPersistable();
        _state.Ledger = ledger.ToPersistable();
        ConnectorBookkeeping.ApplyPollResult(_state, result, nowUtc);

        if (result.Rows.Count > 0)
        {
            await PublishAsync(def.Name, result.Rows);
        }

        await SaveAsync();

        var next = _state.NextRunMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(_state.NextRunMs.Value) : (DateTimeOffset?)null;
        await ArmTimerAsync(DueFrom(next, DateTimeOffset.UtcNow));
    }

    private async Task PublishAsync(string sourceName, List<Dictionary<string, object?>> rows)
    {
        var envelope = new SourceEventsEnvelope { Source = sourceName, Events = rows };
        try
        {
            // sf-sources: the router fans this out in-host (same as GeneratorActor). sf-source-{name}:
            // publish-only egress copy for polyglot subscribers.
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + sourceName, envelope);
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not tear down the timer — drop this cycle's batch (it is
            // already recorded as "ok" in status/dedup/ledger bookkeeping, matching at-least-once
            // semantics: the rows were fetched and deduped successfully, only the publish hop failed) and
            // let the next scheduled tick try again.
            logger.LogWarning(ex, "ConnectorActor[{Source}]: failed to publish a batch of {Count} row(s) — will retry next poll.", sourceName, rows.Count);
        }
    }

    // ------------------------------------------------------------------
    // URL fetch (30s timeout via SharedHttpClient, 10 MB cap, headers applied verbatim)
    // ------------------------------------------------------------------

    private static async Task<PollCycleResult> ExecuteUrlKindAsync(SourceDefinition def, DedupTracker dedup, long nowMs)
    {
        var cfg = def.Connector?.Url;
        if (cfg is null)
        {
            return new PollCycleResult([], $"source '{def.Name}' has kind 'url' but no url config");
        }

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, cfg.Url);
            foreach (var (key, value) in cfg.Headers)
            {
                request.Headers.TryAddWithoutValidation(key, value);
            }

            using var response = await SharedHttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            if (!response.IsSuccessStatusCode)
            {
                return new PollCycleResult([], $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
            }

            var body = await ReadCappedAsync(response.Content, MaxUrlResponseBytes);
            return ConnectorPollCycle.ExecuteUrl(def, body, dedup, nowMs);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException or InvalidOperationException)
        {
            return new PollCycleResult([], $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    private static async Task<string> ReadCappedAsync(HttpContent content, long maxBytes)
    {
        await using var stream = await content.ReadAsStreamAsync();
        using var ms = new MemoryStream();
        var buffer = new byte[81920];
        int read;
        long total = 0;
        while ((read = await stream.ReadAsync(buffer)) > 0)
        {
            total += read;
            if (total > maxBytes)
            {
                throw new InvalidOperationException($"response exceeded the {maxBytes}-byte cap");
            }

            ms.Write(buffer, 0, read);
        }

        return Encoding.UTF8.GetString(ms.ToArray());
    }

    // ------------------------------------------------------------------
    // grpc kind: persistent background subscriber (no timer)
    // ------------------------------------------------------------------

    private void StartGrpcSubscriber(SourceDefinition def)
    {
        var config = def.Connector?.Grpc;
        if (config is null)
        {
            logger.LogWarning("ConnectorActor[{Source}]: kind 'grpc' but no grpc config — not starting subscriber.", def.Name);
            return;
        }

        var cts = new CancellationTokenSource();
        _grpcCts = cts;
        var name = def.Name;

        var core = new GrpcSubscriberCore(
            config,
            onRows: async (rows, _) =>
            {
                // Plan 009 C2: gRPC-decoded rows never pass through ConnectorPollCycle (unlike NATS
                // below), so declared-type coercion happens right here, before anything is stamped or
                // published — "coerce before admission", same rule as every other inbound path.
                var coercion = ConnectorRowCoercion.Apply(def.Fields, rows.ToList(), def.OnCoercionFailure);
                if (coercion.BatchRejected)
                {
                    await ConnectorActorProxy(name).RecordSubscriberBatchAsync(0, "error", $"coercion rejected batch: {coercion.RejectReason}");
                    return;
                }

                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var stamped = new List<Dictionary<string, object?>>(coercion.Rows.Count);
                foreach (var row in coercion.Rows)
                {
                    row["_source"] = name;
                    if (!row.ContainsKey("_ts"))
                    {
                        row["_ts"] = nowMs;
                    }

                    stamped.Add(row);
                }

                var envelope = new SourceEventsEnvelope { Source = name, Events = stamped };
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + name, envelope);

                await ConnectorActorProxy(name).RecordSubscriberBatchAsync(
                    stamped.Count, "ok",
                    coercion.FailureCount > 0
                        ? $"{coercion.FailureCount} field coercion failure(s) this batch; policy={def.OnCoercionFailure}"
                        : null,
                    coercionFailures: coercion.FailureCount);
            },
            onStatus: (status, error) =>
            {
                // Fire-and-forget: onStatus is a synchronous delegate (GrpcSubscriberCore's own shape)
                // invoked from the background subscriber task. A proxy call from that thread back into
                // THIS actor is a normal new inbound turn, not a reentrant nested call (see this class's
                // "Acyclic by construction" doc comment) — but we still must not let a failed marshal call
                // crash the subscriber loop, so failures are swallowed (logged at Debug) here.
                _ = RecordStatusBestEffortAsync(name, status, error);
            });

        _ = Task.Run(() => core.RunAsync(cts.Token));
    }

    // ------------------------------------------------------------------
    // message-transport kinds: persistent background subscriber (no timer) — plan 009 B1, generalized
    // over IInboundTransport in plan 010
    // ------------------------------------------------------------------

    private void StartTransportSubscriber(SourceDefinition def, IInboundTransport transport)
    {
        var cts = new CancellationTokenSource();
        _transportCts = cts;
        var name = def.Name;

        // Closure-local dedup tracker seeded from the persisted snapshot at subscribe-start — the
        // background subscriber task must never touch `_state` directly (see this class's "Acyclic by
        // construction" / reentrancy discipline doc above), so an updated snapshot round-trips through
        // RecordSubscriberBatchAsync's dedupKeys parameter on every accepted batch instead of a second,
        // ad hoc marshal point.
        var dedup = new DedupTracker(_state.DedupKeys);

        var core = new SubscriberCore(
            def, transport, dedup,
            onRows: async (rows, _) =>
            {
                // SubscriberCore already ran these rows through ConnectorPollCycle.Emit (parse/extract/
                // coerce/dedup/"_source"/"_ts" stamping) — unlike gRPC above, nothing left to do here but
                // publish and persist the dedup snapshot.
                var envelope = new SourceEventsEnvelope { Source = name, Events = rows.ToList() };
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope);
                await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + name, envelope);

                await ConnectorActorProxy(name).RecordSubscriberBatchAsync(rows.Count, "ok", null, dedup.ToPersistable());
            },
            onStatus: (status, error) =>
            {
                _ = RecordStatusBestEffortAsync(name, status, error);
            },
            onCoercionFailures: n =>
                _ = ConnectorActorProxy(name).RecordSubscriberBatchAsync(0, "ok", null, null, n));

        _ = Task.Run(() => core.RunAsync(cts.Token));
    }

    private void StopTransportSubscriberIfRunning()
    {
        if (_transportCts is null)
        {
            return;
        }

        _transportCts.Cancel();
        _transportCts.Dispose();
        _transportCts = null;
    }

    private async Task RecordStatusBestEffortAsync(string sourceName, string status, string? error)
    {
        try
        {
            await ConnectorActorProxy(sourceName).RecordSubscriberBatchAsync(0, status, error);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "ConnectorActor[{Source}]: failed to record subscriber status '{Status}'.", sourceName, status);
        }
    }

    private void StopGrpcSubscriberIfRunning()
    {
        if (_grpcCts is null)
        {
            return;
        }

        _grpcCts.Cancel();
        _grpcCts.Dispose();
        _grpcCts = null;
    }

    private static IConnectorActor ConnectorActorProxy(string sourceName) =>
        ActorProxy.Create<IConnectorActor>(new ActorId(sourceName), nameof(ConnectorActor), ActorProxyDefaults.Options);
}

/// <summary>Persisted shape of a <see cref="ConnectorActor"/>'s state (state name "connector") — see
/// that class's doc comment for why every field is persisted (self-healing across deactivation/
/// reactivation, same rationale as <see cref="GeneratorActorState"/>). Plain get/set properties for a
/// clean System.Text.Json round trip through Dapr's actor state store.</summary>
public sealed class ConnectorActorState
{
    public SourceDefinition? Def { get; set; }

    public bool Running { get; set; }

    /// <summary>Persistable snapshot of this connector's <see cref="DedupTracker"/> — round-tripped via
    /// <c>new DedupTracker(DedupKeys)</c> / <c>DedupTracker.ToPersistable()</c> each tick.</summary>
    public List<string> DedupKeys { get; set; } = [];

    /// <summary>Persistable snapshot of this connector's <see cref="FileLedger"/> (file/folder kinds
    /// only — empty and unused for url/grpc).</summary>
    public Dictionary<string, long> Ledger { get; set; } = [];

    public int ConsecutiveFailures { get; set; }

    public long? LastRunMs { get; set; }

    public long? NextRunMs { get; set; }

    public string LastStatus { get; set; } = "never";

    public string? LastError { get; set; }

    public long EventsEmittedTotal { get; set; }
    /// <summary>Plan 009 C2: cumulative field coercion failures — see ConnectorRuntimeStatus's own doc.</summary>
    public long CoercionFailuresTotal { get; set; }

    public int LastBatchCount { get; set; }
}

/// <summary>
/// Pure connector cycle-state bookkeeping, extracted from <see cref="ConnectorActor"/> specifically so it
/// can be unit tested without any actor/timer/Dapr-sidecar machinery — same rationale as
/// <see cref="GeneratorActor"/>'s own <c>GeneratorBatching</c> (see
/// dapr/tests/StreamForge.Dapr.Tests/ConnectorActorLogicTests.cs). Framework-free: takes/returns plain
/// <see cref="ConnectorActorState"/>/<see cref="PollCycleResult"/>/CLR values only.
/// </summary>
public static class ConnectorBookkeeping
{
    /// <summary>Default schedule for a url/file/folder connector that doesn't specify one explicitly: a
    /// fixed 30s interval — well above D-E's 1s floor, a deliberately conservative default so an
    /// unconfigured connector never hot-polls.</summary>
    public static ScheduleSpec EffectiveSchedule(SourceDefinition def) =>
        def.Connector?.Schedule ?? new ScheduleSpec { IntervalMs = 30_000 };

    /// <summary>Applies one poll cycle's outcome to <paramref name="state"/> in place (plan 006 D-E):
    /// a null <see cref="PollCycleResult.Error"/> is success — resets the failure streak, sets
    /// LastStatus "ok", records the batch size and bumps EventsEmittedTotal. A non-null Error increments
    /// ConsecutiveFailures, sets LastStatus "error"/LastError, and reports a zero batch (no emission on
    /// failure). LastRunMs and NextRunMs are ALWAYS recomputed regardless of success/failure — a failing
    /// connector must still get a (backed-off) next run, never freeze forever.</summary>
    public static void ApplyPollResult(ConnectorActorState state, PollCycleResult result, DateTimeOffset nowUtc)
    {
        state.LastRunMs = nowUtc.ToUnixTimeMilliseconds();

        if (result.Error is null)
        {
            state.ConsecutiveFailures = 0;
            state.LastStatus = "ok";
            // Plan 009 C2: Null/DropRow coercion failures never produce a non-null Error (only
            // RejectBatch does, inside ConnectorPollCycle.Emit) — this is the only other channel back to
            // ConnectorRuntimeStatus.LastError (no dedicated counter field exists on that frozen
            // contract), so a clean cycle still surfaces a non-zero count instead of going quiet.
            state.CoercionFailuresTotal += result.CoercionFailures;
            state.LastError = result.CoercionFailures > 0
                ? $"{result.CoercionFailures} field coercion failure(s) this cycle; policy={state.Def?.OnCoercionFailure}"
                : null;
            state.LastBatchCount = result.Rows.Count;
            state.EventsEmittedTotal += result.Rows.Count;
        }
        else
        {
            state.ConsecutiveFailures++;
            state.LastStatus = "error";
            state.LastError = result.Error;
            state.LastBatchCount = 0;
        }

        var schedule = EffectiveSchedule(state.Def ?? throw new InvalidOperationException("ApplyPollResult requires state.Def to be set"));
        var next = BackoffPolicy.NextRun(schedule, nowUtc, state.ConsecutiveFailures);
        state.NextRunMs = next?.ToUnixTimeMilliseconds();
    }

    /// <summary>Applies one gRPC/NATS subscriber batch/status callback to <paramref name="state"/> in
    /// place (plan 006 D-G; plan 009 B1 added the nats kind, same shape). "ok" with
    /// <paramref name="rowCount"/> &gt; 0 is a real batch: resets the failure streak and bumps
    /// LastRunMs/LastBatchCount/EventsEmittedTotal. "ok" with rowCount == 0 is a pure status update (e.g.
    /// right after a successful (re)connect, before any row has arrived yet) — still resets the failure
    /// streak but leaves the batch counters untouched. Either way, <paramref name="error"/> is recorded
    /// verbatim (plan 009 C2, additive behavior change from the pre-009 hardcoded null — every pre-009
    /// call site always passed <c>error: null</c> alongside "ok", so this is byte-identical for them):
    /// an "ok" status can now carry an informational note (e.g. "N field coercion failure(s) this
    /// batch") without a status of "error" that would incorrectly bump ConsecutiveFailures. "error"
    /// increments ConsecutiveFailures and records LastError. Any other status (e.g. "connecting") is
    /// recorded as-is without touching the failure streak. <see cref="ConnectorActorState.NextRunMs"/>
    /// is deliberately left untouched — a persistent subscription has no scheduled "next run" (D-G):
    /// reconnection timing is entirely internal to <see cref="GrpcSubscriberCore"/>/
    /// <see cref="NatsSubscriberCore"/>. <paramref name="dedupKeys"/> (plan 009 B1, additive): the nats
    /// kind's dedup tracker snapshot, applied when non-null — see
    /// <see cref="IConnectorActor.RecordSubscriberBatchAsync"/>'s doc comment for why it round-trips
    /// through here rather than a second marshal point.</summary>
    public static void ApplySubscriberBatch(ConnectorActorState state, int rowCount, string status, string? error, List<string>? dedupKeys = null)
    {
        switch (status)
        {
            case "ok":
                state.ConsecutiveFailures = 0;
                state.LastStatus = "ok";
                state.LastError = error;
                if (rowCount > 0)
                {
                    state.LastRunMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    state.LastBatchCount = rowCount;
                    state.EventsEmittedTotal += rowCount;
                }

                break;

            case "error":
                state.ConsecutiveFailures++;
                state.LastStatus = "error";
                state.LastError = error;
                break;

            default:
                state.LastStatus = status;
                state.LastError = error;
                break;
        }

        if (dedupKeys is not null)
        {
            state.DedupKeys = dedupKeys;
        }
    }
}
