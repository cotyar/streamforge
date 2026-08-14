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
/// <para><b>Plan 014 added a third shape to the poll side, not a fourth timer:</b> a kind registered in
/// <see cref="PolledTransports"/> (a database, say) is PULL-shaped — it wants the same one-shot timer, plus
/// a durable cursor. It gets exactly that. <see cref="StartAsync"/> already falls through to the timer for
/// any kind that is neither grpc nor a registered message transport, so nothing there changed; the whole
/// arm is one branch in <see cref="TickAsync"/> that calls <see cref="PolledSourceCore.RunCycleAsync"/>,
/// and <see cref="ConnectorActorState.Cursor"/> riding along on the <c>SaveAsync</c> that tick already
/// performs. <b>Nothing is re-armed by an actor reminder</b> — including the
/// <see cref="PolledBatch.HasMore"/> "come back immediately" case, which merely sets
/// <see cref="ConnectorActorState.NextRunMs"/> to now so the tick's existing re-arm computes a zero due
/// time. That is not squeamishness about reminders: the containerized Dapr stack runs with timers only
/// (no scheduler container — see AGENTS.md), so a reminder-based path would work in dev and silently never
/// fire in compose.</para>
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
        await ArmTimerAsync(ConnectorBookkeeping.DueFrom(next, nowUtc));
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

    public Task<ConnectorRuntimeStatus> GetStatusAsync() =>
        Task.FromResult(ConnectorBookkeeping.ToStatus(_state, Id.GetId()));

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
        await ArmTimerAsync(ConnectorBookkeeping.DueFrom(next, nowUtc));
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
        var hasMore = false;
        try
        {
            // Plan 014: a registered PULL transport is checked before the built-in switch, so a kind this
            // assembly has never heard of is driven without appearing in any list here — that is the whole
            // extensibility claim. The built-in kinds cannot be shadowed by accident: PolledTransports
            // never resolves them (PolledTransportRegistryTests pins that both ways).
            if (PolledTransports.Find(def.Kind) is { } polled)
            {
                var cycle = await ConnectorBookkeeping.RunPolledCycleAsync(_state, polled, dedup, nowMs, CancellationToken.None);
                result = cycle.Result;
                hasMore = cycle.HasMore;
            }
            else
            {
                result = def.Kind switch
                {
                    SourceKinds.Url => await ExecuteUrlKindAsync(def, dedup, nowMs),
                    SourceKinds.File => ConnectorPollCycle.ExecuteFile(def, ledger, dedup, nowMs),
                    SourceKinds.Folder => ConnectorPollCycle.ExecuteFolder(def, ledger, dedup, nowMs),
                    _ => new PollCycleResult([], $"unsupported connector kind '{def.Kind}' for a scheduled poll cycle"),
                };
            }
        }
        catch (Exception ex)
        {
            // Defensive last resort: ConnectorPollCycle/ExecuteUrlKindAsync already catch their own I/O
            // exceptions into PollCycleResult.Error. An unexpected exception here (e.g. a bug elsewhere
            // in the mapping/format core) must still leave the timer re-armed at the next backoff slot —
            // same "never let a tick's exception silently kill the timer" rule GeneratorActor.TickAsync
            // documents for its own publish try/catch.
            //
            // Landing here on the polled arm also leaves _state.Cursor exactly as it was: the single
            // assignment to it happens after RunCycleAsync has already returned, so an exception on the
            // way there cannot advance it past rows nobody emitted.
            logger.LogWarning(ex, "ConnectorActor[{Source}]: unhandled exception during poll cycle.", def.Name);
            result = new PollCycleResult([], $"{ex.GetType().Name}: {ex.Message}");
        }

        _state.DedupKeys = dedup.ToPersistable();
        _state.Ledger = ledger.ToPersistable();
        ConnectorBookkeeping.ApplyPollResult(_state, result, nowUtc);

        if (hasMore)
        {
            // "Re-arm immediately" expressed as a due time rather than as a second scheduler: the tail of
            // this method already re-arms the one-shot timer from NextRunMs, and DueFrom clamps a past due
            // time to zero. Writing it into the persisted state rather than into a local also means a
            // deactivation mid-snapshot resumes the paging at once on reactivation, instead of waiting out
            // a schedule interval that the half-read snapshot has no reason to observe.
            ConnectorBookkeeping.MarkDueNow(_state, nowUtc);
        }

        if (result.Rows.Count > 0)
        {
            await PublishAsync(def.Name, result.Rows);
        }

        await SaveAsync();

        var next = _state.NextRunMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(_state.NextRunMs.Value) : (DateTimeOffset?)null;
        await ArmTimerAsync(ConnectorBookkeeping.DueFrom(next, DateTimeOffset.UtcNow));
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

    /// <summary>Plan 014: the opaque cursor of a POLLED kind (<see cref="PolledTransports"/>) — null for
    /// every other kind and for a polled source's first ever cycle. Only the transport that minted it knows
    /// whether it is an LSN, a <c>(ts,id)</c> pair or a bigint; this actor persists the string and hands it
    /// back, and that is the entire contract.
    ///
    /// <para>Adding it here is free precisely because this class is a plain POCO rather than a
    /// <c>[GenerateSerializer]</c> contract — no <c>[Id(n)]</c> to burn, no frozen-contract test to satisfy,
    /// and System.Text.Json fills it with null when reading state written before this field existed, which
    /// is exactly the right answer for a source that has never polled.</para>
    ///
    /// <para><b>Not reset by <see cref="ConnectorActor.StartAsync"/></b>, which runs on every catalog upsert:
    /// wiping the cursor on a config edit would silently re-read the whole table. Nor is it seeded from
    /// <see cref="DbSourceConfig.InitialCursor"/> here — the transport receives the whole
    /// <see cref="SourceDefinition"/> along with a null cursor and decides what "start here" means for its
    /// own dialect, which keeps this actor ignorant of what a cursor is.</para></summary>
    public string? Cursor { get; set; }
}

/// <summary>
/// Pure connector cycle-state bookkeeping, extracted from <see cref="ConnectorActor"/> specifically so it
/// can be unit tested without any actor/timer/Dapr-sidecar machinery — same rationale as
/// <see cref="GeneratorActor"/>'s own <c>GeneratorBatching</c> (see
/// dapr/tests/StreamForge.Dapr.Tests/ConnectorActorLogicTests.cs). Framework-free: takes/returns plain
/// <see cref="ConnectorActorState"/>/<see cref="PollCycleResult"/>/CLR values and AppCore interfaces only —
/// no <c>ActorHost</c>, no <c>DaprClient</c>, no sidecar. Plan 014 leaned on that harder than plan 006 did:
/// the polled arm's cursor rules are the part of <see cref="ConnectorActor.TickAsync"/> most worth testing
/// and the least reachable through an actor, so they live here (<see cref="RunPolledCycleAsync"/>) and the
/// tick is one call plus its timer.
/// </summary>
public static class ConnectorBookkeeping
{
    /// <summary>Default schedule for a url/file/folder connector that doesn't specify one explicitly: a
    /// fixed 30s interval — well above D-E's 1s floor, a deliberately conservative default so an
    /// unconfigured connector never hot-polls.</summary>
    public static ScheduleSpec EffectiveSchedule(SourceDefinition def) =>
        def.Connector?.Schedule ?? new ScheduleSpec { IntervalMs = 30_000 };

    /// <summary>Turns a committed next-run instant into the one-shot timer's due time. A due time already
    /// in the past clamps to zero (fire now) rather than throwing, which is what makes
    /// <see cref="MarkDueNow"/> a complete implementation of "re-arm immediately". A null
    /// <paramref name="next"/> means the schedule failed to compute (e.g. an invalid spec that slipped past
    /// source-create validation) — fall back to the D-E base backoff delay rather than never firing again.
    ///
    /// <para>Lives here rather than in the actor for one reason: it is half of the re-arm claim, and a
    /// private static in an actor cannot be asserted on without a sidecar. The actor's three call sites are
    /// unchanged in behaviour.</para></summary>
    public static TimeSpan DueFrom(DateTimeOffset? next, DateTimeOffset nowUtc)
    {
        if (next is null)
        {
            return TimeSpan.FromSeconds(30);
        }

        var due = next.Value - nowUtc;
        return due < TimeSpan.Zero ? TimeSpan.Zero : due;
    }

    /// <summary>Projects the persisted state onto the wire contract the console reads. Every field is a
    /// straight copy — the projection exists as a method rather than an object initializer inside
    /// <see cref="ConnectorActor.GetStatusAsync"/> so that "the cursor actually reaches
    /// <see cref="ConnectorRuntimeStatus"/>" is a testable claim rather than a line nobody can execute
    /// without an actor host.</summary>
    public static ConnectorRuntimeStatus ToStatus(ConnectorActorState state, string sourceName) => new()
    {
        SourceName = sourceName,
        NextRunMs = state.NextRunMs,
        LastRunMs = state.LastRunMs,
        LastStatus = state.LastStatus,
        LastError = state.LastError,
        ConsecutiveFailures = state.ConsecutiveFailures,
        EventsEmittedTotal = state.EventsEmittedTotal,
        CoercionFailuresTotal = state.CoercionFailuresTotal,
        LastBatchCount = state.LastBatchCount,
        Cursor = state.Cursor,
    };

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

    // ------------------------------------------------------------------
    // Plan 014 — the polled (PULL) arm
    // ------------------------------------------------------------------

    /// <summary>Runs one <see cref="IPolledTransport"/> cycle and folds its cursor into
    /// <paramref name="state"/>. Everything a runtime could plausibly get wrong about a cursor —
    /// "unchanged" vs "reset", a failed cycle, a rejected batch, a null batch — is already decided by
    /// <see cref="PolledSourceCore.RunCycleAsync"/>, which is why this method has no branch: it assigns
    /// whatever it was handed. A branch here would be a second opinion on rules that already have one, and
    /// the Orleans twin would eventually disagree with it.
    ///
    /// <para>Only the cursor is written. The dedup snapshot, the status bookkeeping and the next run are the
    /// tick's own business (<see cref="ApplyPollResult"/> / <see cref="MarkDueNow"/>), shared with the
    /// url/file/folder kinds — a polled cycle differs from theirs in what it reads, not in how it reports.</para></summary>
    /// <param name="dedup">The tracker seeded from the persisted snapshot; mutated in place, exactly as the
    /// built-in kinds mutate it, and snapshotted back by the caller.</param>
    public static async Task<PolledTickOutcome> RunPolledCycleAsync(
        ConnectorActorState state, IPolledTransport transport, DedupTracker dedup, long nowMs, CancellationToken ct)
    {
        var def = state.Def ?? throw new InvalidOperationException("RunPolledCycleAsync requires state.Def to be set");

        var outcome = await PolledSourceCore.RunCycleAsync(
            transport, def, state.Cursor, dedup, nowMs, ct, DedupKeyColumn(def));

        state.Cursor = outcome.Cursor;
        return new PolledTickOutcome(outcome.Result, outcome.HasMore);
    }

    /// <summary>Which emitted field suppresses a re-read row, read from the kind's OWN config rather than
    /// from <c>MappingSpec.DedupKeyField</c> — a polled row source has no mapping document, and a source
    /// carrying a stale one would otherwise start dropping rows for a reason nothing on screen explains.
    /// Empty (the contract default) means no dedup: the honest at-least-once default that a
    /// <c>&gt;=</c> cursor implies, not a silent one.</summary>
    public static string? DedupKeyColumn(SourceDefinition def) =>
        def.Connector?.Db?.DedupKeyColumn is { Length: > 0 } column ? column : null;

    /// <summary>Brings the next run forward to now — how <see cref="PolledBatch.HasMore"/> re-arms without a
    /// second scheduler and without a reminder (the compose stack has no Dapr scheduler; see
    /// <see cref="ConnectorActor"/>'s doc). Applied AFTER <see cref="ApplyPollResult"/>, whose scheduled
    /// next run it deliberately overrides: a snapshot with pages left is due now, whatever the interval
    /// says. Persisting it rather than keeping it in a local is what makes a deactivation mid-snapshot
    /// resume at once instead of idling out the interval.</summary>
    public static void MarkDueNow(ConnectorActorState state, DateTimeOffset nowUtc) =>
        state.NextRunMs = nowUtc.ToUnixTimeMilliseconds();
}

/// <summary>What one polled cycle leaves for <see cref="ConnectorActor.TickAsync"/> to act on: the same
/// <see cref="PollCycleResult"/> every other kind reports through, plus the one bit that is new — whether to
/// come straight back. The cursor is deliberately absent: it has already been written to the state the tick
/// is about to persist, so returning it too would invite a caller to persist a different one.</summary>
public sealed record PolledTickOutcome(PollCycleResult Result, bool HasMore);
