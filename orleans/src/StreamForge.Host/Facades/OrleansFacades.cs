using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Auth;
using StreamForge.Host.Grains;
using StreamForge.Host.Streaming;

namespace StreamForge.Host.Facades;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W3: Orleans-side implementations of the runtime-neutral facade
// interfaces (StreamForge.Abstractions/Facades.cs) that shared/StreamForge.Api's endpoints depend on.
//
//   - ICatalogFacade / IUserStoreFacade: IRegistryGrain/IUserStoreGrain already inherit these
//     interfaces (see GrainInterfaces.cs), so a real grain reference IS-A facade with zero adapter
//     code — registered as singletons resolving the grain proxy once.
//   - IPipelineReadFacade / ITableReadFacade / ITableHistoryFacade: keyed read surfaces: the grain's
//     implicit key becomes an explicit first parameter, so these need tiny per-call adapter classes
//     that resolve IClusterClient.GetGrain<T>(key) on every call. Deliberately dumb — all
//     id/name-resolution logic (e.g. mapping a table's REST {id} to its grain-key Name) already lives
//     in the shared endpoints (TablesEndpoints/PipelinesEndpoints), which call these adapters with the
//     already-resolved key.
//   - IArrangementMetaFacade: backs GET /api/meta/arrangements. Partitioned execution (and shared
//     arrangements) is Orleans-only (decision D-F), so this is the one facade whose entire body is
//     Orleans-specific (TableDataflowFactory + IArrangementGrain) — moved here verbatim from the old
//     MetaEndpoints.cs's /arrangements handler.
// ============================================================================

public static class OrleansFacadesExtensions
{
    public static IServiceCollection AddOrleansFacades(this IServiceCollection services)
    {
        services.AddSingleton<ICatalogFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IRegistryGrain>(StreamConstants.RegistryKey));
        services.AddSingleton<IUserStoreFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IUserStoreGrain>(StreamConstants.UsersKey));
        services.AddSingleton<IPipelineReadFacade, OrleansPipelineReadFacade>();
        services.AddSingleton<ITableReadFacade, OrleansTableReadFacade>();
        services.AddSingleton<ITableHistoryFacade, OrleansTableHistoryFacade>();
        services.AddSingleton<ITableShardFacade, OrleansTableShardFacade>();
        services.AddSingleton<IArrangementMetaFacade, OrleansArrangementMetaFacade>();
        services.AddSingleton<IConnectorStatusFacade, OrleansConnectorStatusFacade>();
        // Plan 008 W4: client-push ingress. SourceIngressRegistry is the host-process singleton buffer
        // registry (one SourceIngressBuffer per ingest-kind source); OrleansIngressFacade is the thin
        // Orleans-side adapter IIngressFacade callers (SourcesEndpoints, IngestGrpcService) depend on.
        services.AddSingleton<SourceIngressRegistry>();
        // Plan 009 A1: idempotency cache + per-source push-key usage tracker (both host-process
        // singletons, same lifetime as SourceIngressRegistry) and the "last reported to the stats
        // grain" baseline tracker IngestDrainPumpService/OrleansIngressFacade share — see each class's
        // own doc comment.
        services.AddSingleton<IngestIdempotencyCache>();
        services.AddSingleton<IngestKeyUsageTracker>();
        services.AddSingleton<IngressStatsReportTracker>();
        services.AddSingleton<IIngressFacade, OrleansIngressFacade>();
        return services;
    }
}

internal sealed class OrleansPipelineReadFacade(IClusterClient client) : IPipelineReadFacade
{
    public Task<List<ResultEnvelope>> GetRecentResultsAsync(string pipelineId, int limit) =>
        client.GetGrain<IPipelineGrain>(pipelineId).GetRecentResultsAsync(limit);

    public Task<PipelineMetrics> GetMetricsAsync(string pipelineId) =>
        client.GetGrain<IPipelineGrain>(pipelineId).GetMetricsAsync();
}

internal sealed class OrleansTableReadFacade(IClusterClient client) : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableGrain>(tableName).GetRowsAsync(limit, offset);

    public Task<int> GetRowCountAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetRowCountAsync();

    public Task<long> GetSeqAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetSeqAsync();

    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetSnapshotFrontierEpochAsync();

    public Task<TableMetrics> GetMetricsAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetMetricsAsync();

    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) =>
        client.GetGrain<ITableGrain>(tableName).SearchAsync(query, limit);
}

internal sealed class OrleansTableHistoryFacade(IClusterClient client) : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        client.GetGrain<ITableHistoryGrain>(tableName).GetHistoryAsync(key, limit);

    public Task<TableHistoryStats> GetStatsAsync(string tableName) =>
        client.GetGrain<ITableHistoryGrain>(tableName).GetStatsAsync();
}

/// <summary>Plan 011 D1: the shard tier's read surface. Three of its four members deliberately never
/// touch a shard grain — <see cref="GetInfoAsync"/> and <see cref="GetKeysAsync"/> answer from the router
/// and the directory, so the console can poll them without waking a single idle key. Only
/// <see cref="GetShardAsync"/> (one named key, the point of the tier) and <see cref="ScanAsync"/> (the
/// explicit full scan, kept separate precisely so nothing reaches it by accident) activate shards.</summary>
internal sealed class OrleansTableShardFacade(IClusterClient client) : ITableShardFacade
{
    public Task<TableShardView> GetShardAsync(string tableName, string shardKey, int historyLimitPerKey) =>
        client.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(tableName, shardKey)).GetViewAsync(historyLimitPerKey);

    public Task<TableShardingInfo> GetInfoAsync(string tableName) =>
        client.GetGrain<ITableShardRouterGrain>(tableName).GetInfoAsync();

    public Task<List<string>> GetKeysAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableShardDirectoryGrain>(tableName).GetKeysAsync(limit, offset);

    public async Task<List<TableShardStats>> ScanAsync(string tableName, int limit, int offset)
    {
        var keys = await client.GetGrain<ITableShardDirectoryGrain>(tableName).GetKeysAsync(limit, offset);
        var stats = new List<TableShardStats>(keys.Count);
        // Chunked rather than one WhenAll over the whole page: a scan is the one call that deliberately
        // activates shards, and activating a few hundred at once is a load spike worth not creating.
        foreach (var chunk in keys.Chunk(32))
        {
            stats.AddRange(await Task.WhenAll(chunk.Select(k =>
                client.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(tableName, k)).GetStatsAsync())));
        }
        return stats;
    }
}

/// <summary>Plan 006, D-C: connector runtime status. Generator-kind sources (Kind unset/"generator"),
/// ingest-kind sources (plan 008 W4 — client-push, no IConnectorGrain ever backs one; use
/// IIngressFacade/GET /{name}/ingest instead), and unknown source names all return null — mirroring
/// RegistryGrain's own Kind-dispatch rule (see RegistryGrain.IsGeneratorKind/IsIngestKind) so this
/// facade never spins up a pointless IConnectorGrain activation for a source that was never a
/// connector in the first place.</summary>
internal sealed class OrleansConnectorStatusFacade(IClusterClient client) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var def = await registry.GetSourceAsync(sourceName);
        if (def is null || string.IsNullOrEmpty(def.Kind) || def.Kind is SourceKinds.Generator or SourceKinds.Ingest)
        {
            return null;
        }
        return await client.GetGrain<IConnectorGrain>(sourceName).GetStatusAsync();
    }
}

/// <summary>Plan 008 W4: Orleans-side <see cref="IIngressFacade"/>. A host-process singleton over
/// <see cref="SourceIngressRegistry"/> — deliberately NOT a grain (IIngressFacade's own doc comment:
/// an unbounded, unobservable grain inbox with no admission point would make the buffer's policy
/// choice decorative). The drain pump publishes through the EXACT same door
/// <c>GeneratorGrain.TickAsync</c>/<c>ConnectorGrain</c> already use — <c>client.GetStreamProvider</c>
/// instead of <c>this.GetStreamProvider</c> only because this class isn't a grain; it resolves the
/// identical provider instance from the same shared silo/web-app DI container (see
/// PushStreamHostingExtensions' doc on why that equivalence holds for this co-hosted process) — one
/// EventRecord per row, so every existing subscriber (pipelines, tables, SignalR, gRPC StreamService)
/// sees an ingest source exactly like a generator or connector one.</summary>
internal sealed class OrleansIngressFacade(
    IClusterClient client,
    SourceIngressRegistry registry,
    IngestIdempotencyCache idempotency,
    IngestKeyUsageTracker keyUsage,
    IngressStatsReportTracker statsTracker,
    IServiceProvider services) : IIngressFacade
{
    /// <summary>Plan 009 A1.1: the idempotency check runs BEFORE anything else — even source lookup —
    /// per <see cref="IngestIdempotencyCache"/>'s own doc comment: a repeat of the same key always
    /// replays the ORIGINAL outcome verbatim, never re-derives one.</summary>
    public Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial, string? idempotencyKey = null) =>
        IngestIdempotencyCache.RunAsync(idempotency, sourceName, idempotencyKey, () => PushCoreAsync(sourceName, events, partial));

    private async Task<IngestResult> PushCoreAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial)
    {
        var def = await GetSourceDefAsync(sourceName);
        if (def is null)
        {
            return new IngestResult { Outcome = IngestOutcome.NotFound, Error = $"source '{sourceName}' not found" };
        }

        if (def.Kind != SourceKinds.Ingest)
        {
            // Includes generator-kind: mixing a timer-driven rate with client pushes would make every
            // counter and the rate display unreconcilable (plan 008 W4 brief) — strictness here is
            // reversible, laxity is not.
            return new IngestResult { Outcome = IngestOutcome.WrongKind, Error = $"source '{sourceName}' is kind '{def.Kind}', not ingest" };
        }

        var config = def.Ingest ?? new IngestConfig();

        // Coercion BEFORE admission (IngestModels.cs's header): a 400 must never leave partial state.
        var arrivalMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = IngressRowAcceptance.AcceptBatch(def.Fields, sourceName, config.RejectUnknownFields, events, arrivalMs);
        if (batch.RowErrors.Count > 0 && !partial)
        {
            return new IngestResult
            {
                Outcome = IngestOutcome.Invalid,
                Invalid = batch.RowErrors.Count,
                Error = $"{batch.RowErrors.Count} row(s) failed coercion",
                RowErrors = batch.RowErrors,
            };
        }

        var buffer = registry.GetOrCreate(sourceName, config, (rows, ct) => DrainAsync(sourceName, rows, ct));

        // Plan 009 A1.1: row-level dedup runs AFTER coercion, BEFORE admission — a duplicate never
        // consumes buffer capacity and (together with the whole-batch-Invalid return above) a 400
        // still leaves nothing behind either way.
        var dedup = buffer.FilterRowLevelDuplicates(batch.Accepted);
        var result = await buffer.PushAsync(dedup.Kept);

        if (batch.RowErrors.Count > 0)
        {
            buffer.RecordInvalid(batch.RowErrors.Count);
            result.Invalid = batch.RowErrors.Count;
            result.RowErrors = batch.RowErrors;
        }

        result.Duplicate = dedup.DuplicateCount;
        return result;
    }

    /// <summary>Plan 009 A1.2: null/blank key, unknown source, non-ingest source, and a source with no
    /// configured keys all return false (IIngressFacade.ValidateKeyAsync's own doc comment — an ingest
    /// source with zero keys is JWT-only, not open). Hash comparison is
    /// <see cref="PasswordHasher.Verify"/>'s own fixed-time compare, so this doesn't leak by timing.</summary>
    public async Task<bool> ValidateKeyAsync(string sourceName, string? presentedKey)
    {
        if (string.IsNullOrEmpty(presentedKey))
        {
            return false;
        }

        var def = await GetSourceDefAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return false;
        }

        var keys = def.Ingest?.Keys;
        if (keys is null || keys.Count == 0)
        {
            return false;
        }

        foreach (var key in keys)
        {
            if (PasswordHasher.Verify(presentedKey, key.Hash, key.Salt))
            {
                // Best-effort, in-memory, per-replica — see IngestKeyUsageTracker's own doc comment
                // for why this is never round-tripped through UpsertSourceAsync on the hot path.
                keyUsage.RecordUse(sourceName, key.Id, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                return true;
            }
        }

        return false;
    }

    /// <summary>Plan 009 A1.3: DepthRows/Policy/CapacityRows/MaxBatchRows/LastPushMs stay THIS
    /// replica's own local view (they aren't cumulative counters, so summing them across replicas
    /// wouldn't mean anything); every TotalX/DownstreamDropped counter is answered from
    /// <see cref="IIngressStatsGrain"/> plus whatever this replica hasn't reported yet — see
    /// <see cref="IngressStatsReportTracker"/>'s own doc comment for why that combination, not the
    /// grain alone, is what keeps a push-then-immediately-GET on the same replica accurate.</summary>
    public async Task<IngestStatus?> GetStatusAsync(string sourceName)
    {
        var def = await GetSourceDefAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return null;
        }

        var buffer = registry.TryGet(sourceName);
        var config = def.Ingest ?? new IngestConfig();

        // Never pushed to on THIS replica yet (SourceIngressRegistry.TryGet's own doc): report the
        // configured shape with zeroed local counters instead of creating a buffer as a side effect of
        // a GET — the aggregation below can still show other replicas' totals for this source.
        var local = buffer?.GetStatus() ?? new IngestStatus { Policy = config.Policy, CapacityRows = config.CapacityRows, MaxBatchRows = config.MaxBatchRows };

        var snapshot = await client.GetGrain<IIngressStatsGrain>(sourceName).GetSnapshotAsync();
        var baseline = statsTracker.GetBaseline(sourceName);
        var pending = IngressStatsReportTracker.ComputeDelta(baseline, local);

        local.TotalAccepted = snapshot.TotalAccepted + pending.Accepted;
        local.TotalRejected = snapshot.TotalRejected + pending.Rejected;
        local.TotalDropped = snapshot.TotalDropped + pending.Dropped;
        local.TotalInvalid = snapshot.TotalInvalid + pending.Invalid;
        local.TotalPublished = snapshot.TotalPublished + pending.Published;
        local.DownstreamDropped = snapshot.DownstreamDropped + pending.DownstreamDropped;
        local.TotalDuplicate = snapshot.TotalDuplicate + pending.Duplicate;
        local.InstanceId = IngressInstanceId.Value;
        local.Aggregated = true;
        return local;
    }

    private Task<SourceDefinition?> GetSourceDefAsync(string sourceName) =>
        client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetSourceAsync(sourceName);

    /// <summary>The drain pump handed to every <see cref="SourceIngressBuffer"/> this facade creates:
    /// one <c>OnNextAsync</c> per row, into the ingest source's own stream identity — so it fans out to
    /// every existing consumer unchanged.
    ///
    /// Also where the SECOND loss point (IngestModels.cs's header) is measured: under
    /// <c>Streams:Transport=push</c>, <see cref="PushStreamBus.TotalDropped"/> advances synchronously
    /// inside each <c>OnNextAsync</c> (a full subscriber channel drops the item right there), so the
    /// pre/post delta across this batch is an honest — if approximate under concurrent ingest sources
    /// sharing the one process-wide counter — attribution back to THIS source's
    /// <see cref="IngestStatus.DownstreamDropped"/>. <see cref="PushStreamBus"/> isn't registered at
    /// all under the default pull (memory-streams) transport, so DownstreamDropped simply stays 0
    /// there — pull's own loss point (pulling-agent queue overflow) isn't instrumented today.</summary>
    private async Task DrainAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
    {
        var pushBus = services.GetService<PushStreamBus>();
        var before = pushBus?.TotalDropped ?? 0;

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        foreach (var record in IngressEnvelopeBuilder.ToEventRecords(rows))
        {
            await stream.OnNextAsync(record);
        }

        if (pushBus is null)
        {
            return;
        }

        var delta = pushBus.TotalDropped - before;
        if (delta > 0)
        {
            registry.TryGet(sourceName)?.RecordDownstreamDropped((int)delta);
        }
    }
}

internal sealed class OrleansArrangementMetaFacade(IClusterClient client) : IArrangementMetaFacade
{
    // Moved verbatim from the old StreamForge.Host.Api.MetaEndpoints's /arrangements handler — see
    // that endpoint's original doc comment (plan 003 M3) for the full "recompile-per-grain" rationale.
    public async Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync()
    {
        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var tables = await registry.GetTablesAsync();
        var running = tables.Where(t => t.Status == PipelineStatus.Running && t.Parallelism > 1).ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<ArrangementMetaInfo>();

        foreach (var def in running)
        {
            TableDataflowPlan dataflow;
            try
            {
                (_, dataflow) = await TableDataflowFactory.BuildAsync(client, def);
            }
            catch
            {
                continue; // best-effort — a table that doesn't currently compile just isn't reported
            }

            foreach (var edge in dataflow.ArrangeableExternalEdges)
            {
                var inputName = dataflow.ExternalInputNameOf(edge);
                var keySpec = dataflow.KeySpecOf(edge);
                var hash = ArrangementKeySpec.HashOf(keySpec);
                int pcount = dataflow.PartitionCountOf(edge.ToStageId);
                var setKey = $"{inputName}|{hash}|{pcount}";
                if (!seen.Add(setKey))
                {
                    continue;
                }

                var infos = new List<ArrangementInfo>(pcount);
                for (int p = 0; p < pcount; p++)
                {
                    var key = $"{inputName}:{hash}:{p}";
                    infos.Add(await client.GetGrain<IArrangementGrain>(key).GetInfoAsync());
                }

                if (infos.All(i => i.ConsumerCount == 0))
                {
                    continue; // structurally arrangeable but nothing currently attached — not "live"
                }

                result.Add(new ArrangementMetaInfo
                {
                    InputName = inputName,
                    KeySpec = keySpec,
                    Partitions = pcount,
                    // Every attaching table attaches ALL P partitions (one consumer id per partition —
                    // see TableGrain.StartCoordinatorAsync's attach loop), so ConsumerCount is uniform
                    // across an arrangement set's partitions; Max is a defensive read (vs. Sum, which
                    // would misleadingly scale with P) in case of a transient in-flight attach/detach.
                    Consumers = infos.Count > 0 ? infos.Max(i => i.ConsumerCount) : 0,
                    TotalRows = infos.Sum(i => i.RowCount),
                });
            }
        }

        return result;
    }
}
