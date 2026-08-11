using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
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
        services.AddSingleton<IArrangementMetaFacade, OrleansArrangementMetaFacade>();
        services.AddSingleton<IConnectorStatusFacade, OrleansConnectorStatusFacade>();
        // Plan 008 W4: client-push ingress. SourceIngressRegistry is the host-process singleton buffer
        // registry (one SourceIngressBuffer per ingest-kind source); OrleansIngressFacade is the thin
        // Orleans-side adapter IIngressFacade callers (SourcesEndpoints, IngestGrpcService) depend on.
        services.AddSingleton<SourceIngressRegistry>();
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
internal sealed class OrleansIngressFacade(IClusterClient client, SourceIngressRegistry registry, IServiceProvider services) : IIngressFacade
{
    public async Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial)
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
        var result = await buffer.PushAsync(batch.Accepted);

        if (batch.RowErrors.Count > 0)
        {
            buffer.RecordInvalid(batch.RowErrors.Count);
            result.Invalid = batch.RowErrors.Count;
            result.RowErrors = batch.RowErrors;
        }

        return result;
    }

    public async Task<IngestStatus?> GetStatusAsync(string sourceName)
    {
        var def = await GetSourceDefAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return null;
        }

        var buffer = registry.TryGet(sourceName);
        if (buffer is not null)
        {
            return buffer.GetStatus();
        }

        // Never pushed to yet (SourceIngressRegistry.TryGet's own doc): report the configured
        // shape with zeroed counters instead of creating a buffer as a side effect of a GET.
        var config = def.Ingest ?? new IngestConfig();
        return new IngestStatus { Policy = config.Policy, CapacityRows = config.CapacityRows, MaxBatchRows = config.MaxBatchRows };
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
