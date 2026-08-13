namespace StreamForge.Abstractions;

// ============================================================================
// Plan 005 (Dapr sibling runtime), wave W1, decision D-B: runtime-neutral facade interfaces.
//
// These interfaces exist so the REST/gRPC surface (StreamForge.Host.Api/*Endpoints.cs today;
// shared/StreamForge.Api once wave W3 moves it) can be written once and served by either runtime:
//
//   - ICatalogFacade / IUserStoreFacade are singleton-shaped (one instance for the whole catalog /
//     user store) and are INHERITED by the corresponding Orleans grain interfaces
//     (IRegistryGrain/IUserStoreGrain — see GrainInterfaces.cs) so a real grain reference already
//     satisfies the facade type with zero adapter code on the Orleans side; a Dapr host instead
//     registers a thin actor-proxy adapter class implementing the facade directly.
//   - IPipelineReadFacade / ITableReadFacade / ITableHistoryFacade are keyed-entity read surfaces.
//     A grain method has an implicit key (the grain's own identity) and therefore no key parameter,
//     so these CANNOT be inherited by ITableGrain/IPipelineGrain/ITableHistoryGrain the same way —
//     each method instead takes the entity's name/id as an explicit first parameter. Both runtimes
//     implement these via small standalone adapter classes (Orleans: resolve
//     IClusterClient.GetGrain<T>(key) and forward; Dapr: resolve the actor proxy and forward).
//   - IArrangementMetaFacade backs GET /api/meta/arrangements. Partitioned execution (Parallelism
//     2-16, shared arrangements) is Orleans-only (decision D-F) — the Dapr implementation always
//     returns an empty list.
//
// None of these are grain interfaces themselves (no IGrainWithStringKey) and none of their methods
// are ever invoked over Orleans RPC directly — only the concrete grain interfaces that inherit
// ICatalogFacade/IUserStoreFacade are. The keyed read facades and IArrangementMetaFacade are always
// reached through an in-process adapter, so nothing here needs Orleans serialization codegen of its
// own; the DTOs they return (ResultEnvelope, TableRowDto, TableMetrics, TableHistoryQueryResult,
// TableHistoryStats, ArrangementMetaInfo below) already carry [GenerateSerializer] where needed for
// their OTHER transport paths (Orleans grain calls, SignalR, REST JSON).
// ============================================================================

/// <summary>Catalog of sources/pipelines/tables + orchestration (start/stop) + field-number
/// bookkeeping — everything <see cref="IRegistryGrain"/> exposes except
/// <see cref="IRegistryGrain.EnsureInitializedAsync"/>, which is Orleans-boot-only (seeds storage,
/// reactivates generators, resumes running pipelines/tables on silo start) and has no Dapr-host
/// equivalent in this facade seam.</summary>
public interface ICatalogFacade
{
    Task<List<SourceDefinition>> GetSourcesAsync();
    Task<SourceDefinition?> GetSourceAsync(string name);
    Task UpsertSourceAsync(SourceDefinition def);
    Task<bool> DeleteSourceAsync(string name);

    Task<List<PipelineDefinition>> GetPipelinesAsync();
    Task<PipelineDefinition?> GetPipelineAsync(string id);
    Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def);
    Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def);
    Task<bool> DeletePipelineAsync(string id);
    /// <summary>Start or stop a pipeline. Returns updated definition, null if not found. Sets Failed + Error on compile failure.</summary>
    Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status);

    Task<List<TableDefinition>> GetTablesAsync();
    Task<TableDefinition?> GetTableAsync(string id);
    /// <summary>Validates name uniqueness across sources+tables (throws InvalidOperationException on
    /// collision) and compile-checks the SQL, storing OutputSchema/StreamInputs/TableInputs when it compiles.</summary>
    Task<TableDefinition> CreateTableAsync(TableDefinition def);
    Task<TableDefinition?> UpdateTableAsync(TableDefinition def);
    /// <summary>Throws InvalidOperationException (409-style) if a Running table depends on this one.</summary>
    Task<bool> DeleteTableAsync(string id);
    /// <summary>Start or stop a table. Starting requires all of its table inputs to be Running (sets
    /// Failed + Error otherwise). Stopping throws InvalidOperationException (409-style) if a Running table
    /// depends on this one. Returns updated definition, null if not found.</summary>
    Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status);

    /// <summary>Returns the persisted field-number map (JSON) for a dynamic-protobuf entity
    /// ("source:{name}" / "pipeline:{id}" / "table:{id}"), first evolving it against the supplied
    /// current schema: existing fields keep their numbers, new fields get fresh ones, removed fields'
    /// numbers are reserved forever (never reused). Persists on change. This is the single source of
    /// truth for proto field numbering — gRPC reflection descriptors and downloadable .proto files
    /// must both obtain numbers here so generated clients stay compatible across schema edits.</summary>
    Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields);
}

/// <summary>User credential store — everything <see cref="IUserStoreGrain"/> exposes except
/// <see cref="IUserStoreGrain.EnsureInitializedAsync"/> (Orleans-boot-only: seeds admin/editor/viewer
/// on first run).</summary>
public interface IUserStoreFacade
{
    /// <summary>Returns the user when username+password are valid, else null.</summary>
    Task<UserRecord?> ValidateCredentialsAsync(string username, string password);
    Task<List<UserRecord>> GetUsersAsync();
    Task<bool> CreateUserAsync(string username, string displayName, string role, string password);
    /// <summary>Null params leave the field unchanged.</summary>
    Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password);
    Task<bool> DeleteUserAsync(string username);
}

/// <summary>Keyed read surface over one running pipeline's results/metrics — mirrors the subset of
/// <see cref="IPipelineGrain"/> the REST layer reads (GET /api/pipelines/{id}/results|metrics), with
/// the grain's implicit key made an explicit <c>pipelineId</c> parameter so this can be implemented by
/// a standalone adapter rather than inherited by the grain interface itself.</summary>
public interface IPipelineReadFacade
{
    Task<List<ResultEnvelope>> GetRecentResultsAsync(string pipelineId, int limit);
    Task<PipelineMetrics> GetMetricsAsync(string pipelineId);
}

/// <summary>Keyed read surface over one table's rows/metrics/search — mirrors the subset of
/// <see cref="ITableGrain"/> the REST layer reads (GET /api/tables/{id}/rows|metrics|search), with the
/// grain's implicit key made an explicit <c>tableName</c> parameter.</summary>
public interface ITableReadFacade
{
    Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset);
    Task<int> GetRowCountAsync(string tableName);
    Task<long> GetSeqAsync(string tableName);
    /// <summary>Null for a Parallelism==1 table (no partitioned frontier) — see
    /// <see cref="TableMetrics.SnapshotFrontierEpoch"/>. Always null on the Dapr flavor (partitioned
    /// execution is Orleans-only — decision D-F).</summary>
    Task<long?> GetSnapshotFrontierEpochAsync(string tableName);
    Task<TableMetrics> GetMetricsAsync(string tableName);
    /// <summary>Reverse-index lookup over the table's current rows — empty query or SearchEnabled=false
    /// both yield an empty list (callers needing to distinguish those go through the REST
    /// /api/tables/{id}/search endpoint, which checks SearchEnabled first).</summary>
    Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit);
}

/// <summary>Keyed read surface over one table's opt-in row-version history — mirrors the subset of
/// <see cref="ITableHistoryGrain"/> the REST layer reads (POST /api/tables/{id}/history/lookup, GET
/// .../history/stats), with the grain's implicit key made an explicit <c>tableName</c> parameter.</summary>
public interface ITableHistoryFacade
{
    /// <summary>Version history for one row identity key. limit &lt;= 0 means "all retained versions".
    /// KeyFound is false when the key has never been observed.</summary>
    Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit);
    Task<TableHistoryStats> GetStatsAsync(string tableName);
}

/// <summary>Point-in-time view of one live shared arrangement SET (one per distinct (inputName,
/// keySpec, partitionCount) currently attached by at least one Running table) — backs GET
/// /api/meta/arrangements. Neutral mirror of <c>StreamForge.Host.Api.ArrangementMetaDto</c>
/// (Consumers/TotalRows summed across every partition of the set).</summary>
[GenerateSerializer]
public sealed class ArrangementMetaInfo
{
    [Id(0)] public string InputName { get; set; } = "";
    [Id(1)] public string KeySpec { get; set; } = "";
    [Id(2)] public int Partitions { get; set; }
    [Id(3)] public int Consumers { get; set; }
    [Id(4)] public long TotalRows { get; set; }
}

/// <summary>Backs GET /api/meta/arrangements. Partitioned execution (and therefore shared
/// arrangements) is Orleans-only (decision D-F) — the Dapr implementation always returns an empty
/// list.</summary>
public interface IArrangementMetaFacade
{
    Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync();
}

/// <summary>Plan 011 D1: keyed read surface over one table's SHARD tier — backs
/// <c>POST /api/tables/{id}/shard/lookup</c>, <c>GET .../shards</c> and <c>GET .../shards/scan</c>. A new
/// interface rather than more members on <see cref="ITableReadFacade"/> for the same reason
/// <see cref="IConnectorStatusFacade"/> is one: existing facade members are frozen, and test fakes
/// implement them.
///
/// SHARDING IS ORLEANS-ONLY, exactly as partitioned execution is (decision D-F): the Dapr flavor refuses
/// to START a table with a non-empty <see cref="TableDefinition.ShardBy"/>, so its implementation reports a
/// disabled tier rather than pretending to serve one.
///
/// ADDRESSED BY TABLE NAME, which is what the shard tier's grain keys are derived from — and why plan 011
/// D2 refuses to rename a sharded table (see RegistryGrain.UpdateTableAsync).</summary>
public interface ITableShardFacade
{
    /// <summary>Everything for one shard key: that key's rows and that key's version trails. Strictly
    /// consistent by construction (one grain, one ordered delta stream). ACTIVATES the shard — that is
    /// the intended cost of asking about a key, and the reason a keyless listing must never do it for
    /// every key at once.</summary>
    Task<TableShardView> GetShardAsync(string tableName, string shardKey, int historyLimitPerKey);

    /// <summary>Table-level sharding metrics. Wakes NOTHING — see <see cref="TableShardingInfo"/>.</summary>
    Task<TableShardingInfo> GetInfoAsync(string tableName);

    /// <summary>The live shard keys, from the directory. Wakes nothing.</summary>
    Task<List<string>> GetKeysAsync(string tableName, int limit, int offset);

    /// <summary>The EXPLICIT full scan: activates every shard in the returned range. Separate from every
    /// other read on this facade precisely so that no routine poll can reach it by accident. NOT a
    /// consistent cut — shards are read one after another while ingest continues, and each shard's own
    /// AppliedSeq says where it was. For a cut, use <see cref="ScanFencedAsync"/>.</summary>
    Task<List<TableShardStats>> ScanAsync(string tableName, int limit, int offset);

    /// <summary>Plan 011 D2: the same scan taken as a GENUINE CONSISTENT CUT at a named router sequence —
    /// see <see cref="TableShardScanResult"/>. Opt-in, because the fence pauses the shard tier's ingest for
    /// as long as the scan takes.</summary>
    Task<TableShardScanResult> ScanFencedAsync(string tableName, int limit, int offset);
}

/// <summary>Read facade for connector runtime status (plan 006, D-C). New interface rather than
/// an ICatalogFacade extension — existing facade members are frozen (test fakes implement them).
/// Orleans: IConnectorGrain proxy; Dapr: ConnectorActor proxy. Generator-kind sources → null.</summary>
public interface IConnectorStatusFacade
{
    Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName);
}
