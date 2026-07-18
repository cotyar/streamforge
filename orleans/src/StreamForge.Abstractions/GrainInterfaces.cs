namespace StreamForge.Abstractions;

/// <summary>Singleton (key = StreamConstants.RegistryKey). Catalog of sources + pipelines; orchestrates start/stop.</summary>
public interface IRegistryGrain : IGrainWithStringKey
{
    /// <summary>Seeds defaults on first run, re-activates generators, resumes Running pipelines.</summary>
    Task EnsureInitializedAsync();

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

/// <summary>Key = pipeline id. One activation per running pipeline.</summary>
public interface IPipelineGrain : IGrainWithStringKey
{
    Task StartAsync(PipelineDefinition def);
    Task StopAsync();
    Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit);
    Task<PipelineMetrics> GetMetricsAsync();
}

/// <summary>Key = source name. Publishes synthetic events on a grain timer.</summary>
public interface IGeneratorGrain : IGrainWithStringKey
{
    Task StartAsync(SourceDefinition def);
    Task StopAsync();
    /// <summary>Keep-alive; timers alone don't extend activation lifetime.</summary>
    Task PingAsync();
}

/// <summary>Key = table name. One activation per running table. Materializes a Z-set (DBSP-style)
/// incremental view: subscribes to its SQL's stream and table inputs, feeds deltas through a
/// StreamForge.Engine TableExecutor, and publishes emitted deltas + persists a consolidated snapshot for
/// rehydration-free reads.</summary>
public interface ITableGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def);
    Task StopAsync();
    Task<List<TableRowDto>> GetRowsAsync(int limit, int offset);
    Task<int> GetRowCountAsync();
    Task<TableMetrics> GetMetricsAsync();
    Task<long> GetSeqAsync();
    /// <summary>Reverse-index lookup over this table's current rows (see StreamForge.Host.Search.TableSearchIndex).
    /// Empty query or a table with SearchEnabled=false both yield an empty list — callers that need to tell
    /// those apart go through the /api/tables/{id}/search endpoint instead, which checks SearchEnabled first.</summary>
    Task<List<TableRowDto>> SearchAsync(string query, int limit);
}

/// <summary>Key = table name. One activation per table with row history ever configured. Subscribes to
/// that table's delta stream (StreamConstants.TableDeltaNamespace, table name) and maintains per-row-
/// identity version history per the table's configured retention mode. See
/// StreamForge.Host.Grains.TableHistoryGrain's class comment for the full design: identity-key derivation,
/// retention semantics, and why this is a plain state grain fed by the delta stream rather than a
/// JournaledGrain.</summary>
public interface ITableHistoryGrain : IGrainWithStringKey
{
    /// <summary>(Re)configures history collection from the table's current definition — applies
    /// HistoryEnabled/HistoryMode/HistoryLimit/HistoryByField/HistoryWindowMs, re-derives the row-identity
    /// column mapping from the table's SQL, and subscribes to (or, if HistoryEnabled is now false,
    /// unsubscribes from) the table's delta stream. Always clears previously accumulated history. Call on
    /// table create and on any history-config or SQL change (mirrors TableGrain's own SQL/search-config
    /// restart semantics).</summary>
    Task ResetAsync(TableDefinition def);

    /// <summary>Disables history collection, unsubscribes, and clears all state — call on table delete.</summary>
    Task DisableAsync();

    /// <summary>Re-subscribes to the delta stream (and keeps the grain alive) WITHOUT clearing previously
    /// accumulated history — unlike ResetAsync. Call on silo/table resume (RegistryGrain.
    /// EnsureInitializedAsync) so history survives a restart the same way the persisted Entries dictionary
    /// already does; a no-op when def.HistoryEnabled is false.</summary>
    Task ResumeAsync(TableDefinition def);

    /// <summary>Version history for one row identity key (as produced by TableGroupKeyExtractor +
    /// RowKeyCodec — see the /api/tables/{id}/rows response's historyKeys). limit &lt;= 0 means "all
    /// retained versions". KeyFound is false when the key has never been observed.</summary>
    Task<TableHistoryQueryResult> GetHistoryAsync(string key, int limit);

    Task<TableHistoryStats> GetStatsAsync();
}

// ============================================================================
// Plan 003 M2: partitioned table execution grain topology. Additive — ITableGrain is unchanged; these
// three grain kinds only exist for a table whose Parallelism &gt;= 2 (see TableGrain's coordinator-mode
// doc comment). Every method receives the table's TableDefinition (already Orleans-serializable) rather
// than a serialized StreamForge.Engine.Dataflow.TableDataflowPlan — each grain independently recompiles
// the same SQL (via IRegistryGrain, exactly like TableGrain.StartAsync already does) and calls
// TablePlan.CreateDataflow(def.Parallelism) itself; since compilation is a pure function of
// (Sql, streamSchemas, tableSchemas), every grain deterministically arrives at the identical stage/edge
// graph (same stage ids, same EdgeId values) without transmitting Expr-bearing internals over the wire.
// ============================================================================

/// <summary>Key = "{tableName}:{inputName}". One activation per (table, real external input) — a stream
/// source name or an upstream table name the table's SQL reads from directly. Subscribes to that input's
/// existing stream (StreamConstants.SourcesNamespace or TableDeltaNamespace — same identity a Parallelism==1
/// TableGrain would use), stamps epochs (advance every 250ms tick OR 1000 buffered events, whichever
/// first), and batches routed deltas into the dataflow graph's stage-0 partitions per
/// TableDataflowPlan.EdgesForExternalInput's routing (hash/broadcast/local — see TableStageGrain's
/// PushBatchAsync).</summary>
public interface ITableIngestGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def, string inputName);
    Task StopAsync();
}

/// <summary>Key = "{tableName}:{stageId}:{partition}". One activation per (table, dataflow stage,
/// partition) — see StreamForge.Engine.Dataflow.TableDataflowPlan.Stages. Holds one
/// StreamForge.Engine.Dataflow.ITableStageExecutor plus a FrontierTracker + EpochBuffer per the M0
/// primitives; PushBatchAsync buffers an inbound (edge, epoch) batch, and on frontier advance processes
/// all newly-ready batches deterministically (EpochBuffer.OnFrontier's ordering), routes emitted deltas to
/// downstream stage partitions (or to TableOutputGrain when this stage's outbound edge is terminal), and
/// propagates its own advanced frontier downstream (as an empty-delta DeltaBatch marker on every
/// downstream partition, so a quiet stage doesn't stall a downstream FrontierTracker forever).</summary>
public interface ITableStageGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def, int stageId, int partition);
    Task StopAsync();

    /// <summary>edgeId/fromPartition/epoch are the raw StreamForge.Engine.Dataflow EdgeId.Value/UpstreamId
    /// components — passed as plain ints/longs (not the Dataflow types themselves) so this interface has
    /// no Orleans-serialization dependency on StreamForge.Engine.Dataflow. originName is the real external
    /// input name this batch ultimately traces back to (only meaningful on a Broadcast edge feeding a
    /// scalar-subquery/semi-anti join's nested residual execution — see TableDataflowPlan's class doc);
    /// pass "" when not applicable. deltas.Count == 0 is a valid, expected frontier-marker call.</summary>
    Task PushBatchAsync(int edgeId, int fromPartition, long epoch, string originName, List<TableDeltaDto> deltas);

    Task<TablePartitionMetrics> GetMetricsAsync();
}

/// <summary>Key = table name. One activation per Parallelism &gt;= 2 table. The single terminal publisher
/// (plan 003 M2's terminal-publisher choice — see TableGrain's coordinator-mode doc comment for why a
/// dedicated grain was chosen over "gather to partition 0"): every terminal-stage TableStageGrain (there
/// may be up to Parallelism of them, one per partition of the plan's terminal stage) calls PublishAsync as
/// its own epochs advance; this grain does no buffering/reordering of its own (Z-set consolidation is
/// commutative — see TableDataflowPlan's class doc) and simply republishes each incoming batch onto the
/// SAME (StreamConstants.TableDeltaNamespace, tableName) stream a Parallelism==1 TableGrain would have
/// published to directly — so SignalR (StreamBridgeService), TableHistoryGrain, and any downstream
/// table-over-table subscriber keep working unchanged regardless of which mode produced the table.</summary>
public interface ITableOutputGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def);
    Task StopAsync();
    Task PublishAsync(List<TableDeltaDto> deltas);
}

/// <summary>Singleton (key = StreamConstants.UsersKey).</summary>
public interface IUserStoreGrain : IGrainWithStringKey
{
    /// <summary>Seeds admin/editor/viewer on first run.</summary>
    Task EnsureInitializedAsync();
    /// <summary>Returns the user when username+password are valid, else null.</summary>
    Task<UserRecord?> ValidateCredentialsAsync(string username, string password);
    Task<List<UserRecord>> GetUsersAsync();
    Task<bool> CreateUserAsync(string username, string displayName, string role, string password);
    /// <summary>Null params leave the field unchanged.</summary>
    Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password);
    Task<bool> DeleteUserAsync(string username);
}
