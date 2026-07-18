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

    /// <summary>Plan 003 M4: the epoch a coordinator-mode (Parallelism &gt;= 2) table's read-side snapshot
    /// currently reflects — null for a Parallelism==1 table (see TableMetrics.SnapshotFrontierEpoch, which
    /// carries the identical value; this method exists so the /rows endpoint can read it in O(1) without
    /// paying for GetMetricsAsync's full per-partition fan-out on every poll).</summary>
    Task<long?> GetSnapshotFrontierEpochAsync();

    /// <summary>Plan 003 M4: called by ITableOutputGrain.PublishAsync (one call per terminal-stage
    /// partition's own frontier advance) — the dedicated, epoch-aware ingestion path for a coordinator-mode
    /// table's read-side snapshot, replacing the pre-M4 design of self-subscribing to the table's own output
    /// delta stream (see TableGrain's class doc for why that design couldn't give an honest frontier: batches
    /// from different terminal partitions arrived and were applied independently, so a read between two such
    /// arrivals could observe a partially-applied epoch). This method instead buffers per (partition, epoch)
    /// with the same FrontierTracker+EpochBuffer primitives every other dataflow hop uses, and only
    /// consolidates a batch into the read-side snapshot once EVERY terminal partition has reported reaching
    /// that epoch — making "the snapshot reflects all deltas &lt;= frontierEpoch and none beyond" true by
    /// construction, not by convention. fromPartition/epoch are the terminal stage's own UpstreamId
    /// components (plain ints/longs, matching ITableStageGrain.PushBatchAsync's no-Dataflow-dependency
    /// rule). Not part of the table's public read surface — internal to the M2 grain topology, called only
    /// by this table's own ITableOutputGrain.</summary>
    Task OnOutputBatchAsync(int fromPartition, long epoch, List<TableDeltaDto> deltas);
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
/// table-over-table subscriber keep working unchanged regardless of which mode produced the table.
///
/// PLAN 003 M4: <paramref name="fromPartition"/>/<paramref name="epoch"/> (a caller's own EdgeId.Value/
/// UpstreamId components, plain ints/longs per the same no-Dataflow-dependency rule ITableStageGrain.
/// PushBatchAsync already follows) additively carry the terminal-stage partition's own frontier at the
/// moment it routed this batch out. PublishAsync forwards them, ALONGSIDE the unchanged stream republish,
/// to the owning ITableGrain's OnOutputBatchAsync — see that method's doc comment for why frontier
/// tracking rides a second, dedicated channel instead of being folded into the shared delta stream's
/// payload (answer: because that stream's item type is a public contract several OTHER unrelated
/// consumers depend on unchanged).</summary>
public interface ITableOutputGrain : IGrainWithStringKey
{
    Task StartAsync(TableDefinition def);
    Task StopAsync();
    Task PublishAsync(int fromPartition, long epoch, List<TableDeltaDto> deltas);
}

// ============================================================================
// Plan 003 M3: shared arrangements. Key = "{inputName}:{keySpecHash}:{partition}" — keySpecHash is
// StreamForge.Engine.Dataflow.ArrangementKeySpec.HashOf(canonicalKeySpec); two tables that each join the
// SAME raw input on the SAME raw field(s), at the SAME consuming partition count, compute the IDENTICAL
// keySpecHash and therefore land on the SAME ArrangementGrain activations — that's the whole sharing
// mechanism, no separate directory/registry needed (mirrors the M2 "recompile-per-grain" philosophy: any
// caller that recompiles the same table SQL deterministically arrives at the same key). One activation per
// (input, keySpec, partition) — maintains that partition's consolidated Z-set index of the raw input,
// subscribed to the input's stream directly (replacing what a private TableIngestGrain would do for the
// join edges it covers — see TableDataflowBuilder's arrangeability rule for exactly which edges those are).
//
// LIFECYCLE is refcount-driven, not Start/Stop: AttachAsync/DetachAsync increment/decrement a consumer
// count; 0->1 lazily activates (subscribes, and if a persisted checkpoint exists, seeds the index from it
// and marks Rebuilding until live traffic confirms catch-up); ->0 clears all in-memory state AND the
// persisted checkpoint (a stopped/deleted table's arrangement should rebuild fresh on next attach, not serve
// another table's stale leftover data — "rebuild lazily on first attach" per the M3 plan).
//
// RACE-FREE SEED HANDOFF: AttachAsync itself (not a separate call) pushes the current consolidated snapshot
// directly to the new consumer's ITableStageGrain (via TargetGrainKey/TargetEdgeId in the request) BEFORE
// returning and BEFORE the consumer is added to the live-push list used by subsequent flushes — since this
// all happens synchronously within one grain turn (Orleans serializes calls to one activation; nothing else
// can interleave inside AttachAsync's own execution), the target's FrontierTracker is guaranteed to observe
// the snapshot epoch strictly before any live-delta epoch for this arrangement partition's upstream identity
// — no coordinator-mediated relay, no snapshot/live race. SnapshotAsync (separate, side-effect-free) exists
// for read-only inspection (metrics, tests) without touching the live-push set.
// ============================================================================

/// <summary>See this section's class doc above. Every method receives plain/serializable info rather than
/// StreamForge.Engine.Dataflow types (same InternalsVisibleTo-avoidance rule as ITableStageGrain).</summary>
public interface IArrangementGrain : IGrainWithStringKey
{
    /// <summary>Refcount++ (lazily activates — subscribes to the input's stream, seeds from a persisted
    /// checkpoint if present — on 0-&gt;1); atomically pushes the current consolidated snapshot to
    /// <see cref="ArrangementAttachRequest.TargetGrainKey"/> and starts including this consumer in future
    /// live-delta pushes. Idempotent-ish: re-attaching an already-attached consumerId just re-seeds it (safe,
    /// if occasionally redundant, on a caller retry).</summary>
    Task AttachAsync(ArrangementAttachRequest request);

    /// <summary>Refcount-- ; removes consumerId from the live-push list. At refcount 0: unsubscribes, clears
    /// the in-memory index AND the persisted checkpoint, and allows the grain to deactivate.</summary>
    Task DetachAsync(string consumerId);

    /// <summary>Read-only: the current consolidated index as a batch of assert-weight deltas. No side
    /// effects on the live-push set (use AttachAsync for the race-free seed-then-subscribe handshake).</summary>
    Task<List<TableDeltaDto>> SnapshotAsync();

    /// <summary>Point-in-time metrics — backs TableGrain's Rebuilding fold-in and GET /api/meta/arrangements.</summary>
    Task<ArrangementInfo> GetInfoAsync();
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
