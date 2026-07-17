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
