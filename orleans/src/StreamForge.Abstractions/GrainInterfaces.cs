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
