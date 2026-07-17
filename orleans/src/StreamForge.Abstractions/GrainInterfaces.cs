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
