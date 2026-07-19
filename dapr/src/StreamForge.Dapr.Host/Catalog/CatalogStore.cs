using StreamForge.AppCore;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Lifecycle;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Dapr.Host.Catalog;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: pure(-ish) catalog logic ported from Orleans'
/// <c>RegistryGrain</c> (orleans/src/StreamForge.Host/Grains/RegistryGrain.cs) — same name/id rules,
/// duplicate checks, table-dependency-on-delete checks, SQL compilation on create/update, Parallelism
/// validation, and field-number bookkeeping. Two deliberate differences from the grain it was ported
/// from:
///
/// <list type="number">
/// <item><b>No direct calls into other actors.</b> Every place RegistryGrain reaches for
/// <c>GrainFactory.GetGrain&lt;...&gt;(...)</c> (GeneratorGrain/PipelineGrain/TableGrain/
/// ITableHistoryGrain, plus the lifecycle stream publish) calls <see cref="ILifecycleOrchestrator"/>
/// instead — see that interface's doc comment for the reentrancy rationale (dapr/ARCHITECTURE.md).</item>
/// <item><b>Parallelism &gt; 1 is always rejected</b>, not just &gt; 16: partitioned execution is
/// Orleans-only (plan decision D-F) — see <see cref="ValidateParallelism"/>.</item>
/// </list>
///
/// <para>This class is a plain, actor-framework-free POCO operating on an in-memory
/// <see cref="CatalogState"/> reference — <see cref="Actors.RegistryActor"/> is the only caller,
/// responsible for loading/saving that state via Dapr's <c>StateManager</c> and translating this class's
/// thrown <see cref="InvalidOperationException"/>s into the actor's own result-wrapper return types
/// (see RegistryActor's class doc for why: Dapr actor-to-client exception marshaling isn't a type-safe
/// channel to depend on, so the actor boundary uses explicit result types instead of relying on the
/// SDK to reconstruct exception types across the wire). Being a plain class with no actor/Dapr
/// dependency at all is exactly what makes it unit-testable without a running Dapr sidecar — see
/// dapr/tests/StreamForge.Dapr.Tests/CatalogStoreTests.cs.</para>
/// </summary>
public sealed class CatalogStore(CatalogState state, ILifecycleOrchestrator orchestrator)
{
    /// <summary>Seeds defaults on first run (shared <see cref="SeedCatalog"/> — same demo world as the
    /// Orleans flavor). Plan 005 W4 "seed status" decision (see dapr/ARCHITECTURE.md), UPDATED by W6 for
    /// pipelines: tables are still forced to <see cref="PipelineStatus.Stopped"/> here regardless of what
    /// <see cref="SeedCatalog"/> marks them as, because no table runtime exists yet (W7) — a seeded
    /// "Running" badge with zero rows ever arriving would be a lie the UI tells the user. Pipelines are NO
    /// LONGER force-stopped: <see cref="Actors.PipelineActor"/> now exists, and
    /// <see cref="Services.PipelineSupervisorService"/>'s boot sweep resumes every seeded Running pipeline
    /// for real, exactly mirroring Orleans' own <c>RegistryGrain.EnsureInitializedAsync</c> resume-on-boot
    /// behavior (see that method) — a seeded Running pipeline is now honestly Running, same as sources
    /// have been since W5-A. Returns true if anything was seeded (caller persists only then).</summary>
    public bool EnsureInitialized()
    {
        var dirty = false;

        if (state.Sources.Count == 0)
        {
            state.Sources.AddRange(SeedCatalog.Sources());
            dirty = true;
        }

        if (state.Pipelines.Count == 0)
        {
            state.Pipelines.AddRange(SeedCatalog.Pipelines());
            dirty = true;
        }

        if (state.Tables.Count == 0)
        {
            var streamSchemas = BuildStreamSchemas();
            var tableSchemas = new Dictionary<string, SourceSchema>();
            var seeds = SeedCatalog.Tables();
            foreach (var t in seeds)
            {
                var result = SqlCompiler.CompileTable(t.Sql, streamSchemas, tableSchemas);
                if (result.Ok && result.OutputSchema is not null)
                {
                    t.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
                    t.StreamInputs = result.StreamInputs.ToList();
                    t.TableInputs = result.TableInputs.ToList();
                    tableSchemas[t.Name] = result.OutputSchema;
                }
                else
                {
                    t.Error = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                }

                // Dapr flavor: always Stopped on seed (see this method's doc comment) — Orleans seeds a
                // few of these Running (resumed by EnsureInitializedAsync's boot loop); here there is no
                // boot loop because there is no runtime to resume onto yet.
                t.Status = PipelineStatus.Stopped;
            }
            state.Tables.AddRange(seeds);
            dirty = true;
        }

        return dirty;
    }

    // ------------------------------------------------------------------
    // Sources
    // ------------------------------------------------------------------

    public List<SourceDefinition> GetSources() => state.Sources.ToList();

    public SourceDefinition? GetSource(string name) => state.Sources.FirstOrDefault(s => s.Name == name);

    public async Task UpsertSourceAsync(SourceDefinition def)
    {
        var idx = state.Sources.FindIndex(s => s.Name == def.Name);
        if (idx >= 0)
        {
            state.Sources[idx] = def;
        }
        else
        {
            state.Sources.Add(def);
        }

        await orchestrator.NotifySourceChangedAsync(def);
    }

    public async Task<bool> DeleteSourceAsync(string name)
    {
        var removed = state.Sources.RemoveAll(s => s.Name == name) > 0;
        if (!removed)
        {
            return false;
        }

        await orchestrator.NotifySourceRemovedAsync(name);
        return true;
    }

    // ------------------------------------------------------------------
    // Pipelines
    // ------------------------------------------------------------------

    public List<PipelineDefinition> GetPipelines() => state.Pipelines.ToList();

    public PipelineDefinition? GetPipeline(string id) => state.Pipelines.FirstOrDefault(p => p.Id == id);

    public async Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;

        state.Pipelines.Add(def);
        await orchestrator.PublishLifecycleAsync(def.Id, "created", def.Status);
        return def;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var existing = state.Pipelines.FirstOrDefault(p => p.Id == def.Id);
        if (existing is null)
        {
            return null;
        }

        var sqlChanged = existing.Sql != def.Sql;
        var wasRunning = existing.Status == PipelineStatus.Running;

        existing.Name = def.Name;
        existing.Description = def.Description;
        existing.Sql = def.Sql;
        existing.Tags = def.Tags;
        existing.Metadata = def.Metadata;
        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (sqlChanged && wasRunning)
        {
            await orchestrator.StopPipelineAsync(existing.Id);
            var outcome = await orchestrator.StartPipelineAsync(existing, state.Sources);
            if (outcome.Ok)
            {
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
            }
            else
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = outcome.Error;
            }
        }

        await orchestrator.PublishLifecycleAsync(existing.Id, "updated", existing.Status);
        return existing;
    }

    public async Task<bool> DeletePipelineAsync(string id)
    {
        var existing = state.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return false;
        }

        if (existing.Status == PipelineStatus.Running)
        {
            await orchestrator.StopPipelineAsync(id);
        }

        state.Pipelines.Remove(existing);
        await orchestrator.PublishLifecycleAsync(id, "deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return null;
        }

        if (status == PipelineStatus.Running)
        {
            var outcome = await orchestrator.StartPipelineAsync(existing, state.Sources);
            if (outcome.Ok)
            {
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await orchestrator.PublishLifecycleAsync(id, "started", existing.Status);
            }
            else
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = outcome.Error;
                await orchestrator.PublishLifecycleAsync(id, "failed", existing.Status);
            }
        }
        else
        {
            await orchestrator.StopPipelineAsync(id);
            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await orchestrator.PublishLifecycleAsync(id, "stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return existing;
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    public List<TableDefinition> GetTables() => state.Tables.ToList();

    public TableDefinition? GetTable(string id) => state.Tables.FirstOrDefault(t => t.Id == id);

    /// <summary>Throws <see cref="InvalidOperationException"/> on a name collision or an unsupported
    /// Parallelism value — <see cref="Actors.RegistryActor"/> catches this and turns it into a Failure
    /// result (see that type's class doc), which the Dapr facade adapter re-throws client-side so the
    /// shared TablesEndpoints' existing <c>catch (InvalidOperationException)</c> → 409 pathway fires
    /// identically to the Orleans flavor.</summary>
    public async Task<TableDefinition> CreateTableAsync(TableDefinition def)
    {
        ValidateUniqueTableName(def.Name, excludeTableId: null);
        ValidateParallelism(def.Parallelism);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;

        var compileResult = CompileTableSql(def.Sql, excludeTableId: def.Id);
        ValidateHistoryConfig(def, compileResult);
        ApplyCompileResult(def, compileResult);

        state.Tables.Add(def);
        await orchestrator.PublishLifecycleAsync(def.Name, "table-created", def.Status);
        await orchestrator.ResetTableHistoryAsync(def);
        return def;
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var existing = state.Tables.FirstOrDefault(t => t.Id == def.Id);
        if (existing is null)
        {
            return null;
        }

        if (!string.Equals(existing.Name, def.Name, StringComparison.Ordinal))
        {
            ValidateUniqueTableName(def.Name, excludeTableId: existing.Id);
        }
        ValidateParallelism(def.Parallelism);

        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
        var parallelismChanged = existing.Parallelism != def.Parallelism;
        var historyConfigChanged =
            existing.HistoryEnabled != def.HistoryEnabled ||
            existing.HistoryMode != def.HistoryMode ||
            existing.HistoryLimit != def.HistoryLimit ||
            existing.HistoryByField != def.HistoryByField ||
            existing.HistoryWindowMs != def.HistoryWindowMs;
        var wasRunning = existing.Status == PipelineStatus.Running;

        existing.Name = def.Name;
        existing.Description = def.Description;
        existing.Sql = def.Sql;
        existing.SearchEnabled = def.SearchEnabled;
        existing.SearchMode = def.SearchMode;
        existing.HistoryEnabled = def.HistoryEnabled;
        existing.HistoryMode = def.HistoryMode;
        existing.HistoryLimit = def.HistoryLimit;
        existing.HistoryByField = def.HistoryByField;
        existing.HistoryWindowMs = def.HistoryWindowMs;
        existing.Tags = def.Tags;
        existing.Metadata = def.Metadata;
        existing.Parallelism = def.Parallelism;
        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        ApplyCompileResult(existing, compileResult);

        if ((sqlChanged || searchChanged || parallelismChanged) && wasRunning)
        {
            await orchestrator.StopTableAsync(existing.Name);
            var outcome = await orchestrator.StartTableAsync(existing);
            if (outcome.Ok)
            {
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
            }
            else
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = outcome.Error;
            }
        }

        if (sqlChanged || historyConfigChanged)
        {
            await orchestrator.ResetTableHistoryAsync(existing);
        }

        await orchestrator.PublishLifecycleAsync(existing.Name, "table-updated", existing.Status);
        return existing;
    }

    public async Task<bool> DeleteTableAsync(string id)
    {
        var existing = state.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return false;
        }

        ThrowIfRunningDependents(existing.Name, "delete");

        if (existing.Status == PipelineStatus.Running)
        {
            await orchestrator.StopTableAsync(existing.Name);
        }

        await orchestrator.DisableTableHistoryAsync(existing.Name);

        state.Tables.Remove(existing);
        await orchestrator.PublishLifecycleAsync(existing.Name, "table-deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return null;
        }

        if (status == PipelineStatus.Running)
        {
            var missing = existing.TableInputs
                .Where(name => state.Tables.FirstOrDefault(t => t.Name == name)?.Status != PipelineStatus.Running)
                .ToList();

            if (missing.Count > 0)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = $"table input(s) not running: {string.Join(", ", missing)}";
                existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await orchestrator.PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
                return existing;
            }

            var outcome = await orchestrator.StartTableAsync(existing);
            if (outcome.Ok)
            {
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await orchestrator.PublishLifecycleAsync(existing.Name, "table-started", existing.Status);
            }
            else
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = outcome.Error;
                await orchestrator.PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
            }
        }
        else
        {
            ThrowIfRunningDependents(existing.Name, "stop");

            await orchestrator.StopTableAsync(existing.Name);
            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await orchestrator.PublishLifecycleAsync(existing.Name, "table-stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return existing;
    }

    // ------------------------------------------------------------------
    // Field numbers
    // ------------------------------------------------------------------

    /// <summary>See ICatalogFacade.EnsureFieldNumbersAsync — identical algorithm to RegistryGrain's,
    /// operating on this actor's own persisted map instead of a grain's IPersistentState.</summary>
    public string EnsureFieldNumbers(string entityKey, List<FieldDef> fields)
    {
        var existingJson = state.FieldNumberMaps.GetValueOrDefault(entityKey);
        var existing = existingJson is null
            ? null
            : System.Text.Json.JsonSerializer.Deserialize<FieldNumberMap>(existingJson);
        var updatedJson = System.Text.Json.JsonSerializer.Serialize(FieldNumberMap.Assign(fields, existing));
        if (updatedJson != existingJson)
        {
            state.FieldNumberMaps[entityKey] = updatedJson;
        }

        return updatedJson;
    }

    // ------------------------------------------------------------------
    // Validation / helpers — ported verbatim from RegistryGrain (see that class for the extended
    // rationale in each doc comment).
    // ------------------------------------------------------------------

    private void ValidateUniqueTableName(string name, string? excludeTableId)
    {
        if (state.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by a stream source");
        }
        if (state.Tables.Any(t => t.Id != excludeTableId && string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another table");
        }
    }

    /// <summary>Plan 005 D-F: partitioned execution (Parallelism 2-16) is Orleans-only — the Dapr
    /// registry rejects anything other than 1, not just values outside Orleans' 1..16 range.</summary>
    private static void ValidateParallelism(int parallelism)
    {
        if (parallelism != 1)
        {
            throw new InvalidOperationException(
                $"Parallelism must be 1 on the Dapr flavor (got {parallelism}) — partitioned execution is Orleans-only in the Dapr flavor.");
        }
    }

    private void ThrowIfRunningDependents(string tableName, string action)
    {
        var dependents = state.Tables
            .Where(t => t.Status == PipelineStatus.Running && t.TableInputs.Contains(tableName, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException($"Cannot {action} '{tableName}': running table(s) depend on it: {string.Join(", ", dependents)}");
        }
    }

    private TableCompileResult CompileTableSql(string sql, string? excludeTableId)
    {
        var streamSchemas = BuildStreamSchemas();
        var tableSchemas = BuildTableSchemas(excludeTableId);
        return SqlCompiler.CompileTable(sql, streamSchemas, tableSchemas);
    }

    private static void ApplyCompileResult(TableDefinition def, TableCompileResult result)
    {
        if (result.Ok && result.OutputSchema is not null)
        {
            def.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
            def.StreamInputs = result.StreamInputs.ToList();
            def.TableInputs = result.TableInputs.ToList();
        }
        else
        {
            def.OutputFields = [];
            def.StreamInputs = [];
            def.TableInputs = [];
        }
    }

    private static void ValidateHistoryConfig(TableDefinition def, TableCompileResult compileResult)
    {
        if (!def.HistoryEnabled || def.HistoryMode is not (TableHistoryMode.MinBy or TableHistoryMode.MaxBy))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(def.HistoryByField))
        {
            throw new InvalidOperationException($"History mode {def.HistoryMode} requires historyByField to be set.");
        }

        if (compileResult.OutputSchema is null || !compileResult.OutputSchema.Fields.TryGetValue(def.HistoryByField, out var kind))
        {
            throw new InvalidOperationException($"historyByField '{def.HistoryByField}' is not one of this table's output fields.");
        }

        if (kind is not (FieldKind.Double or FieldKind.Long or FieldKind.Timestamp))
        {
            throw new InvalidOperationException($"historyByField '{def.HistoryByField}' must be numeric or timestamp (found {kind}).");
        }
    }

    private Dictionary<string, SourceSchema> BuildStreamSchemas() =>
        state.Sources.ToDictionary(s => s.Name, s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    private Dictionary<string, SourceSchema> BuildTableSchemas(string? excludeTableId) =>
        state.Tables
            .Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0)
            .ToDictionary(t => t.Name, t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    private static FieldType MapFieldType(FieldKind kind) => kind switch
    {
        FieldKind.String => FieldType.String,
        FieldKind.Double => FieldType.Double,
        FieldKind.Long => FieldType.Long,
        FieldKind.Bool => FieldType.Bool,
        FieldKind.Timestamp => FieldType.Timestamp,
        FieldKind.Json => FieldType.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown field kind"),
    };
}
