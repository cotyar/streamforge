using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Host.Grains;

public sealed class RegistryState
{
    public List<SourceDefinition> Sources { get; set; } = [];
    public List<PipelineDefinition> Pipelines { get; set; } = [];
    public List<TableDefinition> Tables { get; set; } = [];

    /// <summary>entityKey ("source:{name}" / "pipeline:{id}" / "table:{id}") → FieldNumberMap JSON.
    /// See IRegistryGrain.EnsureFieldNumbersAsync.</summary>
    public Dictionary<string, string> FieldNumberMaps { get; set; } = [];
}

/// <summary>Singleton grain (key = StreamConstants.RegistryKey). Catalog of sources + pipelines; orchestrates start/stop.
/// Not [Reentrant] overall (mutations must stay serialized), but the read-only Get* methods are allowed to
/// interleave: PipelineGrain.StartAsync calls back into GetSourcesAsync while it is itself being started
/// from inside RegistryGrain.EnsureInitializedAsync / SetPipelineStatusAsync — without interleaving that
/// call would deadlock waiting on this grain's own in-flight turn.</summary>
[MayInterleave(nameof(MayInterleave))]
public sealed class RegistryGrain(
    [PersistentState("catalog", StreamConstants.StorageName)] IPersistentState<RegistryState> state)
    : Grain, IRegistryGrain
{
    private static readonly HashSet<string> InterleavableMethods = new(StringComparer.Ordinal)
    {
        nameof(IRegistryGrain.GetSourcesAsync),
        nameof(IRegistryGrain.GetSourceAsync),
        nameof(IRegistryGrain.GetPipelinesAsync),
        nameof(IRegistryGrain.GetPipelineAsync),
        nameof(IRegistryGrain.GetTablesAsync),
        nameof(IRegistryGrain.GetTableAsync),
    };

    public static bool MayInterleave(IInvokable req) => InterleavableMethods.Contains(req.GetMethodName());

    public async Task EnsureInitializedAsync()
    {
        var dirty = false;
        if (state.State.Sources.Count == 0)
        {
            state.State.Sources.AddRange(SeedCatalog.Sources());
            dirty = true;
        }

        if (state.State.Pipelines.Count == 0)
        {
            state.State.Pipelines.AddRange(SeedCatalog.Pipelines());
            dirty = true;
        }

        if (state.State.Tables.Count == 0)
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
                    t.Status = PipelineStatus.Stopped;
                    t.Error = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                }
            }
            state.State.Tables.AddRange(seeds);
            dirty = true;
        }

        if (dirty)
        {
            await state.WriteStateAsync();
        }

        foreach (var src in state.State.Sources.Where(s => s.Enabled))
        {
            try
            {
                // Plan 006 D-C: Kind dispatch. "generator" (or unset — pre-006 seeds/sources) keeps the
                // pre-existing IGeneratorGrain path unchanged; every other Kind goes to IConnectorGrain.
                if (IsGeneratorKind(src.Kind))
                {
                    await GrainFactory.GetGrain<IGeneratorGrain>(src.Name).StartAsync(src);
                }
                else
                {
                    await GrainFactory.GetGrain<IConnectorGrain>(src.Name).StartAsync(src);
                }
            }
            catch
            {
                // best-effort on boot; supervisor will retry via PingAsync
            }
        }

        var statusChanged = false;
        foreach (var pipeline in state.State.Pipelines.Where(p => p.Status == PipelineStatus.Running))
        {
            try
            {
                await GrainFactory.GetGrain<IPipelineGrain>(pipeline.Id).StartAsync(pipeline);
            }
            catch (Exception ex)
            {
                pipeline.Status = PipelineStatus.Failed;
                pipeline.Error = ex.Message;
                statusChanged = true;
            }
        }

        // Resume Running tables in dependency order (topological — table-over-table inputs before their
        // dependents) so a chained table's own inputs are already Running when it starts.
        var runningTables = state.State.Tables.Where(t => t.Status == PipelineStatus.Running).ToList();
        foreach (var table in TopoSortByTableInputs(runningTables, state.State.Tables))
        {
            try
            {
                await GrainFactory.GetGrain<ITableGrain>(table.Name).StartAsync(table);
            }
            catch (Exception ex)
            {
                table.Status = PipelineStatus.Failed;
                table.Error = ex.Message;
                statusChanged = true;
            }
        }

        if (statusChanged)
        {
            await state.WriteStateAsync();
        }

        // Re-subscribe history grains for every table with HistoryEnabled — independent of the table's own
        // Running status (history just won't see new deltas until/unless the table is running). Unlike the
        // table-resume loop above, this uses ResumeAsync (not ResetAsync) so previously accumulated history
        // survives a silo restart.
        foreach (var table in state.State.Tables.Where(t => t.HistoryEnabled))
        {
            try
            {
                await GrainFactory.GetGrain<ITableHistoryGrain>(table.Name).ResumeAsync(table);
            }
            catch
            {
                // best-effort — a stale/misconfigured history grain shouldn't block boot.
            }
        }
    }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(state.State.Sources.ToList());

    public Task<SourceDefinition?> GetSourceAsync(string name) =>
        Task.FromResult(state.State.Sources.FirstOrDefault(s => s.Name == name));

    public async Task UpsertSourceAsync(SourceDefinition def)
    {
        var idx = state.State.Sources.FindIndex(s => s.Name == def.Name);
        if (idx >= 0)
        {
            state.State.Sources[idx] = def;
        }
        else
        {
            state.State.Sources.Add(def);
        }

        await state.WriteStateAsync();

        // Plan 006 D-C: Kind dispatch. On an update where Kind CHANGED (e.g. generator -> url), the
        // grain for the OLD kind is still activated and would otherwise keep polling/ticking forever —
        // so both StopAsync calls always run (cheap/idempotent no-ops on whichever kind wasn't actually
        // running) rather than tracking the previous Kind separately just to target one Stop call.
        var generator = GrainFactory.GetGrain<IGeneratorGrain>(def.Name);
        var connector = GrainFactory.GetGrain<IConnectorGrain>(def.Name);
        if (def.Enabled)
        {
            if (IsGeneratorKind(def.Kind))
            {
                await generator.StartAsync(def);
                await connector.StopAsync();
            }
            else
            {
                await connector.StartAsync(def);
                await generator.StopAsync();
            }
        }
        else
        {
            await generator.StopAsync();
            await connector.StopAsync();
        }
    }

    public async Task<bool> DeleteSourceAsync(string name)
    {
        var removed = state.State.Sources.RemoveAll(s => s.Name == name) > 0;
        if (!removed)
        {
            return false;
        }

        await state.WriteStateAsync();
        // Stop both kinds unconditionally — see UpsertSourceAsync's dispatch comment (cheap/idempotent).
        await GrainFactory.GetGrain<IGeneratorGrain>(name).StopAsync();
        await GrainFactory.GetGrain<IConnectorGrain>(name).StopAsync();
        return true;
    }

    /// <summary>Plan 006 D-C: "generator" (the SourceDefinition.Kind default) or an unset/empty value
    /// (pre-006 persisted sources, seeds) both mean "use IGeneratorGrain" — anything else dispatches to
    /// IConnectorGrain instead.</summary>
    private static bool IsGeneratorKind(string? kind) =>
        string.IsNullOrEmpty(kind) || kind == SourceKinds.Generator;

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(state.State.Pipelines.ToList());

    public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
        Task.FromResult(state.State.Pipelines.FirstOrDefault(p => p.Id == id));

    public async Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;

        state.State.Pipelines.Add(def);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Id, "created", def.Status);
        return def;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var existing = state.State.Pipelines.FirstOrDefault(p => p.Id == def.Id);
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
            var pipelineGrain = GrainFactory.GetGrain<IPipelineGrain>(existing.Id);
            try
            {
                await pipelineGrain.StopAsync();
                await pipelineGrain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
            }
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(existing.Id, "updated", existing.Status);
        return existing;
    }

    public async Task<bool> DeletePipelineAsync(string id)
    {
        var existing = state.State.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return false;
        }

        if (existing.Status == PipelineStatus.Running)
        {
            try
            {
                await GrainFactory.GetGrain<IPipelineGrain>(id).StopAsync();
            }
            catch
            {
                // best-effort
            }
        }

        state.State.Pipelines.Remove(existing);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(id, "deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.State.Pipelines.FirstOrDefault(p => p.Id == id);
        if (existing is null)
        {
            return null;
        }

        var grain = GrainFactory.GetGrain<IPipelineGrain>(id);
        if (status == PipelineStatus.Running)
        {
            try
            {
                await grain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await PublishLifecycleAsync(id, "started", existing.Status);
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
                await PublishLifecycleAsync(id, "failed", existing.Status);
            }
        }
        else
        {
            try
            {
                await grain.StopAsync();
            }
            catch
            {
                // best-effort
            }

            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await PublishLifecycleAsync(id, "stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await state.WriteStateAsync();
        return existing;
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(state.State.Tables.ToList());

    public Task<TableDefinition?> GetTableAsync(string id) =>
        Task.FromResult(state.State.Tables.FirstOrDefault(t => t.Id == id));

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

        state.State.Tables.Add(def);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Name, "table-created", def.Status);

        // Configures (and, if HistoryEnabled, subscribes) the history grain up front — independent of
        // whether the table itself is Running, exactly like SearchEnabled's index is built incrementally
        // once the table starts producing deltas.
        await GrainFactory.GetGrain<ITableHistoryGrain>(def.Name).ResetAsync(def);
        return def;
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var existing = state.State.Tables.FirstOrDefault(t => t.Id == def.Id);
        if (existing is null)
        {
            return null;
        }

        if (!string.Equals(existing.Name, def.Name, StringComparison.Ordinal))
        {
            ValidateUniqueTableName(def.Name, excludeTableId: existing.Id);
        }
        ValidateParallelism(def.Parallelism);

        // Compile + validate against the *prospective* SQL/history config before mutating `existing` at
        // all, so a rejected update (bad name, bad historyByField) never leaves partially-applied state
        // behind in the in-memory list entry.
        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
        // Plan 003 M2: a Parallelism change (1->N, N->1, or N->M) changes which grain topology the table's
        // grain(s) run as (classic vs. coordinator mode, or a differently-shaped partitioned graph) — mirror
        // the search-config restart semantics below so it takes effect immediately on a Running table
        // instead of only on the next manual stop/start.
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

        // A running table's grain only picks up SQL/search config/parallelism changes on (re)StartAsync —
        // mirror the SQL-changed restart below for search config and parallelism too, so toggling
        // SearchEnabled/SearchMode/Parallelism on a Running table takes effect immediately instead of only
        // on the next manual stop/start.
        if ((sqlChanged || searchChanged || parallelismChanged) && wasRunning)
        {
            var tableGrain = GrainFactory.GetGrain<ITableGrain>(existing.Name);
            try
            {
                await tableGrain.StopAsync();
                await tableGrain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
            }
        }

        // History config or the SQL it was derived from changed — the row-identity mapping and/or
        // retention policy is now stale, so reset (not resume) the history grain, exactly like a
        // SQL/search-config change restarts the table's own grain above.
        if (sqlChanged || historyConfigChanged)
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(existing.Name).ResetAsync(existing);
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(existing.Name, "table-updated", existing.Status);
        return existing;
    }

    public async Task<bool> DeleteTableAsync(string id)
    {
        var existing = state.State.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return false;
        }

        ThrowIfRunningDependents(existing.Name, "delete");

        if (existing.Status == PipelineStatus.Running)
        {
            try
            {
                await GrainFactory.GetGrain<ITableGrain>(existing.Name).StopAsync();
            }
            catch
            {
                // best-effort
            }
        }

        try
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(existing.Name).DisableAsync();
        }
        catch
        {
            // best-effort
        }

        state.State.Tables.Remove(existing);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(existing.Name, "table-deleted", PipelineStatus.Stopped);
        return true;
    }

    public async Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
    {
        var existing = state.State.Tables.FirstOrDefault(t => t.Id == id);
        if (existing is null)
        {
            return null;
        }

        var grain = GrainFactory.GetGrain<ITableGrain>(existing.Name);
        if (status == PipelineStatus.Running)
        {
            var missing = existing.TableInputs
                .Where(name => state.State.Tables.FirstOrDefault(t => t.Name == name)?.Status != PipelineStatus.Running)
                .ToList();

            if (missing.Count > 0)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = $"table input(s) not running: {string.Join(", ", missing)}";
                existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await state.WriteStateAsync();
                await PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
                return existing;
            }

            try
            {
                await grain.StartAsync(existing);
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await PublishLifecycleAsync(existing.Name, "table-started", existing.Status);
            }
            catch (Exception ex)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = ex.Message;
                await PublishLifecycleAsync(existing.Name, "table-failed", existing.Status);
            }
        }
        else
        {
            ThrowIfRunningDependents(existing.Name, "stop");

            try
            {
                await grain.StopAsync();
            }
            catch
            {
                // best-effort
            }

            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await PublishLifecycleAsync(existing.Name, "table-stopped", existing.Status);
        }

        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await state.WriteStateAsync();
        return existing;
    }

    private void ValidateUniqueTableName(string name, string? excludeTableId)
    {
        if (state.State.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by a stream source");
        }
        if (state.State.Tables.Any(t => t.Id != excludeTableId && string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another table");
        }
    }

    /// <summary>Plan 003 M2: 409-style guard (same pattern as ValidateUniqueTableName) — Parallelism must be
    /// in [1, 16] (see TableDefinition.Parallelism's doc comment: 1 = classic path, 2..16 deploys the
    /// partitioned dataflow graph; the upper bound documents the grain-explosion ceiling from plan
    /// 003's risk section).</summary>
    private static void ValidateParallelism(int parallelism)
    {
        if (parallelism is < 1 or > 16)
        {
            throw new InvalidOperationException($"Parallelism must be between 1 and 16 (got {parallelism}).");
        }
    }

    /// <summary>409-style guard: refuses to stop/delete a table that a currently-Running table depends on.</summary>
    private void ThrowIfRunningDependents(string tableName, string action)
    {
        var dependents = state.State.Tables
            .Where(t => t.Status == PipelineStatus.Running && t.TableInputs.Contains(tableName, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException($"Cannot {action} '{tableName}': running table(s) depend on it: {string.Join(", ", dependents)}");
        }
    }

    /// <summary>Compile-check for diagnostics — draft-friendly like pipelines: never blocks create/update
    /// on its own (see ValidateHistoryConfig for the one thing that DOES block: an invalid MinBy/MaxBy
    /// historyByField). Pure — does not mutate <paramref name="def"/>; pair with ApplyCompileResult.</summary>
    private TableCompileResult CompileTableSql(string sql, string? excludeTableId)
    {
        var streamSchemas = BuildStreamSchemas();
        var tableSchemas = BuildTableSchemas(excludeTableId);
        return SqlCompiler.CompileTable(sql, streamSchemas, tableSchemas);
    }

    /// <summary>Stores OutputFields/StreamInputs/TableInputs from a compile result when it compiles; leaves
    /// them empty otherwise.</summary>
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

    /// <summary>The one history-config check that actually blocks create/update (409-style, like
    /// ValidateUniqueTableName): MinBy/MaxBy needs a historyByField that is one of this table's compiled
    /// output columns and numeric/timestamp-kinded — a MinBy/MaxBy history with no comparable value to
    /// rank on can never produce a sensible "extreme" version. A table with no GROUP BY identity (and thus
    /// no derivable per-row identity beyond the whole row) is explicitly NOT rejected here — see
    /// TableGroupKeyExtractor/RowKeyCodec for that documented whole-row fallback.</summary>
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
        state.State.Sources.ToDictionary(s => s.Name, s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    private Dictionary<string, SourceSchema> BuildTableSchemas(string? excludeTableId) =>
        state.State.Tables
            .Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0)
            .ToDictionary(t => t.Name, t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    /// <summary>Kahn-style topological sort of `running` by TableInputs edges (inputs first) — used to
    /// resume table-over-table chains in dependency order on EnsureInitializedAsync.</summary>
    private static List<TableDefinition> TopoSortByTableInputs(List<TableDefinition> running, List<TableDefinition> all)
    {
        var byName = all.ToDictionary(t => t.Name, StringComparer.Ordinal);
        var runningSet = running.ToHashSet();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<TableDefinition>();

        void Visit(TableDefinition t, HashSet<string> stack)
        {
            if (visited.Contains(t.Name) || !stack.Add(t.Name)) return;
            foreach (var dep in t.TableInputs)
            {
                if (byName.TryGetValue(dep, out var depDef) && runningSet.Contains(depDef))
                {
                    Visit(depDef, stack);
                }
            }
            visited.Add(t.Name);
            result.Add(t);
        }

        foreach (var t in running)
        {
            Visit(t, new HashSet<string>(StringComparer.Ordinal));
        }
        return result;
    }

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

    // Demo sources/pipelines/tables are seeded from shared StreamForge.AppCore.SeedCatalog (plan 005
    // W2) — see EnsureInitializedAsync above — so both the Orleans and Dapr flavors seed the same demo
    // world from one place.

    public async Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields)
    {
        var existingJson = state.State.FieldNumberMaps.GetValueOrDefault(entityKey);
        var existing = existingJson is null
            ? null
            : System.Text.Json.JsonSerializer.Deserialize<FieldNumberMap>(existingJson);
        var updatedJson = System.Text.Json.JsonSerializer.Serialize(FieldNumberMap.Assign(fields, existing));
        if (updatedJson != existingJson)
        {
            state.State.FieldNumberMaps[entityKey] = updatedJson;
            await state.WriteStateAsync();
        }

        return updatedJson;
    }

    private async Task PublishLifecycleAsync(string pipelineId, string kind, PipelineStatus status)
    {
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<LifecycleEvent>(StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.LifecycleEventsKey));
        await stream.OnNextAsync(new LifecycleEvent
        {
            PipelineId = pipelineId,
            Kind = kind,
            Status = status,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }
}
