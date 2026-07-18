using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization.Invocation;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Generators;
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
            state.State.Sources.AddRange(MarketDataProfiles.SeedSources());
            dirty = true;
        }

        if (state.State.Pipelines.Count == 0)
        {
            state.State.Pipelines.AddRange(SeedPipelines());
            dirty = true;
        }

        if (state.State.Tables.Count == 0)
        {
            var streamSchemas = BuildStreamSchemas();
            var tableSchemas = new Dictionary<string, SourceSchema>();
            var seeds = SeedTables();
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
                await GrainFactory.GetGrain<IGeneratorGrain>(src.Name).StartAsync(src);
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

        var generator = GrainFactory.GetGrain<IGeneratorGrain>(def.Name);
        if (def.Enabled)
        {
            await generator.StartAsync(def);
        }
        else
        {
            await generator.StopAsync();
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
        await GrainFactory.GetGrain<IGeneratorGrain>(name).StopAsync();
        return true;
    }

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

        // Compile + validate against the *prospective* SQL/history config before mutating `existing` at
        // all, so a rejected update (bad name, bad historyByField) never leaves partially-applied state
        // behind in the in-memory list entry.
        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
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
        existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        ApplyCompileResult(existing, compileResult);

        // A running table's grain only picks up SQL/search config changes on (re)StartAsync — mirror the
        // SQL-changed restart below for search config too, so toggling SearchEnabled/SearchMode on a
        // Running table takes effect immediately instead of only on the next manual stop/start.
        if ((sqlChanged || searchChanged) && wasRunning)
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

    /// <summary>Demo tables seeded on first run: "positions" is Running (a plain running aggregate over
    /// "trades"), "gold_tier_orders" demonstrates JSON expressions in table mode, and "hot_symbols"
    /// demonstrates table-over-table chaining (FROM "positions"). The latter two are seeded Stopped so
    /// dependency-order start is a deliberate user action, not implicit at boot.</summary>
    private static List<TableDefinition> SeedTables()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        TableDefinition Make(string name, string description, string sql, PipelineStatus status, bool searchEnabled = false, TableSearchMode searchMode = TableSearchMode.Exact) => new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = name,
            Description = description,
            Sql = sql,
            Status = status,
            CreatedBy = "system",
            CreatedAtMs = now,
            UpdatedAtMs = now,
            SearchEnabled = searchEnabled,
            SearchMode = searchMode,
        };

        // "order_states" (Phase L3): current state per order_id, honestly derived from "order_events"
        // with MAX-only running aggregates — the pre-LATEST-BY pattern (plan 002 Phase L3 defers the
        // `LATEST BY` sugar to a later engine wave). MAX is the *correct* "latest" here only because
        // stage_rank/stage_ts/qty/filled_qty are all monotone non-decreasing per order_id by construction
        // (see MarketDataProfiles' "lifecycle" profile) — symbol/side are MAX'd too, which is trivially
        // honest since they're constant per order_id, not because they're monotone. Deliberately NOT
        // MAX(stage): the stage *string* isn't ordered the way stage_rank is (alphabetically "ACK" <
        // "CANCELED" < "FILLED" < "NEW" < "PART_FILL"), so MAX(stage) would silently give a wrong answer;
        // callers derive the display stage from stage_rank instead.
        var orderStates = Make(
            "order_states",
            "Current state per live order (Phase L3): one row per order_id, derived from order_events via " +
            "monotone MAX aggregates (honest pre-LATEST-BY pattern — see plan 002 Phase L3).",
            "SELECT order_id, MAX(symbol) AS symbol, MAX(side) AS side, MAX(stage_rank) AS stage_rank, " +
            "MAX(stage_ts) AS last_ts, MAX(qty) AS qty, MAX(filled_qty) AS filled_qty, COUNT(*) AS events " +
            "FROM order_events GROUP BY order_id",
            PipelineStatus.Running);
        // Row history mode: LastN(8), not MinBy/MaxBy — the demo goal is the STAGE TRAIL (NEW, ACK,
        // PART_FILL, PART_FILL, ..., FILLED/CANCELED) for a clicked order, i.e. the recent-versions trail,
        // not a peak+latest pair. MaxBy(stage_rank) would only ever retain 2 entries (the FILLED/CANCELED
        // extreme + itself as latest) and lose the PART_FILL steps in between — LastN(8) keeps the walk.
        orderStates.HistoryEnabled = true;
        orderStates.HistoryMode = TableHistoryMode.LastN;
        orderStates.HistoryLimit = 8;

        return
        [
            Make(
                "positions",
                "Running per-symbol trade aggregates: count, total quantity, and price stats.",
                "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty, AVG(price) AS avg_price, MIN(price) AS low, MAX(price) AS high " +
                "FROM trades GROUP BY symbol",
                PipelineStatus.Running,
                searchEnabled: true,
                searchMode: TableSearchMode.Fuzzy),
            Make(
                "gold_tier_orders",
                "Order counts per symbol for gold-tier users, extracted from app_events' nested JSON payload via '->'/'->>'.",
                "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
                "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'",
                PipelineStatus.Stopped),
            Make(
                "hot_symbols",
                "Symbols from 'positions' with more than 50 trades — table-over-table chaining demo.",
                "SELECT p.symbol, p.trades, p.avg_price FROM positions p WHERE p.trades > 50",
                PipelineStatus.Stopped),
            orderStates,
        ];
    }

    /// <summary>Demo pipelines seeded on first run. The first two are marked Running here —
    /// EnsureInitializedAsync's resume loop (below) turns that into a real StartAsync call against
    /// the seeded sources, exactly like it would on a normal restart.</summary>
    private static List<PipelineDefinition> SeedPipelines()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        PipelineDefinition Make(string name, string description, string sql, PipelineStatus status) => new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = name,
            Description = description,
            Sql = sql,
            Status = status,
            CreatedBy = "system",
            CreatedAtMs = now,
            UpdatedAtMs = now,
        };

        return
        [
            Make(
                "VWAP by symbol (5s)",
                "Volume-weighted average price per symbol over 5-second tumbling windows.",
                "SELECT symbol, SUM(price * qty) / SUM(qty) AS vwap, COUNT(*) AS trades FROM trades " +
                "GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
                PipelineStatus.Running),
            Make(
                "Trade vs quote spread",
                "Joins BUY trades against the prevailing quote to compare trade price with the bid.",
                "SELECT t.symbol, t.price, q.bid, q.ask, t.price - q.bid AS above_bid FROM trades t " +
                "JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol WHERE t.side = 'BUY'",
                PipelineStatus.Running),
            Make(
                "Order bursts (session)",
                "Groups order activity per symbol into session windows to spot bursts.",
                "SELECT symbol, COUNT(*) AS orders, SUM(qty) AS total_qty FROM orders " +
                "GROUP BY symbol WINDOW SESSION(GAP 3 SECONDS)",
                PipelineStatus.Stopped),
            Make(
                "Unfilled orders (LEFT JOIN)",
                "New orders left-joined against recent trades to surface ones that haven't filled yet.",
                "SELECT o.orderId, o.symbol, o.qty, t.price FROM orders o " +
                "LEFT JOIN trades t WITHIN 10 SECONDS ON o.symbol = t.symbol WHERE o.status = 'NEW'",
                PipelineStatus.Stopped),
            Make(
                "JSON payload join",
                "Extracts user tier and order symbol from app_events' nested JSON payload via '->'/'->>' " +
                "and joins on the extracted symbol to attach the prevailing trade price.",
                "SELECT e.eventType, e.payload -> 'user' ->> 'tier' AS tier, e.payload -> 'order' ->> 'symbol' AS symbol, t.price FROM app_events e " +
                "JOIN trades t WITHIN 10 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol",
                PipelineStatus.Stopped),
            Make(
                "fill-rate-5s",
                "Per-symbol fill activity over 5-second tumbling windows (Phase L3): count of PART_FILL/FILLED " +
                "order_events and their filled_qty. Note: filled_qty is order_events' cumulative-fill field, " +
                "not a per-fill delta, so SUM(filled_qty) here is a windowed sum of cumulative snapshots — " +
                "useful as an activity/volume-scale signal, not a literal 'shares filled in this window' count.",
                "SELECT symbol, COUNT(*) AS fills, SUM(filled_qty) AS filled FROM order_events " +
                "WHERE stage = 'PART_FILL' OR stage = 'FILLED' GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
                PipelineStatus.Running),
        ];
    }

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
