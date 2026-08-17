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
                    t.KeyFields = TableKeyFields.Describe(t.Sql, result.Plan);
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

        // Plan 011 Wave A backfill: pipelines are seeded RAW (unlike tables, above) and Create/Update are
        // the only other writers of SourceNames (ApplyPipelineCompileResult), so any pipeline that has
        // never been through either — freshly seeded above, or durably persisted from before this backfill
        // existed — still carries an empty SourceNames and draws no lineage edge. Runs for BOTH cases
        // (seed and restore) since it's driven off the data (SourceNames.Count == 0), not off whether
        // seeding just happened. Must run after Sources are loaded (above, unconditionally) so
        // BuildStreamSchemas() has something to compile against. Draft-friendly like the create/update
        // paths: a pipeline whose SQL doesn't currently compile is left with an empty SourceNames rather
        // than throwing or blocking initialization.
        foreach (var pipeline in state.State.Pipelines.Where(p => p.SourceNames.Count == 0))
        {
            var compileResult = SqlCompiler.Compile(pipeline.Sql, BuildStreamSchemas());
            if (compileResult.Ok && compileResult.SourceNames.Count > 0)
            {
                ApplyPipelineCompileResult(pipeline, compileResult);
                dirty = true;
            }
        }

        if (dirty)
        {
            await state.WriteStateAsync();
        }

        foreach (var src in state.State.Sources.Where(s => s.Enabled))
        {
            try
            {
                // Plan 006 D-C / plan 008 W4 / plan 009 wave D: three-way Kind dispatch via the shared
                // SourceKindDispatch.Classify (StreamForge.Abstractions — both flavors use it, see its own
                // class doc). Generator (or unset — pre-006 seeds/sources) keeps the pre-existing
                // IGeneratorGrain path unchanged; Ingest starts NO grain at all (rows arrive through
                // IIngressFacade); Connector (everything else) goes to IConnectorGrain.
                var kind = SourceKindDispatch.Classify(src.Kind);
                if (kind == SourceKindDispatch.ActorKind.Generator)
                {
                    await GrainFactory.GetGrain<IGeneratorGrain>(src.Name).StartAsync(src);
                }
                else if (kind != SourceKindDispatch.ActorKind.Ingest)
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
        foreach (var table in state.State.Tables.Where(t => t.HistoryEnabled && t.ShardBy.Count == 0))
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

        // Plan 011 D1: the same treatment for every SHARDED table's router — ResumeAsync (not ResetAsync)
        // so the per-key shards persisted on disk survive a silo restart exactly like the history grain's
        // entries do. Note the loop above now skips sharded tables: on a sharded table the per-key history
        // REPLACES the table-wide one, and resuming both would hold the same version trails twice, which
        // is precisely the memory this wave exists to stop holding.
        foreach (var table in state.State.Tables.Where(t => t.ShardBy.Count > 0))
        {
            try
            {
                await GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name).ResumeAsync(table);
            }
            catch
            {
                // best-effort — a stale/misconfigured shard router shouldn't block boot.
            }
        }
    }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(state.State.Sources.ToList());

    public Task<SourceDefinition?> GetSourceAsync(string name) =>
        Task.FromResult(state.State.Sources.FirstOrDefault(s => s.Name == name));

    /// <summary>Wishlist #8's run-on-demand. The registry owns the catalog, so it is the one place that
    /// can turn a source NAME into "the generator activation for that source" — and forwarding is all it
    /// does: the batch is generated and published inside <see cref="IGeneratorGrain.RunAsync"/>, on the
    /// activation that owns that source's stream. Doing the work here instead would publish from the
    /// wrong activation and bypass the generator's own backpressure.</summary>
    public async Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request)
    {
        var src = state.State.Sources.FirstOrDefault(s => s.Name == name);
        if (src is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        return await GrainFactory.GetGrain<IGeneratorGrain>(name).RunAsync(request);
    }

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

        // Plan 006 D-C / plan 009 wave D: Kind dispatch via the shared SourceKindDispatch.Classify. On an
        // update where Kind CHANGED (e.g. generator -> url), the grain for the OLD kind is still activated
        // and would otherwise keep polling/ticking forever — so both StopAsync calls always run (cheap/
        // idempotent no-ops on whichever kind wasn't actually running) rather than tracking the previous
        // Kind separately just to target one Stop call.
        var generator = GrainFactory.GetGrain<IGeneratorGrain>(def.Name);
        var connector = GrainFactory.GetGrain<IConnectorGrain>(def.Name);
        if (def.Enabled)
        {
            switch (SourceKindDispatch.Classify(def.Kind))
            {
                case SourceKindDispatch.ActorKind.Generator:
                    await generator.StartAsync(def);
                    await connector.StopAsync();
                    break;
                case SourceKindDispatch.ActorKind.Ingest:
                    await generator.StopAsync();
                    await connector.StopAsync();
                    break;
                default: // Connector
                    await connector.StartAsync(def);
                    await generator.StopAsync();
                    break;
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

        ApplyPipelineCompileResult(def, SqlCompiler.Compile(def.Sql, BuildStreamSchemas()));

        state.State.Pipelines.Add(def);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Id, "created", def.Status);
        return def;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var idx = state.State.Pipelines.FindIndex(p => p.Id == def.Id);
        if (idx < 0)
        {
            return null;
        }

        var existing = state.State.Pipelines[idx];
        var sqlChanged = existing.Sql != def.Sql;
        var wasRunning = existing.Status == PipelineStatus.Running;

        // Compile-check against the prospective SQL before anything is stored — draft-friendly like
        // tables' CompileTableSql/ApplyCompileResult pair: never blocks the update on its own, only
        // populates/clears SourceNames.
        var compileResult = SqlCompiler.Compile(def.Sql, BuildStreamSchemas());

        // See CarryServerOwnedFields' doc comment: the incoming definition IS the new record, with only
        // the server-owned fields carried over from the stored one — rather than a hand-written list of
        // editable fields copied onto the stored record.
        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        state.State.Pipelines[idx] = def;

        ApplyPipelineCompileResult(def, compileResult);

        if (sqlChanged && wasRunning)
        {
            var pipelineGrain = GrainFactory.GetGrain<IPipelineGrain>(def.Id);
            try
            {
                await pipelineGrain.StopAsync();
                await pipelineGrain.StartAsync(def);
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            catch (Exception ex)
            {
                def.Status = PipelineStatus.Failed;
                def.Error = ex.Message;
            }
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Id, "updated", def.Status);
        return def;
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
        ValidateFlushMs(def.FlushMs);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;

        var compileResult = CompileTableSql(def.Sql, excludeTableId: def.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);
        ValidateShardBy(def, compileResult);
        ApplyCompileResult(def, compileResult);

        state.State.Tables.Add(def);
        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Name, "table-created", def.Status);

        // Configures (and, if HistoryEnabled, subscribes) the history grain up front — independent of
        // whether the table itself is Running, exactly like SearchEnabled's index is built incrementally
        // once the table starts producing deltas.
        await ResetHistoryTiersAsync(def);
        return def;
    }

    /// <summary>Plan 011 D1: (re)configures BOTH per-row-history tiers from one place, because on any one
    /// table exactly one of them may run. A sharded table's per-key shards hold the version trails, so the
    /// table-wide <see cref="ITableHistoryGrain"/> is disabled rather than left subscribed — running both
    /// over the same delta stream would hold every version twice and hold the sharded copy's worth of it
    /// resident, which is exactly the memory the shard tier exists to stop holding.</summary>
    private async Task ResetHistoryTiersAsync(TableDefinition def)
    {
        if (def.ShardBy.Count > 0)
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(def.Name).DisableAsync();
        }
        else
        {
            await GrainFactory.GetGrain<ITableHistoryGrain>(def.Name).ResetAsync(def);
        }
        await GrainFactory.GetGrain<ITableShardRouterGrain>(def.Name).ResetAsync(def);
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var idx = state.State.Tables.FindIndex(t => t.Id == def.Id);
        if (idx < 0)
        {
            return null;
        }

        var existing = state.State.Tables[idx];
        if (!string.Equals(existing.Name, def.Name, StringComparison.Ordinal))
        {
            ValidateUniqueTableName(def.Name, excludeTableId: existing.Id);
            ValidateShardedTableIsNotRenamed(existing);
        }
        ValidateParallelism(def.Parallelism);
        ValidateFlushMs(def.FlushMs);

        // Compile + validate against the *prospective* SQL/history config before mutating `existing` at
        // all, so a rejected update (bad name, bad historyByField) never leaves partially-applied state
        // behind in the in-memory list entry.
        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);
        ValidateShardBy(def, compileResult);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
        // Plan 003 M2: a Parallelism change (1->N, N->1, or N->M) changes which grain topology the table's
        // grain(s) run as (classic vs. coordinator mode, or a differently-shaped partitioned graph) — mirror
        // the search-config restart semantics below so it takes effect immediately on a Running table
        // instead of only on the next manual stop/start.
        var parallelismChanged = existing.Parallelism != def.Parallelism;
        // Plan 008 W2.5: a Persistence/FlushMs change picks a different write-behind policy for the SAME
        // running grain(s) — mirror the SQL/search-config/Parallelism restart semantics below so it takes
        // effect immediately on a Running table instead of only on the next manual stop/start (TableGrain's
        // StartAsync/StartClassicAsync/StartCoordinatorAsync only re-reads Persistence/FlushMs from the
        // TableDefinition it's (re)started with — see its own class doc's persistence-mode paragraph).
        var persistenceChanged = existing.Persistence != def.Persistence || existing.FlushMs != def.FlushMs
            || existing.JournalMaxEntries != def.JournalMaxEntries;
        // Plan 011 C2: the retention policy is installed on the executor at StartAsync and nowhere else
        // (see TableGrain.ApplyRetentionPolicy), so a change to it has the same "only picked up on
        // (re)start" property SQL/search/parallelism/persistence changes have — restart for the same
        // reason, so tightening or lifting a bound on a Running table takes effect now rather than at the
        // next manual stop/start.
        var retentionChanged = existing.RetentionMaxRows != def.RetentionMaxRows
            || existing.RetentionTtlMs != def.RetentionTtlMs;
        // Plan 011 D1: a ShardBy change re-keys the entire shard tier (or turns it on/off), so it resets
        // the tier below. Plan 011 D2 additionally RESTARTS the table, which D1 explicitly did not: the
        // tier is still only a delta-stream consumer and the grain topology is still unaffected, but
        // ShardBy now also decides whether TableGrain keeps a persisted snapshot mirror at all (see its
        // D2 paragraph), and TableGrain only re-reads its definition on StartAsync. Without the restart,
        // turning sharding on would leave the duplicate copy in place — the entire memory claim — until
        // somebody happened to bounce the table. Same reasoning, and the same code path, as the
        // Persistence/Parallelism changes beside it.
        var shardByChanged = !existing.ShardBy.SequenceEqual(def.ShardBy, StringComparer.Ordinal);
        var historyConfigChanged =
            existing.HistoryEnabled != def.HistoryEnabled ||
            existing.HistoryMode != def.HistoryMode ||
            existing.HistoryLimit != def.HistoryLimit ||
            existing.HistoryByField != def.HistoryByField ||
            existing.HistoryWindowMs != def.HistoryWindowMs;
        var wasRunning = existing.Status == PipelineStatus.Running;

        // See CarryServerOwnedFields' doc comment: the incoming definition IS the new record, with only
        // the server-owned fields carried over from the stored one.
        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        state.State.Tables[idx] = def;

        ApplyCompileResult(def, compileResult);

        // A running table's grain only picks up SQL/search config/parallelism/persistence changes on
        // (re)StartAsync — mirror the SQL-changed restart below for search config, parallelism, and
        // persistence too, so toggling SearchEnabled/SearchMode/Parallelism/Persistence/FlushMs on a
        // Running table takes effect immediately instead of only on the next manual stop/start.
        if ((sqlChanged || searchChanged || parallelismChanged || persistenceChanged || retentionChanged || shardByChanged) && wasRunning)
        {
            var tableGrain = GrainFactory.GetGrain<ITableGrain>(def.Name);
            try
            {
                await tableGrain.StopAsync();
                await tableGrain.StartAsync(def);
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            catch (Exception ex)
            {
                def.Status = PipelineStatus.Failed;
                def.Error = ex.Message;
            }
        }

        // History config or the SQL it was derived from changed — the row-identity mapping and/or
        // retention policy is now stale, so reset (not resume) the history grain, exactly like a
        // SQL/search-config change restarts the table's own grain above.
        if (sqlChanged || historyConfigChanged || shardByChanged)
        {
            await ResetHistoryTiersAsync(def);
        }
        else if (persistenceChanged && def.ShardBy.Count > 0)
        {
            // Plan 011 D1: same "re-read the write-behind policy without discarding what was accumulated"
            // distinction the history grain draws below — ResumeAsync re-derives the TableShardConfig
            // (which carries Persistence/FlushMs down to every shard on the next routed batch) and leaves
            // the existing shards on disk untouched.
            await GrainFactory.GetGrain<ITableShardRouterGrain>(def.Name).ResumeAsync(def);
        }
        else if (persistenceChanged && def.HistoryEnabled)
        {
            // Plan 008 W2.5: a Persistence/FlushMs-only change doesn't invalidate the row-identity mapping
            // or retention policy, so use ResumeAsync (not ResetAsync) — it re-reads Persistence/FlushMs
            // from the updated definition and re-registers the flush timer with the new interval/mode, but (unlike
            // ResetAsync) deliberately leaves Entries/Seq untouched, so accumulated history survives a mode
            // tweak the same way it survives a silo restart.
            await GrainFactory.GetGrain<ITableHistoryGrain>(def.Name).ResumeAsync(def);
        }

        await state.WriteStateAsync();
        await PublishLifecycleAsync(def.Name, "table-updated", def.Status);
        return def;
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

        // Plan 011 D1: unsubscribe the router AND delete every shard's persisted state. Unlike every other
        // teardown here this one genuinely activates each shard once — clearing a grain's persisted state
        // portably means asking the grain to do it — which is the correct trade for an explicit delete and
        // exactly the wrong one for a read (see TableShardRouterGrain.PurgeShardsAsync).
        try
        {
            await GrainFactory.GetGrain<ITableShardRouterGrain>(existing.Name).DisableAsync();
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

    /// <summary>Plan 008 W2.5: 409-style guard (same pattern as ValidateParallelism) — TableDefinition.FlushMs
    /// (see its own doc comment: 0 = the 2000ms default; ignored for MemoryOnly) must be non-negative. A
    /// negative interval has no sane meaning for either Orleans' RegisterGrainTimer (which throws its own,
    /// less caller-friendly ArgumentOutOfRangeException) or the "0 = default" convention.</summary>
    private static void ValidateFlushMs(int flushMs)
    {
        if (flushMs < 0)
        {
            throw new InvalidOperationException($"FlushMs must be >= 0 (got {flushMs}).");
        }
    }

    /// <summary>Plan 011 C2: 409-style guard (same pattern as <see cref="ValidateParallelism"/>) for the
    /// opt-in row retention policy. Everything here is a refusal to accept a policy the runtime could not
    /// actually honor, because the failure mode of accepting one is the worst kind: metrics and the console
    /// would report a bounded table while the structures that hold the memory kept growing.
    ///
    ///  * Negative bounds have no meaning (0 is already "off").
    ///  * Parallelism &gt;= 2 runs the partitioned dataflow, where the rows live in the stage grains rather
    ///    than in this table's own executor — a policy installed on the coordinator's scratch executor would
    ///    evict nothing at all. Retention is Parallelism == 1 only, and says so.
    ///  * A plan shape the Engine cannot reclaim state for (joins, set operations, derived sources, GROUP
    ///    BY/aggregates) is refused with the Engine's own reasoning — see TablePlan.SupportsRetention.
    ///
    /// Draft-friendly in exactly the way <see cref="ValidateHistoryConfig"/> is: SQL that does not compile
    /// is not rejected here (the table is saved as a draft with diagnostics, as always) — the shape check
    /// simply has nothing to check yet and re-runs on the next update that does compile.</summary>
    private static void ValidateRetention(TableDefinition def, TableCompileResult compileResult)
    {
        if (def.RetentionMaxRows < 0 || def.RetentionTtlMs < 0)
        {
            throw new InvalidOperationException(
                $"Retention bounds must be >= 0 (got maxRows={def.RetentionMaxRows}, ttlMs={def.RetentionTtlMs}); 0 means unbounded.");
        }

        if (def.RetentionMaxRows == 0 && def.RetentionTtlMs == 0)
        {
            return; // retention off — the default, and nothing further to check
        }

        if (def.Parallelism > 1)
        {
            throw new InvalidOperationException(
                $"Row retention requires Parallelism = 1 (got {def.Parallelism}) — a partitioned table's rows live in its stage grains, so the policy could not reclaim them.");
        }

        if (!compileResult.Ok || compileResult.Plan is null)
        {
            return; // draft: nothing to validate the shape against yet
        }

        if (!compileResult.Plan.SupportsRetention)
        {
            throw new InvalidOperationException(
                "Row retention is not supported for this table's SQL: joins, set operations, derived sources and GROUP BY/aggregates are excluded, because evicting an output row would leave their per-key state (join indexes, aggregate accumulators) growing — or, for aggregates, would restart the group from zero and emit a wrong value.");
        }
    }

    /// <summary>Plan 011 D1: 409-style guard (same shape as <see cref="ValidateParallelism"/>) for opt-in
    /// KEY SHARDING. Empty <c>ShardBy</c> — the default — returns immediately and nothing below applies, so
    /// an existing table is untouched by the feature's existence.
    ///
    ///  * Blank or duplicate column names are refused: a duplicate would silently widen the key encoding
    ///    for no effect, and a blank one would encode a column that cannot exist.
    ///  * <see cref="TableDefinition.SearchEnabled"/> + ShardBy is refused OUTRIGHT rather than
    ///    half-served. The reverse index is table-wide and row-keyed (see TableSearchIndex — five maps, a
    ///    4-5x multiplier on whatever the table holds); keeping it alongside a shard tier would keep every
    ///    row resident and defeat the entire point, while a per-shard index would answer a table-wide
    ///    query by waking every shard, which is worse. Refusing loudly is the honest option, matching wave
    ///    C2's refusal of retention on plan shapes it could not honor.
    ///  * Every ShardBy column must be one of the table's COMPILED output columns. This is the one place
    ///    the explicit-columns rule is enforced, and it is enforced against the compiler's own output
    ///    rather than against the SQL text: the shard key decides which grain owns a row, so a column that
    ///    silently does not exist would put every row under the same "missing value" key. Draft-friendly
    ///    in exactly the way <see cref="ValidateHistoryConfig"/> and <see cref="ValidateRetention"/> are —
    ///    SQL that does not compile is saved as a draft with diagnostics, and this check re-runs on the
    ///    next update that does compile.
    ///
    /// NOT restricted by Parallelism, deliberately, unlike retention: the shard tier consumes the table's
    /// delta stream, and TableOutputGrain republishes onto that same stream for Parallelism &gt;= 2, so a
    /// shard consumer is identical in both modes.</summary>
    private static void ValidateShardBy(TableDefinition def, TableCompileResult compileResult)
    {
        if (def.ShardBy.Count == 0)
        {
            return; // not sharded — the default, and nothing below applies
        }

        if (def.ShardBy.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException("shardBy column names must be non-empty.");
        }

        var duplicates = def.ShardBy.GroupBy(c => c, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException($"shardBy has duplicate column(s): {string.Join(", ", duplicates)}.");
        }

        if (def.SearchEnabled)
        {
            throw new InvalidOperationException(
                "searchEnabled cannot be combined with shardBy: the reverse index is table-wide and row-keyed, so it would keep every row resident and defeat the point of sharding. Turn search off, or shard off.");
        }

        // Plan 011 D2 — MemoryOnly + ShardBy is now REFUSED, where D1 honored it literally and documented
        // the consequence. The reason it had to change is that D2 is what made it dangerous. A shard's
        // final write on deactivation is not a durability nicety, it IS the swap-out; a mode that never
        // writes turns every idle deactivation into silent data loss for that key, and D1 could still say
        // "the table's own snapshot has the rows" because TableGrain kept a full persisted mirror. D2
        // deliberately stops keeping it (see TableGrain's sharded-tables paragraph), so on a sharded
        // MemoryOnly table there would be nothing behind the shards at all. Refusing beats redefining
        // MemoryOnly's contract underneath the user: the mode promises "a RESTART brings the table back
        // empty", not "an idle minute loses this key".
        if (def.Persistence == TablePersistenceMode.MemoryOnly)
        {
            throw new InvalidOperationException(
                "persistence 'MemoryOnly' cannot be combined with shardBy: a shard's write on deactivation IS how its state is swapped out, so a shard that never writes would lose an idle key's rows and history rather than saving them. Pick Batched, FireAndForget or Journaled, or shard off.");
        }

        if (!compileResult.Ok || compileResult.OutputSchema is null)
        {
            return; // draft: no compiled output columns to validate against yet
        }

        var missing = def.ShardBy.Where(c => !compileResult.OutputSchema.Fields.ContainsKey(c)).ToList();
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"shardBy column(s) {string.Join(", ", missing)} are not among this table's output fields ({string.Join(", ", compileResult.OutputSchema.Fields.Keys)}).");
        }
    }

    /// <summary>Plan 011 D2 — REFUSES TO RENAME A SHARDED TABLE, and this deserves the paragraph because
    /// the alternative fix is better and was not taken.
    ///
    /// THE HAZARD. The whole shard tier is keyed by the table's NAME: the router grain, the directory
    /// grain, and every shard grain (<c>TableShardKeys.GrainKey(tableName, shardKey)</c>). A rename
    /// therefore does not move anything — it makes the table address a fresh, empty tier while every
    /// existing shard keeps its rows and its full version trail on disk under a key nothing will ever look
    /// up again. Nothing throws, nothing logs, and the table simply appears to have lost its per-key
    /// history. A silent loss is the worst shape this could take, which is why it is refused rather than
    /// documented.
    ///
    /// THE BETTER FIX, and why it is not here. Keying the tier on <see cref="TableDefinition.Id"/> — which
    /// is immutable — costs nothing while the feature is unreleased and removes the hazard outright rather
    /// than fencing it off. It was implemented and then backed out for one concrete, non-technical reason:
    /// wave D1's own cluster tests address the tier by name in a dozen places, and this wave's brief
    /// forbids modifying a pre-existing test file. The change is mechanical (grain keys + those call
    /// sites) and is the right thing to do the moment that call is made; until then the rename is refused,
    /// which closes the hole completely, at the cost of one restriction.
    ///
    /// THE WAY AROUND IT for a user who genuinely wants to rename: clear <c>shardBy</c> first, which
    /// deletes the shards explicitly and visibly, then rename, then shard again. The tier rebuilds from
    /// live traffic exactly as it does when sharding is first switched on.
    ///
    /// NOTE the same exposure exists, pre-existing and unchanged, for <c>TableHistoryGrain</c> and for
    /// <c>TableGrain</c> itself, both also keyed by name. Neither is in this wave's scope; this guard
    /// deliberately covers only what this wave is responsible for, rather than quietly widening to a
    /// restriction nobody asked for.</summary>
    private static void ValidateShardedTableIsNotRenamed(TableDefinition existing)
    {
        if (existing.ShardBy.Count == 0)
        {
            return;
        }

        throw new InvalidOperationException(
            $"'{existing.Name}' is sharded by {string.Join(", ", existing.ShardBy)} and cannot be renamed: its shard grains are keyed by the table name, so a rename would strand every shard's rows and version trails under a key nothing looks up again. Clear shardBy first (which deletes the shards), rename, then shard again.");
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

    /// <summary>Plan 008 W5: stores SourceNames from a pipeline compile result when it compiles; leaves it
    /// empty otherwise — the pipeline-side counterpart of <see cref="ApplyCompileResult"/> below (tables'
    /// StreamInputs/TableInputs). Draft-friendly like that one: called from Create/UpdatePipelineAsync,
    /// never blocks either on a compile failure, only decides what SourceNames holds afterward.</summary>
    private static void ApplyPipelineCompileResult(PipelineDefinition def, CompileResult result) =>
        def.SourceNames = result.Ok ? result.SourceNames.ToList() : [];

    /// <summary>Stores OutputFields/StreamInputs/TableInputs from a compile result when it compiles; leaves
    /// them empty otherwise.</summary>
    private static void ApplyCompileResult(TableDefinition def, TableCompileResult result)
    {
        if (result.Ok && result.OutputSchema is not null)
        {
            def.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
            def.StreamInputs = result.StreamInputs.ToList();
            def.TableInputs = result.TableInputs.ToList();
            def.KeyFields = TableKeyFields.Describe(def.Sql, result.Plan);
        }
        else
        {
            def.OutputFields = [];
            def.StreamInputs = [];
            def.TableInputs = [];
            def.KeyFields = null;
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
