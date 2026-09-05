using StreamsForge.AppCore;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Environments;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Lifecycle;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Dapr.Host.Catalog;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: pure(-ish) catalog logic ported from Orleans'
/// <c>RegistryGrain</c> (orleans/src/StreamsForge.Host/Grains/RegistryGrain.cs) — same name/id rules,
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
/// dapr/tests/StreamsForge.Dapr.Tests/CatalogStoreTests.cs.</para>
/// </summary>
/// <summary>
/// <paramref name="environment"/> — plan 021, additive and defaulted so every pre-existing
/// <c>new CatalogStore(state, orchestrator)</c> call site (this project's own tests included) keeps
/// compiling and keeps meaning exactly what it meant before: the DEFAULT environment
/// (<see cref="EnvKeys.Default"/>, the empty string), which is what makes <see cref="Environment"/>'s
/// byte-identical-by-default guarantee (D2) hold without touching a single existing test. Only
/// <see cref="Actors.RegistryActor"/> ever passes a non-default value, derived from its OWN activated
/// actor id (<c>EnvKeys.EnvOf(Id.GetId())</c>) — this store never reads an ambient or a header itself; it
/// is handed its environment once, at construction, the same way it is handed its
/// <see cref="ILifecycleOrchestrator"/>.
/// </summary>
public sealed class CatalogStore(CatalogState state, ILifecycleOrchestrator orchestrator, string environment = EnvKeys.Default)
{
    /// <summary>The environment this store's entire catalog belongs to — see the class doc's
    /// <paramref name="environment"/> paragraph. Every entity this store CREATES is stamped with it
    /// (<see cref="UpsertSourceAsync"/>/<see cref="CreatePipelineAsync"/>/<see cref="CreateTableAsync"/>);
    /// an UPDATE never touches it (<see cref="CatalogRecordMerge.CarryServerOwnedFields"/> is what would
    /// have to change to let a caller move an entity between environments, and it does not).</summary>
    public string Environment => environment;

    /// <summary>Seeds defaults on first run (shared <see cref="SeedCatalog"/> — same demo world as the
    /// Orleans flavor). Plan 005 W4 "seed status" decision (see dapr/ARCHITECTURE.md), updated by W6 for
    /// pipelines and by W7-A for tables: neither is force-stopped anymore. <see cref="Actors.TableActor"/>
    /// now exists, and <see cref="Services.TableSupervisorService"/>'s boot sweep resumes every seeded
    /// Running table for real (compiling its SQL, arming its flush timer, registering
    /// <see cref="Streaming.TableEventRouter"/>) exactly mirroring Orleans' own
    /// <c>RegistryGrain.EnsureInitializedAsync</c> resume-on-boot behavior — a seeded Running table is now
    /// honestly Running, same as sources (W5-A) and pipelines (W6) already are. The one remaining
    /// defensive override, below, is a compile FAILURE forcing <see cref="PipelineStatus.Stopped"/>
    /// regardless of what <see cref="SeedCatalog"/> requested — a table that doesn't compile can never
    /// validly be Running on either flavor. Returns true if anything was seeded (caller persists only
    /// then).</summary>
    public bool EnsureInitialized()
    {
        var dirty = false;

        if (state.Sources.Count == 0)
        {
            var seededSources = SeedCatalog.Sources();
            // Plan 021 D5: only CatalogInitializationService ever calls this against the DEFAULT
            // environment's RegistryActor today (StreamConstants.RegistryKey, unqualified — see that
            // service's own doc comment), so `environment` is always "" in practice here. Stamped anyway
            // — the same rule CreateTableAsync/UpsertSourceAsync apply — so a future caller that seeds a
            // NAMED environment (there is none yet) inherits correct behavior for free instead of a gap
            // nobody meant to leave.
            foreach (var s in seededSources) s.Environment = environment;
            state.Sources.AddRange(seededSources);
            dirty = true;
        }

        if (state.Pipelines.Count == 0)
        {
            var seededPipelines = SeedCatalog.Pipelines();
            foreach (var p in seededPipelines) p.Environment = environment;
            state.Pipelines.AddRange(seededPipelines);
            dirty = true;
        }

        // Plan 011 Wave A backfill, extended by plan 025 (table-over-pipeline) to OutputFields too — see
        // RegistryGrain.EnsureInitializedAsync's identical comment for the full rationale. Pipelines are
        // seeded RAW (unlike tables, below) and Create/Update are the only other writers of SourceNames/
        // OutputFields (ApplyPipelineCompileResult), so any pipeline that has never been through either —
        // freshly seeded above, or durably persisted from before one of these two fields existed — still
        // carries an empty one and draws no lineage edge / offers no relation. Runs for BOTH cases (seed
        // and restore) since it's driven off the data, not off whether seeding just happened. Must run
        // after Sources are loaded (above, unconditionally) so BuildStreamSchemas() has something to
        // compile against, and — this is the plan 025 addition, load-bearing rather than cosmetic — it
        // MOVED ABOVE the table-seed block below: a seeded table over a seeded pipeline needs the
        // pipeline's OutputFields populated before it can appear in BuildStreamSchemas() at all. Nothing
        // in today's SeedCatalog.Tables() reads a pipeline, but a promoted/imported catalog could, and the
        // ordering has to hold regardless. Draft-friendly like the create/update paths: a pipeline whose
        // SQL doesn't currently compile is left with an empty SourceNames/OutputFields rather than
        // throwing or blocking initialization.
        foreach (var pipeline in state.Pipelines.Where(p => p.SourceNames.Count == 0 || p.OutputFields.Count == 0))
        {
            var compileResult = SqlCompiler.Compile(pipeline.Sql, BuildStreamSchemas());
            if (compileResult.Ok && compileResult.SourceNames.Count > 0)
            {
                ApplyPipelineCompileResult(pipeline, compileResult);
                dirty = true;
            }
        }

        if (state.Tables.Count == 0)
        {
            var streamSchemas = BuildTableStreamSchemas();
            var pipelineNames = PipelineNameSet();
            var tableSchemas = new Dictionary<string, SourceSchema>();
            var seeds = SeedCatalog.Tables();
            foreach (var t in seeds)
            {
                t.Environment = environment;
                var result = SqlCompiler.CompileTable(t.Sql, streamSchemas, tableSchemas);
                if (result.Ok && result.OutputSchema is not null)
                {
                    t.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
                    // Table-over-pipeline: split the compiled stream relations by pipeline-name membership
                    // — mirrors ApplyCompileResult exactly (see that method's doc comment for why the
                    // split has to be total).
                    t.StreamInputs = result.StreamInputs.Where(n => !pipelineNames.Contains(n)).ToList();
                    t.PipelineInputs = result.StreamInputs.Where(pipelineNames.Contains).ToList();
                    t.TableInputs = result.TableInputs.ToList();
                    t.KeyFields = TableKeyFields.Describe(t.Sql, result.Plan);
                    tableSchemas[t.Name] = result.OutputSchema;
                }
                else
                {
                    t.Error = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                    // A table that failed to compile can never validly be Running — force Stopped
                    // regardless of what SeedCatalog requested (see this method's doc comment; mirrors the
                    // same rule CreateTableAsync/UpdateTableAsync apply: Status is never set Running off a
                    // failed compile).
                    t.Status = PipelineStatus.Stopped;
                }
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
        // Table-over-pipeline (plan 025) — the mirror of ValidateUniquePipelineName's source check. A
        // source name is a relation name too, and must not become the second meaning of a name a pipeline
        // already owns. Checked on every upsert (create AND update), mirroring
        // RegistryGrain.UpsertSourceAsync exactly: a source cannot be renamed, so an update can only
        // re-assert its own name, and re-asserting a name that has since collided is exactly the case
        // worth catching.
        if (state.Pipelines.Any(p => string.Equals(p.Name, def.Name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{def.Name}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");
        }

        var idx = state.Sources.FindIndex(s => s.Name == def.Name);
        bool schemaChanged;
        if (idx >= 0)
        {
            // Plan 016 wave 2 — mirrors RegistryGrain.UpsertSourceAsync exactly. Sources are the one
            // entity with no CatalogRecordMerge overload (they are upserted whole on both flavours), so
            // the counter carry has to live at the upsert site; CarryAndBumpSource does carry AND bump in
            // one shared call so the two flavours cannot drift.
            var existing = state.Sources[idx];
            CatalogRevisions.CarryAndBumpSource(existing, def);
            // Plan 021 D5: Environment is server-owned the same way — CatalogRecordMerge has no source
            // overload to carry it for us (see the comment two lines up), so an edit here would otherwise
            // let a caller move a source between environments just by round-tripping whatever the client
            // happened to send back.
            def.Environment = existing.Environment;
            schemaChanged = def.SchemaRevision != existing.SchemaRevision;
            state.Sources[idx] = def;
        }
        else
        {
            ValidateQualifiableName(def.Name);
            def.Revision = 1;
            def.SchemaRevision = 1;
            def.Environment = environment;
            state.Sources.Add(def);
            schemaChanged = true;
        }

        if (schemaChanged)
        {
            RefreshTableSchemas();
        }

        RecomputeStaleReasons();
        await orchestrator.NotifySourceChangedAsync(def);
    }

    public async Task<bool> DeleteSourceAsync(string name)
    {
        var removed = state.Sources.RemoveAll(s => s.Name == name) > 0;
        if (!removed)
        {
            return false;
        }

        RecomputeStaleReasons(); // a deleted source breaks every pin that named it.
        await orchestrator.NotifySourceRemovedAsync(name, environment);
        return true;
    }

    // ------------------------------------------------------------------
    // Pipelines
    // ------------------------------------------------------------------

    public List<PipelineDefinition> GetPipelines() => state.Pipelines.ToList();

    public PipelineDefinition? GetPipeline(string id) => state.Pipelines.FirstOrDefault(p => p.Id == id);

    public async Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        ValidateUniquePipelineName(def.Name, excludePipelineId: null);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;
        def.Revision = 1; // see UpsertSourceAsync for why a fresh entity is 1 and not 0.
        // Plan 021 D5: pipelines are keyed by GUID (EnvKeys.Qualify never touches them — see that class's
        // own doc comment), so there is no dotted-name refusal here the way sources/tables need. Stamped
        // once at creation and never again — an update carries it forward (see UpdatePipelineAsync).
        def.Environment = environment;

        ApplyPipelineCompileResult(def, SqlCompiler.Compile(def.Sql, BuildStreamSchemas()));

        state.Pipelines.Add(def);
        RecomputeStaleReasons();
        await orchestrator.PublishLifecycleAsync(def.Id, "created", def.Status);
        return def;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var idx = state.Pipelines.FindIndex(p => p.Id == def.Id);
        if (idx < 0)
        {
            return null;
        }

        var existing = state.Pipelines[idx];

        // Plan 016 wave 1-C: mirrors RegistryGrain.UpdatePipelineAsync — checked on EVERY update, not
        // only when the name changes, so a catalog that already holds two pipelines with one name keeps
        // serving both until somebody edits one.
        ValidateUniquePipelineName(def.Name, excludePipelineId: existing.Id);

        var sqlChanged = existing.Sql != def.Sql;
        var wasRunning = existing.Status == PipelineStatus.Running;

        // Compile-check against the prospective SQL before mutating `existing` — draft-friendly like
        // tables' CompileTableSql/ApplyCompileResult pair: never blocks the update on its own, only
        // populates/clears SourceNames. Mirrors RegistryGrain.UpdatePipelineAsync (plan 008 W5).
        var compileResult = SqlCompiler.Compile(def.Sql, BuildStreamSchemas());

        // Plan 009: the incoming definition IS the new record — see CatalogRecordMerge's doc comment for
        // why this is an inversion of the old field-by-field copy, and which three fields that shape lost.
        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Plan 021 D5: CatalogRecordMerge has no Environment field (it is defined here, not in the shared
        // frozen contract) — carry it explicitly so an update can never move a pipeline between
        // environments, same rule UpsertSourceAsync/UpdateTableAsync apply.
        def.Environment = existing.Environment;
        // Plan 016 wave 2: mirrors RegistryGrain.UpdatePipelineAsync — the carry put the STORED counter
        // on `def`, this moves it, and only if the definition really changed by the same canonical-JSON
        // test ImportPlanner uses for "skipped" vs "updated". Before the restart block below.
        CatalogRevisions.BumpPipeline(existing, def);
        state.Pipelines[idx] = def;

        ApplyPipelineCompileResult(def, compileResult);
        RecomputeStaleReasons();

        if (sqlChanged && wasRunning)
        {
            await orchestrator.StopPipelineAsync(def.Id);
            var outcome = await orchestrator.StartPipelineAsync(def, state.Sources);
            if (outcome.Ok)
            {
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            else
            {
                def.Status = PipelineStatus.Failed;
                def.Error = outcome.Error;
            }
        }

        await orchestrator.PublishLifecycleAsync(def.Id, "updated", def.Status);
        return def;
    }

    /// <summary>Table-over-pipeline (plan 025) — mirrors <c>RegistryGrain.DeletePipelineAsync</c> exactly:
    /// a table reading this pipeline is NOT refused, and is NOT stopped — exactly what already happens
    /// when a SOURCE a table reads is deleted (<see cref="DeleteSourceAsync"/> takes no census of
    /// dependents either). The table stays Running on its already-compiled plan and simply receives
    /// nothing more from this input; its persisted <c>OutputFields</c> are left alone, because
    /// <see cref="RefreshTableSchemas"/> skips a table whose SQL no longer compiles, and stale beats
    /// absent for a published <c>/proto</c> contract. The visible signal is <c>StaleReason</c>, set by
    /// <see cref="RecomputeStaleReasons"/> below when a table/pipeline carries a pin that named this
    /// pipeline. <see cref="ThrowIfRunningDependents"/> (the table-over-TABLE refusal) is deliberately NOT
    /// extended here: it exists because an upstream TABLE's own shard/delta tier is the dependent's
    /// durable state; a pipeline owns no such state on the dependent's behalf.</summary>
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
        RecomputeStaleReasons(); // a deleted pipeline breaks every pin that named it — see doc comment above.
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
        ValidateQualifiableName(def.Name);
        ValidateUniqueTableName(def.Name, excludeTableId: null);
        ValidateParallelism(def.Parallelism);
        ValidateFlushMs(def.FlushMs);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        def.Id = Guid.NewGuid().ToString("n");
        def.Status = PipelineStatus.Stopped;
        def.Error = null;
        def.CreatedAtMs = now;
        def.UpdatedAtMs = now;
        def.Revision = 1;       // see UpsertSourceAsync for why a fresh entity is 1 and not 0.
        def.SchemaRevision = 1;
        def.Environment = environment;

        var compileResult = CompileTableSql(def.Sql, excludeTableId: def.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);
        ApplyCompileResult(def, compileResult);

        state.Tables.Add(def);
        RecomputeStaleReasons();
        // Plan 021 D6: the lifecycle envelope's entity key becomes the QUALIFIED name — a subscriber
        // dispatching on it (or a human reading the SignalR/NATS relay) must be able to tell "orders" in
        // "staging" apart from "orders" in "default". Qualify(environment, ...) is a no-op for the default
        // environment (D2), so this is byte-identical there.
        await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, def.Name), "table-created", def.Status);
        await orchestrator.ResetTableHistoryAsync(def);
        return def;
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var tableIdx = state.Tables.FindIndex(t => t.Id == def.Id);
        if (tableIdx < 0)
        {
            return null;
        }

        var existing = state.Tables[tableIdx];
        var renamed = !string.Equals(existing.Name, def.Name, StringComparison.Ordinal);
        if (renamed)
        {
            ValidateQualifiableName(def.Name);
            ValidateUniqueTableName(def.Name, excludeTableId: existing.Id);
            ValidateTableRenameAllowed(existing);
        }
        ValidateParallelism(def.Parallelism);
        ValidateFlushMs(def.FlushMs);

        var compileResult = CompileTableSql(def.Sql, excludeTableId: existing.Id);
        ValidateHistoryConfig(def, compileResult);
        ValidateRetention(def, compileResult);

        var sqlChanged = existing.Sql != def.Sql;
        var searchChanged = existing.SearchEnabled != def.SearchEnabled || existing.SearchMode != def.SearchMode;
        var parallelismChanged = existing.Parallelism != def.Parallelism;
        // Plan 008: changing either knob restarts the table — see TableDefinition.FlushMs's doc comment
        // ("Changing it restarts the table") and TablePersistenceMode's own doc (the actor's flush-timer
        // cadence/mode is fixed for the lifetime of one StartAsync, exactly like Parallelism's dataflow
        // shape is). Table-history's own copy of these two fields is NOT restarted this way — it converges
        // separately, in place (no Entries loss), via TableHistorySupervisorService's ~15s sweep calling
        // ITableHistoryActor.EnsureConfiguredAsync (see that method's plan-008 addendum).
        var persistenceChanged = existing.Persistence != def.Persistence || existing.FlushMs != def.FlushMs
            || existing.JournalMaxEntries != def.JournalMaxEntries; // plan 009 A2 — this flavor was missing it entirely
        // Plan 011 C2: mirrors RegistryGrain — the retention policy is installed on the executor at
        // (re)start only (see TableActor.ApplyRetentionPolicy), so changing it restarts a Running table
        // for exactly the reason a Persistence/SQL/search change does.
        var retentionChanged = existing.RetentionMaxRows != def.RetentionMaxRows
            || existing.RetentionTtlMs != def.RetentionTtlMs;
        var historyConfigChanged =
            existing.HistoryEnabled != def.HistoryEnabled ||
            existing.HistoryMode != def.HistoryMode ||
            existing.HistoryLimit != def.HistoryLimit ||
            existing.HistoryByField != def.HistoryByField ||
            existing.HistoryWindowMs != def.HistoryWindowMs;
        var wasRunning = existing.Status == PipelineStatus.Running;

        // Plan 016 wave 1-C: BEFORE the store, so a rename never leaves the old name's history tier
        // addressable — the Dapr twin of RegistryGrain.ReleaseRenamedTiersAsync (there is no shard router
        // on this flavor; see ValidateShardBy's own note for why ShardBy is Orleans-only here).
        if (renamed)
        {
            try
            {
                await orchestrator.DisableTableHistoryAsync(existing.Name, environment);
            }
            catch
            {
                // best-effort: a tier that fails to tear down must not roll back a legal rename.
            }
        }

        // Plan 016 wave 2: captured BEFORE the merge, which aliases def.OutputFields to the stored list;
        // ApplyCompileResult then hands `def` a fresh list, leaving this one holding the old shape. It is
        // the comparand SchemaRevision is computed from. Mirrors RegistryGrain.UpdateTableAsync.
        var previousOutputFields = existing.OutputFields;

        // Plan 009: see the note in UpdatePipelineAsync above and CatalogRecordMerge's own doc comment.
        CatalogRecordMerge.CarryServerOwnedFields(existing, def, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Plan 021 D5: Environment is server-owned — see UpsertSourceAsync's identical carry for why
        // CatalogRecordMerge (frozen, shared) cannot do this for us.
        def.Environment = existing.Environment;
        state.Tables[tableIdx] = def;

        ApplyCompileResult(def, compileResult);

        CatalogRevisions.BumpTable(existing, def, previousOutputFields);
        if (def.SchemaRevision != existing.SchemaRevision)
        {
            EnsureFieldNumbers(EntitySchemas.TableKey(def.Id), def.OutputFields);
            RefreshTableSchemas();
        }

        RecomputeStaleReasons();

        if ((sqlChanged || searchChanged || parallelismChanged || persistenceChanged || retentionChanged) && wasRunning)
        {
            await orchestrator.StopTableAsync(def.Name, environment);
            var outcome = await orchestrator.StartTableAsync(def, state.Sources, state.Tables, state.Pipelines);
            if (outcome.Ok)
            {
                def.Status = PipelineStatus.Running;
                def.Error = null;
            }
            else
            {
                def.Status = PipelineStatus.Failed;
                def.Error = outcome.Error;
            }
        }

        // Plan 016 wave 1-C: `renamed` belongs here for the same reason it does in RegistryGrain — the
        // history tier is keyed by NAME, the old one was just disabled, and without this the renamed table
        // would carry no tier at all under its new name.
        if (renamed || sqlChanged || historyConfigChanged)
        {
            await orchestrator.ResetTableHistoryAsync(def);
        }

        await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, def.Name), "table-updated", def.Status);
        return def;
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
            await orchestrator.StopTableAsync(existing.Name, environment);
        }

        await orchestrator.DisableTableHistoryAsync(existing.Name, environment);

        state.Tables.Remove(existing);
        RecomputeStaleReasons(); // a deleted table breaks every pin that named it.
        await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, existing.Name), "table-deleted", PipelineStatus.Stopped);
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
            // Table-over-pipeline (plan 025): deliberately TableInputs only, never PipelineInputs —
            // mirrors RegistryGrain.SetTableStatusAsync exactly. A table over a STOPPED (or deleted)
            // pipeline is legal and starts fine; it simply receives nothing until that pipeline runs,
            // because a pipeline has no replay ring/snapshot to backfill from (hard rule 6) — the same
            // reason DeletePipelineAsync above does not refuse a running dependent either. Only a
            // table-over-table dependency is refused here, because an upstream TABLE's own delta tier is
            // what a stopped/never-started upstream would leave this table permanently missing.
            var missing = existing.TableInputs
                .Where(name => state.Tables.FirstOrDefault(t => t.Name == name)?.Status != PipelineStatus.Running)
                .ToList();

            if (missing.Count > 0)
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = $"table input(s) not running: {string.Join(", ", missing)}";
                existing.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, existing.Name), "table-failed", existing.Status);
                return existing;
            }

            var outcome = await orchestrator.StartTableAsync(existing, state.Sources, state.Tables, state.Pipelines);
            if (outcome.Ok)
            {
                existing.Status = PipelineStatus.Running;
                existing.Error = null;
                await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, existing.Name), "table-started", existing.Status);
            }
            else
            {
                existing.Status = PipelineStatus.Failed;
                existing.Error = outcome.Error;
                await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, existing.Name), "table-failed", existing.Status);
            }
        }
        else
        {
            ThrowIfRunningDependents(existing.Name, "stop");

            await orchestrator.StopTableAsync(existing.Name, environment);
            existing.Status = PipelineStatus.Stopped;
            existing.Error = null;
            await orchestrator.PublishLifecycleAsync(EnvKeys.Qualify(environment, existing.Name), "table-stopped", existing.Status);
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
    // Plan 016 wave 2 — dependent-schema refresh and pin evaluation. Ported from RegistryGrain's
    // RefreshTableSchemas/RecomputeStaleReasons; see those for the full rationale (why dependents are
    // refreshed but NOT restarted, why a table that no longer compiles is left completely alone, and why
    // the sweep is whole-catalog rather than closure-based).
    // ------------------------------------------------------------------

    /// <summary>On an upstream schema change, recompile the tables that read from it and refresh their
    /// persisted <c>OutputFields</c> + field numbers, so <c>/proto</c> and <c>/api/meta/grpc</c> stop
    /// serving a schema the table no longer produces. Dependents are never restarted here (the
    /// restart-on-change machinery is for SELF edits) and a table whose SQL no longer compiles is left
    /// untouched — the refresh may only ever improve what is stored.</summary>
    private void RefreshTableSchemas()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var table in TopoSortByTableInputs(state.Tables))
        {
            var result = CompileTableSql(table.Sql, excludeTableId: table.Id);
            if (!result.Ok || result.OutputSchema is null)
            {
                continue;
            }

            var before = table.OutputFields;
            ApplyCompileResult(table, result);
            if (!SchemaCompatibility.ShapeChanged(before, table.OutputFields))
            {
                continue;
            }

            table.SchemaRevision++;
            table.UpdatedAtMs = now;
            EnsureFieldNumbers(EntitySchemas.TableKey(table.Id), table.OutputFields);
        }
    }

    /// <summary>Inputs before dependents, so a two-hop chain converges in one pass — a table's own
    /// compile reads its input tables' <c>OutputFields</c> (<see cref="BuildTableSchemas"/>). Same
    /// post-order DFS as <c>RegistryGrain.TopoSortByTableInputs</c>, over every table rather than only
    /// the running ones (this flavour has no equivalent resume path to share it with).</summary>
    private static List<TableDefinition> TopoSortByTableInputs(IReadOnlyList<TableDefinition> all)
    {
        var byName = new Dictionary<string, TableDefinition>(StringComparer.Ordinal);
        foreach (var t in all)
        {
            byName.TryAdd(t.Name, t);
        }

        var visited = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<TableDefinition>();

        void Visit(TableDefinition t, HashSet<string> stack)
        {
            if (visited.Contains(t.Name) || !stack.Add(t.Name))
            {
                return;
            }

            foreach (var dep in t.TableInputs)
            {
                if (byName.TryGetValue(dep, out var depDef))
                {
                    Visit(depDef, stack);
                }
            }

            visited.Add(t.Name);
            result.Add(t);
        }

        foreach (var t in all)
        {
            Visit(t, new HashSet<string>(StringComparer.Ordinal));
        }

        return result;
    }

    /// <summary><c>StaleReason</c> is set by the upstream change, at the moment the break happens, and
    /// cleared the moment the pins are satisfied again. Recomputed wholesale — the loop early-outs on an
    /// empty <c>DependsOn</c>, which is the default.</summary>
    private void RecomputeStaleReasons()
    {
        foreach (var t in state.Tables)
        {
            t.StaleReason = CatalogRevisions.EvaluatePins(t.DependsOn, state.Sources, state.Tables);
        }

        foreach (var p in state.Pipelines)
        {
            p.StaleReason = CatalogRevisions.EvaluatePins(p.DependsOn, state.Sources, state.Tables);
        }
    }

    // ------------------------------------------------------------------
    // Validation / helpers — ported verbatim from RegistryGrain (see that class for the extended
    // rationale in each doc comment).
    // ------------------------------------------------------------------

    /// <summary>Plan 021, going-forward guard, same shape plan 016 used for pipeline-name uniqueness: a
    /// source or table name containing <see cref="EnvKeys.Separator"/> can never be safely qualified
    /// (<see cref="EnvKeys.IsQualifiableEntityName"/>'s own doc comment explains why — it would already be
    /// unusable in SQL for the identical reason, so this costs nothing real). Checked at CREATE for both
    /// kinds and at RENAME for tables (sources can never be renamed at all — plan 016). Pipelines are
    /// keyed by GUID and never reach this.</summary>
    private static void ValidateQualifiableName(string name)
    {
        if (!EnvKeys.IsQualifiableEntityName(name))
        {
            throw new InvalidOperationException(
                $"'{name}' cannot be used as a name: it contains '{EnvKeys.Separator}', the character environment-qualified runtime keys are built from. Choose a name without a '{EnvKeys.Separator}' in it.");
        }
    }

    private void ValidateUniqueTableName(string name, string? excludeTableId)
    {
        if (state.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by a stream source");
        }
        // Table-over-pipeline (plan 025) — the third leg of the relation-name uniqueness triangle; see
        // ValidateUniquePipelineName below for the argument. Mirrors RegistryGrain.ValidateUniqueTableName
        // exactly.
        if (state.Pipelines.Any(p => string.Equals(p.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");
        }
        if (state.Tables.Any(t => t.Id != excludeTableId && string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another table");
        }
    }

    /// <summary>Plan 016 wave 1-C — pipeline names unique among PIPELINES; extended by plan 025
    /// (table-over-pipeline) to ALSO be unique against sources and tables. Before that plan a pipeline
    /// named <c>trades</c> reading <c>FROM trades</c> was legal and stayed legal, because pipelines were
    /// not in the SQL namespace at all. They are now: <see cref="PipelineDefinition.OutputFields"/> lets a
    /// TABLE name a pipeline as a relation exactly like a source or another table, so a pipeline name must
    /// be unique against everything else a relation name can resolve to, on the same terms
    /// <see cref="ValidateUniqueTableName"/> already enforces for tables — without this, <c>FROM foo</c> in
    /// a table's SQL would name both a source/table and a pipeline, and which one it got would be decided
    /// by dictionary-insertion order in <see cref="BuildStreamSchemas"/>, an executable catalog whose
    /// meaning is an implementation detail. Ported from <c>RegistryGrain.ValidateUniquePipelineName</c> —
    /// see that method for the full rationale, including why there is no migration and no boot check: a
    /// catalog that already holds such a pair (from before this guard existed) keeps working until one of
    /// the two is next written.</summary>
    private void ValidateUniquePipelineName(string name, string? excludePipelineId)
    {
        if (state.Pipelines.Any(p => p.Id != excludePipelineId && string.Equals(p.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Name '{name}' is already used by another pipeline");
        }

        if (state.Sources.Any(s => string.Equals(s.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a stream source — a table's SQL resolves a relation name to exactly one entity");
        }
        if (state.Tables.Any(t => string.Equals(t.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Name '{name}' is already used by a table — a table's SQL resolves a relation name to exactly one entity");
        }
    }

    /// <summary>Plan 016 wave 1-C — the table rename policy, ported from
    /// <c>RegistryGrain.ValidateTableRenameAllowed</c>: allowed IFF Stopped, IFF <c>ShardBy</c> is empty,
    /// IFF no other table lists it in <c>TableInputs</c>. <b>This flavor had NO rename guard at all</b> —
    /// not even the sharded one Orleans has carried since plan 011 D2 — so a running table could be
    /// renamed out from under its own actor, whose id is the table NAME, leaving the old actor running and
    /// the new name pointing at nothing until somebody restarted it.
    ///
    /// <para><c>ShardBy</c> is checked even though this flavor refuses to START a sharded table (see
    /// <see cref="ValidateParallelism"/>'s "WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" note): the field is stored here, so a definition can carry it, and
    /// a guard that silently means something different per flavor is worse than one redundant
    /// check.</para></summary>
    private void ValidateTableRenameAllowed(TableDefinition existing)
    {
        if (existing.Status != PipelineStatus.Stopped)
        {
            throw new InvalidOperationException(
                $"'{existing.Name}' is {existing.Status} and cannot be renamed: its actor, history and delta topic are all keyed by the table name, so the rename would leave the running copy addressed under the old one. Stop it first, rename, then start it again.");
        }

        if (existing.ShardBy.Count > 0)
        {
            throw new InvalidOperationException(
                $"'{existing.Name}' is sharded by {string.Join(", ", existing.ShardBy)} and cannot be renamed: the shard tier is keyed by the table name, so a rename would strand every shard's rows and version trails under a key nothing looks up again. Clear shardBy first, rename, then shard again.");
        }

        var dependents = state.Tables
            .Where(t => t.Id != existing.Id && t.TableInputs.Contains(existing.Name, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException(
                $"'{existing.Name}' cannot be renamed: table(s) {string.Join(", ", dependents)} read from it by name and nothing rewrites their SQL. Update them first, or rename this table before anything depends on it.");
        }
    }

    /// <summary>Plan 011 D1 — WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR, and why it is not here.
    ///
    /// <see cref="TableDefinition.ShardBy"/> is Orleans-only for the same reason Parallelism is: the shard
    /// tier is three Orleans grain kinds whose entire value comes from Orleans' activation collector
    /// reclaiming an idle shard, and this flavor has no actor equivalent to point at. Parallelism is
    /// refused in TWO places — here at upsert, and defensively in <c>TableActor.StartAsync</c>. ShardBy is
    /// refused in the second of those only.
    ///
    /// That asymmetry is deliberate and is a real constraint, not a shortcut. This flavor's catalog has a
    /// standing contract, asserted by CatalogUpdateRoundTripTests, that EVERY client-owned field on
    /// <see cref="TableDefinition"/> survives an update unchanged — the test sets every writable property
    /// by reflection precisely so a field added tomorrow is guarded automatically. An upsert-time refusal
    /// would make ShardBy the one field that cannot round-trip, i.e. it would break the general contract
    /// to enforce a per-field one. Refusing at START instead keeps both: the field is stored (so a catalog
    /// exported from Orleans imports here intact and can be promoted back without loss), and a sharded
    /// table on this flavor can never RUN — <c>TableActor.StartAsync</c> returns a Failure whose message
    /// says exactly why, which surfaces as Status=Failed + Error on the table, the same visible outcome a
    /// 409 would have produced. What is explicitly NOT allowed is the silent middle: a table that looks
    /// sharded and answers every per-key lookup empty.</summary>
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

    /// <summary>Plan 008: <see cref="TableDefinition.FlushMs"/>'s own doc comment defines only 0 (→ the
    /// 2000 ms default) and positive values; a negative interval has no meaning, so it's rejected here the
    /// same way an out-of-range <see cref="TableDefinition.Parallelism"/> is.</summary>
    private static void ValidateFlushMs(int flushMs)
    {
        if (flushMs < 0)
        {
            throw new InvalidOperationException($"FlushMs must be >= 0 (got {flushMs}).");
        }
    }

    /// <summary>Plan 011 C2: 409-style guard for the opt-in row retention policy — the Dapr mirror of
    /// <c>RegistryGrain.ValidateRetention</c>, same rules and same reasoning (see that method's doc
    /// comment): non-negative bounds, Parallelism == 1 (already forced on this flavor), and a plan shape
    /// whose per-row state the Engine can actually reclaim. Refusing beats accepting a policy that would
    /// bound only what the console shows. Draft-friendly: SQL that does not compile is not rejected here.</summary>
    private static void ValidateRetention(TableDefinition def, TableCompileResult compileResult)
    {
        if (def.RetentionMaxRows < 0 || def.RetentionTtlMs < 0)
        {
            throw new InvalidOperationException(
                $"Retention bounds must be >= 0 (got maxRows={def.RetentionMaxRows}, ttlMs={def.RetentionTtlMs}); 0 means unbounded.");
        }

        if (def.RetentionMaxRows == 0 && def.RetentionTtlMs == 0)
        {
            return;
        }

        if (!compileResult.Ok || compileResult.Plan is null)
        {
            return;
        }

        if (!compileResult.Plan.SupportsRetention)
        {
            throw new InvalidOperationException(
                "Row retention is not supported for this table's SQL: joins, set operations, derived sources and GROUP BY/aggregates are excluded, because evicting an output row would leave their per-key state (join indexes, aggregate accumulators) growing — or, for aggregates, would restart the group from zero and emit a wrong value.");
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
        var streamSchemas = BuildTableStreamSchemas();
        var tableSchemas = BuildTableSchemas(excludeTableId);
        return SqlCompiler.CompileTable(sql, streamSchemas, tableSchemas);
    }

    /// <summary>Plan 008 W5: stores SourceNames from a pipeline compile result when it compiles; leaves it
    /// empty otherwise — the pipeline-side counterpart of <see cref="ApplyCompileResult"/> below (tables'
    /// StreamInputs/PipelineInputs/TableInputs). Mirrors RegistryGrain's identically-named helper.
    ///
    /// <para>Table-over-pipeline (plan 025) also fills <see cref="PipelineDefinition.OutputFields"/> here,
    /// from the same <c>OutputSchema</c>/<see cref="MapFieldType"/> pair the table side has always used —
    /// a pipeline needs a published schema before a table can name it as a relation, and this is the only
    /// place a pipeline's compile result is ever stored. Cleared on a failed compile for the same reason
    /// the table side clears its own: a stale schema for SQL that no longer compiles is worse than none,
    /// because <see cref="BuildStreamSchemas"/> would keep offering it as a live relation.</para></summary>
    private static void ApplyPipelineCompileResult(PipelineDefinition def, CompileResult result)
    {
        def.SourceNames = result.Ok ? result.SourceNames.ToList() : [];
        def.OutputFields = result.Ok && result.OutputSchema is not null
            ? result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList()
            : [];
    }

    /// <summary>Every pipeline NAME in this environment's catalog — the key that splits a compiled
    /// <c>StreamInputs</c> list into source inputs and pipeline inputs (see <see cref="ApplyCompileResult"/>).
    /// Names are unique across sources, tables and pipelines by construction (see
    /// <see cref="ValidateUniquePipelineName"/>, <see cref="ValidateUniqueTableName"/> and
    /// <see cref="UpsertSourceAsync"/>'s guard), so membership here is decisive, not a guess. Mirrors
    /// RegistryGrain's identically-named helper.</summary>
    private HashSet<string> PipelineNameSet() => state.Pipelines.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);

    /// <summary>Stores OutputFields/StreamInputs/PipelineInputs/TableInputs from a compile result when it
    /// compiles; leaves them empty otherwise.
    ///
    /// <para>Table-over-pipeline (plan 025): the engine returns ONE list of stream relations, because a
    /// relation is (name, schema) and it has no reason to care which catalog entity published the schema.
    /// Splitting that list by "is this name a pipeline?" is this method's job — mirrors
    /// <c>RegistryGrain.ApplyCompileResult</c> exactly, including why the split must be total: a name in
    /// neither set cannot occur (the compile would have rejected an unknown relation), and a name in both
    /// cannot occur (the create/upsert paths above refuse that collision).</para></summary>
    private void ApplyCompileResult(TableDefinition def, TableCompileResult result)
    {
        if (result.Ok && result.OutputSchema is not null)
        {
            var pipelineNames = PipelineNameSet();
            def.OutputFields = result.OutputSchema.Fields.Select(kv => new FieldDef(kv.Key, MapFieldType(kv.Value))).ToList();
            def.StreamInputs = result.StreamInputs.Where(n => !pipelineNames.Contains(n)).ToList();
            def.PipelineInputs = result.StreamInputs.Where(pipelineNames.Contains).ToList();
            def.TableInputs = result.TableInputs.ToList();
            def.KeyFields = TableKeyFields.Describe(def.Sql, result.Plan);
        }
        else
        {
            def.OutputFields = [];
            def.StreamInputs = [];
            def.PipelineInputs = [];
            def.TableInputs = [];
            def.KeyFields = null;
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

    /// <summary>The stream-relation dictionary a PIPELINE compiles against: sources only. A pipeline still
    /// reads sources only (never another pipeline, and never a table) — that is what keeps the
    /// source→pipeline→table dependency graph acyclic (AGENTS.md hard rule 6). Mirrors
    /// <c>RegistryGrain.BuildStreamSchemas</c> exactly, including staying the sources-only helper even
    /// after table-over-pipeline (plan 025) added <see cref="BuildTableStreamSchemas"/> alongside it for
    /// TABLE compiles — <see cref="CreatePipelineAsync"/>/<see cref="UpdatePipelineAsync"/>/the pipeline
    /// backfill loop in <see cref="EnsureInitialized"/> all deliberately keep calling this one, not
    /// that one.</summary>
    private Dictionary<string, SourceSchema> BuildStreamSchemas() =>
        state.Sources.ToDictionary(s => s.Name, s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

    /// <summary>The stream-relation dictionary a TABLE compiles against: every source, plus —
    /// table-over-pipeline (plan 025) — every pipeline that has a compiled output schema. A pipeline with
    /// empty <c>OutputFields</c> (draft SQL, or a record written before that field existed and not yet
    /// backfilled — see <see cref="EnsureInitialized"/>) contributes no relation; naming it is then an
    /// ordinary "unknown relation" diagnostic rather than a compile against a schema that is empty because
    /// nobody filled it in. Sources win a name collision here purely as a defensive tiebreak — the write
    /// paths refuse to create one in the first place (<see cref="ValidateUniquePipelineName"/>/
    /// <see cref="UpsertSourceAsync"/>'s guard), because a relation name that resolves to two different
    /// streams is not a preference to be expressed, it is a catalog that cannot be executed unambiguously.
    /// Mirrors Orleans' <c>PipelineInputs.BuildStreamSchemas(sources, pipelines)</c> exactly (this flavor
    /// has no separate helper class for it — see that type's own doc comment for the full "why the engine
    /// never learns about this" rationale, which applies verbatim here). <b>Never used for a PIPELINE's own
    /// compile</b> — see <see cref="BuildStreamSchemas"/>'s doc comment for why a pipeline reading another
    /// pipeline would break the graph's acyclic guarantee.</summary>
    private Dictionary<string, SourceSchema> BuildTableStreamSchemas()
    {
        var schemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);

        foreach (var p in state.Pipelines)
        {
            if (p.OutputFields.Count == 0) continue;
            schemas[p.Name] = new SourceSchema(p.Name, p.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
        }

        foreach (var s in state.Sources)
        {
            schemas[s.Name] = new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
        }

        return schemas;
    }

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
