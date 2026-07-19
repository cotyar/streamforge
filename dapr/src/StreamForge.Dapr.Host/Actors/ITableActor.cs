using Dapr.Actors;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>Request payload for <see cref="ITableActor.StartAsync"/> — a Dapr actor method takes at most
/// one parameter (same constraint <see cref="PipelineStartRequest"/> documents). <see cref="Sources"/> and
/// <see cref="Tables"/> are every source/table the catalog currently knows about (not just the ones this
/// table's SQL references) — exactly what <c>TableGrain.StartClassicAsync</c> builds schemas from via
/// <c>IRegistryGrain.GetSourcesAsync()</c>/<c>GetTablesAsync()</c> on the Orleans side; here
/// <see cref="Catalog.CatalogStore"/> already has both full lists in hand (<c>state.Sources</c>/
/// <c>state.Tables</c>) at every call site, so no lookup back through <c>ICatalogFacade</c> is needed
/// (that would be an actor-proxy call back into <c>RegistryActor</c> from inside its own turn — the exact
/// reentrancy hazard dapr/ARCHITECTURE.md's reentrancy decision exists to avoid).</summary>
public sealed record TableStartRequest(TableDefinition Def, List<SourceDefinition> Sources, List<TableDefinition> Tables);

/// <summary>The stream source names and upstream table names a table's compiled SQL depends on — returned
/// by <see cref="ITableActor.StartAsync"/> (so <see cref="Lifecycle.DaprLifecycleOrchestrator"/> can
/// register <see cref="Streaming.TableEventRouter"/> without a second, redundant compile) and by
/// <see cref="ITableActor.GetInputNamesAsync"/> (so <see cref="Services.TableSupervisorService"/> can
/// repair the router for a self-healed actor with no recompile either).</summary>
public sealed record TableInputNames(List<string> StreamInputs, List<string> TableInputs);

/// <summary>
/// Actor-invocation surface for one running materialized table — actor type "TableActor", key = the
/// table's <see cref="TableDefinition.Name"/> (same key Orleans' <c>ITableGrain</c> uses — see
/// orleans/src/StreamForge.Host/Grains/TableGrain.cs's class doc: "Key = table name"). Dapr counterpart of
/// <c>ITableGrain</c>, CLASSIC (Parallelism==1) PATH ONLY — partitioned execution (Parallelism 2-16,
/// frontier-consistent reads, shared arrangements) is Orleans-only (plan decision D-F); a Dapr
/// <see cref="TableDefinition.Parallelism"/> other than 1 is rejected both at CRUD time
/// (<see cref="Catalog.CatalogStore.CreateTableAsync"/>/<c>UpdateTableAsync</c>) and defensively here (see
/// <see cref="StartAsync"/>).
///
/// <para><b>Acyclic by construction (same discipline as <see cref="IGeneratorActor"/>/
/// <see cref="IPipelineActor"/>):</b> this actor never resolves <c>ICatalogFacade</c>, an
/// <c>IRegistryActor</c> proxy, or any other actor proxy — everything it needs arrives via
/// <see cref="StartAsync"/>'s <see cref="TableStartRequest"/> or a subsequent
/// <see cref="ProcessSourceEventsAsync"/>/<see cref="ProcessTableDeltasAsync"/> call. It only ever talks
/// OUTWARD, to Dapr pub/sub (<c>sf-table-delta</c>).</para>
///
/// <para><b>Where events/deltas come from:</b> unlike Orleans' <c>TableGrain</c> (which subscribes its own
/// Orleans stream handles per source/upstream-table name inside <c>StartClassicAsync</c>), Dapr's
/// fixed-topic transport (decision D-D) means this actor never subscribes to anything itself —
/// <see cref="Streaming.TableEventRouter"/> (registered as one more <c>ISourceEventsSink</c> AND
/// <c>ITableDeltaSink</c>, alongside the SignalR bridge and W7-B's <c>TableHistoryActor</c>) fans
/// <c>sf-sources</c> envelopes out to every table whose SQL reads that stream directly, and
/// <c>sf-table-delta</c> envelopes out to every table whose SQL reads that upstream table directly — the
/// router explicitly filters a table out of its own upstream-table fan-out (a table must never receive its
/// own output deltas back).</para>
/// </summary>
public interface ITableActor : IActor
{
    /// <summary>(Re)starts this table: compiles <see cref="TableStartRequest.Def"/>'s SQL against
    /// <see cref="TableStartRequest.Sources"/>/<see cref="TableStartRequest.Tables"/>' schemas via the
    /// shared Engine (same compile path <c>TableGrain.StartClassicAsync</c> uses:
    /// <see cref="StreamForge.Engine.SqlCompiler.CompileTable"/>), replacing any previous executor/search
    /// index/timer. Rejects <see cref="TableDefinition.Parallelism"/> &gt; 1 defensively — mirrors
    /// <see cref="Catalog.CatalogStore.CreateTableAsync"/>'s CRUD-time rejection (decision D-F);
    /// partitioned execution never reaches this actor by construction, this is belt-and-braces. On
    /// success, arms the 2s flush timer and returns the distinct stream/table input names the compiled
    /// plan depends on (<c>TableCompileResult.StreamInputs</c>/<c>TableInputs</c>) — <c>Lifecycle.
    /// DaprLifecycleOrchestrator</c> uses this to register <see cref="Streaming.TableEventRouter"/>'s
    /// routing table without a second, redundant compile. On compile failure (or a rejected Parallelism),
    /// returns a Failure result (see <see cref="ActorResult{T}"/>'s class doc) instead of letting an
    /// exception cross the Dapr actor-invocation wire; the table is left/kept stopped.</summary>
    Task<ActorResult<TableInputNames>> StartAsync(TableStartRequest request);

    /// <summary>Stops the table (unregisters the flush timer, flushes any pending snapshot write, drops
    /// the compiled executor/search index). Idempotent.</summary>
    Task StopAsync();

    /// <summary>True if this actor currently has a live executor + flush timer armed (backed by persisted
    /// state — see <see cref="TableActor"/>'s class doc). <c>Services.TableSupervisorService</c>'s sweep
    /// checks this before deciding whether a catalog-Running table needs a full (re)start.</summary>
    Task<bool> IsRunningAsync();

    /// <summary>The stream/table input names this table is currently compiled against (empty sets if not
    /// running) — cheap in-memory read, no recompile. Lets <c>Services.TableSupervisorService</c> repair
    /// <see cref="Streaming.TableEventRouter"/>'s routing table for an actor that self-healed on
    /// reactivation without forcing a disruptive full restart.</summary>
    Task<TableInputNames> GetInputNamesAsync();

    /// <summary>Feeds one batch of raw source events through this table's compiled executor exactly like
    /// <c>TableGrain</c>'s per-event stream handler (<c>OnStreamEventAsync</c>) — routed here by
    /// <see cref="Streaming.TableEventRouter"/>, not a direct subscription. A no-op if the table isn't
    /// currently running.
    ///
    /// <para><b>JsonElement re-normalization (see dapr/ARCHITECTURE.md's serialization note and
    /// <see cref="IPipelineActor.ProcessEventsAsync"/>'s identical finding):</b> <paramref name="envelope"/>
    /// crosses the Dapr actor-invocation wire, which round-trips through System.Text.Json with no static
    /// type for <c>Dictionary&lt;string, object?&gt;</c> values — every event dictionary's values come back
    /// out as <see cref="System.Text.Json.JsonElement"/> again even though the router's caller already
    /// normalized this exact envelope once at the <c>sf-sources</c> pub/sub ingress. <see cref="TableActor"/>
    /// re-normalizes before handing rows to the Engine — see <c>dapr/tests/StreamForge.Dapr.Tests/
    /// TableActorWireNormalizationTests.cs</c> for a round-trip test proving this is not a no-op.</para>
    /// </summary>
    Task ProcessSourceEventsAsync(SourceEventsEnvelope envelope);

    /// <summary>Feeds one batch of an upstream table's Z-set deltas through this table's compiled executor
    /// exactly like <c>TableGrain</c>'s <c>OnTableDeltaBatchAsync</c> — routed here by
    /// <see cref="Streaming.TableEventRouter"/> for table-over-table chaining (e.g. the seeded
    /// "hot_symbols" FROM "positions" demo). Same JsonElement re-normalization requirement as
    /// <see cref="ProcessSourceEventsAsync"/> — <paramref name="envelope"/> crosses the actor wire too.
    /// A no-op if the table isn't currently running.</summary>
    Task ProcessTableDeltasAsync(TableDeltaEnvelope envelope);

    /// <summary>Mirrors <c>TableGrain.GetRowsAsync</c> — served from the write-behind-flushed snapshot
    /// (up to ~2s stale, same as Orleans' classic path; see <see cref="TableActor"/>'s class doc), not a
    /// live executor read. Backs <c>GET /api/tables/{id}/rows</c> via
    /// <c>Facades.DaprTableReadFacade</c>.</summary>
    Task<List<TableRowDto>> GetRowsAsync(int limit, int offset);

    /// <summary>Mirrors <c>TableGrain.GetRowCountAsync</c> — same flushed-snapshot staleness as
    /// <see cref="GetRowsAsync"/>.</summary>
    Task<int> GetRowCountAsync();

    /// <summary>Mirrors <c>TableGrain.GetSeqAsync</c> — a flush-generation counter (incremented once per
    /// write-behind flush, NOT once per published delta batch; see <see cref="TableActor"/>'s class doc for
    /// why this is a different counter than the one riding on <c>TableDeltaEnvelope.Seq</c>).</summary>
    Task<long> GetSeqAsync();

    /// <summary>Mirrors <c>TableGrain.GetMetricsAsync</c>'s Parallelism==1 shape exactly:
    /// <c>Partitions</c>/<c>ArrangedInputs</c>/<c>SnapshotFrontierEpoch</c> are always null (partitioned
    /// execution is Orleans-only — decision D-F). Backs <c>GET /api/tables/{id}/metrics</c> via
    /// <c>Facades.DaprTableReadFacade</c>.</summary>
    Task<TableMetrics> GetMetricsAsync();

    /// <summary>Mirrors <c>TableGrain.SearchAsync</c> — reads the LIVE executor snapshot for weight lookup
    /// (not the flushed copy <see cref="GetRowsAsync"/> reads), exactly like Orleans' classic path.</summary>
    Task<List<TableRowDto>> SearchAsync(string query, int limit);
}
