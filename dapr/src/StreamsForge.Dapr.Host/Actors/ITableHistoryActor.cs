using Dapr.Actors;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>Request payload for <see cref="ITableHistoryActor.GetHistoryAsync"/> — a Dapr actor method
/// takes at most one parameter (see <see cref="PipelineStartRequest"/>/<see cref="SetStatusRequest"/>'s
/// doc comments for the same constraint elsewhere in this project). <see cref="Key"/> is the row-identity
/// key ALREADY ENCODED by the caller (see this interface's <see cref="GetHistoryAsync"/> doc comment for
/// where that encoding happens) — this actor never derives it from a raw row.</summary>
public sealed record TableHistoryLookupRequest(string Key, int Limit);

/// <summary>
/// Actor-invocation surface for one table's opt-in row-version history — actor type "TableHistoryActor",
/// key = the table's <see cref="TableDefinition.Name"/>. Dapr counterpart of Orleans'
/// <c>ITableHistoryGrain</c> (orleans/src/StreamsForge.Abstractions/GrainInterfaces.cs) /
/// <c>TableHistoryGrain</c> (orleans/src/StreamsForge.Host/Grains/TableHistoryGrain.cs) — read every method
/// next to its Orleans equivalent; deviations are called out explicitly. See
/// <see cref="TableHistoryActor"/>'s own class doc for the full design (state shape, write-behind cadence,
/// wire re-normalization).
///
/// <para><b>Acyclic by construction, and a pure LEAF (plan 005 W7-B, mirroring
/// <see cref="IGeneratorActor"/>/<see cref="IPipelineActor"/>'s own doc comments):</b> this actor never
/// resolves <see cref="Abstractions.ICatalogFacade"/>, an <c>IRegistryActor</c> proxy, or any other actor —
/// everything it needs arrives via <see cref="ResetAsync"/>'s <see cref="TableDefinition"/> or a subsequent
/// <see cref="ApplyDeltasAsync"/> call. Unlike <c>GeneratorActor</c>/<c>PipelineActor</c> it doesn't even
/// talk OUTWARD to Dapr pub/sub — its only external effect is its own actor-state writes, making it the
/// simplest leaf in this project's actor graph.</para>
///
/// <para><b>Where deltas come from:</b> Dapr's fixed-topic transport (decision D-D) means this actor never
/// subscribes to anything itself — <c>Streaming/TableHistoryDeltaSink.cs</c> (registered as one more
/// <c>ITableDeltaSink</c> alongside the SignalR bridge, see Streaming/Sinks.cs's class doc) forwards every
/// <c>sf-table-delta</c> envelope for a history-enabled table to this actor via
/// <see cref="ApplyDeltasAsync"/>.</para>
/// </summary>
public interface ITableHistoryActor : IActor
{
    /// <summary>(Re)configures history collection from <paramref name="def"/>'s current
    /// HistoryEnabled/HistoryMode/HistoryLimit/HistoryByField/HistoryWindowMs, re-derives the row-identity
    /// column mapping from the table's SQL (<c>TableGroupKeyExtractor.ExtractIdentityColumns</c>), and
    /// ALWAYS clears previously accumulated history — mirrors <c>ITableHistoryGrain.ResetAsync</c> exactly
    /// (see that interface's doc comment: "Always clears previously accumulated history"). Call on table
    /// create and on any SQL/history-config change — see
    /// <c>Lifecycle/DaprLifecycleOrchestrator.History.cs</c>'s <c>ResetTableHistoryAsync</c>, invoked from
    /// exactly those two <c>Catalog/CatalogStore.cs</c> call sites, unchanged this wave.</summary>
    Task ResetAsync(TableDefinition def);

    /// <summary>Disables history collection and clears all state — mirrors
    /// <c>ITableHistoryGrain.DisableAsync</c>. Call on table delete.</summary>
    Task DisableAsync();

    /// <summary>Idempotent configure — mirrors <c>ITableHistoryGrain.ResumeAsync</c>'s "survives a restart
    /// without losing accumulated history" contract (see that method's doc comment:
    /// "Re-subscribe history grains for every table with HistoryEnabled ... uses ResumeAsync (not
    /// ResetAsync) so previously accumulated history survives a silo restart"), adapted to this actor's
    /// idempotent-by-comparison shape rather than Orleans' idempotent-by-construction one (a grain
    /// reactivation always re-subscribes regardless; here <see cref="Services.TableHistorySupervisorService"/>
    /// calls this on every sweep tick, including ones where nothing changed, so it must be safe to call
    /// repeatedly with no effect).
    ///
    /// <para><b>No-op (preserves <c>Entries</c>) when <paramref name="def"/>'s history-relevant config
    /// already matches this actor's current configuration</b> (HistoryEnabled/Mode/Limit/ByField/WindowMs,
    /// and the identity-column mapping derived from <paramref name="def"/>.Sql) — this is what makes a host
    /// restart safe: <see cref="TableHistoryActor.OnActivateAsync"/> reloads the actor's persisted,
    /// already-correctly-configured state from Redis, and a subsequent sweep call here must not clobber
    /// it. Otherwise behaves exactly like <see cref="ResetAsync"/> (first-ever configuration for a table
    /// this actor has never seen a Reset/EnsureConfigured call for — e.g. a SEEDED table, whose definition
    /// never goes through <c>Catalog/CatalogStore.cs</c>'s <c>CreateTableAsync</c> at all — or a genuine
    /// config/SQL change that reached this actor via the sweep instead of
    /// <see cref="Lifecycle.DaprLifecycleOrchestrator.ResetTableHistoryAsync"/>).</para></summary>
    Task EnsureConfiguredAsync(TableDefinition def);

    /// <summary>Applies one batch of table deltas — mirrors <c>TableHistoryGrain.OnDeltaBatchAsync</c>'s
    /// stream handler, fed here by <c>Streaming.TableHistoryDeltaSink</c> instead of a direct stream
    /// subscription (Dapr's fixed-topic transport, decision D-D). A cheap no-op if history isn't currently
    /// enabled for this table or the batch is empty — <see cref="TableHistoryActor"/>'s own state check,
    /// independent of (and a second line of defense behind) <c>TableHistoryDeltaSink</c>'s own
    /// enable-map gate.
    ///
    /// <para><b>JsonElement re-normalization (same finding as
    /// <see cref="IPipelineActor.ProcessEventsAsync"/> — see that method's doc comment):</b>
    /// <paramref name="envelope"/> crosses the Dapr actor-invocation wire, which round-trips through
    /// System.Text.Json with no static type for <c>Dictionary&lt;string, object?&gt;</c> values — so even
    /// though <c>Streaming/StreamingRuntimeSetup.cs</c>'s <c>sf-table-delta</c> endpoint already normalized
    /// every delta's <c>Row</c> dictionary once, at pub/sub ingress, before <c>TableHistoryDeltaSink</c>
    /// ever saw it, every value comes back out as a <see cref="System.Text.Json.JsonElement"/> AGAIN once
    /// it lands inside this actor's method body. <see cref="TableHistoryActor"/>'s implementation
    /// re-normalizes before deriving the row-identity key or appending a version — see
    /// <c>dapr/tests/StreamsForge.Dapr.Tests/TableHistoryApplicationTests.cs</c> for a round-trip test
    /// proving this is not a no-op.</para></summary>
    Task ApplyDeltasAsync(TableDeltaEnvelope envelope);

    /// <summary>Version history for one row-identity key — mirrors
    /// <c>ITableHistoryGrain.GetHistoryAsync(string key, int limit)</c>.
    /// <paramref name="request"/>.Key is the ALREADY-ENCODED identity key
    /// (<c>RowKeyCodec.EncodeIdentity</c>), computed by the shared REST endpoint
    /// (<c>shared/StreamsForge.Api/Endpoints/TablesEndpoints.cs</c>'s <c>/history/lookup</c> handler) BEFORE
    /// this call — this actor never re-derives it from a raw row (see
    /// <c>Facades/DaprTableHistoryFacade.cs</c>'s doc comment for the key-codec-parity note: the same
    /// shared endpoint code runs unmodified on both runtimes, decision D-B).</summary>
    Task<TableHistoryQueryResult> GetHistoryAsync(TableHistoryLookupRequest request);

    /// <summary>Mirrors <c>ITableHistoryGrain.GetStatsAsync</c>.</summary>
    Task<TableHistoryStats> GetStatsAsync();
}
