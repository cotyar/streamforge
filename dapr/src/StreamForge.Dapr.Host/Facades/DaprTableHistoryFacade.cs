using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Dapr.Host.Actors;

namespace StreamForge.Dapr.Host.Facades;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: real <see cref="ITableHistoryFacade"/> — resolves a fresh
/// <see cref="ITableHistoryActor"/> proxy per call (same per-call-resolution style as
/// <see cref="DaprPipelineReadFacade"/>; a table history actor's id varies per call, unlike the singleton
/// catalog/user-store actors, so there is no single proxy instance to cache in a field). Registered by
/// <c>Actors/TableHistoryRuntimeSetup.cs</c>'s <c>AddServices</c>, called from Program.cs AFTER
/// <c>Facades/DaprFacades.cs</c>'s <c>AddDaprFacades</c> registers <see cref="StubTableHistoryFacade"/> —
/// last registration wins (identical pattern to <see cref="DaprPipelineReadFacade"/> replacing its own W4
/// stub during W6, and to <c>TableRuntimeSetup</c>'s doc comment for <c>DaprTableReadFacade</c>).
///
/// <para><b>KEY-CODEC PARITY (the wave brief's explicit ask — "check how the endpoint derives `key` —
/// endpoint or grain? mirror exactly").</b> <see cref="GetHistoryAsync"/>'s <c>key</c> parameter arrives
/// ALREADY ENCODED: the shared REST endpoint
/// (<c>shared/StreamForge.Api/Endpoints/TablesEndpoints.cs</c>'s <c>POST /{id}/history/lookup</c> handler)
/// derives it from the request's raw row via
/// <c>TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql)</c> +
/// <c>RowKeyCodec.EncodeIdentity(req.Row, identityColumns)</c> BEFORE calling
/// <c>ITableHistoryFacade.GetHistoryAsync(def.Name, key, limit)</c> at all — this is the IDENTICAL endpoint
/// code both runtimes share (decision D-B), so key derivation happens exactly once, in one place, on both
/// flavors. This facade therefore does NO key derivation of its own — it only forwards
/// <c>(tableName, key, limit)</c> straight through to the actor's
/// <see cref="ITableHistoryActor.GetHistoryAsync"/>.</para>
///
/// <para><b>Why the keys still match, even though the actor ALSO derives identity columns on its own (for
/// LIVE deltas, inside <see cref="TableHistoryApplication.Reset"/>/<see cref="TableHistoryApplication.ApplyDeltas"/>):</b>
/// both call sites run the IDENTICAL pure function, <c>TableGroupKeyExtractor.ExtractIdentityColumns</c>,
/// against the IDENTICAL <c>TableDefinition.Sql</c> string — the endpoint re-derives it from
/// <c>registry.GetTableAsync(id)</c>'s current definition on every lookup call; the actor derived it once,
/// from the same definition, inside its own last <c>ResetAsync</c>. Same input, same pure function, same
/// output, on both sides of the actor-invocation boundary — exactly the guarantee the Orleans flavor's
/// <c>TableHistoryGrain</c>/<c>TablesEndpoints</c> pairing already relies on (this facade changes nothing
/// about that guarantee, it just crosses an actor-proxy call instead of a grain-proxy call to reach
/// it).</para>
/// </summary>
internal sealed class DaprTableHistoryFacade : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        TableHistoryActorProxy(tableName).GetHistoryAsync(new TableHistoryLookupRequest(key, limit));

    public Task<TableHistoryStats> GetStatsAsync(string tableName) =>
        TableHistoryActorProxy(tableName).GetStatsAsync();

    /// <summary>Plan 021: same ambient-read rule as <see cref="DaprTableReadFacade.TableActorProxy"/> —
    /// this facade backs a REST request and <paramref name="tableName"/> arrives bare.</summary>
    private static ITableHistoryActor TableHistoryActorProxy(string tableName) =>
        ActorProxy.Create<ITableHistoryActor>(new ActorId(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)), nameof(TableHistoryActor), ActorProxyDefaults.Options);
}
