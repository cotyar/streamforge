using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// W7-B's half of the orchestrator (partial class so wave W7's two parallel agents own disjoint
/// files: table methods live in DaprLifecycleOrchestrator.cs — W7-A; history methods here — W7-B).
///
/// <para><b>Real as of W7-B</b> (was W4's warn-and-succeed no-op, verbatim-copied into the main file's
/// <c>StartTableAsync</c>/<c>StopTableAsync</c>, which remain W7-A's to replace). Mirrors this class's own
/// <see cref="StartPipelineAsync"/>/<see cref="StopPipelineAsync"/> precedent exactly: an inline,
/// synchronously-awaited call into <see cref="ITableHistoryActor"/>'s proxy, safe from the reentrancy
/// hazard this class's own doc comment describes because <see cref="TableHistoryActor"/> is a pure LEAF
/// (see that actor's class doc) — it never calls back into <c>RegistryActor</c>, <c>ICatalogFacade</c>, or
/// any other actor, so there is no cycle to deadlock on.</para>
///
/// <para><b><see cref="TableHistoryEnabledMap"/> bookkeeping:</b> every successful call here also updates
/// the shared enable-map <see cref="Streaming.TableHistoryDeltaSink"/> gates its own forwarding on — see
/// that map's own doc comment for why it's a static singleton instance rather than a constructor-injected
/// one (this partial class cannot add a dependency to the primary constructor declared in
/// <c>DaprLifecycleOrchestrator.cs</c>, W7-A's file this wave).</para>
/// </summary>
public sealed partial class DaprLifecycleOrchestrator
{
    /// <summary>Mirrors <c>ITableHistoryGrain.ResetAsync</c>'s call sites exactly: invoked by
    /// <c>Catalog/CatalogStore.cs</c> on table create, and on update when the SQL or any history-config
    /// field changed — see that file's <c>CreateTableAsync</c>/<c>UpdateTableAsync</c>, unchanged this
    /// wave. <paramref name="def"/>.HistoryEnabled can be true OR false here (a Reset can just as well turn
    /// history off as on) — <see cref="TableHistoryEnabledMap.SetEnabled"/> always records the CURRENT
    /// value, it never assumes "enabled".</summary>
    public async Task ResetTableHistoryAsync(TableDefinition def)
    {
        await TableHistoryActorProxy(def.Name).ResetAsync(def);
        TableHistoryEnabledMap.Instance.SetEnabled(def.Name, def.HistoryEnabled);
    }

    /// <summary>Mirrors <c>ITableHistoryGrain.DisableAsync</c>'s one call site: <c>Catalog/CatalogStore.cs</c>'s
    /// <c>DeleteTableAsync</c>, unchanged this wave.</summary>
    public async Task DisableTableHistoryAsync(string tableName)
    {
        await TableHistoryActorProxy(tableName).DisableAsync();
        TableHistoryEnabledMap.Instance.Remove(tableName);
    }

    private static ITableHistoryActor TableHistoryActorProxy(string tableName) =>
        ActorProxy.Create<ITableHistoryActor>(new ActorId(tableName), nameof(TableHistoryActor), ActorProxyDefaults.Options);
}
