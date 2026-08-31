using System.Collections.Concurrent;
using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: table -&gt; history-enabled lookup shared between
/// <see cref="TableHistoryDeltaSink"/> (reads it, to decide whether a table's delta batch is worth
/// forwarding at all) and <see cref="Lifecycle.DaprLifecycleOrchestrator"/>'s history methods
/// (<c>Lifecycle/DaprLifecycleOrchestrator.History.cs</c> — writes it, on every table create/update/
/// delete).
///
/// <para><b>Why a static singleton instance, not an ordinary DI-constructed one:</b>
/// <c>Lifecycle/DaprLifecycleOrchestrator.History.cs</c> cannot add a constructor-injected dependency to
/// <c>DaprLifecycleOrchestrator</c>'s primary constructor the way <c>Streaming.PipelineEventRouter</c> was
/// added during W6 (see that class's own field) — that constructor lives in
/// <c>Lifecycle/DaprLifecycleOrchestrator.cs</c>, the MAIN partial-class file, owned by wave W7's OTHER
/// parallel agent (W7-A) this wave, not W7-B. <see cref="Instance"/> is a single process-wide instance
/// (mirrors <c>Actors/ActorProxyDefaults.cs</c>'s own static-shared-instance precedent) that
/// <c>TableHistoryRuntimeSetup.AddServices</c> also registers as ITS OWN DI singleton
/// (<c>services.AddSingleton(TableHistoryEnabledMap.Instance)</c>) — so <see cref="TableHistoryDeltaSink"/>
/// still gets it via ordinary constructor injection, and both consumers observe the exact same
/// dictionary.</para>
/// </summary>
public sealed class TableHistoryEnabledMap
{
    public static TableHistoryEnabledMap Instance { get; } = new();

    private readonly ConcurrentDictionary<string, bool> _enabled = new(StringComparer.Ordinal);

    /// <summary>Called by <c>DaprLifecycleOrchestrator.ResetTableHistoryAsync</c> with
    /// <c>def.HistoryEnabled</c> — a Reset can just as easily turn history OFF (e.g. an update that
    /// disables it, or one that keeps it off) as ON, so this always records the current value, it never
    /// assumes "enabled".</summary>
    public void SetEnabled(string tableName, bool enabled) => _enabled[tableName] = enabled;

    /// <summary>Called by <c>DaprLifecycleOrchestrator.DisableTableHistoryAsync</c> (table delete) —
    /// removes the entry entirely rather than setting it false, so a later table created with the same
    /// name starts with no stale entry at all (defense in depth; see this class's own doc comment on why
    /// there is no actual race here today).</summary>
    public void Remove(string tableName) => _enabled.TryRemove(tableName, out _);

    public bool IsEnabled(string tableName) => _enabled.TryGetValue(tableName, out var enabled) && enabled;
}

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: registered <see cref="ITableDeltaSink"/> (see Sinks.cs's class
/// doc) that feeds <see cref="TableHistoryActor"/> from the <c>sf-table-delta</c> topic — the Dapr
/// counterpart of <c>TableHistoryGrain</c>'s own per-table stream subscription
/// (orleans/src/StreamsForge.Host/Grains/TableHistoryGrain.cs), translated to Dapr's fixed-topic fan-out
/// (decision D-D) exactly like <c>Streaming/PipelineEventRouter.cs</c>'s own W6 precedent for
/// <c>sf-sources</c>.
///
/// <para><b>Enable-map gate, not "forward everything and let the actor no-op" — the wave brief's explicit
/// choice point, resolved here in favor of the cheaper option:</b> EVERY table's delta batch crosses
/// <c>sf-table-delta</c>, whether or not that table has row history enabled (<c>DaprStreamBridge</c>
/// already relays every one of them to SignalR unconditionally, regardless of history). Forwarding every
/// batch to an <see cref="ITableHistoryActor"/> proxy call regardless of enablement would mean a needless
/// Dapr actor-invocation sidecar round trip (activation + JSON serialize/deserialize) for every delta batch
/// of every table that has never turned history on, forever — exactly the kind of "sidecar hop for
/// nothing" decision D-E already rejects for the generator's per-event cadence, just on the consumption
/// side instead of the publish side. <see cref="TableHistoryEnabledMap"/> is a cheap, in-memory,
/// no-sidecar-hop lookup that lets this sink skip forwarding entirely for a disabled/unknown table — the
/// actor itself ALSO checks <c>HistoryEnabled</c> in <see cref="TableHistoryApplication.ApplyDeltas"/> (see
/// that method's doc comment) as a second, defensive line, but this sink's job is to avoid ever manifesting
/// the actor at all for the common case.</para>
///
/// <para><b>Why this is safe to trust (no lazy-repair path needed):</b> <c>DaprLifecycleOrchestrator</c>'s
/// <c>ResetTableHistoryAsync</c>/<c>DisableTableHistoryAsync</c> update the map SYNCHRONOUSLY, inline, as
/// part of the very same <c>Catalog/CatalogStore.cs</c> call that creates/updates/deletes the table — and a
/// newly created table starts <c>Stopped</c> (not publishing anything to <c>sf-table-delta</c> yet) until a
/// LATER, separate start call, by which point <c>ResetTableHistoryAsync</c> has already completed. There is
/// therefore no observable window where a table publishes deltas before its map entry reflects its current
/// <c>HistoryEnabled</c> value — an "unknown table" (no map entry at all) can only mean a delta for a table
/// this map has never heard of, which <see cref="TableHistoryEnabledMap.IsEnabled"/> already treats as
/// "not enabled" (default false for a missing key), the correct answer in every such case.</para>
/// </summary>
public sealed class TableHistoryDeltaSink(TableHistoryEnabledMap enabledMap) : ITableDeltaSink
{
    public Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        if (!enabledMap.IsEnabled(envelope.Table))
        {
            return Task.CompletedTask;
        }

        return TableHistoryActorProxy(envelope.Table).ApplyDeltasAsync(envelope);
    }

    private static ITableHistoryActor TableHistoryActorProxy(string tableName) =>
        ActorProxy.Create<ITableHistoryActor>(new ActorId(tableName), nameof(TableHistoryActor), ActorProxyDefaults.Options);
}
