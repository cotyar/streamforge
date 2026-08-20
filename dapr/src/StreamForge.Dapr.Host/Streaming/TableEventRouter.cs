using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Actors;

namespace StreamForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: <c>Sinks.cs</c>'s class doc anticipates this exactly ("W7's
/// TableActor routes matching sources into Z-set ingestion... both register an extra sink, they don't
/// replace this one") — registered as BOTH <see cref="ISourceEventsSink"/> AND <see cref="ITableDeltaSink"/>
/// (in <c>Actors/TableRuntimeSetup.cs</c>'s <c>AddServices</c>, per the wave brief — NOT
/// <c>StreamingRuntimeSetup.cs</c>, to keep this router's entire registration surface inside this wave's
/// own owned file), so every <c>sf-sources</c> envelope this process receives is fanned out to it exactly
/// like <see cref="PipelineEventRouter"/>, AND every <c>sf-table-delta</c> envelope is fanned out to it
/// alongside W7-B's <c>TableHistoryActor</c> sink.
///
/// <para><b>What it tracks — two independent routing tables, split by input kind:</b> a table's SQL can
/// read stream sources directly (<c>TableCompileResult.StreamInputs</c>) and/or other tables' output
/// directly (<c>TableCompileResult.TableInputs</c>, table-over-table chaining — e.g. the seeded
/// "hot_symbols" FROM "positions" demo). <see cref="OnSourceEventsAsync"/> fans a <c>sf-sources</c>
/// envelope out to every table subscribed to that STREAM source; <see cref="OnTableDeltaAsync"/> fans a
/// <c>sf-table-delta</c> envelope out to every table subscribed to that UPSTREAM TABLE. Both indexes are
/// pure in-memory (never persisted) many-to-many maps.</para>
///
/// <para><b>Who registers, and when — changed by PARITY.md debt item D2:</b> <see cref="Register"/> used
/// to be called only from <see cref="Lifecycle.DaprLifecycleOrchestrator.StartTableAsync"/>, AFTER
/// <see cref="Actors.TableActor.StartAsync"/> had already returned. It is now called from INSIDE
/// <see cref="Actors.TableActor.StartAsync"/>/<c>OnActivateAsync</c>'s self-heal branch themselves
/// (<c>RegisterRouterAndAttachToTableInputsAsync</c>), BEFORE those methods read any upstream table's
/// snapshot for a table-over-table warm attach — see that method's own doc comment for why registering
/// first is what makes a subsequent <see cref="OnTableDeltaAsync"/>/<see cref="OnSourceEventsAsync"/> call
/// against the newly-registered table queue behind the still-in-flight StartAsync/OnActivateAsync turn
/// (Dapr actors process one invocation at a time per actor id) rather than arrive too early and be
/// dropped. <see cref="Unregister"/> is still called from <see cref="Lifecycle.
/// DaprLifecycleOrchestrator.StopTableAsync"/> (and defensively from <see cref="Lifecycle.
/// DaprLifecycleOrchestrator.StartTableAsync"/> on a pre-registration failure) — there is no equivalent
/// ordering hazard on the way down. <see cref="Services.TableSupervisorService"/>'s sweep still repairs
/// this router for a <see cref="TableActor"/> that self-healed on reactivation without going through
/// either of those two calls, unchanged by this item — the same relationship <see cref="PipelineEventRouter"/>
/// has to <see cref="Services.PipelineSupervisorService"/>.</para>
///
/// <para><b>Self-filter (a table must never receive its own output deltas):</b>
/// <see cref="OnTableDeltaAsync"/> skips any consumer whose name equals the delta batch's own
/// <see cref="TableDeltaEnvelope.Table"/> — the SQL compiler would never legitimately produce a
/// self-referential <c>TableInputs</c> entry (a table can't FROM/JOIN itself), so this is cheap defensive
/// insurance against that specific footgun rather than a case this router expects to hit in practice.</para>
///
/// <para><b>Fan-out, not a direct subscription</b> — same rationale as <see cref="PipelineEventRouter"/>'s
/// own doc comment: Dapr's fixed topics (decision D-D) mean this router is what makes them behave like a
/// per-source/per-upstream-table subscription from a <see cref="TableActor"/>'s point of view.</para>
/// </summary>
public sealed class TableEventRouter(ILogger<TableEventRouter> logger) : ISourceEventsSink, ITableDeltaSink
{
    private readonly object _gate = new();

    /// <summary>Stream source name → set of table names consuming it directly.</summary>
    private readonly Dictionary<string, HashSet<string>> _byStreamSource = new(StringComparer.Ordinal);

    /// <summary>Upstream table name → set of (downstream) table names consuming it directly.</summary>
    private readonly Dictionary<string, HashSet<string>> _byUpstreamTable = new(StringComparer.Ordinal);

    /// <summary>Table name → (its own stream inputs, its own table inputs) — reverse index so
    /// <see cref="Unregister"/> is O(subscriptions for this table) instead of a full scan of both maps
    /// above.</summary>
    private readonly Dictionary<string, (HashSet<string> Streams, HashSet<string> Tables)> _byTable = new(StringComparer.Ordinal);

    /// <summary>Replaces (or creates) <paramref name="tableName"/>'s subscription sets with exactly
    /// <paramref name="streamInputs"/>/<paramref name="tableInputs"/> — idempotent; safe to call repeatedly
    /// with the same sets (e.g. a supervisor sweep re-registering an already-routed table).</summary>
    public void Register(string tableName, IReadOnlyList<string> streamInputs, IReadOnlyList<string> tableInputs)
    {
        lock (_gate)
        {
            UnregisterLocked(tableName);

            if (streamInputs.Count == 0 && tableInputs.Count == 0)
            {
                return;
            }

            var streams = new HashSet<string>(streamInputs, StringComparer.Ordinal);
            var tables = new HashSet<string>(tableInputs, StringComparer.Ordinal);
            _byTable[tableName] = (streams, tables);

            foreach (var source in streams)
            {
                if (!_byStreamSource.TryGetValue(source, out var consumers))
                {
                    consumers = new HashSet<string>(StringComparer.Ordinal);
                    _byStreamSource[source] = consumers;
                }
                consumers.Add(tableName);
            }

            foreach (var upstream in tables)
            {
                if (!_byUpstreamTable.TryGetValue(upstream, out var consumers))
                {
                    consumers = new HashSet<string>(StringComparer.Ordinal);
                    _byUpstreamTable[upstream] = consumers;
                }
                consumers.Add(tableName);
            }
        }
    }

    /// <summary>Removes every subscription for <paramref name="tableName"/>. Idempotent — a no-op if it
    /// wasn't registered.</summary>
    public void Unregister(string tableName)
    {
        lock (_gate)
        {
            UnregisterLocked(tableName);
        }
    }

    private void UnregisterLocked(string tableName)
    {
        if (!_byTable.Remove(tableName, out var prev))
        {
            return;
        }

        foreach (var source in prev.Streams)
        {
            if (_byStreamSource.TryGetValue(source, out var consumers))
            {
                consumers.Remove(tableName);
                if (consumers.Count == 0)
                {
                    _byStreamSource.Remove(source);
                }
            }
        }

        foreach (var upstream in prev.Tables)
        {
            if (_byUpstreamTable.TryGetValue(upstream, out var consumers))
            {
                consumers.Remove(tableName);
                if (consumers.Count == 0)
                {
                    _byUpstreamTable.Remove(upstream);
                }
            }
        }
    }

    /// <summary>Point-in-time snapshot of the table names subscribed to stream source
    /// <paramref name="sourceName"/> — exposed for tests (see dapr/tests/StreamForge.Dapr.Tests/
    /// TableEventRouterTests.cs); the dispatch path below takes its own lock-protected snapshot inline
    /// rather than calling this.</summary>
    public IReadOnlyCollection<string> StreamSubscribersOf(string sourceName)
    {
        lock (_gate)
        {
            return _byStreamSource.TryGetValue(sourceName, out var consumers) ? consumers.ToList() : [];
        }
    }

    /// <summary>Point-in-time snapshot of the table names subscribed to upstream table
    /// <paramref name="upstreamTableName"/> — exposed for tests, same rationale as
    /// <see cref="StreamSubscribersOf"/>.</summary>
    public IReadOnlyCollection<string> TableSubscribersOf(string upstreamTableName)
    {
        lock (_gate)
        {
            return _byUpstreamTable.TryGetValue(upstreamTableName, out var consumers) ? consumers.ToList() : [];
        }
    }

    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        List<string> tableNames;
        lock (_gate)
        {
            if (!_byStreamSource.TryGetValue(envelope.Source, out var consumers) || consumers.Count == 0)
            {
                return;
            }
            tableNames = consumers.ToList();
        }

        foreach (var tableName in tableNames)
        {
            try
            {
                var actor = ActorProxy.Create<ITableActor>(new ActorId(tableName), nameof(TableActor), ActorProxyDefaults.Options);
                await actor.ProcessSourceEventsAsync(envelope);
            }
            catch (Exception ex)
            {
                // Best-effort per table, mirroring PipelineEventRouter's own per-subscriber try/catch —
                // one misbehaving/unreachable table actor must never stop this batch from reaching the
                // rest, nor tear down the router.
                logger.LogWarning(
                    ex,
                    "TableEventRouter: failed to forward {Count} event(s) from source '{Source}' to table '{Table}'.",
                    envelope.Events.Count, envelope.Source, tableName);
            }
        }
    }

    public async Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        List<string> tableNames;
        lock (_gate)
        {
            if (!_byUpstreamTable.TryGetValue(envelope.Table, out var consumers) || consumers.Count == 0)
            {
                return;
            }
            tableNames = consumers.ToList();
        }

        foreach (var tableName in ExcludeSelf(tableNames, envelope.Table))
        {
            try
            {
                var actor = ActorProxy.Create<ITableActor>(new ActorId(tableName), nameof(TableActor), ActorProxyDefaults.Options);
                await actor.ProcessTableDeltasAsync(envelope);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "TableEventRouter: failed to forward {Count} delta(s) from table '{Upstream}' to table '{Table}'.",
                    envelope.Deltas.Count, envelope.Table, tableName);
            }
        }
    }

    /// <summary>Pure self-filter — a table must never receive its own output deltas (see class doc). Kept
    /// as its own static method, rather than inlined in <see cref="OnTableDeltaAsync"/>, purely so this
    /// safety rule is unit-testable without a live actor/sidecar — see dapr/tests/StreamForge.Dapr.Tests/
    /// TableEventRouterTests.cs.</summary>
    public static IEnumerable<string> ExcludeSelf(IEnumerable<string> consumers, string upstreamTableName) =>
        consumers.Where(name => !string.Equals(name, upstreamTableName, StringComparison.Ordinal));
}
