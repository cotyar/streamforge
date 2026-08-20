using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Dapr.Host.Facades;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 021: the decision-and-teardown half of <see cref="EnvironmentRegistryActor.DeleteAsync"/>,
/// factored out into a plain, actor-framework-free class — same rationale as
/// <see cref="Catalog.CatalogStore"/>/<see cref="RegistryActor"/>'s own split (see that class's doc
/// comment): this needs I/O (<see cref="ICatalogFacadeFactory"/>, an actor-proxy adapter, to read/tear
/// down another environment's catalog) so it cannot be as purely synchronous as
/// <see cref="EnvironmentRegistryStore"/>, but it needs NO <c>ActorHost</c>/<c>StateManager</c> — which is
/// exactly what makes it unit-testable against a fake <see cref="ICatalogFacade"/> with no Dapr sidecar,
/// the same testability <see cref="Catalog.CatalogStore"/> gets from the identical split.
///
/// <para>Mutates <paramref name="store"/> directly (removes the row) on a successful delete — the actor
/// still owns persisting that mutation (<c>StateManager.SetStateAsync</c>), which is the one thing this
/// class correctly has no access to.</para>
/// </summary>
public sealed class EnvironmentDeleteWorkflow(EnvironmentRegistryStore store, ICatalogFacadeFactory catalogFactory)
{
    /// <summary>See <see cref="EnvironmentRegistryActor.DeleteAsync"/>'s own doc comment for the full
    /// behavior contract (refuses default always; <c>Success(false)</c> for an unknown name; refuses a
    /// non-empty environment without <paramref name="force"/>; the worklist-with-retries table deletion
    /// order; what a forced delete does NOT clean up). <paramref name="onTablesLeftBehind"/> is called at
    /// most once, with a positive count, if the worklist could not remove every table (an unresolvable
    /// running-dependent cycle) — the caller decides how to surface that (a log line, in production; an
    /// assertion, in a test).</summary>
    public async Task<ActorResult<bool>> DeleteAsync(string name, bool force, Action<int>? onTablesLeftBehind = null)
    {
        var normalized = EnvKeys.Normalize(name);
        if (normalized == EnvKeys.Default)
        {
            return ActorResult<bool>.Failure("the default environment always exists and cannot be deleted.");
        }

        if (!store.Exists(normalized))
        {
            return ActorResult<bool>.Success(false);
        }

        var catalog = catalogFactory.For(normalized);
        var sources = await catalog.GetSourcesAsync();
        var pipelines = await catalog.GetPipelinesAsync();
        var tables = await catalog.GetTablesAsync();
        var total = sources.Count + pipelines.Count + tables.Count;

        if (total > 0 && !force)
        {
            return ActorResult<bool>.Failure(
                $"environment '{normalized}' is not empty ({total} entit{(total == 1 ? "y" : "ies")}: {sources.Count} source(s), {pipelines.Count} pipeline(s), {tables.Count} table(s)) — pass force=true to delete it and everything in it.");
        }

        if (force)
        {
            foreach (var pipeline in pipelines)
            {
                await catalog.DeletePipelineAsync(pipeline.Id);
            }

            // Worklist with retries: a table with a still-Running dependent throws (CatalogStore.
            // ThrowIfRunningDependents) — rather than compute a topological order here, just keep sweeping
            // the remainder until a full pass makes no progress, mirroring the self-healing "retry next
            // sweep" philosophy every supervisor in this project already uses.
            var remaining = tables.Select(t => t.Id).ToList();
            bool progressed;
            do
            {
                progressed = false;
                var stillRemaining = new List<string>();
                foreach (var tableId in remaining)
                {
                    try
                    {
                        if (await catalog.DeleteTableAsync(tableId))
                        {
                            progressed = true;
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        stillRemaining.Add(tableId);
                    }
                }

                remaining = stillRemaining;
            } while (progressed && remaining.Count > 0);

            if (remaining.Count > 0)
            {
                onTablesLeftBehind?.Invoke(remaining.Count);
            }

            foreach (var source in sources)
            {
                await catalog.DeleteSourceAsync(source.Name);
            }
        }

        store.Delete(normalized);
        return ActorResult<bool>.Success(true);
    }
}
