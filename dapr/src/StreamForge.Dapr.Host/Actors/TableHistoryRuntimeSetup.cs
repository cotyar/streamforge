using Dapr.Actors.Runtime;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W7-B seam, called from Program.cs (frozen during wave W7). W7-B fills these in:
/// TableHistoryActor registration + real ITableHistoryFacade (last registration wins over the
/// stub) + its ITableDeltaSink router registration.
/// </summary>
public static class TableHistoryRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        // ponytail: W7-B fills this.
    }

    public static void AddServices(IServiceCollection services)
    {
        // ponytail: W7-B fills this.
    }
}
