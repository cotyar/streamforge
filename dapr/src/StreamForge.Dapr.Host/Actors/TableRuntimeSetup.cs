using Dapr.Actors.Runtime;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W7-A seam, called from Program.cs (frozen during wave W7). W7-A fills these in:
/// TableActor registration + real ITableReadFacade (registered here, AFTER AddDaprFacades'
/// stub, so the last registration wins) + table supervisor service.
/// </summary>
public static class TableRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        // ponytail: W7-A fills this.
    }

    public static void AddServices(IServiceCollection services)
    {
        // ponytail: W7-A fills this.
    }
}
