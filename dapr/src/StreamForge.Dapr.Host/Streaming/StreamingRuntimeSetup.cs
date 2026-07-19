namespace StreamForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 W5-B seam: streaming spine registration, called from Program.cs (frozen during wave W5).
/// W5-B fills these in: AddServices registers the topic router / SignalR bridge services;
/// MapTopicEndpoints maps the Dapr pub/sub subscription endpoints (sf-sources, sf-pipeline-out,
/// sf-table-delta, sf-lifecycle, sf-metrics) ahead of MapSubscribeHandler discovery.
/// </summary>
public static class StreamingRuntimeSetup
{
    public static void AddServices(IServiceCollection services)
    {
        // ponytail: W5-B fills this.
    }

    public static void MapTopicEndpoints(WebApplication app)
    {
        // ponytail: W5-B fills this.
    }
}
