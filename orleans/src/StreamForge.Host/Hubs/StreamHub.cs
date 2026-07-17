using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;

namespace StreamForge.Host.Hubs;

[Authorize(Policy = "Viewer")]
public sealed class StreamHub : Hub
{
    public Task SubscribePipeline(string id) => Groups.AddToGroupAsync(Context.ConnectionId, $"pipeline:{id}");

    public Task UnsubscribePipeline(string id) => Groups.RemoveFromGroupAsync(Context.ConnectionId, $"pipeline:{id}");

    public Task SubscribeSource(string name) => Groups.AddToGroupAsync(Context.ConnectionId, $"source:{name}");

    public Task UnsubscribeSource(string name) => Groups.RemoveFromGroupAsync(Context.ConnectionId, $"source:{name}");

    public Task SubscribeMetrics() => Groups.AddToGroupAsync(Context.ConnectionId, "metrics");
}
