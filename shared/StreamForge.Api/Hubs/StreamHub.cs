using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api.Hubs;

/// <summary>
/// The realtime relay's client-facing surface: a caller joins the SignalR group whose frames
/// <c>StreamBridgeService</c> (Orleans) / <c>DaprStreamBridge</c> (Dapr) is already pushing.
///
/// <para><b>Plan 015 wave 3-B — what changed and why it had to.</b> The hub was gated exactly once, by
/// <c>[Authorize("Viewer")]</c> on the class, and every subscribe method checked nothing: any
/// authenticated user could join <i>any</i> pipeline's or source's group and receive its rows, no matter
/// what their entitlements said. Since the frames are the entity's data, that made the hub the way
/// around every read entitlement REST enforces. Each subscribe now asks <see cref="AccessGuard"/> for
/// the same read action the REST route serving that entity's rows asks for, at that entity, with its
/// <c>Tags</c>. The class attribute stays: it is the compatibility floor and the thing that still
/// rejects an unauthenticated connection at negotiate time.</para>
///
/// <para><b>How a refusal reaches the client, and why <see cref="HubException"/>.</b> An ordinary
/// exception thrown from a hub method is scrubbed by SignalR — the client sees "An unexpected error
/// occurred invoking 'SubscribePipeline'" and nothing else, which is the unhelpful shape this plan
/// exists to remove. <see cref="HubException"/> is the one exception type whose <i>message</i> SignalR
/// sends to the caller verbatim, with no <c>EnableDetailedErrors</c> needed, and it surfaces in the
/// browser as a rejected <c>invoke()</c> promise carrying that text. So the refusal is a HubException
/// whose message is <see cref="StreamForge.AppCore.Access.AccessResult.Reason"/> — the same string the
/// REST 403 body carries, so an operator debugging "why is this table empty" reads the same sentence
/// whichever transport they are on. <c>web/src/realtime/hub.ts</c> already awaits the invoke for tables
/// (<c>subscribeTable().ready</c> rejects) and fires-and-forgets the rest; making those three surface
/// the message in the UI is wave 6's job, and it needs no server change when it happens.</para>
///
/// <para><b>ponytail: entitlements are checked at SUBSCRIBE time only.</b> A grant revoked while a
/// subscription is live keeps delivering frames until that connection drops — the bridge relays to a
/// group and never looks at who is in it. Ceiling stated plainly: revocation reaches REST in one
/// <c>Auth:PolicyCacheSeconds</c> and reaches an established stream only at the next reconnect (the SPA
/// re-invokes every subscribe on <c>onreconnected</c>, so it IS re-checked then) or logout. Re-authorizing
/// per message was explicitly rejected: it would put a permission evaluation on the hot path of every
/// delta batch for every connected client. Upgrade path, when the exposure matters: have the resolver
/// raise an event when the document version moves, and on that event re-run the checks for each
/// connection's remembered subscriptions, removing the groups that no longer pass — periodic and
/// per-connection, not per-frame. That needs a per-connection subscription registry the hub does not
/// keep today, which is the actual work and the reason it is not in this wave.</para>
///
/// <para><b>Unsubscribe is deliberately ungated.</b> Leaving a group takes nothing away from anybody, and
/// a caller whose entitlement was just revoked must still be able to detach cleanly — refusing an
/// unsubscribe would strand exactly the subscription we most want gone.</para>
/// </summary>
[Authorize(Policy = "Viewer")]
public sealed class StreamHub(AccessGuard guard, ICatalogFacade catalog) : Hub
{
    public async Task SubscribePipeline(string id)
    {
        // Tags are read best-effort: subscribing before the entity exists has always been legal here
        // (the group simply stays silent), so a miss checks with no tags rather than becoming an error.
        await EnsureAsync(Actions.PipelineRead, id, (await catalog.GetPipelineAsync(id))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"pipeline:{id}");
    }

    public Task UnsubscribePipeline(string id) => Groups.RemoveFromGroupAsync(Context.ConnectionId, $"pipeline:{id}");

    public async Task SubscribeSource(string name)
    {
        await EnsureAsync(Actions.SourceRead, name, (await catalog.GetSourceAsync(name))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"source:{name}");
    }

    public Task UnsubscribeSource(string name) => Groups.RemoveFromGroupAsync(Context.ConnectionId, $"source:{name}");

    /// <summary>The metrics group is cluster-wide and names no entity, so it asks
    /// <see cref="Actions.CatalogRead"/> at <c>*</c> — the action the wave-1 equivalence matrix already
    /// assigns to this hub and to the other platform-wide read routes (<c>/api/meta/*</c>,
    /// <c>/api/transports</c>).</summary>
    public async Task SubscribeMetrics()
    {
        await EnsureAsync(Actions.CatalogRead, "*", null);
        await Groups.AddToGroupAsync(Context.ConnectionId, "metrics");
    }

    /// <summary>Table deltas are grouped by NAME (the delta stream's key), and
    /// <see cref="ICatalogFacade.GetTableAsync"/> takes an id — hence the list scan for the tags. The
    /// entitlement is checked at the NAME the caller asked for, which is the string an operator would
    /// have written into a scope.</summary>
    public async Task SubscribeTable(string name)
    {
        var table = (await catalog.GetTablesAsync()).FirstOrDefault(t => t.Name == name);
        await EnsureAsync(Actions.TableRead, name, table?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"table:{name}");
    }

    public Task UnsubscribeTable(string name) => Groups.RemoveFromGroupAsync(Context.ConnectionId, $"table:{name}");

    /// <summary>Returns normally when the caller may subscribe; throws the one exception type SignalR
    /// relays verbatim otherwise.
    ///
    /// <para>A <see cref="AccessDecision.RequiresApproval"/> answer is refused too, and says so in its own
    /// words ("grant … requires approval") — it is NOT a denial, but filing the request is waves 4-5's
    /// job and no machinery exists yet. "Approve a live stream subscription" is also a shape worth
    /// thinking about before building, rather than falling into. Refusing fails closed in the meantime,
    /// and the message tells the caller which of the two happened.</para></summary>
    private async Task EnsureAsync(string action, string scope, IReadOnlyCollection<string>? tags)
    {
        var result = await guard.CheckAsync(Context.User ?? new System.Security.Claims.ClaimsPrincipal(), action, scope, tags);
        if (!result.IsAllowed)
        {
            throw new HubException(result.Reason);
        }
    }
}
