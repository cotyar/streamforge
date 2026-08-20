using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using StreamForge.Abstractions;
using StreamForge.AppCore;
using StreamForge.AppCore.Environments;
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
///
/// <para><b>Plan 021 wave 2 — group names are qualified by the CONNECTION's environment, pinned
/// verbatim against <c>StreamBridgeService</c>'s publish half.</b> A hub method invocation runs over an
/// already-established WebSocket, not through <c>EnvironmentSelectionMiddleware</c>, so
/// <c>EnvironmentAmbient</c> — an <c>AsyncLocal</c> set only by that HTTP middleware — is empty here, and
/// reading it would silently group every subscriber under the default environment regardless of what
/// they asked for. What DID go through the middleware is the HTTP request that established this
/// connection, and its <see cref="Microsoft.AspNetCore.Http.HttpContext"/> stays reachable for the
/// connection's lifetime via <c>Context.GetHttpContext()</c> — so <see cref="ConnectionEnv"/> reads the
/// environment the middleware stamped onto <c>HttpContext.Items</c>
/// (<see cref="StreamForge.Api.EnvironmentSelectionMiddleware.HttpContextItemKey"/>) rather than the
/// ambient. The <c>"metrics"</c> group stays unqualified — it names no entity, it is cluster-wide by
/// design (see <see cref="SubscribeMetrics"/>).</para>
/// </summary>
[Authorize(Policy = "Viewer")]
public sealed class StreamHub(AccessGuard guard, IServiceProvider services) : Hub
{
    /// <summary>The environment this connection selected, read off the HTTP request that established it
    /// — see the class remarks. A connection whose <c>HttpContext</c> somehow never reached
    /// <c>EnvironmentSelectionMiddleware</c> (there is no route today that maps the hub without it) falls
    /// back to <see cref="EnvKeys.Default"/> rather than throwing, so a subscribe request degrades to
    /// today's behaviour instead of failing outright.</summary>
    private string ConnectionEnv =>
        Context.GetHttpContext()?.Items[StreamForge.Api.EnvironmentSelectionMiddleware.HttpContextItemKey] as string
        ?? EnvKeys.Default;

    /// <summary>Reads the catalog of THIS CONNECTION's environment. It takes an
    /// <see cref="IServiceProvider"/> rather than an injected <see cref="ICatalogFacade"/> for a reason
    /// that is easy to get wrong: <c>ICatalogFacade</c> is registered Transient and resolves the
    /// environment from <see cref="EnvironmentAmbient"/> AT RESOLUTION TIME, and a hub is activated
    /// outside any HTTP request — so a constructor-injected facade is bound to the DEFAULT environment
    /// forever, for every connection, whatever environment that connection selected. The group names
    /// would still be right (they come from <see cref="ConnectionEnv"/>), but the entitlement check would
    /// be run against the wrong environment's tags: a <c>tag:finance</c> grant would be tested against
    /// whatever entity of that name happens to live in <c>default</c>. So the facade is resolved per
    /// call, inside the connection's environment.
    ///
    /// <para><see cref="EnvironmentAmbient.WithAsync"/> is the sanctioned non-middleware writer — the
    /// case its own doc comment describes, "code that has read an entity's environment and wants the
    /// facades it calls to agree". For the default environment this is byte-identical to the old
    /// behaviour: the ambient is already default, and the same facade comes back.</para></summary>
    private async Task<T> ReadCatalogAsync<T>(Func<ICatalogFacade, Task<T>> read)
    {
        var result = default(T)!;
        await EnvironmentAmbient.WithAsync(ConnectionEnv, async () =>
            result = await read(services.GetRequiredService<ICatalogFacade>()));
        return result;
    }

    public async Task SubscribePipeline(string id)
    {
        // Tags are read best-effort: subscribing before the entity exists has always been legal here
        // (the group simply stays silent), so a miss checks with no tags rather than becoming an error.
        await EnsureAsync(Actions.PipelineRead, id, (await ReadCatalogAsync(c => c.GetPipelineAsync(id)))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"pipeline:{EnvKeys.Qualify(ConnectionEnv, id)}");
    }

    public Task UnsubscribePipeline(string id) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"pipeline:{EnvKeys.Qualify(ConnectionEnv, id)}");

    public async Task SubscribeSource(string name)
    {
        await EnsureAsync(Actions.SourceRead, name, (await ReadCatalogAsync(c => c.GetSourceAsync(name)))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"source:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    public Task UnsubscribeSource(string name) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"source:{EnvKeys.Qualify(ConnectionEnv, name)}");

    /// <summary>The metrics group is cluster-wide and names no entity, so it asks
    /// <see cref="Actions.CatalogRead"/> at <c>*</c> — the action the wave-1 equivalence matrix already
    /// assigns to this hub and to the other platform-wide read routes (<c>/api/meta/*</c>,
    /// <c>/api/transports</c>).</summary>
    public async Task SubscribeMetrics()
    {
        await EnsureAsync(Actions.CatalogRead, "*", null);
        // Plan 021 wave 2, DELIBERATELY NOT QUALIFIED, and this is a stated limitation rather than an
        // oversight. The frames this group carries are per-pipeline throughput, so an unqualified group
        // does hand every environment's live rates to every connection holding catalog.read at *. It is
        // left cluster-wide because qualifying it correctly means the group name, BOTH flavours' bridge
        // publishers, and the payload's own PipelineId all have to move together — the payload id must
        // stay bare, since every client keys its metrics state on it — and a half-qualified metrics
        // stream is silent, not loud. The exposure is also strictly smaller than what D9 already grants:
        // any authenticated caller may address any environment through the API outright, so this leaks
        // less than the REST surface does by design. The console filters metrics to its own
        // environment's pipeline list (web/src/pages/DashboardPage.tsx); a second client would have to do
        // the same until this is qualified properly.
        await Groups.AddToGroupAsync(Context.ConnectionId, "metrics");
    }

    /// <summary>Table deltas are grouped by NAME (the delta stream's key), so whatever the caller passed
    /// has to become a name before it can name a group.
    ///
    /// <para>Plan 016 wave 1: this accepts an id or a name, like every read surface now does. It was
    /// left name-only when the REST and gRPC sites were migrated because this file belonged to no agent
    /// that wave — and the result was that gRPC's <c>SubscribeTable</c> took an id while this one did
    /// not, for the same subscription. Two transports disagreeing about what addresses an entity is the
    /// exact divergence the shared resolver exists to prevent, so it is closed here rather than
    /// recorded.</para>
    ///
    /// <para><b>The group is keyed on the RESOLVED name, never on what was passed</b> — an id-addressed
    /// subscriber has to land in the same group as a name-addressed one, or it silently receives
    /// nothing. Unresolvable input keeps its pre-existing behaviour: the entitlement is checked at the
    /// raw string and the caller joins a group nothing publishes to, which is what subscribing to a
    /// table that does not exist has always done here. An AMBIGUOUS name is refused, because there is no
    /// honest group to join — but only after the guard has run, so an unentitled caller learns
    /// nothing about which entities exist.</para></summary>
    public async Task SubscribeTable(string idOrName)
    {
        var hit = EntityRef.Resolve(await ReadCatalogAsync(c => c.GetTablesAsync()), idOrName);
        var name = hit.Value?.Name ?? idOrName;

        await EnsureAsync(Actions.TableRead, name, hit.Value?.Tags);

        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new HubException(hit.Message);
        }

        await Groups.AddToGroupAsync(Context.ConnectionId, $"table:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    public Task UnsubscribeTable(string name) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"table:{EnvKeys.Qualify(ConnectionEnv, name)}");

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
