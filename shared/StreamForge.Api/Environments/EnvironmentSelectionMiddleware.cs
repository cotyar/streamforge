using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;

namespace StreamForge.Api;

/// <summary>
/// Plan 021 wave 1 (track C, decision D4) — the ONE writer of <see cref="EnvironmentAmbient"/> in the
/// entire codebase. Reads <c>X-StreamForge-Environment</c> (a <c>?env=</c> query parameter overrides it,
/// for a browser navigation or a <c>curl</c> that cannot set a header), validates it against
/// <see cref="IEnvironmentFacade"/>, and sets the ambient for the rest of the request. Nothing else in
/// <c>shared/</c>, <c>orleans/src/</c> or <c>dapr/src/</c> may call <see cref="EnvironmentAmbient.Set"/>.
///
/// <para><b>D2 — the default environment costs nothing.</b> When no environment is named (no header, an
/// empty header, the literal <c>"default"</c>, or nothing after <see cref="EnvKeys.Normalize"/>), this
/// middleware calls <c>next()</c> and returns — no <see cref="IEnvironmentFacade.ExistsAsync"/> call, no
/// ambient write, no round trip. <see cref="EnvironmentAmbient.Current"/> already answers
/// <see cref="EnvKeys.Default"/> when nothing has set it, so skipping the write is not an optimization
/// that changes behaviour — it is the literal absence of a per-request cost for a deployment that never
/// mentions an environment, which is the acceptance criterion the plan states in the same words.</para>
///
/// <para><b>D7 — an unknown environment is a 404, before any facade call.</b> Implicit creation on first
/// use would make <c>X-StreamForge-Environment: stagng</c> a successful deploy into a new empty
/// environment nobody meant to make. <see cref="IEnvironmentFacade.ExistsAsync"/> is asked first, and a
/// <c>false</c> answer short-circuits the pipeline — the request never reaches its endpoint, so nothing
/// downstream (a catalog write, a table create) can happen against the typo'd name.</para>
///
/// <para><b>Placement: after <c>UseAuthentication</c>/<c>UseAuthorization</c>, before the endpoint
/// runs.</b> Both run as ASP.NET Core middleware ahead of endpoint execution; a request that fails
/// authentication or authorization short-circuits to 401/403 <i>before</i> reaching this middleware. That
/// is deliberate, not incidental: this middleware's 404 tells an authenticated, authorized caller "no
/// such environment", and an anonymous caller must never be able to distinguish "environment does not
/// exist" (404) from "you are not signed in" (401) by probing <c>X-StreamForge-Environment</c> values
/// against a protected route — that distinction is exactly a list of every environment name, for free,
/// with no credentials. Putting this middleware ahead of authentication would hand out that oracle;
/// putting it here means a 404 from this middleware is only ever seen by someone who already cleared
/// authn/authz for the route they asked for.</para>
///
/// <para><b>Wave 2 addition — <see cref="HttpContextItemKey"/>, for a caller that is not inside an HTTP
/// request when it needs to know the environment.</b> A SignalR hub method runs over an already-established
/// WebSocket, not through this middleware, so <c>EnvironmentAmbient</c> (an <c>AsyncLocal</c>) is empty
/// there — reading it from <c>StreamHub</c> would silently group every subscriber under the default
/// environment. What DID go through this middleware is the HTTP request that established the connection
/// (SignalR's negotiate/connect request), and ASP.NET Core keeps that request's <see cref="HttpContext"/>
/// reachable for the life of the connection via <c>HubCallerContext.GetHttpContext()</c>. So this
/// middleware also stamps the resolved environment onto <c>HttpContext.Items[HttpContextItemKey]</c>, and
/// <c>StreamHub</c> reads it from there instead of from the ambient. It is stamped on BOTH the default
/// path and the named-environment path — a value of <see cref="EnvKeys.Default"/> and a missing key both
/// mean "no environment was selected" as far as any other reader is concerned, but only the stamped value
/// lets <c>StreamHub</c> tell "this connection is on default" apart from "this connection's HttpContext
/// never went through this middleware at all" (which would be a bug worth being loud about, not one to
/// paper over by treating a missing key as default too).</para>
/// </summary>
public static class EnvironmentSelectionMiddleware
{
    /// <summary>The header a client sets to select a non-default environment.</summary>
    public const string HeaderName = "X-StreamForge-Environment";

    /// <summary>Overrides <see cref="HeaderName"/> when present — for a browser navigation or any other
    /// caller that cannot set a header on the request that matters (a downloaded CSV link, an image src,
    /// a plain browser address-bar hit).</summary>
    public const string QueryParam = "env";

    /// <summary><c>HttpContext.Items</c> key this middleware stores the resolved environment under —
    /// see the class remarks. Public so <c>StreamHub</c> (and any future non-HTTP reader of a request's
    /// resolved environment) can read it without depending on this middleware's internals.</summary>
    public const string HttpContextItemKey = "sf.environment";

    /// <summary>Routes this middleware does not touch at all — it calls <c>next()</c> immediately,
    /// before even reading the header. <c>/healthz</c>/<c>/api/healthz</c> are anonymous liveness probes
    /// with no catalog behind them; <c>/api/meta/instance</c> is anonymous and describes THIS SERVER
    /// PROCESS (flavor, version, instance id), not a catalog, so it has no environment to select; every
    /// <c>/api/auth/*</c> route (login, logout, "me") authenticates or describes a user account, which is
    /// global and not partitioned by environment — a caller should never need to guess an environment
    /// just to log in or to read their own profile.</summary>
    private static readonly string[] ExactExclusions = ["/healthz", "/api/healthz", "/api/meta/instance"];

    /// <summary>Plan 021 wave 2 — <c>/api/environments</c> is excluded, and the reason is recovery, not
    /// convenience. Its handlers never consult the ambient (they go to <see cref="IEnvironmentFacade"/>,
    /// a fixed singleton that is the thing answering "which environments exist"), so selection has
    /// nothing to do there. But leaving it gated created a deadlock: a client still pointing at an
    /// environment somebody force-deleted got a 404 on EVERY route — including the one route it would use
    /// to discover that its selection is gone and fall back. The SPA found this the hard way; a curl user
    /// or the admin CLI would have had no way out but to unset the header by hand.</summary>
    private const string EnvironmentsPrefix = "/api/environments";

    private const string AuthPrefix = "/api/auth/";

    public static void UseEnvironmentSelection(this WebApplication app)
    {
        app.Use(async (context, next) =>
        {
            var path = context.Request.Path.Value ?? "";
            if (IsExcluded(path))
            {
                await next(context);
                return;
            }

            var headerValue = context.Request.Headers[HeaderName].ToString();
            var queryValue = context.Request.Query[QueryParam].ToString();
            var typed = string.IsNullOrEmpty(queryValue) ? headerValue : queryValue;
            var env = EnvKeys.Normalize(typed);

            if (env == EnvKeys.Default)
            {
                // D2: the untouched path. No facade call, no ambient write, no round trip — see the
                // class remarks for why this branch has to stay exactly this cheap. The Items write
                // below is a plain dictionary write (no I/O, no round trip) and is NOT the thing D2
                // protects — it is what lets StreamHub tell "this connection resolved to default"
                // apart from "this connection's HttpContext never reached this middleware".
                context.Items[HttpContextItemKey] = EnvKeys.Default;
                await next(context);
                return;
            }

            var environments = context.RequestServices.GetRequiredService<IEnvironmentFacade>();
            if (!await environments.ExistsAsync(env))
            {
                var notFound = Results.Json(
                    new ErrorResponse($"environment '{env}' does not exist"),
                    statusCode: StatusCodes.Status404NotFound);
                await notFound.ExecuteAsync(context);
                return;
            }

            EnvironmentAmbient.Set(env);
            context.Items[HttpContextItemKey] = env;
            try
            {
                await next(context);
            }
            finally
            {
                // Not required for correctness — an AsyncLocal does not leak past this request's
                // execution context — but it is cheap and it is what makes "the ambient does not leak
                // across requests on a reused thread" a fact this middleware asserts about itself
                // rather than one that merely happens to be true of AsyncLocal today.
                EnvironmentAmbient.Clear();
            }
        });
    }

    private static bool IsExcluded(string path) =>
        ExactExclusions.Contains(path, StringComparer.OrdinalIgnoreCase) ||
        path.StartsWith(AuthPrefix, StringComparison.OrdinalIgnoreCase) ||
        path.Equals(EnvironmentsPrefix, StringComparison.OrdinalIgnoreCase) ||
        path.StartsWith(EnvironmentsPrefix + "/", StringComparison.OrdinalIgnoreCase);
}
