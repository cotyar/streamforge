using Microsoft.AspNetCore.Http;

namespace StreamsForge.Api.Auth;

/// <summary>
/// The httpOnly, same-origin cookie that lets the per-entity <b>documentation</b> pages
/// (<c>/scalar/tables/{id}</c> and friends) and the documents they render
/// (<c>/api/tables/{id}/openapi.json</c> and friends) be gated behind the <c>Viewer</c> policy
/// while still rendering.
///
/// <para><b>Why a cookie at all.</b> Scalar's page fetches its OpenAPI document from the browser with a
/// plain <c>fetch(url)</c> — verified in Scalar 2.16's own bundle, whose document loader is
/// <c>e.fetch ? e.fetch : ((t, n) =&gt; fetch(yx(e.proxyUrl, t.toString()), n))</c> with no
/// <c>credentials</c> override — and offers no hook for an <c>Authorization</c> header. A plain
/// <c>fetch</c> defaults to <c>credentials: "same-origin"</c>, so a same-origin cookie <em>is</em> sent,
/// where a Bearer header can never be. Top-level navigation to the page itself sends it too. This is the
/// same class of carve-out the SignalR hubs already need (they cannot send headers either, and read the
/// token from <c>?access_token=</c>) — but a cookie, not a query parameter, because a documentation URL
/// is the kind of thing people paste into a chat window and a JWT must not ride along.</para>
///
/// <para><b>Why it is not a general authentication mechanism.</b> <see cref="IsDocumentationPath"/>
/// restricts where the JWT wiring will even look at this cookie: the two read-only documentation route
/// families and nothing else. Every mutating endpoint — indeed every endpoint that serves data rather
/// than a description of the shape of data — remains reachable only with an <c>Authorization: Bearer</c>
/// header, which a cross-site form or image tag cannot forge. That is what keeps CSRF off the table
/// without a token-pair dance: there is nothing a cookie-only request can do.</para>
/// </summary>
public static class DocsAuthCookie
{
    /// <summary>Cookie name. <c>__Host-</c> is deliberately avoided: that prefix demands
    /// <c>Secure</c>, which would make the cookie unusable on the plain-HTTP dev and container ports.</summary>
    public const string Name = "sf_docs";

    /// <summary>Entity route segments that have per-entity documentation. Mirrors the three routes in
    /// <see cref="EntityOpenApiEndpoints"/> and the three Scalar prefixes mapped alongside them.</summary>
    private static readonly string[] Segments = ["tables", "pipelines", "sources"];

    /// <summary>
    /// True for exactly the read-only documentation paths this cookie may authenticate:
    /// <c>/scalar/{segment}/{key}</c> (and the Scalar bundle served beneath it) and
    /// <c>/api/{segment}/{key}/openapi.json</c>. Anything else — every catalog read, every mutation —
    /// is false, and so is unreachable with the cookie alone.
    /// </summary>
    public static bool IsDocumentationPath(PathString path)
    {
        if (!path.HasValue)
        {
            return false;
        }

        var parts = path.Value!.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // /scalar/{segment}/{key}[/scalar.js|/scalar.aspnetcore.js|…] — the page and its own assets.
        if (parts.Length >= 3 &&
            parts[0].Equals("scalar", StringComparison.OrdinalIgnoreCase) &&
            IsSegment(parts[1]))
        {
            return true;
        }

        // /api/{segment}/{key}/openapi.json — the document the page fetches.
        return parts.Length == 4 &&
               parts[0].Equals("api", StringComparison.OrdinalIgnoreCase) &&
               IsSegment(parts[1]) &&
               parts[3].Equals(EntityOpenApiEndpoints.RouteSuffix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Issues the cookie alongside a freshly minted JWT, with the same lifetime as the token
    /// inside it, so the two expire together and a stale cookie cannot outlive its own validity.</summary>
    public static void Append(HttpContext http, string token, TimeSpan lifetime) =>
        http.Response.Cookies.Append(Name, token, Options(http, lifetime));

    /// <summary>Clears the cookie. Options must match <see cref="Append"/> or the browser keeps it.</summary>
    public static void Delete(HttpContext http) =>
        http.Response.Cookies.Delete(Name, Options(http, lifetime: null));

    private static CookieOptions Options(HttpContext http, TimeSpan? lifetime) => new()
    {
        // The page's own scripts must not be able to read the token — the whole point of not putting it
        // in the URL or in localStorage for this purpose.
        HttpOnly = true,

        // Secure whenever the hop is actually TLS. Cloud Run terminates TLS at the front end and forwards
        // plain HTTP, so Request.IsHttps is false there; the forwarded scheme is what tells the truth, and
        // is read directly rather than via ForwardedHeaders middleware, which this app does not install.
        Secure = http.Request.IsHttps ||
                 string.Equals(http.Request.Headers["X-Forwarded-Proto"], "https", StringComparison.OrdinalIgnoreCase),

        // Lax, not Strict: a documentation link shared in a chat window should still open for someone
        // already signed in, and a top-level GET navigation is all that needs to carry it. Lax already
        // withholds the cookie from cross-site POSTs and subresource loads — and even if it did not,
        // IsDocumentationPath means no mutating endpoint would accept it.
        SameSite = SameSiteMode.Lax,

        // Both documentation families live under different prefixes (/scalar and /api), so the cookie is
        // scoped to the origin and narrowed by IsDocumentationPath instead of by Path.
        Path = "/",

        // Authentication, not analytics: not subject to consent-based cookie suppression.
        IsEssential = true,

        MaxAge = lifetime,
    };

    private static bool IsSegment(string part) =>
        Segments.Contains(part, StringComparer.OrdinalIgnoreCase);
}
