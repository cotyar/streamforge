using Microsoft.AspNetCore.Http;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Unit tests for <see cref="DocsAuthCookie.IsDocumentationPath"/> — the predicate that decides where the
/// documentation sign-in cookie may stand in for an <c>Authorization</c> header. Its blast radius is the
/// whole point: a Scalar page cannot send a header, so the per-entity documentation paths accept a cookie
/// instead, and the value of that trade rests entirely on the set staying exactly those read-only paths.
/// Every negative case below is a route that must never become cookie-reachable, because a cross-site
/// page can make a browser send a cookie and cannot make it send a header.
/// </summary>
public class DocsAuthCookieTests
{
    [Theory]
    // The three documents themselves.
    [InlineData("/api/tables/15a6bf12075741ab8defd40fe92c2b8e/openapi.json")]
    [InlineData("/api/pipelines/bdd126709e024bb7b6d6b1100241574b/openapi.json")]
    [InlineData("/api/sources/trades/openapi.json")]
    // The Scalar pages that render them, with and without the trailing slash Scalar redirects to.
    [InlineData("/scalar/tables/15a6bf12075741ab8defd40fe92c2b8e")]
    [InlineData("/scalar/tables/15a6bf12075741ab8defd40fe92c2b8e/")]
    [InlineData("/scalar/pipelines/bdd126709e024bb7b6d6b1100241574b/")]
    [InlineData("/scalar/sources/trades/")]
    // …and the bundle each page loads beneath its own prefix.
    [InlineData("/scalar/tables/15a6bf12075741ab8defd40fe92c2b8e/scalar.js")]
    [InlineData("/scalar/sources/trades/scalar.aspnetcore.js")]
    // Routing matches paths case-insensitively, so the predicate must too, or the gate is bypassable by
    // changing the case of a segment.
    [InlineData("/API/Tables/abc/openapi.json")]
    [InlineData("/Scalar/Sources/trades/")]
    public void Recognises_the_documentation_paths(string path) =>
        Assert.True(DocsAuthCookie.IsDocumentationPath(new PathString(path)));

    [Theory]
    // Catalog reads: real data, Bearer-only.
    [InlineData("/api/tables")]
    [InlineData("/api/tables/15a6bf12075741ab8defd40fe92c2b8e")]
    [InlineData("/api/tables/15a6bf12075741ab8defd40fe92c2b8e/rows")]
    [InlineData("/api/tables/15a6bf12075741ab8defd40fe92c2b8e/proto")]
    [InlineData("/api/sources/trades")]
    [InlineData("/api/sources/trades/events")]
    [InlineData("/api/pipelines/bdd126709e024bb7b6d6b1100241574b/results")]
    [InlineData("/api/auth/me")]
    [InlineData("/api/users")]
    [InlineData("/api/chat")]
    [InlineData("/api/config/export")]
    // A deeper route that merely ends in the suffix must not qualify — the shape is fixed at four
    // segments, so a hypothetical /api/tables/{id}/history/openapi.json would not be a way in.
    [InlineData("/api/tables/15a6bf12075741ab8defd40fe92c2b8e/history/openapi.json")]
    [InlineData("/api/tables/openapi.json")]
    // An entity family that has no per-entity documentation.
    [InlineData("/api/users/admin/openapi.json")]
    // The whole-application document and reference: out of scope, and already anonymous — they must not
    // start consuming the cookie either.
    [InlineData("/openapi/v1.json")]
    [InlineData("/scalar")]
    [InlineData("/scalar/")]
    [InlineData("/scalar/scalar.js")]
    // Hubs keep their own (query-string) carve-out; the cookie is not a second door into them.
    [InlineData("/hubs/stream")]
    [InlineData("/")]
    [InlineData("")]
    public void Rejects_everything_else(string path) =>
        Assert.False(DocsAuthCookie.IsDocumentationPath(new PathString(path)));

    /// <summary>The predicate is derived from the same constant the routes are mapped with, so a rename
    /// of the suffix cannot silently leave the gate matching the old spelling.</summary>
    [Fact]
    public void Tracks_the_route_suffix_the_endpoints_are_mapped_with()
    {
        Assert.True(DocsAuthCookie.IsDocumentationPath(
            new PathString($"/api/tables/abc/{EntityOpenApiEndpoints.RouteSuffix}")));
        Assert.False(DocsAuthCookie.IsDocumentationPath(new PathString("/api/tables/abc/openapi.yaml")));
    }

    /// <summary>The cookie must be unreadable from page scripts and expire with the token it carries —
    /// httpOnly is what keeps a JWT out of reach of anything running on the documentation page.</summary>
    [Fact]
    public void Append_issues_an_httpOnly_lax_cookie_that_expires_with_the_token()
    {
        var http = new DefaultHttpContext();
        DocsAuthCookie.Append(http, "a.b.c", JwtTokenService.Lifetime);

        var header = Assert.Single(http.Response.Headers.SetCookie!)!;
        Assert.StartsWith($"{DocsAuthCookie.Name}=a.b.c;", header);
        Assert.Contains("httponly", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("samesite=lax", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("path=/", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"max-age={(int)JwtTokenService.Lifetime.TotalSeconds}", header, StringComparison.OrdinalIgnoreCase);
        // Plain HTTP hop (dev/container ports): Secure would make the cookie undeliverable there.
        Assert.DoesNotContain("secure", header, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Append_marks_the_cookie_secure_behind_a_TLS_terminating_proxy()
    {
        // Cloud Run terminates TLS and forwards plain HTTP, so IsHttps is false and the forwarded scheme
        // is the only honest signal.
        var forwarded = new DefaultHttpContext();
        forwarded.Request.Headers["X-Forwarded-Proto"] = "https";
        DocsAuthCookie.Append(forwarded, "a.b.c", JwtTokenService.Lifetime);
        Assert.Contains("secure", Assert.Single(forwarded.Response.Headers.SetCookie!)!, StringComparison.OrdinalIgnoreCase);

        var direct = new DefaultHttpContext();
        direct.Request.IsHttps = true;
        DocsAuthCookie.Append(direct, "a.b.c", JwtTokenService.Lifetime);
        Assert.Contains("secure", Assert.Single(direct.Response.Headers.SetCookie!)!, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Deleting has to repeat the issuing options or the browser keeps the original cookie.</summary>
    [Fact]
    public void Delete_expires_the_cookie_on_the_same_path()
    {
        var http = new DefaultHttpContext();
        DocsAuthCookie.Delete(http);

        var header = Assert.Single(http.Response.Headers.SetCookie!)!;
        Assert.StartsWith($"{DocsAuthCookie.Name}=;", header);
        Assert.Contains("expires=Thu, 01 Jan 1970", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("path=/", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("httponly", header, StringComparison.OrdinalIgnoreCase);
    }
}
