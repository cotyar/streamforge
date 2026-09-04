using System.Text.Json;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// The OTHER deployment shape for TLS: a proxy terminates it and the host stays cleartext behind it.
/// The host must then believe the proxy about the scheme — otherwise every self-reported URL
/// (<c>GET /api/meta/instance</c>'s <c>endpoints.rest</c>/<c>endpoints.grpc</c>, which is what a peer
/// federates against) says <c>http://</c> for an instance the world reaches over <c>https://</c>.
///
/// <para><b>No middleware was written for this.</b> ASP.NET Core already ships it:
/// <c>ASPNETCORE_FORWARDEDHEADERS_ENABLED=true</c> makes <c>ConfigureWebDefaults</c> register a startup
/// filter that runs <c>UseForwardedHeaders</c> at the very front of the pipeline with
/// <c>KnownNetworks</c>/<c>KnownProxies</c> cleared (i.e. trusting whatever proxy is in front). These
/// tests are the verification that it is actually reachable in THIS host — a minimal-hosting
/// <c>WebApplication</c> with a hand-configured Kestrel is exactly the shape where a "the framework
/// handles it" assumption quietly turns out to be false.</para>
///
/// <para><b>The control fact is the important one.</b> Trusting <c>X-Forwarded-Proto</c> from anybody
/// who sends it is fine behind a proxy and a lie-amplifier when directly exposed, so the default must
/// be to IGNORE the header. A host booted WITHOUT the flag is asserted to keep reporting
/// <c>http://</c> no matter what the request claims.</para>
///
/// <para>Ports: enabled host 6799 / 6899, silo 16799 / 36799; control host 7199 / 7299, silo
/// 17199 / 37199. Separate sets so the two facts do not depend on xunit's ordering.</para>
/// </summary>
[Collection(ChainHostCollection.Name)]
public sealed class ForwardedHeadersTests
{
    [Fact]
    public async Task With_the_flag_set_X_Forwarded_Proto_decides_the_reported_scheme()
    {
        var skip = HostProcess.Preflight(6799, 6899, 16799, 36799);
        Assert.True(skip is null, $"skipped: {skip}");

        await using var host = new HostProcess("fwd-on", 6799, 6899, 16799, 36799);
        host.EnvVars["ASPNETCORE_FORWARDEDHEADERS_ENABLED"] = "true";
        host.Start();
        await host.WaitHealthyAsync();

        Assert.Equal("https://", await SchemeReportedAsync(host, forwardedProto: "https"));

        // Same host, same request, no header: nothing to believe, so the real (cleartext) scheme stands.
        Assert.Equal("http://", await SchemeReportedAsync(host, forwardedProto: null));
    }

    [Fact]
    public async Task Without_the_flag_the_header_is_ignored_so_the_default_is_fail_closed()
    {
        var skip = HostProcess.Preflight(7199, 7299, 17199, 37199);
        Assert.True(skip is null, $"skipped: {skip}");

        await using var host = new HostProcess("fwd-off", 7199, 7299, 17199, 37199);
        host.Start();
        await host.WaitHealthyAsync();

        Assert.Equal("http://", await SchemeReportedAsync(host, forwardedProto: "https"));
    }

    /// <summary>The scheme prefix of <c>endpoints.rest</c> from <c>GET /api/meta/instance</c> — the
    /// value <c>MetaEndpoints</c> builds straight out of <c>request.Scheme</c>, which is exactly what
    /// the forwarded-headers middleware rewrites.</summary>
    private static async Task<string> SchemeReportedAsync(HostProcess host, string? forwardedProto)
    {
        using var client = host.NewClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "api/meta/instance");
        if (forwardedProto is not null)
        {
            request.Headers.TryAddWithoutValidation("X-Forwarded-Proto", forwardedProto);
        }

        var resp = await client.SendAsync(request);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"GET api/meta/instance -> {(int)resp.StatusCode}: {text}");

        using var doc = JsonDocument.Parse(text);
        var rest = doc.RootElement.GetProperty("endpoints").GetProperty("rest").GetString()!;
        // grpc is built from the same request.Scheme, so a disagreement between the two would mean the
        // rewrite reached one code path and not the other.
        var grpc = doc.RootElement.GetProperty("endpoints").GetProperty("grpc").GetString()!;
        Assert.True(
            rest.Split("//")[0] == grpc.Split("//")[0],
            $"endpoints.rest ({rest}) and endpoints.grpc ({grpc}) disagree about the scheme");

        return rest.StartsWith("https://", StringComparison.Ordinal) ? "https://" : "http://";
    }
}
