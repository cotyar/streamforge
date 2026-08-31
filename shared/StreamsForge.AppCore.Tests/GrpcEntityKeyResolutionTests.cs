using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Grpc;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>Plan 016 wave 5, track B, part 2 — <c>GrpcSubscriberCore.ResolveMessageIdentAsync</c> (made
/// <c>internal</c> for exactly this file, see the <c>InternalsVisibleTo</c> on that source file) is the
/// round trip that turns a <c>table:</c>/<c>pipeline:</c> <c>EntityKey</c> — id OR, as of this wave, a
/// display NAME — into the display name the gRPC reflection symbol is built from. Wave 1 already made
/// the remote's <c>GET /api/{tables|pipelines}/{id}</c> routes accept id-or-name and answer 409 (not 404)
/// on an ambiguous name; these tests pin that this method (a) actually resolves a name, not just an id,
/// and (b) turns a 404 and a 409 into two DISTINCT, actionable messages rather than one generic
/// "status code does not indicate success" exception, relaying the remote's own 409 body rather than
/// reinventing it.
///
/// <para>Runs against an in-process one-shot fake HTTP server (raw sockets, no ASP.NET Core dependency
/// in this test project) rather than a real StreamsForge instance - deliberately unit-level, per this
/// track's brief: the two-live-instance federation gate is the orchestrator's job, not this file's.</para>
/// </summary>
public class GrpcEntityKeyResolutionTests
{
    // ------------------------------------------------------------------
    // A single-shot fake REST server: accepts exactly one connection, discards the request, writes the
    // canned response, and stops. ResolveMessageIdentAsync makes exactly one GET per call, so one shot is
    // all any of these tests need.
    // ------------------------------------------------------------------

    private static string StartOneShotServer(int statusCode, string reasonPhrase, string bodyJson)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        _ = Task.Run(async () =>
        {
            try
            {
                using var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                using var stream = client.GetStream();

                // Discard the request line + headers (a GET has no body) - read until the blank line.
                var buffer = new byte[8192];
                var accumulated = "";
                while (!accumulated.Contains("\r\n\r\n", StringComparison.Ordinal))
                {
                    var read = await stream.ReadAsync(buffer).ConfigureAwait(false);
                    if (read == 0)
                    {
                        break;
                    }
                    accumulated += Encoding.ASCII.GetString(buffer, 0, read);
                }

                var bodyBytes = Encoding.UTF8.GetBytes(bodyJson);
                var header =
                    $"HTTP/1.1 {statusCode} {reasonPhrase}\r\n" +
                    "Content-Type: application/json\r\n" +
                    $"Content-Length: {bodyBytes.Length}\r\n" +
                    "Connection: close\r\n\r\n";
                var headerBytes = Encoding.ASCII.GetBytes(header);

                await stream.WriteAsync(headerBytes).ConfigureAwait(false);
                await stream.WriteAsync(bodyBytes).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                listener.Stop();
            }
        });

        return $"http://127.0.0.1:{port}";
    }

    // ---- source: short-circuits - no REST call at all ----------------------

    [Fact]
    public async Task SourceKind_ReturnsTheIdentUnchanged_NoRestCallMade()
    {
        // restAddress deliberately points nowhere reachable - if this path made a REST call it would
        // hang/throw; returning immediately proves it didn't.
        var name = await GrpcSubscriberCore.ResolveMessageIdentAsync(
            restAddress: "http://127.0.0.1:1", peerName: null,
            kind: "source", ident: "trades", token: null, CancellationToken.None);

        Assert.Equal("trades", name);
    }

    // ---- table:<name> resolves (not just table:<id>) -----------------------

    [Fact]
    public async Task TableKind_AuthoredAsAName_ResolvesToItsOwnName()
    {
        // The remote's GET /api/tables/{idOrName} (wave 1) answers a NAME query with that same name in
        // its "name" field - exactly what an id query would also return. This is the round trip plan 016
        // wave 5 says "costs nothing new" once wave 1 landed.
        var baseUrl = StartOneShotServer(200, "OK", """{"id":"t-abc123","name":"daily_pnl"}""");

        var name = await GrpcSubscriberCore.ResolveMessageIdentAsync(
            restAddress: baseUrl, peerName: null,
            kind: "table", ident: "daily_pnl", token: null, CancellationToken.None);

        Assert.Equal("daily_pnl", name);
    }

    [Fact]
    public async Task PipelineKind_AuthoredAsAName_ResolvesToItsOwnName()
    {
        var baseUrl = StartOneShotServer(200, "OK", """{"id":"p-999","name":"enrich_trades"}""");

        var name = await GrpcSubscriberCore.ResolveMessageIdentAsync(
            restAddress: baseUrl, peerName: null,
            kind: "pipeline", ident: "enrich_trades", token: null, CancellationToken.None);

        Assert.Equal("enrich_trades", name);
    }

    // ---- 404 and 409 produce distinct, actionable messages -----------------

    [Fact]
    public async Task A404_NamesTheKeyAndTheRemote_NotAGenericStatusCodeSentence()
    {
        var baseUrl = StartOneShotServer(404, "Not Found", "");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: baseUrl, peerName: null,
                kind: "table", ident: "ghost_table", token: null, CancellationToken.None));

        Assert.Contains("table:ghost_table", ex.Message);
        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(baseUrl, ex.Message);
        // Must NOT be EnsureSuccessStatusCode's generic wording.
        Assert.DoesNotContain("does not indicate success", ex.Message);
    }

    [Fact]
    public async Task A409_RelaysTheRemotesOwnAmbiguityMessage_RatherThanInventingOne()
    {
        // Mirrors exactly what EntityLookup.Reject emits on the remote for EntityRefOutcome.Ambiguous:
        // Results.Conflict(new ErrorResponse(hit.Message)) -> {"error":"..."}.
        const string remoteMessage = "2 tables are named 'trades' — address one by id: t-1, t-2";
        var baseUrl = StartOneShotServer(409, "Conflict", $$"""{"error":"{{remoteMessage}}"}""");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: baseUrl, peerName: null,
                kind: "table", ident: "trades", token: null, CancellationToken.None));

        Assert.Contains("table:trades", ex.Message);
        Assert.Contains(remoteMessage, ex.Message);
    }

    [Fact]
    public async Task A404AndA409_ProduceDifferentMessages()
    {
        var notFoundUrl = StartOneShotServer(404, "Not Found", "");
        var conflictUrl = StartOneShotServer(409, "Conflict", """{"error":"ambiguous"}""");

        var notFoundEx = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: notFoundUrl, peerName: null,
                kind: "table", ident: "x", token: null, CancellationToken.None));
        var conflictEx = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: conflictUrl, peerName: null,
                kind: "table", ident: "x", token: null, CancellationToken.None));

        Assert.NotEqual(notFoundEx.Message, conflictEx.Message);
    }

    // ---- peer-aware "missing REST endpoint" wording -------------------------

    [Fact]
    public async Task MissingRestAddress_WithAPeerName_NamesThePeer()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: null, peerName: "prod",
                kind: "table", ident: "daily_pnl", token: null, CancellationToken.None));

        Assert.Contains("prod", ex.Message);
        Assert.Contains("REST endpoint", ex.Message);
    }

    [Fact]
    public async Task MissingRestAddress_WithNoPeer_KeepsThePre016Wording()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            GrpcSubscriberCore.ResolveMessageIdentAsync(
                restAddress: null, peerName: null,
                kind: "table", ident: "daily_pnl", token: null, CancellationToken.None));

        Assert.Contains("needs RestAddress set", ex.Message);
    }
}
