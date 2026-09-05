using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D1/G2, live: <b>the Dapr host actually SERVES gRPC</b>, on this project's 5899 — the fact
/// AGENTS.md's port table used to deny ("gRPC reserved 5499, not yet served — phase 2") and
/// <c>dapr/PARITY.md</c> D1 carried as the flavor's largest owed item.
///
/// <para><b>Why the probe is grpcurl and not a generated client.</b> The question this class asks is the
/// one a federated peer and an operator both ask: <i>holding nothing but the address, what can an
/// outside tool discover and successfully call here?</i> A client generated from this repo's own
/// <c>Protos/streamsforge.proto</c> already knows every answer, so it would still pass against a host
/// whose reflection service was broken or absent — and reflection is precisely what D1 moved into
/// <c>shared/StreamsForge.Api/Grpc/</c> and had to make work on a second runtime. See
/// <see cref="Grpcurl"/> for the mechanics, including why a streaming call's exit code is not a
/// verdict.</para>
///
/// <para><b>Six service names, not seven, and the difference is not a bug.</b>
/// <c>StreamsForgeGrpc.StaticServiceNames</c> lists SEVEN entries because it also names
/// <c>ServerReflection</c> — the hand-rolled <c>DynamicReflectionService</c> that answers this very
/// query. A reflection client never sees the reflection service in its own <c>list</c> output (it is not
/// in the descriptor set it serves), so six is the correct expectation from outside and seven the
/// correct count from inside. The names are asserted individually rather than as a count, so a host that
/// mapped five and something unrelated could not pass.</para>
///
/// <para><b>"At least one delta" is the right assertion for the streaming fact, and the only one
/// available.</b> Everywhere else in this project an exact count is demanded, because loss is the thing
/// being ruled out. Here the subject is a LIVE subscription to a continuously-updating seeded table:
/// there is no total to be exact about, the deltas are produced by generators that never stop, and any
/// number would be a description of how fast this machine happened to be. What is being proven is that
/// the RPC is reachable, authenticated, and actually pushes server-streamed messages — one message
/// proves all three and a thousand prove nothing more.</para>
///
/// <para><b>TLS is covered next door.</b> <c>TlsHostTests</c> owns the gRPC-over-TLS fact
/// (<c>grpcurl -cacert</c>), because every class here shares one app-id and one port pair, so a TLS
/// assertion in this class would mean a second full boot to re-ask a question that fixture's host can
/// already answer. The two public members below are what it borrows.</para>
///
/// <para><b>Not covered:</b> the unary control-plane RPCs (<c>SourceService</c>/<c>PipelineService</c>/
/// <c>TableService</c>) beyond their presence in the reflection listing — they are the gRPC face of REST
/// handlers this project already exercises over REST, and D1's risk was never in them but in reflection
/// and the streaming primitive. Also not covered: the per-entity DYNAMIC descriptors
/// (<c>DynamicStreamService</c> encodes rows against descriptors generated for the current catalog);
/// proving those needs a generated client per entity, which is what <c>/sf-client-gen</c> and the
/// Orleans-side proto tests already do against the shared implementation.</para>
/// </summary>
public sealed class GrpcServingFixture : LiveHostFixture
{
    protected override string Label => "grpc-serving";

    protected override string? ExtraPreflight() => Grpcurl.Preflight();
}

[Collection(DaprLiveTestCollection.Name)]
public sealed class GrpcServingTests(GrpcServingFixture fixture) : IClassFixture<GrpcServingFixture>
{
    /// <summary>The service names a reflection client sees. Shared with <c>TlsHostTests</c>, which
    /// asserts the identical list over TLS.</summary>
    public static readonly string[] ExpectedServices =
    [
        "streamsforge.v1.SourceService",
        "streamsforge.v1.PipelineService",
        "streamsforge.v1.TableService",
        "streamsforge.v1.StreamService",
        "streamsforge.v1.IngestService",
        "streamsforge.dynamic.v1.DynamicStreamService",
    ];

    private DaprHostProcess? _host => fixture.Host;
    private string? _skipReason => fixture.SkipReason;

    [Fact]
    public async Task Reflection_lists_the_six_services_an_outside_client_can_see()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        var list = await Grpcurl.RunAsync(
            TimeSpan.FromSeconds(30), "-plaintext", $"127.0.0.1:{DaprHostProcess.GrpcPort}", "list");
        Assert.True(
            list.ExitCode == 0,
            $"grpcurl -plaintext 127.0.0.1:{DaprHostProcess.GrpcPort} list failed\n{list.Combined}\n"
          + $"--- host log tail ---\n{host.LogTailText()}");

        foreach (var service in ExpectedServices)
        {
            Assert.Contains(service, list.Stdout, StringComparison.Ordinal);
        }

        var listed = list.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Equal(ExpectedServices.Length, listed.Length);
    }

    [Fact]
    public async Task SubscribeTable_streams_at_least_one_delta_for_a_seeded_table()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        // Unauthenticated first: the gRPC surface carries the same auth as REST, and a streaming endpoint
        // that would hand a live table's rows to anyone is a hole worth a dedicated assertion rather
        // than an implied one.
        var anonymous = await Grpcurl.RunAsync(
            TimeSpan.FromSeconds(30),
            "-plaintext", "-max-time", "10",
            "-d", "{\"name\":\"positions\"}",
            $"127.0.0.1:{DaprHostProcess.GrpcPort}",
            "streamsforge.v1.StreamService/SubscribeTable");
        Assert.True(
            anonymous.ExitCode != 0,
            $"an UNAUTHENTICATED SubscribeTable succeeded — the gRPC port is not enforcing auth\n{anonymous.Combined}");
        Assert.DoesNotContain("\"table_name\"", anonymous.Stdout, StringComparison.Ordinal);

        var token = await BearerTokenAsync(host);
        var stream = await Grpcurl.RunAsync(
            TimeSpan.FromSeconds(45),
            "-plaintext",
            "-H", $"authorization: Bearer {token}",
            "-max-time", "30",
            "-d", "{\"name\":\"positions\"}",
            $"127.0.0.1:{DaprHostProcess.GrpcPort}",
            "streamsforge.v1.StreamService/SubscribeTable");

        // Exit code is NOT the verdict here — a live subscription ended by -max-time exits non-zero with
        // DeadlineExceeded after printing everything it received. See Grpcurl's class doc.
        Assert.Contains(
            "\"table_name\": \"positions\"",
            stream.Stdout,
            StringComparison.Ordinal);
        Assert.Contains("\"deltas\"", stream.Stdout, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Meta_instance_advertises_the_grpc_endpoint_and_capability()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        // Anonymous on purpose: /api/meta/instance is what a PEER reads before it holds any credential,
        // so a version of this check behind a login would not be testing the discovery path at all.
        using var anon = host.NewClient();
        using var doc = JsonDocument.Parse(await anon.GetStringAsync($"{host.BaseUrl}/api/meta/instance"));

        Assert.Equal("dapr", doc.RootElement.GetProperty("flavor").GetString());
        Assert.Equal(
            $"http://127.0.0.1:{DaprHostProcess.GrpcPort}",
            doc.RootElement.GetProperty("endpoints").GetProperty("grpc").GetString());
        Assert.Contains(
            "grpc",
            doc.RootElement.GetProperty("capabilities").EnumerateArray().Select(c => c.GetString()));
    }

    /// <summary>The raw JWT, for tools that need the token itself rather than a configured
    /// <see cref="HttpClient"/> (grpcurl's <c>-H authorization: Bearer …</c>).
    /// <see cref="DaprHostProcess.LoginAsync"/> hands back a client with the header already set and no
    /// way to read the value out, which is right for every REST caller and useless here. Shared with
    /// <c>TlsHostTests</c>; it goes through <see cref="DaprHostProcess.NewClient"/> so it works
    /// unchanged against a TLS instance.</summary>
    public static async Task<string> BearerTokenAsync(DaprHostProcess host)
    {
        using var http = host.NewClient();
        var resp = await http.PostAsJsonAsync(
            $"{host.BaseUrl}/api/auth/login",
            new { username = DaprHostProcess.AdminUser, password = DaprHostProcess.AdminPassword });
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("token").GetString()!;
    }
}
