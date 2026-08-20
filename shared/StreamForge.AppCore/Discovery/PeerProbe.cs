using System.Net.Http.Json;
using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Discovery;

/// <summary>
/// Plan 016 wave 5: probes one configured peer's anonymous <c>GET {RestEndpoint}/api/meta/instance</c>
/// and records the outcome into <see cref="PeerDirectory"/>.
///
/// <para><b>Never throws.</b> A probe is triggered by an operator-facing route
/// (<c>POST /api/meta/peers/{name}/probe</c>) and, later, by the federated <c>grpc</c> source at each
/// reconnect (the plan's actual payoff) — neither call site should have to wrap this in a try/catch to
/// stay up. Any failure (bad address, connection refused, non-2xx, malformed body, timeout) is folded
/// into <see cref="PeerDirectory.RecordProbe"/>'s <c>error</c> parameter and the method returns
/// <c>null</c>.</para>
///
/// <para><b>Short timeout.</b> A directory listing that has to wait out a hung peer before it can answer
/// defeats the point of a cheap discovery call — <see cref="TimeoutMs"/> bounds each probe well under
/// any human-perceptible page load.</para>
///
/// <para>Reuses one static <see cref="HttpClient"/>, the pattern <c>GrpcSubscriberCore.SharedHttp</c>
/// already uses for the same reason: a fresh <see cref="HttpClient"/> per call exhausts sockets under
/// load (the classic .NET pitfall), and this type has no lifetime of its own to hang a scoped instance
/// off — it is called from static <see cref="PeerDirectory"/> call sites and, later, from a grain/actor
/// with a DI container that is not the host's.</para>
/// </summary>
public static class PeerProbe
{
    private const int TimeoutMs = 5_000;

    private static readonly HttpClient SharedHttp = new();

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>Probes <paramref name="peer"/>, records the result via
    /// <see cref="PeerDirectory.RecordProbe"/>, and returns the peer's own answer on success or
    /// <c>null</c> on any failure. <paramref name="peer"/>'s <see cref="PeerRecord.RestEndpoint"/> must be
    /// set — a peer configured with no REST endpoint at all is recorded as an error rather than attempted,
    /// since there is nothing to GET.</summary>
    public static async Task<InstanceInfo?> ProbeAsync(PeerRecord peer, CancellationToken ct = default)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (string.IsNullOrWhiteSpace(peer.RestEndpoint))
        {
            PeerDirectory.RecordProbe(peer.Name, null, "no restEndpoint configured for this peer", nowMs);
            return null;
        }

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeoutMs);

            var url = $"{peer.RestEndpoint.TrimEnd('/')}/api/meta/instance";
            using var response = await SharedHttp.GetAsync(url, cts.Token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                PeerDirectory.RecordProbe(peer.Name, null, $"HTTP {(int)response.StatusCode}", nowMs);
                return null;
            }

            var info = await response.Content.ReadFromJsonAsync<InstanceInfo>(JsonOptions, cts.Token).ConfigureAwait(false);
            if (info is null || string.IsNullOrEmpty(info.InstanceId))
            {
                PeerDirectory.RecordProbe(peer.Name, null, "response had no instanceId", nowMs);
                return null;
            }

            PeerDirectory.RecordProbe(peer.Name, info, null, nowMs);
            return info;
        }
        catch (Exception ex)
        {
            // Every failure mode this method is documented to swallow: the CancelAfter timeout (an
            // OperationCanceledException), connection failures, non-2xx already handled above, and
            // malformed JSON. Caller cancellation (ct itself firing) is swallowed too rather than
            // propagated — "must never throw" is the whole contract a probe-and-record call offers.
            PeerDirectory.RecordProbe(peer.Name, null, ex.Message, nowMs);
            return null;
        }
    }
}
