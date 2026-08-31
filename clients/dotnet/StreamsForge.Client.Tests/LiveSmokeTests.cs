using Xunit;
using Xunit.Abstractions;

namespace StreamsForge.Client.Tests;

/// <summary>
/// Read-only smoke test against the demo already running at http://localhost:6199
/// (admin/admin123!). That instance was started with <c>--urls</c>, so Program.cs's guard binds
/// no gRPC port at all -- it has REST + SignalR only, which makes it the natural real-world check
/// for <see cref="TransportKind.Auto"/>'s fallback path: gRPC's connect probe genuinely fails here
/// (not a mock), and the client should fall back to SignalR and say so.
///
/// STRICTLY READ-ONLY: this test snapshots and subscribes only. It never restarts, reconfigures,
/// mutates, or kills the demo -- and never pushes to it, since a push is the one operation with a
/// side effect on shared, already-running infrastructure.
/// </summary>
public sealed class LiveSmokeTests
{
    private const string DemoUrl = "http://localhost:6199";
    private const string DemoUser = "admin";
    private const string DemoPassword = "admin123!";

    private readonly ITestOutputHelper _output;

    public LiveSmokeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task AutoFallsBackToSignalRAndSeqAdvancesOnTriggerMonitor()
    {
        await using var client = await StreamsForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = DemoUrl,
            User = DemoUser,
            Password = DemoPassword,
            Transport = TransportKind.Auto, // the demo has no gRPC port bound -- must fall back
        });

        // The demo was started with --urls, so gRPC genuinely is not there: Auto must have fallen
        // back to a SignalR wire mode, never silently claimed gRPC.
        Assert.StartsWith("signalr:", client.TransportName);
        _output.WriteLine($"connected via {client.TransportName}");

        var snapshot = await client.SnapshotAsync("trigger_monitor", limit: 500);
        _output.WriteLine($"snapshot: {snapshot.Count} rows");

        await using var table = await client.TableAsync("trigger_monitor", ["counterparty_id", "agreement_id"], TimeSpan.FromSeconds(20));
        var initialSeq = table.Seq;
        _output.WriteLine($"subscribed; initial seq={initialSeq}, rows={table.Rows.Count}");

        // Watch seq advance for a bit -- the demo's generators push continuously, so on a healthy
        // connection this table's seq should move within a few seconds. Not asserted as a hard
        // requirement (a quiet window is possible), only reported -- the hard assertions are that
        // the subscription came up, stayed up, and the transport really is SignalR.
        var advanced = false;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            if (table.Seq != initialSeq) { advanced = true; break; }
            await Task.Delay(250);
        }
        _output.WriteLine($"seq after wait: {table.Seq} (advanced={advanced}, reconnects={table.Reconnects})");

        Assert.True(table.Ready);
        Assert.Equal(0, table.Reconnects); // a clean run never needed to reconnect
    }

    [Fact]
    public async Task TableAsyncWithNoKeyFieldsStillConnectsAgainstAnEngineThatPredatesWishlist18()
    {
        // The demo at :6199 is an older build with no `keyFields` on GET /api/tables at all (the
        // absent-field case wishlist #18's compatibility rule is about, not an explicit null) --
        // this exercises StreamsForgeClient.ResolveKeyFieldsAsync's fallback path for real: it must
        // read the missing property as "unknown", resolve to null (whole-row identity, exactly
        // this client's pre-#18 default for a table it had no map entry for), and connect cleanly
        // rather than throwing on a property that isn't there.
        await using var client = await StreamsForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = DemoUrl,
            User = DemoUser,
            Password = DemoPassword,
            Transport = TransportKind.Auto,
        });

        await using var table = await client.TableAsync("trigger_monitor", timeout: TimeSpan.FromSeconds(20));
        Assert.True(table.Ready);
        _output.WriteLine($"connected with no keyFields override; rows={table.Rows.Count}, seq={table.Seq}");
    }
}
