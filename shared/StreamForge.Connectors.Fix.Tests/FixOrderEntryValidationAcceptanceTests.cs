using System.Collections.Concurrent;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>
/// Plan 019 wave F (D6): the required-field gate proven against a REAL QuickFIX/n acceptor, not just the
/// pure <see cref="FixRequiredFieldsTests"/> — a row missing a required tag must never reach the wire,
/// and a valid one still must. Same shape as <see cref="FixDuplexAcceptanceTests"/> (wave E's own
/// send-side acceptance test), duplicated rather than shared for the same reason that file gives for
/// duplicating <see cref="FixAcceptanceTests"/>'s helper: small, and touching a locked test file to
/// extract a shared helper is out of bounds.
///
/// <para><b>Yet another distinct SenderCompID/TargetCompID pair</b> ("OEVCLIENT"/"OEVVENUE") — see
/// <see cref="FixDuplexAcceptanceTests"/>'s own doc comment for why: QuickFIX/n's <see cref="Session"/>
/// registry is keyed by <see cref="SessionID"/> process-globally, and xUnit runs test classes in this
/// project in parallel by default.</para>
/// </summary>
public class FixOrderEntryValidationAcceptanceTests
{
    private static (ThreadedSocketAcceptor Acceptor, int Port) StartAcceptorOnAFreePort(CounterpartyApp app)
    {
        for (var port = 7000; port < 8000; port++)
        {
            var settings = new SessionSettings(new StringReader(AcceptorSettingsText(port)));
            var acceptor = new ThreadedSocketAcceptor(app, new MemoryStoreFactory(), settings, new NullLogFactory());
            try
            {
                acceptor.Start();
            }
            catch (System.Net.Sockets.SocketException)
            {
                acceptor.Dispose();
                continue;
            }

            return (acceptor, port);
        }

        throw new InvalidOperationException("no bindable port in 7000-7999, the range CLAUDE.md reserves for test instances");
    }

    [Fact]
    public async Task ARowMissingARequiredFieldIsRefusedAndNeverReachesTheAcceptor()
    {
        var acceptorApp = new CounterpartyApp();
        var (acceptor, port) = StartAcceptorOnAFreePort(acceptorApp);
        try
        {
            var config = FixTestSupport.ValidConfig();
            config.Host = "127.0.0.1";
            config.Port = port;
            config.SenderCompId = "OEVCLIENT";
            config.TargetCompId = "OEVVENUE";

            var transport = new FixDuplexTransport();
            var def = FixDuplexTestSupport.FixDuplexSource(config);
            def.Name = $"fx-oev-{Guid.NewGuid():N}";

            var session = transport.OpenDuplex(def);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var enumerator = session.SubscribeAsync(cts.Token).GetAsyncEnumerator(cts.Token);
            try
            {
                var pump = enumerator.MoveNextAsync().AsTask();
                await WaitForAsync(() => session.IsReady, "the duplex session never logged on");

                // Missing Side/OrdType/OrderQty -- refused by FixRequiredFields before ever touching
                // Session.SendToTarget.
                var incomplete = new List<Dictionary<string, object?>>
                {
                    new() { ["MsgType"] = "D", ["ClOrdID"] = "OEV-BAD-1", ["Symbol"] = "EUR/USD" },
                };

                var badOutcome = await session.SendAsync(incomplete, CancellationToken.None);

                Assert.Equal(0, badOutcome.Sent);
                Assert.Equal(1, badOutcome.Failed);
                var failure = Assert.Single(badOutcome.Failures);
                Assert.Equal("OEV-BAD-1", failure.CorrelationId);
                Assert.Contains("Side", failure.Reason);
                Assert.DoesNotContain("not logged on", failure.Reason, StringComparison.OrdinalIgnoreCase);

                // A valid row, right after, on the SAME session -- proves the gate is per-row, not a
                // session-wide latch, and that a complete NewOrderSingle still goes out.
                var complete = new List<Dictionary<string, object?>>
                {
                    new()
                    {
                        ["MsgType"] = "D",
                        ["ClOrdID"] = "OEV-GOOD-1",
                        ["Symbol"] = "EUR/USD",
                        ["Side"] = "1",
                        ["OrdType"] = "2",
                        ["OrderQty"] = 1000000L,
                    },
                };

                var goodOutcome = await session.SendAsync(complete, CancellationToken.None);
                Assert.Equal(1, goodOutcome.Sent);
                Assert.Equal(0, goodOutcome.Failed);

                await WaitForAsync(() => !acceptorApp.Received.IsEmpty, "the acceptor never received the valid order");
                Assert.True(acceptorApp.Received.TryDequeue(out var received));
                Assert.Equal("OEV-GOOD-1", received!.GetString(11));

                // The incomplete row's ClOrdID must NEVER have reached the acceptor.
                Assert.True(acceptorApp.Received.IsEmpty);

                cts.Cancel();
                try
                {
                    await pump;
                }
                catch (OperationCanceledException)
                {
                }
            }
            finally
            {
                await enumerator.DisposeAsync();
                await session.DisposeAsync();
            }
        }
        finally
        {
            acceptor.Stop();
        }
    }

    private static async Task WaitForAsync(Func<bool> condition, string timeoutMessage)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.Fail(timeoutMessage);
    }

    private static string AcceptorSettingsText(int port) => $"""
        [DEFAULT]
        ConnectionType=acceptor
        StartTime=00:00:00
        EndTime=00:00:00
        UseDataDictionary=N

        [SESSION]
        BeginString=FIX.4.4
        SenderCompID=OEVVENUE
        TargetCompID=OEVCLIENT
        SocketAcceptPort={port}
        HeartBtInt=30
        ResetOnLogon=Y
        """;

    private sealed class CounterpartyApp : IApplication
    {
        public readonly ConcurrentQueue<Message> Received = new();

        public void ToAdmin(Message message, SessionID sessionID)
        {
        }

        public void FromAdmin(Message message, SessionID sessionID)
        {
        }

        public void ToApp(Message message, SessionID sessionID)
        {
        }

        public void FromApp(Message message, SessionID sessionID) => Received.Enqueue(message);

        public void OnCreate(SessionID sessionID)
        {
        }

        public void OnLogon(SessionID sessionID)
        {
        }

        public void OnLogout(SessionID sessionID)
        {
        }
    }
}
