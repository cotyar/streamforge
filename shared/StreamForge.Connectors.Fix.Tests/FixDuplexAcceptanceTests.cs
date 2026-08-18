using System.Collections.Concurrent;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>
/// Plan 019 wave E's headline acceptance test: the platform, as FIX INITIATOR, actually SENDS an
/// application message that a real QuickFIX/n ACCEPTOR receives — the send-side twin of plan 018-C's
/// <see cref="FixAcceptanceTests"/> (which proves the RECEIVE side against the same kind of fixture). Same
/// shape: a <see cref="ThreadedSocketAcceptor"/> counterparty on the first bindable port in the 7xxx band
/// (see <c>FixAcceptanceTests.StartAcceptorOnAFreePort</c>'s own doc comment for exactly why — this test
/// duplicates rather than shares that helper for the same reason <see cref="FixRowMapper"/>'s tag table is
/// duplicated: it is small, and touching a locked test file to extract a shared helper is explicitly
/// out of bounds for this wave).
///
/// <para><b>Deliberately DIFFERENT SenderCompID/TargetCompID from <see cref="FixAcceptanceTests"/></b>
/// ("DPXCLIENT"/"DPXVENUE" here vs. that file's "CLIENT"/"VENUE") — xUnit runs test classes in this project
/// in parallel by default, and QuickFIX/n's <c>Session.SendToTarget</c>/<c>Session</c> registry is keyed by
/// <see cref="SessionID"/> process-globally; two DIFFERENT tests opening a session under the SAME
/// SessionID at the same time would collide on that registry regardless of which port each one's socket
/// actually uses.</para>
/// </summary>
public class FixDuplexAcceptanceTests
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
                continue; // in use by another test run, or by something like AirPlay -- try the next one.
            }

            return (acceptor, port);
        }

        throw new InvalidOperationException("no bindable port in 7000-7999, the range CLAUDE.md reserves for test instances");
    }

    [Fact]
    public async Task TheInitiatorSendsAnOrderThatARealAcceptorReceives()
    {
        var acceptorApp = new CounterpartyApp();
        var (acceptor, port) = StartAcceptorOnAFreePort(acceptorApp);
        try
        {
            var config = FixTestSupport.ValidConfig();
            config.Host = "127.0.0.1";
            config.Port = port;
            config.SenderCompId = "DPXCLIENT";
            config.TargetCompId = "DPXVENUE";

            var transport = new FixDuplexTransport();
            var def = FixDuplexTestSupport.FixDuplexSource(config);
            def.Name = $"fx-duplex-acc-{Guid.NewGuid():N}";

            var session = transport.OpenDuplex(def);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var enumerator = session.SubscribeAsync(cts.Token).GetAsyncEnumerator(cts.Token);
            try
            {
                // The iterator body -- where the SocketInitiator actually gets constructed and started --
                // does not run until the first MoveNextAsync drives it forward (FixInboundTransport.Open's
                // own documented contract, which FixDuplexTransport.OpenDuplex/Open follow identically), so
                // the connection attempt has to be kicked off before polling for readiness.
                var pump = enumerator.MoveNextAsync().AsTask();

                await WaitForAsync(() => session.IsReady, "the duplex session never logged on");

                var rows = new List<Dictionary<string, object?>>
                {
                    new()
                    {
                        ["MsgType"] = "D",
                        ["ClOrdID"] = "ORD-ACC-1",
                        ["Symbol"] = "EUR/USD",
                        ["Side"] = "1",
                        ["OrderQty"] = 1000000L,
                        ["OrdType"] = "2",
                        ["Price"] = 1.2345,
                    },
                };

                var outcome = await session.SendAsync(rows, CancellationToken.None);

                Assert.Equal(1, outcome.Sent);
                Assert.Equal(0, outcome.Failed);
                Assert.Equal(1, session.SentTotal);
                Assert.Equal(0, session.FailedTotal);

                await WaitForAsync(() => !acceptorApp.Received.IsEmpty, "the acceptor never received the order");

                Assert.True(acceptorApp.Received.TryDequeue(out var received));
                Assert.Equal("D", received!.Header.GetString(35));
                Assert.Equal("ORD-ACC-1", received.GetString(11));
                Assert.Equal("EUR/USD", received.GetString(55));
                Assert.Equal("1", received.GetString(54));
                Assert.Equal("1000000", received.GetString(38));
                Assert.Equal("2", received.GetString(40));
                Assert.Equal("1.2345", received.GetString(44));

                cts.Cancel();
                try
                {
                    await pump;
                }
                catch (OperationCanceledException)
                {
                    // Expected: cancelling ct while the iterator is inside ReadAllAsync ends the pump task
                    // this way rather than by a clean MoveNextAsync() == false.
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
        SenderCompID=DPXVENUE
        TargetCompID=DPXCLIENT
        SocketAcceptPort={port}
        HeartBtInt=30
        ResetOnLogon=Y
        """;

    /// <summary>The fixture counterparty: notices logon (nothing to do beyond that -- the platform's
    /// SendAsync learns readiness from its OWN OnLogon, not from anything this app reports) and records
    /// every application message it receives, for the test to inspect.</summary>
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
