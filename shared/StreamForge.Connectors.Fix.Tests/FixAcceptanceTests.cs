using System.Text;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Store;
using StreamForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>
/// The acceptance test plan 018-C calls for: a QuickFIX/n <see cref="ThreadedSocketAcceptor"/> as the
/// counterparty, on a 7xxx port — no external venue, no Docker, no recorded capture (plan 018's own
/// feasibility probe verified this shape before the wave was written: an acceptor and an initiator log on
/// to each other in one process). It logs on, the transport's subscription yields the <c>35=W</c> the
/// acceptor sends, <see cref="FixParser.Parse"/> on that payload produces the expected row, and a
/// mid-test acceptor stop is survived — polling for logon with a timeout rather than a fixed sleep, and
/// always tearing the acceptor down in a <c>finally</c>.
/// </summary>
public class FixAcceptanceTests
{
    /// <summary>Starts the counterparty acceptor on the first port in the 7xxx band it can actually bind,
    /// replacing the fixed 7891 this test shipped with. Two reasons, both real:
    ///
    /// <para><b>The same test project is in BOTH solutions</b> (<c>orleans/StreamForge.sln</c> and
    /// <c>dapr/StreamForge.Dapr.sln</c>), so running the two suites back to back binds this acceptor twice
    /// within minutes — a fixed port makes those two runs contend for one socket for no reason.</para>
    ///
    /// <para><b>A port can be held by something that is not a test at all.</b> On macOS, ControlCenter's
    /// AirPlay Receiver listens on <c>*:7000</c> — so "pick a port in the reserved band" has to mean "one
    /// that binds", not "one that looks free". Note this loop tries the REAL acceptor rather than probing
    /// with a throwaway <see cref="System.Net.Sockets.TcpListener"/> first: a probe binds what the probe
    /// chose (loopback) while QuickFIX/n binds <c>Any</c>, and a probe that succeeds where the acceptor
    /// then fails is worse than no probe. <see cref="ThreadedSocketAcceptor.Start"/> throws
    /// <see cref="System.Net.Sockets.SocketException"/> on a failed bind — it does NOT swallow it into its
    /// logger — which is what makes retrying on exactly that exception correct here.</para></summary>
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
                continue; // in use by another test run, or by something like AirPlay — try the next one.
            }

            return (acceptor, port);
        }

        throw new InvalidOperationException("no bindable port in 7000-7999, the range CLAUDE.md reserves for test instances");
    }

    [Fact]
    public async Task ARealAcceptorLogsOnAndTheYieldedMessageParsesToTheExpectedRow()
    {
        var acceptorApp = new CounterpartyApp();
        var (acceptor, port) = StartAcceptorOnAFreePort(acceptorApp);
        try
        {
            var config = FixTestSupport.ValidConfig();
            config.Host = "127.0.0.1";
            config.Port = port;
            config.SenderCompId = "CLIENT";
            config.TargetCompId = "VENUE";

            var transport = new FixInboundTransport();
            var subscription = transport.Open(FixTestSupport.FixSource(config));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var enumerator = subscription.SubscribeAsync(cts.Token).GetAsyncEnumerator(cts.Token);
            try
            {
                // The iterator body — which is where the SocketInitiator actually gets constructed and
                // started — does not run at all until the FIRST MoveNextAsync call drives it forward, so
                // the connection attempt has to be kicked off before polling for logon, not after.
                var firstMessage = enumerator.MoveNextAsync().AsTask();

                await WaitForAsync(() => acceptorApp.LoggedOn, "the acceptor never saw the initiator log on");

                acceptorApp.SendRaw("35=W|262=REQ1|55=EUR/USD|268=1|269=0|270=1.2345|271=1000000");

                Assert.True(await firstMessage, "the subscription ended before yielding a message");
                var msg = enumerator.Current;

                Assert.Equal("W", msg.Subject);
                Assert.Null(msg.AckAsync); // plan 018-C: QuickFIX/n's sequence layer is not "the platform processed this row".

                var rows = FixParser.Parse(Encoding.UTF8.GetString(msg.Payload));
                var row = Assert.Single(rows);
                Assert.Equal("EUR/USD", row.GetProperty("Symbol").GetString());
                Assert.Equal("W", row.GetProperty("MsgType").GetString());

                // Mid-test disconnect: stopping the acceptor must end the subscription (via OnLogout
                // completing the bridge channel) rather than hanging the test or crashing the process.
                acceptor.Stop();

                var deadline = DateTime.UtcNow.AddSeconds(15);
                var ended = false;
                while (DateTime.UtcNow < deadline)
                {
                    if (!await enumerator.MoveNextAsync())
                    {
                        ended = true;
                        break;
                    }
                }

                Assert.True(ended, "the subscription did not end after the acceptor stopped");
            }
            finally
            {
                await enumerator.DisposeAsync();
                await subscription.DisposeAsync();
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
        SenderCompID=VENUE
        TargetCompID=CLIENT
        SocketAcceptPort={port}
        HeartBtInt=30
        ResetOnLogon=Y
        """;

    /// <summary>The fixture counterparty — everything a real venue's session layer would do, minus
    /// anything but what this one test needs: notice logon, and send one raw message on request.</summary>
    private sealed class CounterpartyApp : IApplication
    {
        public volatile bool LoggedOn;
        private SessionID? _sessionId;

        public void SendRaw(string pipeDelimited)
        {
            if (_sessionId is null)
            {
                throw new InvalidOperationException("no session logged on yet");
            }

            const char soh = '\x01';
            var wire = pipeDelimited.Replace('|', soh) + soh;
            Session.SendToTarget(new Message(wire, false), _sessionId);
        }

        public void ToAdmin(Message message, SessionID sessionID)
        {
        }

        public void FromAdmin(Message message, SessionID sessionID)
        {
        }

        public void ToApp(Message message, SessionID sessionID)
        {
        }

        public void FromApp(Message message, SessionID sessionID)
        {
        }

        public void OnCreate(SessionID sessionID)
        {
        }

        public void OnLogon(SessionID sessionID)
        {
            _sessionId = sessionID;
            LoggedOn = true;
        }

        public void OnLogout(SessionID sessionID)
        {
        }
    }
}
