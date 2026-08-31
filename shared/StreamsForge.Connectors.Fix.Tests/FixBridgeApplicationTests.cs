using System.Text;
using System.Threading.Channels;
using Xunit;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>
/// The MsgTypes filter and the bounded drop-oldest+counter bridge — plan 018-C's "fake seam (no socket)":
/// <see cref="FixBridgeApplication"/> is driven directly with hand-built <c>QuickFix.Message</c> objects
/// (see <see cref="FixTestSupport.BuildMessage"/>), never a real <c>SocketInitiator</c>/acceptor pair. Only
/// <c>FixAcceptanceTests</c> opens a socket.
/// </summary>
public class FixBridgeApplicationTests
{
    private static Channel<FixInboundMessage> BoundedDropOldest(int capacity) =>
        Channel.CreateBounded<FixInboundMessage>(new BoundedChannelOptions(capacity) { FullMode = BoundedChannelFullMode.DropOldest });

    private static List<string> DrainMsgTypes(Channel<FixInboundMessage> channel)
    {
        var result = new List<string>();
        while (channel.Reader.TryRead(out var msg))
        {
            result.Add(msg.MsgType);
        }

        return result;
    }

    // ------------------------------------------------------------------
    // MsgTypes filter
    // ------------------------------------------------------------------

    [Fact]
    public void FromAppAcceptsEveryMsgTypeWhenNoFilterIsConfigured()
    {
        var config = FixTestSupport.ValidConfig(); // MsgTypes empty by default
        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);
        var sid = FixTestSupport.FakeSessionId();

        app.FromApp(FixTestSupport.BuildMessage("35=W|55=EUR/USD"), sid);
        app.FromApp(FixTestSupport.BuildMessage("35=8|37=1"), sid);

        Assert.Equal(["W", "8"], DrainMsgTypes(channel));
    }

    [Fact]
    public void FromAppFiltersToOnlyTheConfiguredMsgTypesAndTrimsWhitespace()
    {
        var config = FixTestSupport.ValidConfig();
        config.MsgTypes = "W, X"; // a user pasting a comma list is not expected to skip the space

        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);
        var sid = FixTestSupport.FakeSessionId();

        app.FromApp(FixTestSupport.BuildMessage("35=W|55=EUR/USD"), sid);
        app.FromApp(FixTestSupport.BuildMessage("35=8|37=1"), sid); // excluded
        app.FromApp(FixTestSupport.BuildMessage("35=X|262=1"), sid);

        Assert.Equal(["W", "X"], DrainMsgTypes(channel));
        // A message the filter excludes never reaches the bounded queue at all, so it cannot count as a
        // capacity drop either.
        Assert.Equal(0, app.Dropped);
    }

    // ------------------------------------------------------------------
    // The bounded, drop-oldest, counted channel
    // ------------------------------------------------------------------

    [Fact]
    public void TheChannelKeepsOnlyTheNewestMessagesUnderPressureAndCountsWhatItDropped()
    {
        const int capacity = 3;
        var config = FixTestSupport.ValidConfig();
        var channel = BoundedDropOldest(capacity);
        var app = new FixBridgeApplication(config, channel, capacity);
        var sid = FixTestSupport.FakeSessionId();

        for (var i = 1; i <= 5; i++)
        {
            app.FromApp(FixTestSupport.BuildMessage($"35=W|262=REQ{i}"), sid);
        }

        var received = new List<byte[]>();
        while (channel.Reader.TryRead(out var msg))
        {
            received.Add(msg.Payload);
        }

        Assert.Equal(capacity, received.Count);
        // The oldest two (REQ1, REQ2) were discarded; REQ3..REQ5 survive.
        Assert.DoesNotContain(received, p => Encoding.UTF8.GetString(p).Contains("REQ1", StringComparison.Ordinal));
        Assert.DoesNotContain(received, p => Encoding.UTF8.GetString(p).Contains("REQ2", StringComparison.Ordinal));
        Assert.Contains(received, p => Encoding.UTF8.GetString(p).Contains("REQ5", StringComparison.Ordinal));
        Assert.Equal(2, app.Dropped);
    }

    [Fact]
    public void UnderCapacityNothingIsEverDropped()
    {
        var config = FixTestSupport.ValidConfig();
        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);
        var sid = FixTestSupport.FakeSessionId();

        for (var i = 1; i <= 4; i++)
        {
            app.FromApp(FixTestSupport.BuildMessage($"35=W|262=REQ{i}"), sid);
        }

        Assert.Equal(4, DrainMsgTypes(channel).Count);
        Assert.Equal(0, app.Dropped);
    }

    // ------------------------------------------------------------------
    // OnLogon — raw FIX text sent after logon; failures reported, never swallowed
    // ------------------------------------------------------------------

    [Fact]
    public void OnLogonIsANoOpWhenNotConfigured()
    {
        var config = FixTestSupport.ValidConfig(); // OnLogon null
        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);

        app.OnLogon(FixTestSupport.FakeSessionId());

        Assert.False(channel.Reader.Completion.IsCompleted);
    }

    [Fact]
    public async Task OnLogonFailureFailsTheWholeConnectionAttemptInsteadOfSwallowingTheError()
    {
        var config = FixTestSupport.ValidConfig();
        config.OnLogon = "35=V|262=1|263=1|55=EUR/USD|264=1|265=0";

        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);

        // No live QuickFIX/n session is registered under this SessionID (no socket was ever opened), so
        // Session.SendToTarget cannot possibly succeed — exactly the "the request never went out" case
        // this method's doc comment says must be reported, not swallowed.
        app.OnLogon(FixTestSupport.FakeSessionId());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in channel.Reader.ReadAllAsync())
            {
            }
        });

        Assert.Contains("onLogon", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OnLogonSniffsAPipeDelimitedLineTheSameWayItSniffsSoh()
    {
        // Whatever delimiter the config text uses, the failure path below proves the line was at least
        // PARSED (a malformed frame would throw a different, earlier exception) before the send itself
        // failed for lack of a live session.
        var config = FixTestSupport.ValidConfig();
        config.OnLogon = "35=V^262=1^263=1^55=EUR/USD"; // caret-delimited this time

        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);

        app.OnLogon(FixTestSupport.FakeSessionId());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in channel.Reader.ReadAllAsync())
            {
            }
        });

        Assert.Contains("onLogon", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ------------------------------------------------------------------
    // OnLogout — a clean session end must end the subscription, not hang it forever
    // ------------------------------------------------------------------

    [Fact]
    public async Task OnLogoutCompletesTheChannelCleanlySoTheSubscriptionEndsInsteadOfHanging()
    {
        var config = FixTestSupport.ValidConfig();
        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);

        app.OnLogout(FixTestSupport.FakeSessionId());

        // Completes WITHOUT an exception — SubscriberCore's "reached here without throwing -> clean
        // disconnect, reconnect with no backoff" rule, not the error path OnLogon's failure uses.
        var items = new List<FixInboundMessage>();
        await foreach (var msg in channel.Reader.ReadAllAsync())
        {
            items.Add(msg);
        }

        Assert.Empty(items);
        Assert.True(channel.Reader.Completion.IsCompletedSuccessfully);
    }

    // ------------------------------------------------------------------
    // ToAdmin — credential injection, Logon only
    // ------------------------------------------------------------------

    [Fact]
    public void ToAdminInjectsUsernameAndPasswordOnlyIntoTheLogonMessage()
    {
        var config = FixTestSupport.ValidConfig();
        config.Username = "trader1";
        config.Password = "s3cr3t";

        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);
        var sid = FixTestSupport.FakeSessionId();

        var logon = FixTestSupport.BuildMessage("35=A|98=0|108=30");
        app.ToAdmin(logon, sid);
        Assert.Equal("trader1", logon.GetString(553));
        Assert.Equal("s3cr3t", logon.GetString(554));

        var heartbeat = FixTestSupport.BuildMessage("35=0");
        app.ToAdmin(heartbeat, sid);
        Assert.False(heartbeat.IsSetField(553));
        Assert.False(heartbeat.IsSetField(554));
    }

    [Fact]
    public void ToAdminSetsNeitherFieldWhenNoCredentialsAreConfigured()
    {
        var config = FixTestSupport.ValidConfig(); // Username/Password both null
        var channel = BoundedDropOldest(10);
        var app = new FixBridgeApplication(config, channel, 10);

        var logon = FixTestSupport.BuildMessage("35=A|98=0|108=30");
        app.ToAdmin(logon, FixTestSupport.FakeSessionId());

        Assert.False(logon.IsSetField(553));
        Assert.False(logon.IsSetField(554));
    }
}
