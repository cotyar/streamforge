using StreamForge.Abstractions;
using StreamForge.AppCore.Discovery;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>Plan 016 wave 6, track A — <c>@name</c> resolving at the FIX connector's connect site,
/// <see cref="QuickFixMessageSource.BuildSettingsText"/> (internal — see that method's doc comment; also
/// what <c>FixDuplexSession</c>'s inbound half calls, so this covers both the receive-only <c>fix</c> kind
/// and the <c>fix-duplex</c> kind with one fix).
///
/// <para><c>BuildSettingsText</c> is internal and this project declares no <c>InternalsVisibleTo</c>, so
/// this test drives the resolution black-box through the public <see cref="QuickFixMessageSource.SubscribeAsync"/>
/// entry point instead of calling it directly — proving the property that actually matters operationally:
/// an unresolvable <c>@name</c> <see cref="FixSourceConfig.Host"/> fails FAST, before any
/// <c>SessionSettings</c>/<c>SocketInitiator</c> is ever constructed (no socket, no QuickFIX/n thread, no
/// timeout to wait out), with the resolver's own actionable message, and that failure propagates out of the
/// very first <c>MoveNextAsync</c> on the returned async-enumerable — exactly where <c>SubscriberCore</c>'s
/// reconnect loop (<see cref="StreamForge.AppCore.Transports.SubscriberCore"/>) already expects a connect
/// failure to surface, landing on this source's existing status-error/backoff path unchanged.</para>
///
/// <para>The literal-unchanged and embedded-<c>@</c> cases are NOT re-proven here: they exercise the exact
/// same <see cref="NamedEndpoints.Resolve"/> call this file's sibling test files
/// (<c>StreamForge.AppCore.Tests/Discovery/NamedEndpointConnectSitesTests.cs</c>,
/// <c>StreamForge.Connectors.Database.Tests/NamedEndpointResolutionTests.cs</c>) already cover, and a
/// literal <see cref="FixSourceConfig.Host"/> would otherwise need a real (or fake) TCP listener to observe
/// past the settings-build step — not worth the added machinery for a code path that is one shared function
/// call, not FIX-specific logic.</para></summary>
public class NamedEndpointResolutionTests
{
    [Fact]
    public async Task UnknownHostReference_ThrowsBeforeAnySessionSettingsAreBuilt()
    {
        NamedEndpoints.Clear();
        try
        {
            var config = new FixSourceConfig
            {
                Host = "@no-such-fix-venue",
                Port = 9999,
                SenderCompId = "SF",
                TargetCompId = "CPTY",
            };

            var source = new QuickFixMessageSource();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in source.SubscribeAsync(config, CancellationToken.None))
                {
                    // Never reached — resolution fails before a SocketInitiator is even created.
                }
            });

            Assert.Contains("no-such-fix-venue", ex.Message);
            Assert.Contains("not configured here", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void KnownHostReference_ResolvesToTheConfiguredValue()
    {
        // The same NamedEndpoints.Resolve call BuildSettingsText makes, exercised directly rather than
        // through a live (or attempted) FIX connection — QuickFIX/n's SocketInitiator retries a dial
        // failure internally on its own background thread rather than throwing it back through
        // SubscribeAsync (ReconnectInterval=5 in the generated ini), so driving this case end-to-end would
        // mean either a real listener or an indeterminate wait. Proving the resolver call itself succeeds
        // for a mapped name is what actually distinguishes this case from the unknown-reference test above.
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("test-fix-venue", "prod-fix-host")]);

            var resolved = NamedEndpoints.Resolve("@test-fix-venue");

            Assert.Equal("prod-fix-host", resolved);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }
}
