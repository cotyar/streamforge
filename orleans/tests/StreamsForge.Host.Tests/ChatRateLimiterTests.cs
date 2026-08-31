using System.Threading.Tasks;
using StreamsForge.Api;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Per-session cap on POST /api/chat (<see cref="ChatRateLimiter"/>). The demo logins are
/// public, so this counter is the only thing between one visitor and the whole Gemini quota — the
/// endpoint wiring around it is a one-line lambda, consistent with this project's no-HTTP-harness
/// convention (see GeminiChatServiceTests.cs).</summary>
public class ChatRateLimiterTests
{
    [Fact]
    public void Allows_exactly_the_cap_then_refuses()
    {
        var limiter = new ChatRateLimiter(10);

        for (var i = 1; i <= 10; i++)
        {
            Assert.True(limiter.TryAcquire("session-a", out var remaining), $"call {i} should be allowed");
            Assert.Equal(10 - i, remaining);
        }

        Assert.False(limiter.TryAcquire("session-a", out var none));
        Assert.Equal(0, none);
        // Still refused after further hammering — the counter must not wrap or drift.
        Assert.False(limiter.TryAcquire("session-a", out _));
        Assert.False(limiter.TryAcquire("session-a", out _));
    }

    [Fact]
    public void Budgets_are_per_session_not_global()
    {
        var limiter = new ChatRateLimiter(2);

        Assert.True(limiter.TryAcquire("session-a", out _));
        Assert.True(limiter.TryAcquire("session-a", out _));
        Assert.False(limiter.TryAcquire("session-a", out _));

        // A different login (different jti) gets its own budget.
        Assert.True(limiter.TryAcquire("session-b", out var remaining));
        Assert.Equal(1, remaining);
    }

    [Fact]
    public void Non_positive_cap_disables_the_limiter()
    {
        var limiter = new ChatRateLimiter(0);

        for (var i = 0; i < 50; i++)
        {
            Assert.True(limiter.TryAcquire("session-a", out _));
        }
    }

    [Fact]
    public async Task Concurrent_calls_never_exceed_the_cap()
    {
        var limiter = new ChatRateLimiter(10);
        var granted = 0;

        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() =>
        {
            if (limiter.TryAcquire("session-a", out _))
            {
                Interlocked.Increment(ref granted);
            }
        })));

        Assert.Equal(10, granted);
    }
}
