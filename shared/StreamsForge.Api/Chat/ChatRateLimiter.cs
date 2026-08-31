using System;
using System.Collections.Concurrent;
using System.Threading;

namespace StreamsForge.Api;

/// <summary>Caps how many AI-chat calls one login session may make (<c>Chat:MaxRequestsPerSession</c>,
/// default 10). The demo credentials are printed on the login page, so without this a single visitor
/// can burn the whole Gemini quota — and every call is billable/rate-limited upstream.
///
/// Counts live in memory per process, like the rest of this system's state: a restart clears them,
/// which is fine for the thing it protects.
///
/// ponytail: keyed by session (the token's <c>jti</c>), so logging in again buys another 10. That is
/// the literal "per client per session" ask and it stops casual over-use; if a scripted abuser ever
/// shows up, key this by client IP instead — the only change needed is the key passed in.</summary>
public sealed class ChatRateLimiter(int maxPerSession)
{
    private sealed class Session(DateTimeOffset firstSeen)
    {
        public readonly DateTimeOffset FirstSeen = firstSeen;
        public int Used;
    }

    /// <summary>Sessions idle longer than a token lifetime are swept once the map gets big — the
    /// bound that stops a public instance from accumulating one entry per login forever.</summary>
    private static readonly TimeSpan SessionTtl = TimeSpan.FromHours(12);
    private const int SweepThreshold = 1_000;

    private readonly ConcurrentDictionary<string, Session> _sessions = new(StringComparer.Ordinal);

    public int MaxPerSession { get; } = maxPerSession;

    /// <summary>Registers one call against <paramref name="sessionKey"/>. Returns false once the
    /// session is over its cap; <paramref name="remaining"/> is how many calls are left afterwards
    /// (0 when refused). A non-positive cap disables the limiter entirely.</summary>
    public bool TryAcquire(string sessionKey, out int remaining)
    {
        if (MaxPerSession <= 0)
        {
            remaining = int.MaxValue;
            return true;
        }

        var now = DateTimeOffset.UtcNow;
        if (_sessions.Count >= SweepThreshold)
        {
            Sweep(now);
        }

        var session = _sessions.GetOrAdd(sessionKey, _ => new Session(now));
        var used = Interlocked.Increment(ref session.Used);
        if (used > MaxPerSession)
        {
            // Keep the counter pinned at the cap so a hammering client can't overflow it.
            Interlocked.Exchange(ref session.Used, MaxPerSession + 1);
            remaining = 0;
            return false;
        }

        remaining = MaxPerSession - used;
        return true;
    }

    private void Sweep(DateTimeOffset now)
    {
        foreach (var (key, session) in _sessions)
        {
            if (now - session.FirstSeen > SessionTtl)
            {
                _sessions.TryRemove(key, out _);
            }
        }
    }
}
