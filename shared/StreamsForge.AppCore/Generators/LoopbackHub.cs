using System.Collections.Concurrent;
using System.Threading.Channels;

namespace StreamsForge.Host.Generators;

/// <summary>
/// Wishlist #9(b): the native in-process loopback sink→source pair — a table's deltas feed a source
/// directly, no HTTP hop, no serialize/parse round trip (contrast wishlist #9(a)'s <c>HttpSinkClient</c>,
/// which POSTs JSON to <c>/api/sources/{name}/events</c>). This hub IS the entire "wire": a process-static,
/// thread-safe registry of one unbounded <see cref="Channel{T}"/> per attached source name.
/// <c>LoopbackSinkClient</c> (shared/StreamsForge.AppCore/Sinks/LoopbackSinkClient.cs) WRITES into it via
/// <see cref="TryPublish"/>; each runtime's generator — Orleans <c>GeneratorGrain</c>, Dapr
/// <c>GeneratorActor</c> — DRAINS it on its own timer tick via <see cref="Drain"/>, then republishes
/// whatever it got exactly as it would a synthetic tick (same stream, same <c>_source</c>/<c>_ts</c>
/// stamping). A table/pipeline reading the target source cannot tell a loopback-fed row from a tick-fed
/// one.
///
/// <para><b>Why a hub at all, instead of the sink calling straight into a grain/actor reference.</b>
/// <c>StreamsForge.AppCore</c> is the runtime-neutral core both Orleans and Dapr depend on (AGENTS.md rule
/// 2 — no Orleans/Dapr/ASP.NET types inside the shared layers) and <see cref="ISinkClient"/> instances are
/// constructed with no facade/grain-factory access at all (see <c>ISinkTransport.Create</c>'s signature) —
/// there is nothing a sink COULD call directly even if it wanted to. A plain static registry that the
/// generator attaches to and the sink writes to is the smallest thing that bridges the two without either
/// side depending on the other's runtime.</para>
///
/// <para><b>Why draining happens on a TIMER tick, never an async reader loop reacting to a write — this
/// is the whole answer to "the engine must not deadlock or stack-overflow on a tight cycle".</b></para>
/// <list type="number">
/// <item><description><see cref="TryPublish"/> (the sink side) is a synchronous, allocation-light
/// <c>Channel&lt;T&gt;.Writer.TryWrite</c> call. It returns immediately, calls nothing downstream, and
/// never awaits — so it can never grow a call stack, no matter how many times it is (transitively) called
/// in a row.</description></item>
/// <item><description><see cref="Drain"/> (the source side) is read ONLY from a generator's own already-
/// scheduled timer callback (Orleans <c>RegisterGrainTimer</c> / Dapr <c>RegisterTimerAsync</c>) — a
/// callback the RUNTIME invokes on its own schedule, never a continuation chained directly off
/// <see cref="TryPublish"/>. There is, by construction, no code path from a write straight into a
/// read.</description></item>
/// </list>
/// <para>Put together: every lap of a loopback cycle (table computes a delta → its sink calls
/// <see cref="TryPublish"/> → [nothing happens synchronously] → some milliseconds later, the target
/// generator's OWN timer fires → <see cref="Drain"/> → republish onto the source's stream → the table
/// recomputes) crosses at least one asynchronous scheduling boundary that is entirely outside this hub's
/// (or the sink's) control. The native call stack cannot grow across a lap because nothing on the write
/// side ever calls anything on the read side directly — there is no recursion to overflow, and nothing
/// blocks waiting on the other side either (write never blocks; the timer that reads is never itself
/// waited on by the write), so there is no deadlock to have.</para>
///
/// <para><b>The unbounded-cycle case, said explicitly (wishlist #9(b)'s own ask).</b> If the user's SQL
/// has no <c>WHERE step &lt; D</c> bound, or the loopback sink's own <c>MaxDepth</c> is left at 0 (off),
/// this hub does NOT detect or interrupt the cycle — "termination is the user's job" (the wishlist's own
/// words) applies here exactly as it does to any dataflow iterate. What DOES hold, by the structural
/// argument above: the cycle runs — forever, or until something stops it — WITHOUT ever deadlocking and
/// WITHOUT ever overflowing the stack. It is a genuine, intentional live loop: CPU keeps getting spent,
/// the target table's row/delta count keeps growing, at the pace of whichever generator's drain-timer
/// period this hub's caller chose. The only two ways it ends are (a) a bound — <c>MaxDepth</c> on the sink
/// or a SQL predicate that eventually makes the table stop emitting — or (b) an operator action:
/// <see cref="Detach"/> (called from the target generator's own StopAsync), after which every further
/// <see cref="TryPublish"/> for that name returns false and is reported as a failure by the caller, never
/// silently swallowed. See <c>LoopbackCycleTests</c> (orleans/tests/StreamsForge.Host.Tests) for the test
/// that proves this against the real hub + a real Orleans <c>GeneratorGrain</c> drain loop: a tight,
/// unbounded cycle runs for hundreds of laps without throwing and without hanging, and stops growing the
/// instant it is stopped.</para>
/// </summary>
public static class LoopbackHub
{
    private static readonly ConcurrentDictionary<string, Channel<Dictionary<string, object?>>> Channels =
        new(StringComparer.Ordinal);

    /// <summary>Called by a generator's StartAsync (Orleans <c>GeneratorGrain</c> / Dapr
    /// <c>GeneratorActor</c>) — ALWAYS, regardless of <c>GeneratorProfile</c>/<c>EventsPerSecond</c>, so
    /// any generator-kind source can be a loopback target the moment it is started, exactly as it can
    /// already receive a tick. Creates (or REPLACES) this source's inbound channel; a second Attach for
    /// the same name drops whatever a stale channel still held, mirroring StartAsync's own "replaces any
    /// timer from a previous call" idempotence — a restart is a fresh start, not a resume.</summary>
    public static void Attach(string sourceName)
    {
        var channel = Channel.CreateUnbounded<Dictionary<string, object?>>(new UnboundedChannelOptions
        {
            // One dedicated drain-timer callback reads; any number of LoopbackSinkClient instances
            // (potentially on different threads/grains/actors) may write concurrently.
            SingleReader = true,
            SingleWriter = false,
        });
        Channels[sourceName] = channel;
    }

    /// <summary>Called by a generator's StopAsync — removes and completes this source's channel. Any
    /// <c>LoopbackSinkClient</c> still targeting this name after Detach gets a reported drop from
    /// <see cref="TryPublish"/> (false), never a silent one — see this class's doc comment.</summary>
    public static void Detach(string sourceName)
    {
        if (Channels.TryRemove(sourceName, out var channel))
        {
            channel.Writer.TryComplete();
        }
    }

    /// <summary>The sink side. Synchronous, never throws, never blocks — see this class's doc comment for
    /// why that is exactly what keeps a cycle from deadlocking or overflowing the stack. Returns false
    /// (nothing written) when no generator has Attach'd <paramref name="sourceName"/> — not started, wrong
    /// kind, or already stopped — which the caller (<c>LoopbackSinkClient</c>) reports as a failure exactly
    /// like a network error, per wishlist #9's "the drop must be observable, not silent" rule.</summary>
    public static bool TryPublish(string sourceName, Dictionary<string, object?> row) =>
        Channels.TryGetValue(sourceName, out var channel) && channel.Writer.TryWrite(row);

    /// <summary>The source side — called ONLY from a generator's own timer tick (see this class's doc
    /// comment). Drains up to <paramref name="max"/> currently-buffered rows for
    /// <paramref name="sourceName"/> WITHOUT blocking (a plain <c>TryRead</c> loop — never awaits, so a
    /// tick that finds nothing pending returns immediately). Empty when nothing is attached, or nothing is
    /// pending; never throws. <paramref name="max"/> caps how much one tick can drain so a burst does not
    /// hold up the calling grain's/actor's turn indefinitely — anything left over is simply picked up on
    /// the NEXT tick, not lost.</summary>
    public static List<Dictionary<string, object?>> Drain(string sourceName, int max)
    {
        if (max <= 0 || !Channels.TryGetValue(sourceName, out var channel))
        {
            return [];
        }

        var rows = new List<Dictionary<string, object?>>();
        while (rows.Count < max && channel.Reader.TryRead(out var row))
        {
            rows.Add(row);
        }

        return rows;
    }
}
