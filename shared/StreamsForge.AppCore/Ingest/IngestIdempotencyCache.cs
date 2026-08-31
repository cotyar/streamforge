using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Ingest;

/// <summary>
/// Plan 009 A1.1 — batch-level idempotency: the actual fix for "retry after a 429 duplicates rows".
/// A host-process singleton (like <see cref="SourceIngressRegistry"/>) remembering the last
/// <see cref="MaxEntries"/> <c>(source, idempotency key)</c> pairs together with the exact
/// <see cref="IngestResult"/> each produced. Same idempotency-key idiom this style of API usually
/// ships (Stripe et al.): a key names ONE intended push attempt, so a repeat of the same key always
/// replays that attempt's original outcome verbatim — including a non-Accepted one — rather than
/// re-running coercion/admission and possibly getting a different answer. That is what makes "retry
/// the identical body" safe: nothing is ever admitted twice for the same key.
///
/// <para>Bounded FIFO across ALL sources combined (not per-source) — "the last N pairs", per the plan
/// brief — evicted oldest-first exactly like <see cref="StreamsForge.AppCore.Connectors.Polling.DedupTracker"/>'s
/// own key set.</para>
///
/// <para><b>Known race, same tolerance as DedupTracker's own documented ceiling:</b> two concurrent
/// pushes presenting the same BRAND NEW key can both miss the cache and both proceed to admit — this
/// only protects a genuine retry-after-a-response, not two callers racing on the very first attempt.
/// Solving that would need a per-key lock for a case the plan doesn't ask for.</para>
/// </summary>
public sealed class IngestIdempotencyCache
{
    /// <summary>Bound on the number of remembered (source, key) pairs, across every source.</summary>
    public const int MaxEntries = 1_000;

    private readonly object _gate = new();
    private readonly Dictionary<string, IngestResult> _results = new(StringComparer.Ordinal);
    private readonly Queue<string> _order = new();

    /// <summary>Null when this exact (source, key) pair has never been remembered.</summary>
    public IngestResult? TryGet(string sourceName, string idempotencyKey)
    {
        lock (_gate)
        {
            return _results.TryGetValue(CacheKey(sourceName, idempotencyKey), out var result) ? result : null;
        }
    }

    /// <summary>Records <paramref name="result"/> as the outcome for this (source, key) pair. A no-op
    /// if already remembered (the first recorded outcome wins — a concurrent duplicate computation
    /// must never overwrite it with a second, potentially different, result).</summary>
    public void Remember(string sourceName, string idempotencyKey, IngestResult result)
    {
        lock (_gate)
        {
            var key = CacheKey(sourceName, idempotencyKey);
            if (_results.ContainsKey(key))
            {
                return;
            }

            _results[key] = result;
            _order.Enqueue(key);
            if (_order.Count > MaxEntries)
            {
                _results.Remove(_order.Dequeue());
            }
        }
    }

    /// <summary>Number of (source, key) pairs currently remembered (never exceeds <see cref="MaxEntries"/>).</summary>
    public int Count { get { lock (_gate) return _order.Count; } }

    /// <summary>Shallow copy of <paramref name="original"/> with <see cref="IngestResult.Replayed"/>
    /// set — returned instead of the cached instance itself so a caller can never mutate the cached
    /// result out from under a later replay.</summary>
    public static IngestResult AsReplay(IngestResult original) => new()
    {
        Outcome = original.Outcome,
        Accepted = original.Accepted,
        Dropped = original.Dropped,
        Invalid = original.Invalid,
        RetryAfterMs = original.RetryAfterMs,
        Error = original.Error,
        RowErrors = original.RowErrors,
        Duplicate = original.Duplicate,
        Replayed = true,
    };

    /// <summary>The whole "check cache, else compute and remember" flow in one place — used
    /// identically by both runtime flavors' <c>IIngressFacade.PushAsync</c> so the ordering (idempotency
    /// check first, before coercion/admission ever runs) can't drift between them. A null/empty key
    /// skips the cache entirely, matching <c>IIngressFacade.PushAsync</c>'s own doc comment.</summary>
    public static async Task<IngestResult> RunAsync(
        IngestIdempotencyCache cache, string sourceName, string? idempotencyKey, Func<Task<IngestResult>> core)
    {
        if (string.IsNullOrEmpty(idempotencyKey))
        {
            return await core().ConfigureAwait(false);
        }

        var cached = cache.TryGet(sourceName, idempotencyKey);
        if (cached is not null)
        {
            return AsReplay(cached);
        }

        var result = await core().ConfigureAwait(false);
        cache.Remember(sourceName, idempotencyKey, result);
        return result;
    }

    // '\0' can't appear in a source name (SourceValidation rejects control characters) or realistically
    // in a client-supplied key, so joining with it can't collide the way a plain '|'/':' join could.
    private static string CacheKey(string sourceName, string idempotencyKey) => sourceName + '\0' + idempotencyKey;
}
