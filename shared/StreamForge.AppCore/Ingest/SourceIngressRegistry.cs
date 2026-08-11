using StreamForge.Abstractions;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Host-process singleton: one <see cref="SourceIngressBuffer"/> per ingest-kind source name, safe
/// for concurrent <see cref="GetOrCreate"/>/<see cref="TryGet"/>/<see cref="Remove"/>. Keyed by name
/// AND a config fingerprint, so editing a source's <see cref="IngestConfig"/> (policy, capacity,
/// ...) rebuilds the buffer under it instead of silently keeping the old capacity/policy alive.
///
/// <para>A rebuild starts a fresh, empty buffer — whatever the old one still had queued is not
/// migrated. Editing ingress config while rows are in flight is rare and already has no ordering
/// guarantee against concurrent pushes; this trades that edge case for never running with a stale
/// policy, which is the more dangerous failure mode (e.g. a shrunk capacity silently not applying).</para>
/// </summary>
public sealed class SourceIngressRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, SourceIngressBuffer> _buffers = new(StringComparer.Ordinal);
    private readonly Func<long>? _clock;
    private readonly Func<TimeSpan, CancellationToken, Task>? _delay;

    /// <param name="clock">Propagated to every buffer this registry creates; see
    /// <see cref="SourceIngressBuffer"/>'s constructor. Null uses the real wall clock.</param>
    /// <param name="delay">Propagated to every buffer this registry creates. Null uses
    /// <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</param>
    public SourceIngressRegistry(Func<long>? clock = null, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _clock = clock;
        _delay = delay;
    }

    /// <summary>Returns the current buffer for <paramref name="sourceName"/>, creating one — or
    /// rebuilding it, on a fingerprint change — that drains via <paramref name="drain"/>.</summary>
    public SourceIngressBuffer GetOrCreate(
        string sourceName, IngestConfig config,
        Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task> drain)
    {
        var fingerprint = Fingerprint(config);
        lock (_gate)
        {
            if (_buffers.TryGetValue(sourceName, out var existing) && existing.ConfigFingerprint == fingerprint)
            {
                return existing;
            }

            var fresh = new SourceIngressBuffer(sourceName, config, fingerprint, drain, _clock, _delay);
            _buffers[sourceName] = fresh;
            return fresh;
        }
    }

    /// <summary>Null when no buffer has been created for this source (never pushed to / never
    /// looked up via <see cref="GetOrCreate"/>).</summary>
    public SourceIngressBuffer? TryGet(string sourceName)
    {
        lock (_gate)
        {
            return _buffers.TryGetValue(sourceName, out var buffer) ? buffer : null;
        }
    }

    /// <summary>Drops the buffer entirely — call when a source is deleted or its kind changes away
    /// from ingest. A no-op if none exists.</summary>
    public void Remove(string sourceName)
    {
        lock (_gate) { _buffers.Remove(sourceName); }
    }

    /// <summary>Drops every buffer whose source is not in <paramref name="liveSourceNames"/>. Deletion
    /// reaches this registry by several routes (the DELETE endpoint, a kind change, a replace-mode
    /// config import), so the hosts reconcile against the catalog on their existing source sweep rather
    /// than trying to intercept each route — a buffer whose source is gone otherwise keeps its rows and
    /// counters until the process restarts.</summary>
    public void RetainOnly(IReadOnlySet<string> liveSourceNames)
    {
        lock (_gate)
        {
            foreach (var name in _buffers.Keys.Where(n => !liveSourceNames.Contains(n)).ToList())
            {
                _buffers.Remove(name);
            }
        }
    }

    /// <summary>Every field that changes buffer behavior, joined so two configs that differ in any
    /// of them never compare equal.</summary>
    private static string Fingerprint(IngestConfig c)
        => string.Join('|', c.Policy, c.CapacityRows, c.MaxWaitMs, c.MaxBatchRows, c.RejectUnknownFields);
}
