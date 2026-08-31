namespace StreamsForge.Engine.Dataflow;

/// <summary>Outcome of <see cref="FrontierTracker.Observe"/>.</summary>
public enum FrontierObserveResult
{
    /// <summary>The combined frontier moved forward as a result of this observation.</summary>
    Advanced,

    /// <summary>The observation was accepted (recorded) but the combined frontier is unchanged
    /// — either it's a duplicate of the upstream's current high-water mark, or another upstream
    /// is still holding the combined frontier back.</summary>
    NoChange,

    /// <summary>The observed epoch is below the upstream's current high-water mark. Rejected:
    /// the observation is NOT applied. This is always a bug in the sender — epochs must be
    /// non-decreasing per upstream — never a legitimate protocol event.</summary>
    Regression,
}

/// <summary>The result of a single <see cref="FrontierTracker.Observe"/> call.</summary>
public readonly record struct FrontierObservation(FrontierObserveResult Result, Epoch Frontier)
{
    public bool Advanced => Result == FrontierObserveResult.Advanced;
    public bool Regressed => Result == FrontierObserveResult.Regression;
}

/// <summary>
/// Per-upstream high-water marks with min-combine (plans/003-materialize-territory.md, "Protocol
/// details": "Frontier tracker is a pure class ... property-tested to death; every historical
/// dataflow bug is a frontier bug"). An operator's combined frontier is the minimum epoch
/// reported across all of its registered upstreams — the whole point of DBSP-style progress
/// tracking: the operator may only act as if it has seen everything up to the SLOWEST upstream.
///
/// INVARIANTS this type maintains:
///  - The combined frontier never regresses: once <see cref="Frontier"/> reaches E, it never
///    reports a value below E again.
///  - A single upstream's high-water mark never regresses either. <see cref="Observe"/> detects
///    (and reports via <see cref="FrontierObserveResult.Regression"/>, never applies) any epoch
///    below what that upstream already reported — an assert-style signal surfaced to the caller
///    rather than a thrown exception, so one misbehaving upstream doesn't take the whole operator
///    down; the caller decides how loudly to fail.
///  - A registered upstream that has never called Observe holds the combined frontier at
///    <see cref="Epoch.NegativeInfinity"/> ("silent upstream" semantics) — the tracker never
///    assumes silence means "caught up".
///  - The upstream set is fixed once observation starts: <see cref="RegisterUpstream"/> after the
///    first <see cref="Observe"/> call throws. This mirrors the M2 deployment model, where a
///    partition topology change tears down and rebuilds the whole operator graph rather than
///    mutating a live tracker (see plan, M0 section: "no runtime add/remove").
/// </summary>
public sealed class FrontierTracker
{
    private readonly Dictionary<UpstreamId, Epoch> _highWater = [];
    private bool _observing;

    public FrontierTracker() { }

    public FrontierTracker(IEnumerable<UpstreamId> upstreams)
    {
        foreach (var id in upstreams) RegisterUpstream(id);
    }

    /// <summary>The upstreams currently registered with this tracker.</summary>
    public IReadOnlyCollection<UpstreamId> Upstreams => _highWater.Keys;

    /// <summary>The combined frontier: min over every registered upstream's high-water mark.
    /// <see cref="Epoch.NegativeInfinity"/> until at least one upstream has been registered and
    /// every registered upstream has observed at least once.</summary>
    public Epoch Frontier { get; private set; } = Epoch.NegativeInfinity;

    /// <summary>Adds an upstream this tracker will combine into its frontier. Setup-only: throws
    /// once <see cref="Observe"/> has been called (see class docs — upstream sets are fixed once
    /// the tracker goes live).</summary>
    public void RegisterUpstream(UpstreamId id)
    {
        if (_observing)
            throw new InvalidOperationException(
                $"Cannot register upstream {id}: FrontierTracker's upstream set is fixed once observation starts.");
        if (!_highWater.TryAdd(id, Epoch.NegativeInfinity))
            throw new ArgumentException($"Upstream {id} is already registered.", nameof(id));
    }

    /// <summary>
    /// Records that <paramref name="id"/> has reached <paramref name="epoch"/> — i.e. it will
    /// never send anything below <paramref name="epoch"/> again — and recomputes the combined
    /// frontier. Idempotent for a repeated observation of the same epoch (returns NoChange, not
    /// Regression). Rejects (does not apply) any epoch below that upstream's current mark.
    /// </summary>
    public FrontierObservation Observe(UpstreamId id, Epoch epoch)
    {
        if (!_highWater.TryGetValue(id, out var current))
            throw new ArgumentException($"Upstream {id} was never registered with this tracker.", nameof(id));

        _observing = true;

        if (epoch < current)
            return new FrontierObservation(FrontierObserveResult.Regression, Frontier);

        _highWater[id] = epoch; // no-op write when epoch == current (duplicate observation)

        var combined = Epoch.NegativeInfinity;
        var first = true;
        foreach (var mark in _highWater.Values)
        {
            combined = first ? mark : Epoch.Min(combined, mark);
            first = false;
        }

        if (combined > Frontier)
        {
            Frontier = combined;
            return new FrontierObservation(FrontierObserveResult.Advanced, Frontier);
        }

        return new FrontierObservation(FrontierObserveResult.NoChange, Frontier);
    }
}
