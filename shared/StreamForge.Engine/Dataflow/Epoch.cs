namespace StreamForge.Engine.Dataflow;

/// <summary>
/// A DBSP-style epoch marker (see plans/003-materialize-territory.md, "Protocol details"): the
/// partitioned dataflow batches every delta into a totally-ordered, monotonically increasing
/// epoch. Epochs are the unit of progress — an operator's "frontier" (see
/// <see cref="FrontierTracker"/>) is expressed in epochs, and a batch's epoch says "this data
/// belongs to tick N; nothing earlier will ever follow it from this sender."
///
/// INVARIANT: epochs only ever increase for a given sender — see FrontierTracker's regression
/// detection, which is the enforcement point for that invariant.
/// </summary>
public readonly record struct Epoch(long Value) : IComparable<Epoch>
{
    /// <summary>The epoch value assigned to the very first tick.</summary>
    public static readonly Epoch Zero = new(0);

    /// <summary>The floor below every real epoch. Never assigned to an actual batch — it exists
    /// only as the starting value of a combined frontier before every upstream has reported at
    /// least once (see FrontierTracker's "silent upstream" semantics).</summary>
    public static readonly Epoch NegativeInfinity = new(long.MinValue);

    public int CompareTo(Epoch other) => Value.CompareTo(other.Value);

    public static bool operator <(Epoch left, Epoch right) => left.Value < right.Value;
    public static bool operator <=(Epoch left, Epoch right) => left.Value <= right.Value;
    public static bool operator >(Epoch left, Epoch right) => left.Value > right.Value;
    public static bool operator >=(Epoch left, Epoch right) => left.Value >= right.Value;

    /// <summary>The next epoch after this one.</summary>
    public Epoch Next() => new(Value + 1);

    public static Epoch Min(Epoch a, Epoch b) => a.Value <= b.Value ? a : b;
    public static Epoch Max(Epoch a, Epoch b) => a.Value >= b.Value ? a : b;

    public override string ToString() => Value == long.MinValue ? "Epoch(-inf)" : $"Epoch({Value})";
}
