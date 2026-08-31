namespace StreamsForge.Engine.Dataflow;

/// <summary>
/// Identifies one edge of the dataflow graph (a producer stage → consumer stage link), assigned
/// at plan time. Wrapped rather than a bare <see cref="int"/> so a stray partition index or
/// epoch value can't be passed where an edge id is expected.
/// </summary>
public readonly record struct EdgeId(int Value) : IComparable<EdgeId>
{
    public int CompareTo(EdgeId other) => Value.CompareTo(other.Value);

    public override string ToString() => $"Edge({Value})";
}
