namespace StreamsForge.Engine.Runtime;

/// <summary>Subtractable (Z-set-aware) aggregate accumulator: Add is replaced with Apply(value, weight),
/// where a negative weight retracts a previously-applied contribution. NULL inputs are skipped (except
/// COUNT(*), which counts rows regardless of value).</summary>
public interface IZAggregator
{
    void Apply(object? value, long weight);
    object? Result { get; }
}

internal static class ZAggregator
{
    public static IZAggregator Create(Sql.AggregateCallExpr node)
    {
        string name = StatAggregatorNames.Canonical(node.Name) ?? node.Name;
        if (node.IsDistinct) return new DistinctCountAggregator();
        if (name == StatAggregatorNames.PercentileCont) return new PercentileZAggregator(AggregateParameters.Probability(node));
        return CreateCanonical(name, node.IsStar);
    }

    private static IZAggregator CreateCanonical(string name, bool isStar) => name switch
    {
        "COUNT" => new CountZAggregator(isStar),
        "SUM" => new SumZAggregator(),
        "AVG" => new AvgZAggregator(),
        "MIN" => new MinMaxZAggregator(isMin: true),
        "MAX" => new MinMaxZAggregator(isMin: false),
        StatAggregatorNames.VarSamp => new VarianceZAggregator(sample: true, stdDev: false),
        StatAggregatorNames.VarPop => new VarianceZAggregator(sample: false, stdDev: false),
        StatAggregatorNames.StdDevSamp => new VarianceZAggregator(sample: true, stdDev: true),
        StatAggregatorNames.StdDevPop => new VarianceZAggregator(sample: false, stdDev: true),
        StatAggregatorNames.Median => new MedianZAggregator(),
        // See Aggregator.Create: built-ins first, registry second, never the other way round.
        _ => Sql.SqlFunctions.FindAggregate(name)?.CreateZ()
             ?? throw new ArgumentException($"Unknown aggregate '{name}'"),
    };
}

internal sealed class CountZAggregator(bool isStar) : IZAggregator
{
    private long _count;
    public void Apply(object? value, long weight)
    {
        if (isStar || value is not null) _count += weight;
    }
    public object? Result => _count;
}

internal sealed class SumZAggregator : IZAggregator
{
    private double _sum;
    private long _longSum;
    private bool _sawDouble;

    public void Apply(object? value, long weight)
    {
        switch (value)
        {
            case double d: _sum += d * weight; _sawDouble = true; break;
            case long l: _sum += l * weight; _longSum += l * weight; break;
        }
    }

    public object? Result => _sawDouble ? (object)_sum : (object)_longSum;
}

internal sealed class AvgZAggregator : IZAggregator
{
    private double _sum;
    private long _count;

    public void Apply(object? value, long weight)
    {
        switch (value)
        {
            case double d: _sum += d * weight; _count += weight; break;
            case long l: _sum += l * weight; _count += weight; break;
        }
    }

    public object? Result => _count == 0 ? null : _sum / _count;
}

/// <summary>Per-group multiset of contributing values, ordered by SqlValues.Compare — MIN/MAX read the
/// first/last key. Retraction (negative weight) decrements the count; a value's entry is pruned once its
/// count reaches zero, so removing one of two equal minimums leaves the min unchanged, and removing both
/// exposes the next value.</summary>
internal sealed class MinMaxZAggregator(bool isMin) : IZAggregator
{
    private readonly SortedDictionary<object, long> _multiset = new(Comparer<object>.Create(SqlValues.Compare));

    public void Apply(object? value, long weight)
    {
        if (value is null) return;
        if (_multiset.TryGetValue(value, out var existing))
        {
            long newWeight = existing + weight;
            if (newWeight <= 0) _multiset.Remove(value);
            else _multiset[value] = newWeight;
        }
        else if (weight > 0)
        {
            _multiset[value] = weight;
        }
    }

    public object? Result => _multiset.Count == 0 ? null : isMin ? _multiset.Keys.First() : _multiset.Keys.Last();
}
