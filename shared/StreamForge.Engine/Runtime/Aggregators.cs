namespace StreamForge.Engine.Runtime;

/// <summary>Incremental (streaming) aggregate accumulator. NULL inputs are skipped (except COUNT(*), which
/// counts rows regardless of value).</summary>
public abstract class Aggregator
{
    public abstract void Add(object? value);
    public abstract object? Result { get; }

    public static Aggregator Create(string name, bool isStar) => name.ToUpperInvariant() switch
    {
        "COUNT" => new CountAggregator(isStar),
        "SUM" => new SumAggregator(),
        "AVG" => new AvgAggregator(),
        "MIN" => new MinMaxAggregator(isMin: true),
        "MAX" => new MinMaxAggregator(isMin: false),
        // Registered aggregates are consulted only after the built-ins, and SqlFunctions refuses to
        // register a built-in's name, so this lookup can never change what an existing query means.
        _ => Sql.SqlFunctions.FindAggregate(name)?.CreateStream()
             ?? throw new ArgumentException($"Unknown aggregate '{name}'"),
    };
}

internal sealed class CountAggregator(bool isStar) : Aggregator
{
    private long _count;
    public override void Add(object? value)
    {
        if (isStar || value is not null) _count++;
    }
    public override object? Result => _count;
}

internal sealed class SumAggregator : Aggregator
{
    private double _sum;
    private long _longSum;
    private bool _sawDouble;
    private bool _any;

    public override void Add(object? value)
    {
        switch (value)
        {
            case double d: _sum += d; _sawDouble = true; _any = true; break;
            case long l: _sum += l; _longSum += l; _any = true; break;
            case null: break;
        }
    }

    // Branches are boxed explicitly to avoid C# unifying long/double arms to double before boxing.
    public override object? Result => !_any ? (object)0L : _sawDouble ? (object)_sum : (object)_longSum;
}

internal sealed class AvgAggregator : Aggregator
{
    private double _sum;
    private long _count;

    public override void Add(object? value)
    {
        switch (value)
        {
            case double d: _sum += d; _count++; break;
            case long l: _sum += l; _count++; break;
            case null: break;
        }
    }

    public override object? Result => _count == 0 ? null : _sum / _count;
}

internal sealed class MinMaxAggregator(bool isMin) : Aggregator
{
    private object? _best;
    private bool _any;

    public override void Add(object? value)
    {
        if (value is null) return;
        if (!_any) { _best = value; _any = true; return; }
        int cmp = SqlValues.Compare(value, _best!);
        if ((isMin && cmp < 0) || (!isMin && cmp > 0)) _best = value;
    }

    public override object? Result => _best;
}
