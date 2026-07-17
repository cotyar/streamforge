using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime;

/// <summary>One stage of a left-to-right folded interval join: `(accumulatedLeft) JOIN rightAlias WITHIN d [ON ...]`.
/// Buffers both sides; on arrival matches against the opposite buffer immediately; on eviction (watermark passing
/// entry.Ts + Within) null-pads unmatched LEFT/RIGHT/FULL entries. CROSS matches every currently buffered
/// opposite-side row, ignoring key and residual.</summary>
internal sealed class JoinStage
{
    private sealed class BufEntry
    {
        public required WorkingRow Row;
        public object? Key;
        public bool Matched;
    }

    private readonly JoinKind _kind;
    private readonly long _withinMs;
    private readonly Expr? _leftKey;
    private readonly Expr? _rightKey;
    private readonly Expr? _residual;
    private readonly IReadOnlyDictionary<Expr, (string Alias, string Field)> _bindings;
    private readonly WorkingRow _nullRight;
    private readonly WorkingRow _nullLeft;

    private readonly List<BufEntry> _left = [];
    private readonly List<BufEntry> _right = [];

    public JoinStage(
        JoinKind kind,
        TimeSpan within,
        Expr? leftKey,
        Expr? rightKey,
        Expr? residual,
        IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings,
        IReadOnlyList<(string Alias, SourceSchema Schema)> leftAliasesSoFar,
        (string Alias, SourceSchema Schema) rightSide)
    {
        _kind = kind;
        _withinMs = (long)Math.Round(within.TotalMilliseconds);
        _leftKey = leftKey;
        _rightKey = rightKey;
        _residual = residual;
        _bindings = bindings;
        _nullRight = WorkingRow.NullSide([rightSide]);
        _nullLeft = WorkingRow.NullSide(leftAliasesSoFar);
    }

    public List<WorkingRow> OnLeft(WorkingRow row) => OnArrival(row, isLeft: true);

    public List<WorkingRow> OnRight(WorkingRow row) => OnArrival(row, isLeft: false);

    private List<WorkingRow> OnArrival(WorkingRow row, bool isLeft)
    {
        var results = new List<WorkingRow>();
        var otherBuf = isLeft ? _right : _left;
        var ownBuf = isLeft ? _left : _right;

        if (_kind == JoinKind.Cross)
        {
            foreach (var other in otherBuf)
            {
                results.Add(isLeft ? WorkingRow.Combine(row, other.Row) : WorkingRow.Combine(other.Row, row));
            }
            ownBuf.Add(new BufEntry { Row = row, Matched = false, Key = null });
            return results;
        }

        var keyExpr = isLeft ? _leftKey : _rightKey;
        var key = keyExpr is null ? null : ExpressionEvaluator.Eval(keyExpr, new EvalContext(row, _bindings));
        bool matchedAny = false;

        foreach (var other in otherBuf)
        {
            if (!KeyEquals(key, other.Key)) continue;
            if (Math.Abs(row.Ts - other.Row.Ts) > _withinMs) continue;

            var combined = isLeft ? WorkingRow.Combine(row, other.Row) : WorkingRow.Combine(other.Row, row);
            if (_residual is not null)
            {
                var ok = ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(_residual, new EvalContext(combined, _bindings)));
                if (!ok) continue;
            }

            matchedAny = true;
            other.Matched = true;
            results.Add(combined);
        }

        ownBuf.Add(new BufEntry { Row = row, Matched = matchedAny, Key = key });
        return results;
    }

    public List<WorkingRow> Evict(long watermark)
    {
        var results = new List<WorkingRow>();

        _left.RemoveAll(e =>
        {
            if (watermark <= e.Row.Ts + _withinMs) return false;
            if (!e.Matched && (_kind == JoinKind.Left || _kind == JoinKind.Full))
            {
                results.Add(WorkingRow.Combine(e.Row, _nullRight));
            }
            return true;
        });

        _right.RemoveAll(e =>
        {
            if (watermark <= e.Row.Ts + _withinMs) return false;
            if (!e.Matched && (_kind == JoinKind.Right || _kind == JoinKind.Full))
            {
                results.Add(WorkingRow.Combine(_nullLeft, e.Row));
            }
            return true;
        });

        return results;
    }

    private static bool KeyEquals(object? a, object? b)
    {
        if (a is null || b is null) return false;
        if (SqlValues.IsNumber(a) && SqlValues.IsNumber(b)) return SqlValues.ToDouble(a) == SqlValues.ToDouble(b);
        if (a is string sa && b is string sb) return string.Equals(sa, sb, StringComparison.Ordinal);
        if (a is bool ba && b is bool bb) return ba == bb;
        return Equals(a, b);
    }
}
