using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime;

/// <summary>Resolves an already-bound AggregateCallExpr node to its aggregator's current result,
/// used only while evaluating SELECT items in a windowed/grouped context.</summary>
internal delegate object? AggregateLookup(AggregateCallExpr node);

internal readonly struct EvalContext(WorkingRow row, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings, AggregateLookup? aggregates = null)
{
    public WorkingRow Row { get; } = row;
    public IReadOnlyDictionary<Expr, (string Alias, string Field)> Bindings { get; } = bindings;
    public AggregateLookup? Aggregates { get; } = aggregates;
}

/// <summary>Tree-walking interpreter for the expression AST. Implements SQL-style three-valued NULL logic:
/// NULL propagates through arithmetic/comparisons; AND/OR use Kleene logic; WHERE requires a literal `true`.</summary>
internal static class ExpressionEvaluator
{
    public static object? Eval(Expr expr, EvalContext ctx) => expr switch
    {
        NumberLiteral n => n.Value,
        StringLiteral s => s.Value,
        BoolLiteral b => b.Value,
        NullLiteral => null,
        StarExpr => null,
        Identifier or QualifiedIdentifier => EvalColumn(expr, ctx),
        UnaryExpr u => EvalUnary(u, ctx),
        BinaryExpr b => EvalBinary(b, ctx),
        FunctionCallExpr f => EvalFunction(f, ctx),
        AggregateCallExpr agg => ctx.Aggregates?.Invoke(agg),
        _ => null,
    };

    /// <summary>WHERE/ON residual semantics: only a literal `true` passes; NULL and `false` both filter the row out.</summary>
    public static bool IsTrue(object? value) => value is true;

    private static object? EvalColumn(Expr id, EvalContext ctx)
    {
        if (!ctx.Bindings.TryGetValue(id, out var b)) return null;
        var key = $"{b.Alias}_{b.Field}";
        return ctx.Row.Fields.TryGetValue(key, out var v) ? v : null;
    }

    private static object? EvalUnary(UnaryExpr u, EvalContext ctx)
    {
        if (u.Op == "NOT")
        {
            var v = Eval(u.Operand, ctx) as bool?;
            return v is null ? null : !v.Value;
        }
        var val = Eval(u.Operand, ctx);
        // Each arm is boxed explicitly: a bare `long l => -l, double d => -d` switch expression would
        // let C# unify the arm types via the long->double implicit conversion before boxing to object,
        // silently turning every negated long into a double (see NumberLiteral.Value for the same pitfall).
        return val switch { long l => (object)(-l), double d => (object)(-d), _ => null };
    }

    private static object? EvalBinary(BinaryExpr b, EvalContext ctx)
    {
        if (b.Op == "AND") return EvalAnd(b, ctx);
        if (b.Op == "OR") return EvalOr(b, ctx);

        var l = Eval(b.Left, ctx);
        var r = Eval(b.Right, ctx);
        return b.Op switch
        {
            "+" => Arith(l, r, static (x, y) => x + y, static (x, y) => x + y),
            "-" => Arith(l, r, static (x, y) => x - y, static (x, y) => x - y),
            "*" => Arith(l, r, static (x, y) => x * y, static (x, y) => x * y),
            "/" => Divide(l, r),
            "%" => Mod(l, r),
            "=" => Eq(l, r),
            "!=" or "<>" => NegateBool(Eq(l, r)),
            "<" => CompareOp(l, r, static c => c < 0),
            "<=" => CompareOp(l, r, static c => c <= 0),
            ">" => CompareOp(l, r, static c => c > 0),
            ">=" => CompareOp(l, r, static c => c >= 0),
            _ => null,
        };
    }

    private static object? EvalAnd(BinaryExpr b, EvalContext ctx)
    {
        var l = Eval(b.Left, ctx) as bool?;
        if (l == false) return false;
        var r = Eval(b.Right, ctx) as bool?;
        if (r == false) return false;
        return l is null || r is null ? null : true;
    }

    private static object? EvalOr(BinaryExpr b, EvalContext ctx)
    {
        var l = Eval(b.Left, ctx) as bool?;
        if (l == true) return true;
        var r = Eval(b.Right, ctx) as bool?;
        if (r == true) return true;
        return l is null || r is null ? null : false;
    }

    private static object? Arith(object? l, object? r, Func<long, long, long> longOp, Func<double, double, double> doubleOp)
    {
        if (l is null || r is null) return null;
        if (l is long ll && r is long rl) return longOp(ll, rl);
        if (SqlValues.IsNumber(l) && SqlValues.IsNumber(r)) return doubleOp(SqlValues.ToDouble(l), SqlValues.ToDouble(r));
        return null;
    }

    // Division always promotes to double, even for long/long, to avoid silent integer truncation.
    private static object? Divide(object? l, object? r)
    {
        if (l is null || r is null || !SqlValues.IsNumber(l) || !SqlValues.IsNumber(r)) return null;
        double rd = SqlValues.ToDouble(r);
        return rd == 0 ? null : SqlValues.ToDouble(l) / rd;
    }

    private static object? Mod(object? l, object? r)
    {
        if (l is null || r is null) return null;
        if (l is long ll && r is long rl) return rl == 0 ? null : ll % rl;
        if (SqlValues.IsNumber(l) && SqlValues.IsNumber(r))
        {
            double rd = SqlValues.ToDouble(r);
            return rd == 0 ? null : SqlValues.ToDouble(l) % rd;
        }
        return null;
    }

    private static object? Eq(object? l, object? r)
    {
        if (l is null || r is null) return null;
        if (SqlValues.IsNumber(l) && SqlValues.IsNumber(r)) return SqlValues.ToDouble(l) == SqlValues.ToDouble(r);
        if (l is string ls && r is string rs) return string.Equals(ls, rs, StringComparison.Ordinal);
        if (l is bool lb && r is bool rb) return lb == rb;
        return false;
    }

    private static object? NegateBool(object? v) => v is null ? null : !(bool)v;

    private static object? CompareOp(object? l, object? r, Func<int, bool> pred)
    {
        if (l is null || r is null) return null;
        bool sameKind = (SqlValues.IsNumber(l) && SqlValues.IsNumber(r)) ||
                         (l is string && r is string) ||
                         (l is bool && r is bool);
        if (!sameKind) return null;
        return pred(SqlValues.Compare(l, r));
    }

    private static object? EvalFunction(FunctionCallExpr f, EvalContext ctx)
    {
        var args = f.Args.Select(a => Eval(a, ctx)).ToList();
        return f.Name.ToUpperInvariant() switch
        {
            "ABS" => args.Count > 0 ? Abs(args[0]) : null,
            "ROUND" => Round(args),
            "UPPER" => args.Count > 0 && args[0] is string su ? su.ToUpperInvariant() : null,
            "LOWER" => args.Count > 0 && args[0] is string sl ? sl.ToLowerInvariant() : null,
            "COALESCE" => args.FirstOrDefault(v => v is not null),
            _ => null,
        };
    }

    private static object? Abs(object? v) => v switch { long l => (object)Math.Abs(l), double d => (object)Math.Abs(d), _ => null };

    private static object? Round(List<object?> args)
    {
        if (args.Count == 0 || args[0] is null || !SqlValues.IsNumber(args[0]!)) return null;
        int digits = 0;
        if (args.Count > 1)
        {
            digits = args[1] switch { long dl => (int)dl, double dd => (int)dd, _ => 0 };
        }
        return Math.Round(SqlValues.ToDouble(args[0]!), Math.Max(0, digits), MidpointRounding.AwayFromZero);
    }
}
