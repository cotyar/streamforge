using System.Collections.Generic;

namespace StreamForge.Engine.Sql;

/// <summary>A resolved, validated JOIN: its equi-key (null only when validation already failed) and residual filter.</summary>
internal sealed class JoinBinding(JoinKind kind, string alias, string sourceName, TimeSpan? within, Expr? leftKey, Expr? rightKey, Expr? residual)
{
    public JoinKind Kind { get; } = kind;
    public string Alias { get; } = alias;
    public string SourceName { get; } = sourceName;
    public TimeSpan? Within { get; } = within;
    public Expr? LeftKey { get; } = leftKey;
    public Expr? RightKey { get; } = rightKey;
    public Expr? Residual { get; } = residual;
}

internal sealed class ValidationResult
{
    public required List<SqlDiagnostic> Diagnostics { get; init; }
    public required List<(string Alias, string SourceName, SourceSchema Schema)> Sources { get; init; }
    public required Dictionary<Expr, (string Alias, string Field)> Bindings { get; init; }
    public required List<JoinBinding> Joins { get; init; }
    public required List<AggregateCallExpr> UsedAggregates { get; init; }
    public bool HasAggregates => UsedAggregates.Count > 0;
    public bool HasErrors => Diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error);
}

/// <summary>Semantic analysis: source/column/alias resolution, aggregate/window/emit structural rules,
/// and equi-join key extraction. Never throws — every problem becomes a <see cref="SqlDiagnostic"/>.</summary>
internal sealed class Validator
{
    private readonly IReadOnlyDictionary<string, SourceSchema> _schemas;
    private readonly List<SqlDiagnostic> _diags = [];
    private readonly Dictionary<Expr, (string Alias, string Field)> _bindings = new(ReferenceEqualityComparer.Instance);
    private readonly List<AggregateCallExpr> _usedAggregates = [];

    private Validator(IReadOnlyDictionary<string, SourceSchema> schemas) => _schemas = schemas;

    public static ValidationResult Validate(SelectQuery query, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        var v = new Validator(schemas);
        return v.Run(query);
    }

    private ValidationResult Run(SelectQuery q)
    {
        var sources = new List<(string Alias, string SourceName, SourceSchema Schema)>();
        var aliasSeen = new HashSet<string>(StringComparer.Ordinal);
        var joins = new List<JoinBinding>();

        // FROM
        RegisterSource(q.From.Source, sources, aliasSeen);

        // JOINs, left-to-right, each ON scoped to (aliases so far) ∪ (this join's alias).
        foreach (var j in q.From.Joins)
        {
            var leftAliasesBefore = sources.Select(s => s.Alias).ToHashSet(StringComparer.Ordinal);
            bool sourceOk = RegisterSource(j.Source, sources, aliasSeen, addToSourcesNow: false);
            _schemas.TryGetValue(j.Source.Name, out var jSchema);

            if (j.Within is null)
            {
                _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN requires a WITHIN clause", j.Line, j.Column));
            }

            Expr? leftKey = null, rightKey = null, residual = null;

            if (j.Kind == JoinKind.Cross)
            {
                // Grammar already forbids ON for CROSS; nothing further to resolve.
            }
            else if (j.On is null)
            {
                _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN requires an ON clause", j.Line, j.Column));
            }
            else
            {
                var scope = sources.Select(s => (s.Alias, s.Schema)).ToList();
                if (sourceOk) scope.Add((j.Source.Alias, jSchema!));
                ResolveExpr(j.On, scope, aggDepth: 0);

                var extracted = ExtractEquiKey(j.On, leftAliasesBefore, j.Source.Alias);
                if (extracted is null)
                {
                    _diags.Add(new SqlDiagnostic(
                        $"{JoinLabel(j.Kind)} JOIN's ON clause must contain an equi-comparison between {string.Join(", ", leftAliasesBefore)} and {j.Source.Alias}",
                        j.On.Line, j.On.Column));
                }
                else
                {
                    (leftKey, rightKey, residual) = extracted.Value;
                }
            }

            if (sourceOk) sources.Add((j.Source.Alias, j.Source.Name, jSchema!));

            joins.Add(new JoinBinding(j.Kind, j.Source.Alias, j.Source.Name, j.Within, leftKey, rightKey, residual));
        }

        var fullScope = sources.Select(s => (s.Alias, s.Schema)).ToList();

        if (q.Where is not null) ResolveExpr(q.Where, fullScope, aggDepth: 0);

        if (q.GroupBy is not null)
        {
            foreach (var g in q.GroupBy) ResolveExpr(g, fullScope, aggDepth: 0);
        }

        if (!q.Select.IsStar)
        {
            foreach (var item in q.Select.Items) ResolveExpr(item.Expression, fullScope, aggDepth: 0);
        }

        bool hasAggregates = _usedAggregates.Count > 0;

        if (hasAggregates && q.Window is null)
        {
            var first = _usedAggregates[0];
            _diags.Add(new SqlDiagnostic("Aggregate functions require a WINDOW clause", first.Line, first.Column));
        }
        if (q.GroupBy is not null && q.Window is null)
        {
            _diags.Add(new SqlDiagnostic("GROUP BY requires a WINDOW clause", q.GroupByLine ?? 1, q.GroupByColumn ?? 1));
        }
        if (q.Emit is not null && q.Window is null)
        {
            _diags.Add(new SqlDiagnostic("EMIT requires a WINDOW clause", q.EmitLine ?? 1, q.EmitColumn ?? 1));
        }

        if (q.Window is not null && !q.Select.IsStar)
        {
            // Every non-aggregate select item must be one of the GROUP BY columns (or there is nothing to
            // group by at all, in which case only aggregate expressions are meaningful under a WINDOW).
            var groupByList = q.GroupBy ?? [];
            foreach (var item in q.Select.Items)
            {
                if (ContainsAggregate(item.Expression)) continue;
                bool matchesGroupBy = groupByList.Any(g => StructurallyEqual(item.Expression, g));
                if (!matchesGroupBy)
                {
                    _diags.Add(new SqlDiagnostic(
                        "Non-aggregate select item must appear in GROUP BY",
                        item.Expression.Line, item.Expression.Column));
                }
            }
        }

        return new ValidationResult
        {
            Diagnostics = _diags,
            Sources = sources,
            Bindings = _bindings,
            Joins = joins,
            UsedAggregates = _usedAggregates,
        };
    }

    private bool RegisterSource(SourceRef sref, List<(string Alias, string SourceName, SourceSchema Schema)> sources, HashSet<string> aliasSeen, bool addToSourcesNow = true)
    {
        if (!aliasSeen.Add(sref.Alias))
        {
            _diags.Add(new SqlDiagnostic($"Duplicate alias '{sref.Alias}'", sref.Line, sref.Column));
        }

        if (!_schemas.TryGetValue(sref.Name, out var schema))
        {
            var available = string.Join(", ", _schemas.Keys.OrderBy(k => k, StringComparer.Ordinal));
            _diags.Add(new SqlDiagnostic($"Unknown source '{sref.Name}' — available: {available}", sref.Line, sref.Column));
            return false;
        }

        if (addToSourcesNow) sources.Add((sref.Alias, sref.Name, schema));
        return true;
    }

    private static string JoinLabel(JoinKind kind) => kind switch
    {
        JoinKind.Inner => "INNER",
        JoinKind.Left => "LEFT",
        JoinKind.Right => "RIGHT",
        JoinKind.Full => "FULL",
        JoinKind.Cross => "CROSS",
        _ => kind.ToString(),
    };

    private static bool IsReservedName(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(name, EventRecord.SourceField, StringComparison.OrdinalIgnoreCase);

    // ------------------------------------------------------------------
    // Column / alias resolution
    // ------------------------------------------------------------------

    private void ResolveExpr(Expr e, List<(string Alias, SourceSchema Schema)> scope, int aggDepth)
    {
        switch (e)
        {
            case NumberLiteral or StringLiteral or BoolLiteral or NullLiteral or StarExpr:
                return;
            case Identifier id:
                ResolveBareIdentifier(id, scope);
                return;
            case QualifiedIdentifier qid:
                ResolveQualifiedIdentifier(qid, scope);
                return;
            case UnaryExpr u:
                ResolveExpr(u.Operand, scope, aggDepth);
                return;
            case BinaryExpr b:
                ResolveExpr(b.Left, scope, aggDepth);
                ResolveExpr(b.Right, scope, aggDepth);
                return;
            case FunctionCallExpr f:
                ValidateFunctionArity(f);
                foreach (var a in f.Args) ResolveExpr(a, scope, aggDepth);
                return;
            case AggregateCallExpr agg:
                if (aggDepth > 0)
                {
                    _diags.Add(new SqlDiagnostic("Aggregate functions cannot be nested", agg.Line, agg.Column));
                }
                _usedAggregates.Add(agg);
                if (!agg.IsStar && agg.Arg is not null) ResolveExpr(agg.Arg, scope, aggDepth + 1);
                return;
            default:
                return;
        }
    }

    private void ResolveBareIdentifier(Identifier id, List<(string Alias, SourceSchema Schema)> scope)
    {
        bool reserved = IsReservedName(id.Name);
        var matches = new List<(string Alias, string Field)>();
        foreach (var (alias, schema) in scope)
        {
            if (reserved)
            {
                matches.Add((alias, CanonicalReserved(id.Name)));
                continue;
            }
            var field = schema.Fields.Keys.FirstOrDefault(k => string.Equals(k, id.Name, StringComparison.OrdinalIgnoreCase));
            if (field is not null) matches.Add((alias, field));
        }

        if (matches.Count == 0)
        {
            _diags.Add(new SqlDiagnostic($"Unknown column '{id.Name}'", id.Line, id.Column));
            return;
        }
        if (matches.Count > 1)
        {
            _diags.Add(new SqlDiagnostic(
                $"Ambiguous column '{id.Name}' — present in: {string.Join(", ", matches.Select(m => m.Alias))}",
                id.Line, id.Column));
            return;
        }
        _bindings[id] = matches[0];
    }

    private void ResolveQualifiedIdentifier(QualifiedIdentifier qid, List<(string Alias, SourceSchema Schema)> scope)
    {
        var entryIndex = scope.FindIndex(s => string.Equals(s.Alias, qid.Qualifier, StringComparison.Ordinal));
        if (entryIndex < 0)
        {
            var available = string.Join(", ", scope.Select(s => s.Alias));
            _diags.Add(new SqlDiagnostic($"Unknown source '{qid.Qualifier}' — available: {available}", qid.Line, qid.Column));
            return;
        }
        var (alias, schema) = scope[entryIndex];
        if (IsReservedName(qid.Name))
        {
            _bindings[qid] = (alias, CanonicalReserved(qid.Name));
            return;
        }
        var field = schema.Fields.Keys.FirstOrDefault(k => string.Equals(k, qid.Name, StringComparison.OrdinalIgnoreCase));
        if (field is null)
        {
            _diags.Add(new SqlDiagnostic($"Unknown column '{qid.Name}' on '{alias}'", qid.Line, qid.Column));
            return;
        }
        _bindings[qid] = (alias, field);
    }

    private static string CanonicalReserved(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ? EventRecord.TimestampField : EventRecord.SourceField;

    private static readonly HashSet<string> KnownFunctions = new(StringComparer.OrdinalIgnoreCase) { "ABS", "ROUND", "UPPER", "LOWER", "COALESCE" };

    private void ValidateFunctionArity(FunctionCallExpr f)
    {
        if (!KnownFunctions.Contains(f.Name))
        {
            _diags.Add(new SqlDiagnostic($"Unknown function '{f.Name}'", f.Line, f.Column));
            return;
        }
        int n = f.Args.Count;
        bool ok = f.Name.ToUpperInvariant() switch
        {
            "ABS" or "UPPER" or "LOWER" => n == 1,
            "ROUND" => n is 1 or 2,
            "COALESCE" => n >= 1,
            _ => true,
        };
        if (!ok)
        {
            _diags.Add(new SqlDiagnostic($"Function '{f.Name}' called with wrong number of arguments", f.Line, f.Column));
        }
    }

    // ------------------------------------------------------------------
    // Equi-join key extraction
    // ------------------------------------------------------------------

    private static IEnumerable<Expr> FlattenAnd(Expr e)
    {
        if (e is BinaryExpr { Op: "AND" } b)
        {
            foreach (var x in FlattenAnd(b.Left)) yield return x;
            foreach (var x in FlattenAnd(b.Right)) yield return x;
        }
        else
        {
            yield return e;
        }
    }

    private HashSet<string> CollectAliases(Expr e)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        void Walk(Expr node)
        {
            switch (node)
            {
                case Identifier or QualifiedIdentifier:
                    if (_bindings.TryGetValue(node, out var b)) result.Add(b.Alias);
                    break;
                case UnaryExpr u: Walk(u.Operand); break;
                case BinaryExpr bin: Walk(bin.Left); Walk(bin.Right); break;
                case FunctionCallExpr f: foreach (var a in f.Args) Walk(a); break;
                case AggregateCallExpr agg: if (agg.Arg is not null) Walk(agg.Arg); break;
            }
        }
        Walk(e);
        return result;
    }

    private (Expr Left, Expr Right, Expr? Residual)? ExtractEquiKey(Expr onExpr, HashSet<string> leftAliases, string rightAlias)
    {
        var conjuncts = FlattenAnd(onExpr).ToList();
        Expr? leftKey = null, rightKey = null;
        var residuals = new List<Expr>();

        foreach (var c in conjuncts)
        {
            if (leftKey is null && c is BinaryExpr { Op: "=" } be)
            {
                var la = CollectAliases(be.Left);
                var ra = CollectAliases(be.Right);
                if (la.Count > 0 && la.IsSubsetOf(leftAliases) && ra.Count == 1 && ra.Contains(rightAlias))
                {
                    leftKey = be.Left;
                    rightKey = be.Right;
                    continue;
                }
                if (ra.Count > 0 && ra.IsSubsetOf(leftAliases) && la.Count == 1 && la.Contains(rightAlias))
                {
                    leftKey = be.Right;
                    rightKey = be.Left;
                    continue;
                }
            }
            residuals.Add(c);
        }

        if (leftKey is null) return null;
        Expr? residual = residuals.Count == 0 ? null : residuals.Aggregate((a, b) => new BinaryExpr("AND", a, b, a.Line, a.Column));
        return (leftKey, rightKey!, residual);
    }

    // ------------------------------------------------------------------
    // Structural helpers
    // ------------------------------------------------------------------

    internal static bool ContainsAggregate(Expr e) => e switch
    {
        AggregateCallExpr => true,
        UnaryExpr u => ContainsAggregate(u.Operand),
        BinaryExpr b => ContainsAggregate(b.Left) || ContainsAggregate(b.Right),
        FunctionCallExpr f => f.Args.Any(ContainsAggregate),
        _ => false,
    };

    private bool StructurallyEqual(Expr a, Expr b) => StructurallyEqual(a, b, _bindings);

    internal static bool StructurallyEqual(Expr a, Expr b, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings)
    {
        if (a is Identifier or QualifiedIdentifier)
        {
            if (b is not (Identifier or QualifiedIdentifier)) return false;
            return bindings.TryGetValue(a, out var ba) && bindings.TryGetValue(b, out var bb) && ba == bb;
        }
        return (a, b) switch
        {
            (NumberLiteral na, NumberLiteral nb) => na.IsDouble == nb.IsDouble && Equals(na.Value, nb.Value),
            (StringLiteral sa, StringLiteral sb) => sa.Value == sb.Value,
            (BoolLiteral ba, BoolLiteral bb) => ba.Value == bb.Value,
            (NullLiteral, NullLiteral) => true,
            (StarExpr, StarExpr) => true,
            (UnaryExpr ua, UnaryExpr ub) => ua.Op == ub.Op && StructurallyEqual(ua.Operand, ub.Operand, bindings),
            (BinaryExpr xa, BinaryExpr xb) => xa.Op == xb.Op && StructurallyEqual(xa.Left, xb.Left, bindings) && StructurallyEqual(xa.Right, xb.Right, bindings),
            (FunctionCallExpr fa, FunctionCallExpr fb) => string.Equals(fa.Name, fb.Name, StringComparison.OrdinalIgnoreCase) &&
                fa.Args.Count == fb.Args.Count && fa.Args.Zip(fb.Args).All(p => StructurallyEqual(p.First, p.Second, bindings)),
            (AggregateCallExpr aa, AggregateCallExpr ab) => string.Equals(aa.Name, ab.Name, StringComparison.OrdinalIgnoreCase) &&
                aa.IsStar == ab.IsStar && (aa.Arg is null && ab.Arg is null || aa.Arg is not null && ab.Arg is not null && StructurallyEqual(aa.Arg, ab.Arg, bindings)),
            _ => false,
        };
    }
}
