using System.Collections.Generic;

namespace StreamForge.Engine.Sql;

/// <summary>A resolved, validated JOIN: its equi-key (null only when validation already failed) and residual filter.</summary>
internal sealed class JoinBinding(JoinKind kind, string alias, string sourceName, TimeSpan? within, Expr? leftKey, Expr? rightKey, Expr? residual, bool isTable = false)
{
    public JoinKind Kind { get; } = kind;
    public string Alias { get; } = alias;
    public string SourceName { get; } = sourceName;
    public TimeSpan? Within { get; } = within;
    public Expr? LeftKey { get; } = leftKey;
    public Expr? RightKey { get; } = rightKey;
    public Expr? Residual { get; } = residual;
    /// <summary>Table mode only: whether SourceName resolved against the table namespace (vs. streams).</summary>
    public bool IsTable { get; } = isTable;
}

internal sealed class ValidationResult
{
    public required List<SqlDiagnostic> Diagnostics { get; init; }
    public required List<(string Alias, string SourceName, SourceSchema Schema, bool IsTable)> Sources { get; init; }
    public required Dictionary<Expr, (string Alias, string Field)> Bindings { get; init; }
    public required List<JoinBinding> Joins { get; init; }
    public required List<AggregateCallExpr> UsedAggregates { get; init; }
    /// <summary>Every expression node's inferred FieldKind (column refs, JSON access results, literals,
    /// arithmetic/aggregate results, ...) — populated in both modes, consumed by table-mode OutputSchema
    /// derivation and by the JSON bare-operand checks.</summary>
    public required Dictionary<Expr, FieldKind> ExprKinds { get; init; }
    /// <summary>Table mode only: distinct stream source names referenced (FROM/JOIN).</summary>
    public required List<string> StreamInputs { get; init; }
    /// <summary>Table mode only: distinct other-table names referenced (FROM/JOIN).</summary>
    public required List<string> TableInputs { get; init; }
    public bool HasAggregates => UsedAggregates.Count > 0;
    public bool HasErrors => Diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error);
}

internal enum ValidationMode { Stream, Table }

/// <summary>Semantic analysis: source/column/alias resolution, aggregate/window/emit structural rules,
/// and equi-join key extraction. Never throws — every problem becomes a <see cref="SqlDiagnostic"/>.
/// Two modes: Stream (windowed pipelines — the original dialect) and Table (persistent materialized
/// tables — unwindowed running aggregates over a combined streams+tables namespace).</summary>
internal sealed class Validator
{
    private readonly ValidationMode _mode;
    private readonly IReadOnlyDictionary<string, SourceSchema> _schemas;
    private readonly HashSet<string> _tableNames = new(StringComparer.Ordinal);
    private readonly HashSet<string> _ambiguousNames = new(StringComparer.Ordinal);
    private readonly List<string> _streamInputs = [];
    private readonly List<string> _tableInputs = [];

    private readonly List<SqlDiagnostic> _diags = [];
    private readonly Dictionary<Expr, (string Alias, string Field)> _bindings = new(ReferenceEqualityComparer.Instance);
    private readonly List<AggregateCallExpr> _usedAggregates = [];

    // Tracks the resolved FieldKind of expression nodes: a column reference gets its schema kind, a
    // JsonAccessExpr gets Json ('->') or String ('->>'), and (for table-mode OutputSchema derivation)
    // literals/arithmetic/aggregate/function results get their inferred kind too. Nodes absent from this
    // map (StarExpr, NullLiteral, ...) simply have no meaningful kind for our purposes.
    private readonly Dictionary<Expr, FieldKind> _exprKind = new(ReferenceEqualityComparer.Instance);

    private Validator(IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        _mode = ValidationMode.Stream;
        _schemas = schemas;
    }

    private Validator(IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        _mode = ValidationMode.Table;
        _tableNames = new HashSet<string>(tableSchemas.Keys, StringComparer.Ordinal);
        _ambiguousNames = new HashSet<string>(streamSchemas.Keys.Intersect(tableSchemas.Keys, StringComparer.Ordinal), StringComparer.Ordinal);

        var merged = new Dictionary<string, SourceSchema>(streamSchemas, StringComparer.Ordinal);
        foreach (var kv in tableSchemas) merged[kv.Key] = kv.Value;
        _schemas = merged;
    }

    public static ValidationResult Validate(SelectQuery query, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        var v = new Validator(schemas);
        return v.Run(query);
    }

    /// <summary>Table-mode validation: sql is a SELECT over streams AND/OR other tables, without windows.
    /// A FROM/JOIN identifier must exist in exactly one of streamSchemas/tableSchemas — present in both
    /// is an "ambiguous name" diagnostic.</summary>
    public static ValidationResult ValidateTable(SelectQuery query, IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        var v = new Validator(streamSchemas, tableSchemas);
        return v.Run(query);
    }

    private ValidationResult Run(SelectQuery q)
    {
        var sources = new List<(string Alias, string SourceName, SourceSchema Schema, bool IsTable)>();
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
            bool jIsTable = _mode == ValidationMode.Table && _tableNames.Contains(j.Source.Name);

            if (_mode == ValidationMode.Stream)
            {
                if (j.Within is null)
                {
                    _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN requires a WITHIN clause", j.Line, j.Column));
                }
            }
            else
            {
                if (j.Within is not null)
                {
                    _diags.Add(new SqlDiagnostic(
                        $"{JoinLabel(j.Kind)} JOIN may not have a WITHIN clause in table mode — state is unbounded/consolidated, not time-bounded",
                        j.Line, j.Column));
                }
                if (j.Kind == JoinKind.Cross)
                {
                    _diags.Add(new SqlDiagnostic("CROSS JOIN is not allowed in table mode", j.Line, j.Column));
                }
                else if (j.Kind != JoinKind.Inner)
                {
                    _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN is not allowed in table mode — only INNER equi-joins are supported", j.Line, j.Column));
                }
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

            if (sourceOk) sources.Add((j.Source.Alias, j.Source.Name, jSchema!, jIsTable));

            joins.Add(new JoinBinding(j.Kind, j.Source.Alias, j.Source.Name, j.Within, leftKey, rightKey, residual, jIsTable));
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

        if (_mode == ValidationMode.Stream)
        {
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
        }
        else
        {
            // Table mode: aggregates + GROUP BY are allowed WITHOUT a window (running aggregates —
            // that's the point of a table). WINDOW and EMIT are structurally forbidden outright.
            if (q.Window is not null)
            {
                _diags.Add(new SqlDiagnostic(
                    "WINDOW clause not allowed in table mode — tables maintain state continuously; windows belong to stream pipelines",
                    q.WindowLine ?? 1, q.WindowColumn ?? 1));
            }
            if (q.Emit is not null)
            {
                _diags.Add(new SqlDiagnostic("EMIT not allowed in table mode", q.EmitLine ?? 1, q.EmitColumn ?? 1));
            }
        }

        if ((q.Window is not null || q.GroupBy is not null || hasAggregates) && !q.Select.IsStar)
        {
            // Every non-aggregate select item must be one of the GROUP BY columns (or there is nothing to
            // group by at all, in which case only aggregate expressions are meaningful).
            var groupByList = q.GroupBy ?? [];
            foreach (var item in q.Select.Items)
            {
                if (item.Expression is QualifiedStarExpr qStar)
                {
                    // A qualified star expands to N columns at plan time; validating "every expanded column
                    // is a grouping column" would require re-resolving the alias's schema here too. Simplest
                    // correct rule instead: disallow alias.* outright alongside GROUP BY/aggregates.
                    _diags.Add(new SqlDiagnostic("star is not allowed with GROUP BY/aggregates", qStar.Line, qStar.Column));
                    continue;
                }
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
            ExprKinds = _exprKind,
            StreamInputs = _streamInputs.Distinct(StringComparer.Ordinal).ToList(),
            TableInputs = _tableInputs.Distinct(StringComparer.Ordinal).ToList(),
        };
    }

    private bool RegisterSource(SourceRef sref, List<(string Alias, string SourceName, SourceSchema Schema, bool IsTable)> sources, HashSet<string> aliasSeen, bool addToSourcesNow = true)
    {
        if (!aliasSeen.Add(sref.Alias))
        {
            _diags.Add(new SqlDiagnostic($"Duplicate alias '{sref.Alias}'", sref.Line, sref.Column));
        }

        if (_mode == ValidationMode.Table && _ambiguousNames.Contains(sref.Name))
        {
            _diags.Add(new SqlDiagnostic($"Ambiguous name '{sref.Name}' — present in both streams and tables", sref.Line, sref.Column));
            return false;
        }

        if (!_schemas.TryGetValue(sref.Name, out var schema))
        {
            var available = string.Join(", ", _schemas.Keys.OrderBy(k => k, StringComparer.Ordinal));
            _diags.Add(new SqlDiagnostic($"Unknown source '{sref.Name}' — available: {available}", sref.Line, sref.Column));
            return false;
        }

        bool isTable = _mode == ValidationMode.Table && _tableNames.Contains(sref.Name);
        if (_mode == ValidationMode.Table)
        {
            if (isTable) _tableInputs.Add(sref.Name); else _streamInputs.Add(sref.Name);
        }

        if (addToSourcesNow) sources.Add((sref.Alias, sref.Name, schema, isTable));
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
            case NumberLiteral nl:
                _exprKind[e] = nl.IsDouble ? FieldKind.Double : FieldKind.Long;
                return;
            case StringLiteral:
                _exprKind[e] = FieldKind.String;
                return;
            case BoolLiteral:
                _exprKind[e] = FieldKind.Bool;
                return;
            case NullLiteral or StarExpr:
                return;
            case Identifier id:
                ResolveBareIdentifier(id, scope);
                return;
            case QualifiedIdentifier qid:
                ResolveQualifiedIdentifier(qid, scope);
                return;
            case QualifiedStarExpr qs:
                ResolveQualifiedStar(qs, scope);
                return;
            case UnaryExpr u:
                ResolveExpr(u.Operand, scope, aggDepth);
                RecordUnaryKind(e, u);
                return;
            case BinaryExpr b:
                ResolveExpr(b.Left, scope, aggDepth);
                ResolveExpr(b.Right, scope, aggDepth);
                if (ArithmeticOrComparisonOps.Contains(b.Op))
                {
                    CheckBareJsonOperand(b.Left, b.Op, b.Right);
                    CheckBareJsonOperand(b.Right, b.Op, b.Left);
                }
                RecordBinaryKind(e, b);
                return;
            case JsonAccessExpr j:
                ResolveJsonAccess(j, scope, aggDepth);
                return;
            case FunctionCallExpr f:
                ValidateFunctionArity(f);
                foreach (var a in f.Args) ResolveExpr(a, scope, aggDepth);
                RecordFunctionKind(e, f);
                return;
            case AggregateCallExpr agg:
                if (aggDepth > 0)
                {
                    _diags.Add(new SqlDiagnostic("Aggregate functions cannot be nested", agg.Line, agg.Column));
                }
                _usedAggregates.Add(agg);
                if (!agg.IsStar && agg.Arg is not null) ResolveExpr(agg.Arg, scope, aggDepth + 1);
                RecordAggregateKind(e, agg);
                return;
            default:
                return;
        }
    }

    private void RecordUnaryKind(Expr node, UnaryExpr u)
    {
        if (u.Op == "NOT") { _exprKind[node] = FieldKind.Bool; return; }
        if (GetExprKind(u.Operand) is { } k && k != FieldKind.Json) _exprKind[node] = k;
    }

    private static readonly HashSet<string> ComparisonOps = new(StringComparer.Ordinal)
    {
        "=", "!=", "<>", "<", "<=", ">", ">=", "AND", "OR",
    };

    private void RecordBinaryKind(Expr node, BinaryExpr b)
    {
        if (ComparisonOps.Contains(b.Op)) { _exprKind[node] = FieldKind.Bool; return; }
        if (b.Op == "/") { _exprKind[node] = FieldKind.Double; return; }
        var lk = GetExprKind(b.Left);
        var rk = GetExprKind(b.Right);
        if (lk == FieldKind.Long && rk == FieldKind.Long) _exprKind[node] = FieldKind.Long;
        else if ((lk is FieldKind.Long or FieldKind.Double) && (rk is FieldKind.Long or FieldKind.Double)) _exprKind[node] = FieldKind.Double;
    }

    private void RecordFunctionKind(Expr node, FunctionCallExpr f)
    {
        switch (f.Name.ToUpperInvariant())
        {
            case "ABS" or "ROUND":
                _exprKind[node] = f.Args.Count > 0 && GetExprKind(f.Args[0]) is FieldKind.Long ? FieldKind.Long : FieldKind.Double;
                break;
            case "UPPER" or "LOWER":
                _exprKind[node] = FieldKind.String;
                break;
            case "COALESCE":
                if (f.Args.Count > 0 && GetExprKind(f.Args[0]) is { } k) _exprKind[node] = k;
                break;
        }
    }

    private void RecordAggregateKind(Expr node, AggregateCallExpr agg)
    {
        var argKind = agg.Arg is not null ? GetExprKind(agg.Arg) : null;
        _exprKind[node] = agg.Name switch
        {
            "COUNT" => FieldKind.Long,
            "SUM" => argKind == FieldKind.Long ? FieldKind.Long : FieldKind.Double,
            "AVG" => FieldKind.Double,
            "MIN" or "MAX" => argKind ?? FieldKind.Double,
            _ => FieldKind.Double,
        };
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
        if (reserved) _exprKind[id] = ReservedKind(id.Name);
        else RecordColumnKind(id, scope, matches[0]);
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
            _exprKind[qid] = ReservedKind(qid.Name);
            return;
        }
        var field = schema.Fields.Keys.FirstOrDefault(k => string.Equals(k, qid.Name, StringComparison.OrdinalIgnoreCase));
        if (field is null)
        {
            _diags.Add(new SqlDiagnostic($"Unknown column '{qid.Name}' on '{alias}'", qid.Line, qid.Column));
            return;
        }
        _bindings[qid] = (alias, field);
        if (schema.Fields.TryGetValue(field, out var kind)) _exprKind[qid] = kind;
    }

    /// <summary>Resolves `alias.*`'s qualifier against the query's sources — same "unknown alias" shape as
    /// ResolveQualifiedIdentifier's qualifier check, but there's no single field/kind to record: the
    /// Planner re-walks the (now-known-valid) alias at plan time to expand it into one output column per
    /// field of that source's schema.</summary>
    private void ResolveQualifiedStar(QualifiedStarExpr qs, List<(string Alias, SourceSchema Schema)> scope)
    {
        bool found = scope.Any(s => string.Equals(s.Alias, qs.Alias, StringComparison.Ordinal));
        if (!found)
        {
            var available = string.Join(", ", scope.Select(s => s.Alias));
            _diags.Add(new SqlDiagnostic($"Unknown source/alias '{qs.Alias}' — available: {available}", qs.Line, qs.Column));
        }
    }

    private void RecordColumnKind(Expr node, List<(string Alias, SourceSchema Schema)> scope, (string Alias, string Field) resolved)
    {
        var schema = scope.First(s => string.Equals(s.Alias, resolved.Alias, StringComparison.Ordinal)).Schema;
        if (schema.Fields.TryGetValue(resolved.Field, out var kind)) _exprKind[node] = kind;
    }

    private static FieldKind ReservedKind(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ? FieldKind.Long : FieldKind.String;

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') validation
    // ------------------------------------------------------------------

    private static readonly HashSet<string> ArithmeticOrComparisonOps = new(StringComparer.Ordinal)
    {
        "+", "-", "*", "/", "%", "=", "!=", "<>", "<", "<=", ">", ">=",
    };

    private FieldKind? GetExprKind(Expr e) => _exprKind.TryGetValue(e, out var k) ? k : null;

    /// <summary>Bare-JSON guard for comparisons/arithmetic: a Json-kind operand (a raw Json column, or the
    /// Json-kind result of a terminal '->') can't be compared/computed on directly — except `= NULL` /
    /// `!= NULL`, which is how NULL-ness of a JSON column is legitimately tested. Fires for both a plain
    /// Json column used bare and for a `->` chain that never got its final '->>'; the hint applies to both.</summary>
    private void CheckBareJsonOperand(Expr operand, string op, Expr otherOperand)
    {
        if (GetExprKind(operand) != FieldKind.Json) return;
        bool isNullEquality = op is "=" or "!=" or "<>" && otherOperand is NullLiteral;
        if (isNullEquality) return;

        _diags.Add(new SqlDiagnostic(
            $"JSON value used directly in '{op}' — extract a value first with '->>' (e.g. col ->> 'key' {op} ...); " +
            "a bare JSON column (or a '->' chain without a final '->>') can only be compared to NULL",
            operand.Line, operand.Column));
    }

    private void ResolveJsonAccess(JsonAccessExpr j, List<(string Alias, SourceSchema Schema)> scope, int aggDepth)
    {
        ResolveExpr(j.Left, scope, aggDepth);

        var leftKind = GetExprKind(j.Left);
        if (leftKind != FieldKind.Json)
        {
            string op = j.ReturnText ? "->>" : "->";
            _diags.Add(new SqlDiagnostic(
                $"'{op}' left operand must be a JSON column (or another '->' result), not {DescribeKind(leftKind)}",
                j.Line, j.Column));
        }

        // Result kind: '->>' always yields TEXT; a terminal '->' yields Json (so chains keep validating,
        // and — via CheckBareJsonOperand above — using that Json result directly in a comparison is
        // itself an error that nudges the caller toward '->>').
        _exprKind[j] = j.ReturnText ? FieldKind.String : FieldKind.Json;
    }

    private static string DescribeKind(FieldKind? kind) => kind switch
    {
        null => "a non-JSON value",
        FieldKind.Json => "JSON", // unreachable (Json left operands never fail the check) — kept for completeness
        _ => kind.Value.ToString(),
    };

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
                case JsonAccessExpr j: Walk(j.Left); break; // Key is always a literal — no aliases there
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
        JsonAccessExpr j => ContainsAggregate(j.Left),
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
            (JsonAccessExpr ja, JsonAccessExpr jb) => ja.ReturnText == jb.ReturnText &&
                StructurallyEqual(ja.Left, jb.Left, bindings) && StructurallyEqual(ja.Key, jb.Key, bindings),
            _ => false,
        };
    }
}
