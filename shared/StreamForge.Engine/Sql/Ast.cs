namespace StreamForge.Engine.Sql;

// ============================================================================
// AST — produced by Parser, consumed by Validator/Planner.
// ============================================================================

internal abstract class Expr(int line, int column)
{
    public int Line { get; } = line;
    public int Column { get; } = column;
}

internal sealed class NumberLiteral(double? doubleValue, long? longValue, int line, int col) : Expr(line, col)
{
    public double? DoubleValue { get; } = doubleValue;
    public long? LongValue { get; } = longValue;
    public bool IsDouble => DoubleValue is not null;

    // NOTE: each branch is boxed explicitly (not left to the ternary's built-in numeric promotion) —
    // otherwise C# unifies `double` and `long` operands to `double` *before* boxing to `object`,
    // silently turning every integer literal into a double.
    public object Value => IsDouble ? (object)DoubleValue!.Value : (object)LongValue!.Value;
}

internal sealed class StringLiteral(string value, int line, int col) : Expr(line, col)
{
    public string Value { get; } = value;
}

internal sealed class BoolLiteral(bool value, int line, int col) : Expr(line, col)
{
    public bool Value { get; } = value;
}

internal sealed class NullLiteral(int line, int col) : Expr(line, col);

internal sealed class Identifier(string name, int line, int col) : Expr(line, col)
{
    public string Name { get; } = name;
}

internal sealed class QualifiedIdentifier(string qualifier, string name, int line, int col) : Expr(line, col)
{
    public string Qualifier { get; } = qualifier;
    public string Name { get; } = name;
}

internal sealed class StarExpr(int line, int col) : Expr(line, col);

/// <summary>Postgres-style qualified star in a SELECT list: `alias.*` — expands to every column of that
/// alias's FROM/JOIN input. Only meaningful as a top-level select-item expression (see Parser.ParseSelectItem);
/// never appears nested inside another expression. `Alias` carries the raw qualifier text; the Validator
/// resolves it against the query's sources.</summary>
internal sealed class QualifiedStarExpr(string alias, int line, int col) : Expr(line, col)
{
    public string Alias { get; } = alias;
}

internal sealed class UnaryExpr(string op, Expr operand, int line, int col) : Expr(line, col)
{
    public string Op { get; } = op;
    public Expr Operand { get; } = operand;
}

internal sealed class BinaryExpr(string op, Expr left, Expr right, int line, int col) : Expr(line, col)
{
    public string Op { get; } = op;
    public Expr Left { get; } = left;
    public Expr Right { get; } = right;
}

/// <summary>Postgres-style JSON access: `left -> key` (object field / array element, returns the JSON
/// value) or `left ->> key` (same access, returns TEXT). Binds tighter than arithmetic/comparison — see
/// Parser.ParsePostfix. `Key` is always a literal (StringLiteral for `-> 'k'`, NumberLiteral with
/// LongValue for `-> N`); the grammar restricts the right operand to literals (Postgres itself allows
/// arbitrary expressions there, but this dialect does not).</summary>
internal sealed class JsonAccessExpr(Expr left, bool returnText, Expr key, int line, int col) : Expr(line, col)
{
    public Expr Left { get; } = left;

    /// <summary>false = `->` (returns dict/list/primitive or NULL), true = `->>` (returns TEXT or NULL).</summary>
    public bool ReturnText { get; } = returnText;

    public Expr Key { get; } = key;
}

internal sealed class FunctionCallExpr(string name, List<Expr> args, int line, int col) : Expr(line, col)
{
    public string Name { get; } = name;
    public List<Expr> Args { get; } = args;
}

internal static class AggregateNames
{
    public static readonly string[] All = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

    /// <summary>Built-ins first, then <see cref="SqlFunctions"/>'s registered aggregates — this is what
    /// makes the parser build an <see cref="AggregateCallExpr"/> (rather than a plain function call) for
    /// a name the Engine has never heard of. A name that is neither still parses as a function call and
    /// gets the Validator's "Unknown function" diagnostic, which is the right error either way.</summary>
    public static bool IsAggregate(string name) =>
        All.Contains(name, StringComparer.OrdinalIgnoreCase) || SqlFunctions.FindAggregate(name) is not null;
}

internal sealed class AggregateCallExpr(string name, Expr? arg, bool isStar, int line, int col) : Expr(line, col)
{
    public string Name { get; } = name.ToUpperInvariant();
    public Expr? Arg { get; } = arg;
    public bool IsStar { get; } = isStar;
}

/// <summary>Plan 004 N2: `expr [NOT] IN ( SELECT ... )`. Position-restricted (WHERE top-level AND-conjunct
/// only — see Validator's `_resolvingWhere`/`_whereTopLevelConjuncts`); the subquery is uncorrelated in
/// this tier (no outer-alias visibility — same rule N1's DerivedSource uses). Plan-time rewrite: Planner
/// turns this into a semi-join (Negated=false) or anti-join (Negated=true) join stage appended to the
/// query's join chain, and removes this conjunct from the residual WHERE — see Planner.RewriteWhereForSubqueryPredicates.</summary>
internal sealed class InSubqueryExpr(Expr left, SelectQuery subquery, bool negated, int line, int col) : Expr(line, col)
{
    public Expr Left { get; } = left;
    public SelectQuery Subquery { get; } = subquery;
    public bool Negated { get; } = negated;
}

/// <summary>Plan 004 N2: `[NOT] EXISTS ( SELECT ... )`. Same position restriction and uncorrelated-subquery
/// rule as <see cref="InSubqueryExpr"/> — Planner rewrites this to the same semi/anti join machinery using
/// a constant join key on both sides (existence of ANY row, not a specific key match).</summary>
internal sealed class ExistsExpr(SelectQuery subquery, bool negated, int line, int col) : Expr(line, col)
{
    public SelectQuery Subquery { get; } = subquery;
    public bool Negated { get; } = negated;
}

/// <summary>Plan 004 N3/N4: `( SELECT agg(...) FROM ... [WHERE ... [AND inner.k = outer.k]] )` used as a
/// value expression (WHERE/SELECT). Parser doesn't distinguish N3 (uncorrelated) from N4 (single-level
/// equality-correlated) — that split happens in the Validator, based on whether the inner query's WHERE
/// references an outer-scope alias. Must resolve to an aggregate query with no explicit GROUP BY (N4's
/// GROUP BY is synthesized at validation time by decorrelation — never written by the SQL author). Planner
/// rewrites every occurrence into a bound reference to a synthetic join stage's output column — see
/// Planner.RewriteScalarSubqueries.</summary>
internal sealed class ScalarSubqueryExpr(SelectQuery query, int line, int col) : Expr(line, col)
{
    public SelectQuery Query { get; } = query;
}

internal sealed class SelectItem(Expr expression, string? alias)
{
    public Expr Expression { get; } = expression;
    public string? Alias { get; } = alias;
}

internal sealed class SelectClause(bool isStar, List<SelectItem> items)
{
    public bool IsStar { get; } = isStar;
    public List<SelectItem> Items { get; } = items;
}

/// <summary>One FROM/JOIN item — either a plain named source (stream/table) or a derived table (plan 004
/// N1: `FROM ( SELECT ... ) alias`, and WITH-list CTEs, which desugar to this same node at parse time —
/// see Parser's CTE substitution pass). A single base type lets Validator/Planner walk FROM/JOIN sources
/// uniformly regardless of which kind resolved there.</summary>
internal abstract class FromItem(string alias, int line, int col)
{
    public string Alias { get; } = alias;
    public int Line { get; } = line;
    public int Column { get; } = col;
}

internal sealed class NamedSource(string name, string alias, int line, int col) : FromItem(alias, line, col)
{
    public string Name { get; } = name;
}

/// <summary>A parenthesized derived table `( SELECT ... ) alias`, or a WITH-list CTE reference already
/// substituted in-place by the parser (see Parser.SubstituteCtes) — either way, by the time Validator sees
/// this node, it is a fully self-contained inner query with no outer-scope visibility (N1 derived tables
/// are uncorrelated; correlation is N4 territory and doesn't reuse this node).</summary>
internal sealed class DerivedSource(SelectQuery query, string alias, int line, int col) : FromItem(alias, line, col)
{
    public SelectQuery Query { get; } = query;
}

/// <summary>Plan 002 L2: `UNNEST(expr) AS alias` — either the JOIN-position form (`JOIN UNNEST(...) AS l`,
/// `CROSS JOIN UNNEST(...) AS l`) or the comma form (`FROM src, UNNEST(...) AS l`), which the parser
/// desugars into this same node at parse time (see Parser.ParseSelectQuery's comma-UNNEST loop) — both
/// forms produce a <see cref="JoinClause"/> of <see cref="JoinKind.Unnest"/> wrapping one of these.
/// <see cref="Expr"/> is evaluated once per input row (against whatever real FROM/JOIN sources precede it
/// — see Validator.ResolveUnnestJoin: an UNNEST argument may reference only real sources, never another
/// UNNEST alias) and must yield a JSON array; the alias binds to ONE ELEMENT per output row (element
/// typing is dynamic — see WorkingRow/ExpressionEvaluator: the alias behaves as a single Json-kind
/// pseudo-column, addressed only via '->'/'->>' , never 'alias.field' dot access).</summary>
internal sealed class UnnestSource(Expr expr, string alias, int line, int col) : FromItem(alias, line, col)
{
    public Expr Expr { get; } = expr;
}

/// <summary>Semi/Anti/Scalar are never produced by the parser (ParseJoinClause only yields Inner/Left/
/// Right/Full/Cross/Unnest) — they're synthesized by Planner at plan-time when rewriting a WHERE-position
/// IN/EXISTS predicate (Semi = IN/EXISTS, Anti = NOT IN/NOT EXISTS) or a scalar subquery expression
/// (Scalar, N3/N4) into an extra join stage appended after the query's real joins. See
/// Planner.RewriteWhereForSubqueryPredicates / RewriteScalarSubqueries and Runtime/Ops/TableSemiAntiOp.cs
/// / Runtime/Ops/PipelineSubqueryOp.cs. Unnest (plan 002 L2) IS parser-produced (see UnnestSource) — it
/// sits in the same JoinClause chain as an ordinary join but has no ON/WITHIN and no right-hand driving
/// source of its own (see Runtime/Ops/PipelineUnnestOp.cs / TableUnnestOp.cs).</summary>
internal enum JoinKind { Inner, Left, Right, Full, Cross, Semi, Anti, Scalar, Unnest }

internal sealed class JoinClause(JoinKind kind, FromItem source, TimeSpan? within, Expr? on, int line, int col)
{
    public JoinKind Kind { get; } = kind;
    public FromItem Source { get; } = source;
    public TimeSpan? Within { get; } = within;
    public Expr? On { get; } = on;
    public int Line { get; } = line;
    public int Column { get; } = col;
}

internal sealed class FromClause(FromItem source, List<JoinClause> joins)
{
    public FromItem Source { get; } = source;
    public List<JoinClause> Joins { get; } = joins;
}

internal abstract class WindowSpec;

internal sealed class TumblingWindowSpec(TimeSpan size) : WindowSpec
{
    public TimeSpan Size { get; } = size;
}

internal sealed class HoppingWindowSpec(TimeSpan size, TimeSpan advance) : WindowSpec
{
    public TimeSpan Size { get; } = size;
    public TimeSpan Advance { get; } = advance;
}

internal sealed class SessionWindowSpec(TimeSpan gap) : WindowSpec
{
    public TimeSpan Gap { get; } = gap;
}

internal enum EmitMode { Final, Changes }

/// <summary>Plan 008 W3: `SELECT ... UNION [ALL] SELECT ... [UNION [ALL] SELECT ...]` — the first AST node
/// that sits ABOVE a single <see cref="SelectQuery"/> (see Sql/Parser.cs's ParseTopLevel loop). A whole
/// statement's set-operation chain is flattened into one node: every branch shares the SAME
/// <see cref="All"/>-ness (mixing bare UNION and UNION ALL within one chain is rejected by the parser with
/// its own diagnostic — see Parser.ParseSetOperationOrSelect — rather than silently picking one). Accepted
/// in exactly two grammar positions (v1 scope): top level (this is literally the parser's own top-level
/// result) and derived-table position (`FROM ( ... ) alias` — see <see cref="DerivedSetOperationSource"/>).
/// Rejected with a positioned diagnostic inside IN/EXISTS/scalar subqueries (see ParseComparison/
/// ParseExistsBody/ParsePrimary's own UNION checks) — those synthesize their own joins and are the
/// highest-risk surface for no benefit (plan 008's own risk note).
///
/// <see cref="All"/> == true is UNION ALL (both pipeline and table mode); false is UNION (distinct,
/// table mode only — pipeline mode rejects it with a diagnostic naming UNION ALL as the fix, since pipeline
/// mode has no Z-set weights to dedup with and an unbounded distinct over an unbounded stream is unbounded
/// state — see Sql/Validator.cs's set-operation validation and DESIGN.md §D11).</summary>
internal sealed class SetOperationQuery(bool all, List<SelectQuery> branches, int line, int col)
{
    public bool All { get; } = all;
    public List<SelectQuery> Branches { get; } = branches;
    public int Line { get; } = line;
    public int Column { get; } = col;
}

/// <summary>Plan 008 W3: derived-table-position set operation — `FROM ( SELECT ... UNION [ALL] SELECT ... )
/// alias`. Parallels <see cref="DerivedSource"/> (a plain derived SELECT) but wraps a
/// <see cref="SetOperationQuery"/> instead of a single <see cref="SelectQuery"/> — kept as its own FromItem
/// subtype (rather than widening DerivedSource itself) so the far more common plain-derived-table path's
/// shape and every existing call site touching DerivedSource.Query stays completely unchanged.</summary>
internal sealed class DerivedSetOperationSource(SetOperationQuery setOp, string alias, int line, int col) : FromItem(alias, line, col)
{
    public SetOperationQuery SetOp { get; } = setOp;
}

internal sealed class SelectQuery(
    SelectClause select,
    FromClause from,
    Expr? where,
    List<Expr>? groupBy,
    WindowSpec? window,
    EmitMode? emit,
    int? emitLine,
    int? emitColumn,
    int? groupByLine,
    int? groupByColumn,
    int? windowLine = null,
    int? windowColumn = null,
    List<Expr>? latestBy = null,
    int? latestByLine = null,
    int? latestByColumn = null)
{
    public SelectClause Select { get; } = select;
    public FromClause From { get; } = from;
    public Expr? Where { get; } = where;
    public List<Expr>? GroupBy { get; } = groupBy;
    public WindowSpec? Window { get; } = window;
    public EmitMode? Emit { get; } = emit;
    public int? EmitLine { get; } = emitLine;
    public int? EmitColumn { get; } = emitColumn;
    public int? GroupByLine { get; } = groupByLine;
    public int? GroupByColumn { get; } = groupByColumn;
    public int? WindowLine { get; } = windowLine;
    public int? WindowColumn { get; } = windowColumn;

    /// <summary>Plan 002 L3 (deferred sugar, landed alongside L2): `LATEST BY (col[, col...])` —
    /// table-mode-only clause (see Validator's "LATEST BY is table-mode only" diagnostic), mutually
    /// exclusive with GROUP BY/WINDOW/aggregates. Null when absent.</summary>
    public List<Expr>? LatestBy { get; } = latestBy;
    public int? LatestByLine { get; } = latestByLine;
    public int? LatestByColumn { get; } = latestByColumn;
}
