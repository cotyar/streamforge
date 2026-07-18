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
    public static bool IsAggregate(string name) => All.Contains(name, StringComparer.OrdinalIgnoreCase);
}

internal sealed class AggregateCallExpr(string name, Expr? arg, bool isStar, int line, int col) : Expr(line, col)
{
    public string Name { get; } = name.ToUpperInvariant();
    public Expr? Arg { get; } = arg;
    public bool IsStar { get; } = isStar;
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

internal enum JoinKind { Inner, Left, Right, Full, Cross }

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
    int? windowColumn = null)
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
}
