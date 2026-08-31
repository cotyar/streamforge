namespace StreamsForge.Engine.Sql;

/// <summary>
/// Plan 016 wave 3-A — the implementation behind <see cref="SqlCompiler.ExtractReferences"/>: parse the
/// statement and collect the name of every <see cref="NamedSource"/> reachable from it, i.e. every
/// relation the SQL READS FROM. It lives in the Engine (and not in the caller that wants it) for exactly
/// one reason: the AST is <c>internal</c>, so nobody outside this assembly can walk it.
///
/// <para>This is a NAME SCAN, not a resolution: nothing here consults a schema dictionary, so a name that
/// no source/table will ever have is reported just the same. That is the point — the caller (AppCore's
/// import planner) asks about entities that do not exist yet, where <see cref="CompileResult.SourceNames"/>
/// cannot help because nothing has compiled.</para>
///
/// <para>CTEs need no special handling here and deliberately get none: <see cref="Parser"/> substitutes
/// every WITH-list CTE reference in place at PARSE time (see Parser.SubstituteCtes), so by the time this
/// walk runs a CTE is a <see cref="DerivedSource"/> and its name is simply not in the tree. That gives the
/// wanted answer for free — <c>WITH recent AS (SELECT … FROM trades) SELECT * FROM recent</c> reports
/// <c>trades</c>, never <c>recent</c> — with one honest edge: a CTE that is DECLARED but never referenced
/// is dropped by that same substitution, so the relations only its body reads are not reported. A query
/// does not read them either, so the omission matches what the statement actually does.</para>
///
/// <para>ponytail: a hand-rolled recursive switch, no visitor framework — the AST has one FromItem
/// hierarchy of four shapes and one Expr hierarchy of which only six carry children. If the grammar grows
/// a node that can hide a nested query (a set operation inside IN/EXISTS, a lateral join, INTERSECT/
/// EXCEPT), add a case here; the switch's <c>default</c> is "no children worth walking", which fails by
/// under-reporting rather than by throwing.</para>
/// </summary>
internal static class ReferenceExtractor
{
    /// <summary>Names in first-appearance order, ordinal-distinct; empty on any tokenize/parse failure and
    /// on null/blank input. See <see cref="SqlCompiler.ExtractReferences"/> for the caller-facing contract.</summary>
    public static IReadOnlyList<string> Extract(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return [];
        }

        var names = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            var (tokens, tokenDiags) = new Tokenizer(sql).Tokenize();
            if (tokenDiags.Exists(d => d.Severity == DiagnosticSeverity.Error))
            {
                return [];
            }

            var (query, setOp, parseDiags) = Parser.ParseStatement(tokens);
            if (parseDiags.Exists(d => d.Severity == DiagnosticSeverity.Error))
            {
                return [];
            }

            if (setOp is not null)
            {
                foreach (var branch in setOp.Branches)
                {
                    WalkQuery(branch, names, seen);
                }
            }
            else if (query is not null)
            {
                WalkQuery(query, names, seen);
            }
        }
        catch
        {
            // The parser signals syntax errors through diagnostics, but a tokenizer/parser bug on hostile
            // input must not become the caller's exception: an import DRY RUN that crashes on one broken
            // document is strictly worse than one that reports no dependencies for it.
            return [];
        }

        return names;
    }

    /// <summary>Clause order, so "first appearance" is a stable, explainable thing: FROM source, then each
    /// JOIN (its source, then its ON), then WHERE, then the SELECT list, then GROUP BY / LATEST BY. Not the
    /// order the clauses are typed in (SELECT is written first) — the order they read in.</summary>
    private static void WalkQuery(SelectQuery query, List<string> names, HashSet<string> seen)
    {
        WalkFromItem(query.From.Source, names, seen);
        foreach (var join in query.From.Joins)
        {
            WalkFromItem(join.Source, names, seen);
            WalkExpr(join.On, names, seen);
        }

        WalkExpr(query.Where, names, seen);
        foreach (var item in query.Select.Items)
        {
            WalkExpr(item.Expression, names, seen);
        }
        foreach (var g in query.GroupBy ?? [])
        {
            WalkExpr(g, names, seen);
        }
        foreach (var l in query.LatestBy ?? [])
        {
            WalkExpr(l, names, seen);
        }
    }

    private static void WalkFromItem(FromItem item, List<string> names, HashSet<string> seen)
    {
        switch (item)
        {
            case NamedSource ns:
                if (seen.Add(ns.Name))
                {
                    names.Add(ns.Name);
                }
                break;
            case DerivedSource ds:
                WalkQuery(ds.Query, names, seen);
                break;
            case DerivedSetOperationSource dso:
                foreach (var branch in dso.SetOp.Branches)
                {
                    WalkQuery(branch, names, seen);
                }
                break;
            case UnnestSource us:
                // UNNEST's argument reads from sources already in scope, but it is still an expression and
                // could carry a nested query; walking it costs one call.
                WalkExpr(us.Expr, names, seen);
                break;
        }
    }

    private static void WalkExpr(Expr? e, List<string> names, HashSet<string> seen)
    {
        switch (e)
        {
            case null:
                return;
            case InSubqueryExpr ins:
                WalkExpr(ins.Left, names, seen);
                WalkQuery(ins.Subquery, names, seen);
                break;
            case ExistsExpr ex:
                WalkQuery(ex.Subquery, names, seen);
                break;
            case ScalarSubqueryExpr sse:
                WalkQuery(sse.Query, names, seen);
                break;
            case UnaryExpr u:
                WalkExpr(u.Operand, names, seen);
                break;
            case BinaryExpr b:
                WalkExpr(b.Left, names, seen);
                WalkExpr(b.Right, names, seen);
                break;
            case JsonAccessExpr j:
                WalkExpr(j.Left, names, seen);
                WalkExpr(j.Key, names, seen);
                break;
            case FunctionCallExpr f:
                // CASE parses to a FunctionCallExpr too (see Parser.ParseCaseBody), so its branches are
                // covered by this one case.
                foreach (var a in f.Args)
                {
                    WalkExpr(a, names, seen);
                }
                break;
            case AggregateCallExpr agg:
                WalkExpr(agg.Arg, names, seen);
                WalkExpr(agg.Parameter, names, seen);
                break;
        }
    }
}
