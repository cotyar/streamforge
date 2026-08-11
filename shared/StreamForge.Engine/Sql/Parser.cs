namespace StreamForge.Engine.Sql;

internal sealed class ParseException(SqlDiagnostic diagnostic) : Exception(diagnostic.Message)
{
    public SqlDiagnostic Diagnostic { get; } = diagnostic;
}

/// <summary>Recursive-descent parser with a Pratt-style precedence chain for expressions.
/// Case-insensitive keywords; identifiers preserve source casing. Never throws outward —
/// <see cref="Parse"/> catches <see cref="ParseException"/> and turns it into a diagnostic.</summary>
internal sealed class Parser
{
    // Identifiers that must not be swallowed as an implicit (AS-less) alias.
    private static readonly HashSet<string> ClauseKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "WINDOW", "EMIT", "ON", "WITHIN", "FROM",
        // Plan 004 N2: reserved so `expr IN (...)`/`EXISTS (...)` never get mistaken for an AS-less alias.
        "IN", "EXISTS",
        // Plan 002 L2/L3: reserved so `FROM src UNNEST(...)`/`... LATEST BY (...)` never get mistaken for
        // an AS-less alias (mirrors how JOIN/WHERE/GROUP are already reserved above).
        "UNNEST", "LATEST",
        // Plan 008 W3: reserved so `SELECT a FROM t UNION ...` never swallows UNION as an AS-less alias
        // for `a` (a silent mis-parse) — see ParseSelectOrSetOperation.
        "UNION",
    };

    private readonly List<Token> _tokens;
    private int _pos;

    private Parser(List<Token> tokens) => _tokens = tokens;

    /// <summary>Pre-008 entry point — kept byte-for-byte behavior-preserving for every non-set-operation
    /// input (every existing caller/test predates plan 008 W3 and never feeds UNION SQL through this
    /// method). A thin compatibility wrapper over <see cref="ParseStatement"/>: for a plain statement it's
    /// exactly the old (query, diagnostics) shape; for a top-level set operation (which this 2-tuple has no
    /// room to represent) it returns a null Query with whatever diagnostics ParseStatement produced — no
    /// existing caller exercises that path (a set operation always parses successfully as a SetOp when the
    /// SQL is well-formed, so this fallback only engages for callers that don't yet know how to ask for
    /// one). New code should call <see cref="ParseStatement"/> instead.</summary>
    public static (SelectQuery? Query, List<SqlDiagnostic> Diagnostics) Parse(List<Token> tokens)
    {
        var (query, setOp, diags) = ParseStatement(tokens);
        return setOp is not null ? (null, diags) : (query, diags);
    }

    /// <summary>Plan 008 W3: exactly one of <c>Query</c>/<c>SetOp</c> is non-null on success — a plain
    /// statement parses to <c>Query</c>; a top-level `SELECT ... UNION [ALL] SELECT ...` chain (optionally
    /// under a WITH list) parses to <c>SetOp</c> instead. Both null (with a non-empty Diagnostics) is the
    /// existing failure shape. Planner.cs/TablePlanner.cs call this (not the legacy <see cref="Parse"/>).</summary>
    public static (SelectQuery? Query, SetOperationQuery? SetOp, List<SqlDiagnostic> Diagnostics) ParseStatement(List<Token> tokens)
    {
        var parser = new Parser(tokens);
        try
        {
            var (query, setOp) = parser.ParseTopLevel();
            if (parser.Current.Kind != TokenKind.EndOfInput)
            {
                throw parser.Error(parser.Current, $"Unexpected token '{parser.Current.Text}'");
            }
            return (query, setOp, []);
        }
        catch (ParseException ex)
        {
            return (null, null, [ex.Diagnostic]);
        }
    }

    // ------------------------------------------------------------------
    // WITH (CTEs) — plan 004 N1: "CTEs desugar to derived tables at parse time (single mechanism
    // downstream)". Parsed as an ordered name -> body list, then substituted into every FROM/JOIN
    // NamedSource whose name matches a CTE name — in declaration order, so a CTE body may only see CTEs
    // declared strictly before it (no self/forward reference: that's recursion, out of scope per plan
    // 004's header, and gets a positioned diagnostic instead of silently mis-resolving).
    // ------------------------------------------------------------------

    private (SelectQuery? Query, SetOperationQuery? SetOp) ParseTopLevel()
    {
        if (!Current.IsKeyword("WITH")) return ParseSelectOrSetOperation();

        Advance(); // WITH
        var cteNames = new List<string>();
        var cteBodies = new List<SelectQuery>();
        var ctePositions = new List<(int Line, int Column)>();

        while (true)
        {
            var nameTok = ExpectIdentifierToken();
            if (cteNames.Contains(nameTok.Text, StringComparer.OrdinalIgnoreCase))
            {
                throw Error(nameTok, $"Duplicate CTE name '{nameTok.Text}' in WITH list");
            }
            ExpectKeyword("AS");
            ExpectSymbol("(");
            var body = ParseSelectQuery();
            ExpectSymbol(")");
            cteNames.Add(nameTok.Text);
            cteBodies.Add(body);
            ctePositions.Add((nameTok.Line, nameTok.Column));

            if (Current.IsSymbol(",")) { Advance(); continue; }
            break;
        }

        // Plan 008 W3: the main query following WITH may itself be a UNION chain — CTE substitution must
        // recurse into EVERY branch (see SetOperationQuery's own doc comment on "WITH ... UNION ... must
        // keep working").
        var (mainQuery, mainSetOp) = ParseSelectOrSetOperation();

        // Desugar in declaration order: CTE i's body may only reference CTE 0..i-1 (already desugared);
        // referencing itself or a later CTE name is the recursion this dialect rejects.
        var allCteNames = new HashSet<string>(cteNames, StringComparer.OrdinalIgnoreCase);
        var resolved = new Dictionary<string, SelectQuery>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < cteNames.Count; i++)
        {
            resolved[cteNames[i]] = SubstituteCtes(cteBodies[i], resolved, allCteNames);
        }

        if (mainSetOp is not null)
        {
            var branches = mainSetOp.Branches.Select(b => SubstituteCtes(b, resolved, allCteNames)).ToList();
            return (null, new SetOperationQuery(mainSetOp.All, branches, mainSetOp.Line, mainSetOp.Column));
        }
        return (SubstituteCtes(mainQuery!, resolved, allCteNames), null);
    }

    /// <summary>Plan 008 W3: `SELECT ... [UNION [ALL|DISTINCT] SELECT ...]*` — a single SelectQuery when no
    /// UNION follows (the overwhelmingly common case), else a flattened <see cref="SetOperationQuery"/>.
    /// Every UNION in the chain must share the same ALL-ness: `UNION DISTINCT` is accepted as standard-SQL
    /// sugar for plain `UNION` (same as the bare keyword), but mixing e.g. `a UNION ALL b UNION c` is
    /// rejected with its own positioned diagnostic (pointing at the mismatched UNION keyword) rather than
    /// silently preferring one interpretation — <see cref="SetOperationQuery"/> carries a single <c>All</c>
    /// flag for the whole chain, so there is no per-branch operator to fall back to anyway.</summary>
    private (SelectQuery? Query, SetOperationQuery? SetOp) ParseSelectOrSetOperation()
    {
        var first = ParseSelectQuery();
        if (!Current.IsKeyword("UNION")) return (first, null);

        var chainTok = Current;
        var branches = new List<SelectQuery> { first };
        bool? all = null;

        while (Current.IsKeyword("UNION"))
        {
            var opTok = Advance(); // 'UNION'
            bool thisAll = MatchKeyword("ALL");
            if (!thisAll) MatchKeyword("DISTINCT"); // 'UNION DISTINCT' is sugar for plain 'UNION'

            if (all is null)
            {
                all = thisAll;
            }
            else if (all != thisAll)
            {
                throw Error(opTok, "Mixing UNION and UNION ALL in the same statement is not supported — use the same set-operation kind throughout the chain");
            }

            branches.Add(ParseSelectQuery());
        }

        return (null, new SetOperationQuery(all!.Value, branches, chainTok.Line, chainTok.Column));
    }

    /// <summary>Rewrites every FROM/JOIN NamedSource in <paramref name="query"/> (recursing into any
    /// already-nested DerivedSource) whose name matches a resolved CTE into a DerivedSource wrapping that
    /// CTE's (already-desugared) body. A name that matches <paramref name="allCteNames"/> but is NOT yet in
    /// <paramref name="resolved"/> is a self/forward reference — recursion, rejected here with a positioned
    /// diagnostic naming the offending reference.</summary>
    private SelectQuery SubstituteCtes(SelectQuery query, IReadOnlyDictionary<string, SelectQuery> resolved, HashSet<string> allCteNames)
    {
        var newFrom = new FromClause(
            SubstituteFromItem(query.From.Source, resolved, allCteNames),
            query.From.Joins.Select(j => new JoinClause(
                j.Kind, SubstituteFromItem(j.Source, resolved, allCteNames), j.Within,
                j.On is null ? null : SubstituteCtesInExpr(j.On, resolved, allCteNames), j.Line, j.Column)).ToList());

        // Plan 004 N2/N3/N4: a CTE reference can ALSO appear inside a WHERE-position IN/EXISTS predicate's
        // or a scalar subquery expression's OWN nested SelectQuery (e.g. `WHERE symbol IN (SELECT symbol
        // FROM hot)`) — those inner SelectQuery nodes need the SAME CTE substitution pass applied to them,
        // recursively, or a CTE name used only inside a subquery predicate would never resolve (it's not
        // reachable through FROM/JOIN's own SubstituteFromItem walk at all). Select items and WHERE both
        // need this; GROUP BY expressions can't syntactically contain one of these subquery forms in this
        // dialect but are walked too for robustness against future grammar growth.
        var newWhere = query.Where is null ? null : SubstituteCtesInExpr(query.Where, resolved, allCteNames);
        var newSelect = new SelectClause(query.Select.IsStar,
            query.Select.Items.Select(i => new SelectItem(SubstituteCtesInExpr(i.Expression, resolved, allCteNames), i.Alias)).ToList());
        var newGroupBy = query.GroupBy?.Select(g => SubstituteCtesInExpr(g, resolved, allCteNames)).ToList();

        return new SelectQuery(newSelect, newFrom, newWhere, newGroupBy, query.Window, query.Emit,
            query.EmitLine, query.EmitColumn, query.GroupByLine, query.GroupByColumn, query.WindowLine, query.WindowColumn,
            query.LatestBy, query.LatestByLine, query.LatestByColumn);
    }

    /// <summary>Walks an expression tree substituting CTE references inside any nested SelectQuery it
    /// carries (InSubqueryExpr/ExistsExpr/ScalarSubqueryExpr) — see SubstituteCtes's doc comment on why
    /// this is needed at all. Every other expression shape just recurses structurally.</summary>
    private Expr SubstituteCtesInExpr(Expr e, IReadOnlyDictionary<string, SelectQuery> resolved, HashSet<string> allCteNames)
    {
        switch (e)
        {
            case InSubqueryExpr ins:
                return new InSubqueryExpr(SubstituteCtesInExpr(ins.Left, resolved, allCteNames), SubstituteCtes(ins.Subquery, resolved, allCteNames), ins.Negated, ins.Line, ins.Column);
            case ExistsExpr ex:
                return new ExistsExpr(SubstituteCtes(ex.Subquery, resolved, allCteNames), ex.Negated, ex.Line, ex.Column);
            case ScalarSubqueryExpr sse:
                return new ScalarSubqueryExpr(SubstituteCtes(sse.Query, resolved, allCteNames), sse.Line, sse.Column);
            case UnaryExpr u:
                return new UnaryExpr(u.Op, SubstituteCtesInExpr(u.Operand, resolved, allCteNames), u.Line, u.Column);
            case BinaryExpr b:
                return new BinaryExpr(b.Op, SubstituteCtesInExpr(b.Left, resolved, allCteNames), SubstituteCtesInExpr(b.Right, resolved, allCteNames), b.Line, b.Column);
            case FunctionCallExpr f:
                return new FunctionCallExpr(f.Name, f.Args.Select(a => SubstituteCtesInExpr(a, resolved, allCteNames)).ToList(), f.Line, f.Column);
            case AggregateCallExpr agg:
                return agg.Arg is null ? agg : new AggregateCallExpr(agg.Name, SubstituteCtesInExpr(agg.Arg, resolved, allCteNames), agg.IsStar, agg.Line, agg.Column);
            case JsonAccessExpr j:
                return new JsonAccessExpr(SubstituteCtesInExpr(j.Left, resolved, allCteNames), j.ReturnText, j.Key, j.Line, j.Column);
            default:
                return e;
        }
    }

    private FromItem SubstituteFromItem(FromItem item, IReadOnlyDictionary<string, SelectQuery> resolved, HashSet<string> allCteNames)
    {
        switch (item)
        {
            case NamedSource ns:
                if (resolved.TryGetValue(ns.Name, out var body))
                {
                    return new DerivedSource(body, ns.Alias, ns.Line, ns.Column);
                }
                if (allCteNames.Contains(ns.Name))
                {
                    throw Error(new Token(TokenKind.Identifier, ns.Name, ns.Line, ns.Column), $"Recursive or forward CTE reference '{ns.Name}' is not supported — a CTE may only reference CTEs declared earlier in the same WITH list");
                }
                return ns;
            case DerivedSource ds:
                return new DerivedSource(SubstituteCtes(ds.Query, resolved, allCteNames), ds.Alias, ds.Line, ds.Column);
            case DerivedSetOperationSource dus:
                // Plan 008 W3: derived-table-position set operation — recurse CTE substitution into every
                // branch, same reasoning as the top-level ParseTopLevel case.
                var branches = dus.SetOp.Branches.Select(b => SubstituteCtes(b, resolved, allCteNames)).ToList();
                return new DerivedSetOperationSource(new SetOperationQuery(dus.SetOp.All, branches, dus.SetOp.Line, dus.SetOp.Column), dus.Alias, dus.Line, dus.Column);
            default:
                return item;
        }
    }

    private Token Current => _tokens[_pos];

    private Token Advance()
    {
        var t = _tokens[_pos];
        if (_pos < _tokens.Count - 1) _pos++;
        return t;
    }

    private ParseException Error(Token at, string message) => new(new SqlDiagnostic(message, at.Line, at.Column));

    /// <summary>Plan 008 W3: IN/EXISTS/scalar-subquery position is explicitly OUT of set-operation v1 scope
    /// (those paths synthesize their own joins and are the highest-risk surface for no benefit — see
    /// SetOperationQuery's doc comment) — called right after each of those three inner ParseSelectQuery()
    /// calls, before the closing ')', so a UNION there gets its own clear, positioned diagnostic instead of
    /// falling through to whatever generic "expected ')'" message the caller's own ExpectSymbol would
    /// otherwise produce.</summary>
    private void RejectUnionInSubqueryPosition(string context)
    {
        if (Current.IsKeyword("UNION"))
        {
            throw Error(Current, $"UNION is not supported inside {context} in this dialect — restructure the query without a set operation there");
        }
    }

    private void ExpectKeyword(string keyword)
    {
        if (!Current.IsKeyword(keyword)) throw Error(Current, $"Expected '{keyword}', got '{Current.Text}'");
        Advance();
    }

    private bool MatchKeyword(string keyword)
    {
        if (Current.IsKeyword(keyword)) { Advance(); return true; }
        return false;
    }

    private void ExpectSymbol(string symbol)
    {
        if (!Current.IsSymbol(symbol)) throw Error(Current, $"Expected '{symbol}', got '{Current.Text}'");
        Advance();
    }

    private Token ExpectIdentifierToken()
    {
        if (Current.Kind != TokenKind.Identifier) throw Error(Current, $"Expected an identifier, got '{Current.Text}'");
        return Advance();
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    private SelectQuery ParseSelectQuery()
    {
        ExpectKeyword("SELECT");
        var select = ParseSelectClause();
        ExpectKeyword("FROM");
        var from = ParseFromItem();
        var joins = new List<JoinClause>();
        // Plan 002 L2: comma form `FROM src alias, UNNEST(expr) AS l[, UNNEST(expr2) AS l2, ...]` desugars
        // to the JOIN form right here — every comma-separated item after the primary FROM source must be
        // an UNNEST (this dialect has no general comma cross-join); real JOINs (if any) follow afterward.
        while (Current.IsSymbol(","))
        {
            var commaTok = Advance();
            if (!Current.IsKeyword("UNNEST"))
            {
                throw Error(Current, "Expected UNNEST after ',' in FROM — this dialect's only comma-FROM sugar is 'FROM src, UNNEST(expr) AS alias' (plan 002 L2)");
            }
            var unnest = ParseUnnestSource();
            joins.Add(new JoinClause(JoinKind.Unnest, unnest, null, null, commaTok.Line, commaTok.Column));
        }
        while (IsJoinStart()) joins.Add(ParseJoinClause());
        var fromClause = new FromClause(from, joins);

        Expr? where = null;
        if (MatchKeyword("WHERE")) where = ParseOr();

        List<Expr>? latestBy = null;
        int? latestByLine = null, latestByCol = null;
        if (Current.IsKeyword("LATEST"))
        {
            var tok = Current;
            latestByLine = tok.Line; latestByCol = tok.Column;
            Advance();
            ExpectKeyword("BY");
            ExpectSymbol("(");
            latestBy = [ParseOr()];
            while (Current.IsSymbol(",")) { Advance(); latestBy.Add(ParseOr()); }
            ExpectSymbol(")");
        }

        List<Expr>? groupBy = null;
        int? gbLine = null, gbCol = null;
        if (Current.IsKeyword("GROUP"))
        {
            var tok = Current;
            gbLine = tok.Line; gbCol = tok.Column;
            Advance();
            ExpectKeyword("BY");
            // DuckDB/Snowflake sugar: `GROUP BY ALL` expands to every non-aggregate select-list expression,
            // in select-list order. "ALL" is contextual, not a reserved word — only checked right here,
            // immediately after GROUP BY — so `SELECT all FROM t` (a column literally named "all") still
            // parses as a column reference everywhere else, including in the select list itself.
            if (Current.IsKeyword("ALL"))
            {
                var allTok = Current;
                Advance();
                if (select.IsStar)
                {
                    // `SELECT *` hasn't been expanded into columns yet — that only happens at plan time —
                    // so ALL has nothing to enumerate here. Reject with a positioned diagnostic rather than
                    // silently degrading to "no GROUP BY" (which would change aggregation semantics).
                    throw Error(allTok, "GROUP BY ALL cannot be used with 'SELECT *' — star expansion happens after grouping is resolved; list the grouping columns explicitly instead of '*'");
                }
                // Reuse the SAME Expr instances as the select items (not clones): Validator.StructurallyEqual
                // and AssignGroupByIndexes match trivially on shared references, and this is exactly what
                // ContainsAggregate (internal static, same assembly/namespace) is for — it already treats a
                // scalar subquery as aggregate-like, so `GROUP BY ALL` correctly excludes one here too.
                groupBy = select.Items.Where(i => !Validator.ContainsAggregate(i.Expression)).Select(i => i.Expression).ToList();
                // Empty expansion (e.g. `SELECT COUNT(*) FROM t GROUP BY ALL`) means every select item is
                // an aggregate — fall back to null (the implicit single global group), matching what writing
                // no GROUP BY at all produces; an empty non-null list would trip the `GroupBy is not null`
                // gates in TablePlanner/Validator/TableDataflowBuilder.
                if (groupBy.Count == 0) groupBy = null;
            }
            else
            {
                groupBy = [ParseOr()];
                while (Current.IsSymbol(",")) { Advance(); groupBy.Add(ParseOr()); }
            }
        }

        WindowSpec? window = null;
        int? windowLine = null, windowCol = null;
        if (Current.IsKeyword("WINDOW"))
        {
            var tok = Current;
            windowLine = tok.Line; windowCol = tok.Column;
            Advance();
            window = ParseWindowSpec();
        }

        EmitMode? emit = null;
        int? emitLine = null, emitCol = null;
        if (Current.IsKeyword("EMIT"))
        {
            var tok = Current;
            emitLine = tok.Line; emitCol = tok.Column;
            Advance();
            if (MatchKeyword("CHANGES")) emit = EmitMode.Changes;
            else if (MatchKeyword("FINAL")) emit = EmitMode.Final;
            else throw Error(Current, $"Expected CHANGES or FINAL after EMIT, got '{Current.Text}'");
        }

        return new SelectQuery(select, fromClause, where, groupBy, window, emit, emitLine, emitCol, gbLine, gbCol, windowLine, windowCol, latestBy, latestByLine, latestByCol);
    }

    private SelectClause ParseSelectClause()
    {
        if (Current.IsSymbol("*"))
        {
            Advance();
            return new SelectClause(isStar: true, items: []);
        }

        var items = new List<SelectItem>();
        items.Add(ParseSelectItem());
        while (Current.IsSymbol(","))
        {
            Advance();
            items.Add(ParseSelectItem());
        }
        return new SelectClause(isStar: false, items: items);
    }

    private SelectItem ParseSelectItem()
    {
        // Qualified star `alias.*` — only recognized here (top-level select-item position), not inside
        // general expressions. Peek three tokens ahead without consuming so a plain `alias.field` (the
        // common case) falls through to the normal ParseOr() path untouched.
        if (Current.Kind == TokenKind.Identifier && PeekIsSymbol(1, ".") && PeekIsSymbol(2, "*"))
        {
            var aliasTok = Advance(); // alias identifier
            Advance(); // '.'
            Advance(); // '*'
            var qualifiedStar = new QualifiedStarExpr(aliasTok.Text, aliasTok.Line, aliasTok.Column);

            // Postgres rejects `t.* AS x` (and an implicit alias makes just as little sense — a star
            // expands to many columns, so it can't be renamed to a single identifier).
            if (Current.IsKeyword("AS") || (Current.Kind == TokenKind.Identifier && !ClauseKeywords.Contains(Current.Text)))
            {
                throw Error(Current, "'alias.*' cannot be given an alias");
            }
            return new SelectItem(qualifiedStar, null);
        }

        var expr = ParseOr();
        string? alias = null;
        if (MatchKeyword("AS"))
        {
            alias = ExpectIdentifierToken().Text;
        }
        else if (Current.Kind == TokenKind.Identifier && !ClauseKeywords.Contains(Current.Text))
        {
            alias = Advance().Text;
        }
        return new SelectItem(expr, alias);
    }

    /// <summary>Looks ahead `offset` tokens from the current position (0 = Current) without consuming,
    /// clamped to the final token (EndOfInput) so callers never index past the token list.</summary>
    private Token PeekAt(int offset)
    {
        int idx = _pos + offset;
        return _tokens[idx < _tokens.Count ? idx : _tokens.Count - 1];
    }

    private bool PeekIsSymbol(int offset, string symbol) => PeekAt(offset).IsSymbol(symbol);

    private bool PeekIsKeyword(int offset, string keyword) => PeekAt(offset).IsKeyword(keyword);

    private NamedSource ParseSourceRef()
    {
        var nameTok = ExpectIdentifierToken();
        string alias = nameTok.Text;
        if (MatchKeyword("AS"))
        {
            alias = ExpectIdentifierToken().Text;
        }
        else if (Current.Kind == TokenKind.Identifier && !ClauseKeywords.Contains(Current.Text))
        {
            alias = Advance().Text;
        }
        return new NamedSource(nameTok.Text, alias, nameTok.Line, nameTok.Column);
    }

    /// <summary>FROM/JOIN item: either a plain named source, or a derived table `( SELECT ... ) alias`
    /// (plan 004 N1) — an alias is mandatory for a derived table (Postgres's own rule; also required here
    /// since the derived table has no name of its own to fall back on).</summary>
    private FromItem ParseFromItem()
    {
        if (Current.IsKeyword("UNNEST"))
        {
            // UNNEST can never be the FIRST FROM item (plan 002 L2: its expr must reference a real,
            // already-in-scope FROM source) — see ParseSelectQuery's comma-loop and ParseJoinClause's own
            // UNNEST branch for the two positions where it IS allowed.
            throw Error(Current, "UNNEST cannot be the first FROM item — it must follow a real source, e.g. 'FROM src, UNNEST(expr) AS alias' or '... JOIN UNNEST(expr) AS alias'");
        }
        if (Current.IsSymbol("("))
        {
            var openTok = Advance();
            // Plan 008 W3: derived-table position accepts a set operation too — `FROM ( SELECT ... UNION
            // [ALL] SELECT ... ) alias` — see SetOperationQuery's doc comment on the v1 scope of where a
            // set operation is legal.
            var (inner, innerSetOp) = ParseSelectOrSetOperation();
            ExpectSymbol(")");
            string alias = ParseMandatoryDerivedAlias();

            return innerSetOp is not null
                ? new DerivedSetOperationSource(innerSetOp, alias, openTok.Line, openTok.Column)
                : new DerivedSource(inner!, alias, openTok.Line, openTok.Column);
        }
        return ParseSourceRef();
    }

    /// <summary>Shared by both derived-table-position shapes (plain and set-operation) — Postgres itself
    /// requires an alias for a derived table; also required here since neither has a name of its own to
    /// fall back on.</summary>
    private string ParseMandatoryDerivedAlias()
    {
        if (MatchKeyword("AS"))
        {
            return ExpectIdentifierToken().Text;
        }
        if (Current.Kind == TokenKind.Identifier && !ClauseKeywords.Contains(Current.Text))
        {
            return Advance().Text;
        }
        throw Error(Current, "A derived table (subquery in FROM/JOIN) requires an alias");
    }

    private bool IsJoinStart() =>
        Current.IsKeyword("JOIN") || Current.IsKeyword("INNER") || Current.IsKeyword("LEFT") ||
        Current.IsKeyword("RIGHT") || Current.IsKeyword("FULL") || Current.IsKeyword("CROSS");

    /// <summary>Plan 002 L2: `UNNEST(expr) AS alias` (or bare `alias` — same implicit-alias sugar every
    /// other FROM item supports), used by both the comma form (ParseSelectQuery's loop) and the JOIN form
    /// (ParseJoinClause below). Alias is mandatory (no fallback name exists for an expression).</summary>
    private UnnestSource ParseUnnestSource()
    {
        var startTok = Current;
        ExpectKeyword("UNNEST");
        ExpectSymbol("(");
        var expr = ParseOr();
        ExpectSymbol(")");

        string alias;
        if (MatchKeyword("AS"))
        {
            alias = ExpectIdentifierToken().Text;
        }
        else if (Current.Kind == TokenKind.Identifier && !ClauseKeywords.Contains(Current.Text))
        {
            alias = Advance().Text;
        }
        else
        {
            throw Error(Current, "UNNEST requires an alias, e.g. UNNEST(expr) AS alias");
        }
        return new UnnestSource(expr, alias, startTok.Line, startTok.Column);
    }

    private JoinClause ParseJoinClause()
    {
        var startTok = Current;
        JoinKind kind;
        bool isOuterKind = false;
        if (MatchKeyword("CROSS")) { ExpectKeyword("JOIN"); kind = JoinKind.Cross; }
        else if (MatchKeyword("INNER")) { ExpectKeyword("JOIN"); kind = JoinKind.Inner; }
        else if (MatchKeyword("LEFT")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Left; isOuterKind = true; }
        else if (MatchKeyword("RIGHT")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Right; isOuterKind = true; }
        else if (MatchKeyword("FULL")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Full; isOuterKind = true; }
        else { ExpectKeyword("JOIN"); kind = JoinKind.Inner; }

        // Plan 002 L2: '[CROSS] JOIN UNNEST(expr) AS alias' — no ON, no WITHIN, no NULL-padding outer-join
        // variant (see UnnestSource's doc comment: "no LEFT UNNEST" is a deliberate dialect limitation).
        if (Current.IsKeyword("UNNEST"))
        {
            if (isOuterKind)
            {
                throw Error(startTok, "UNNEST may only be used with JOIN or CROSS JOIN, not LEFT/RIGHT/FULL — there is no LEFT UNNEST NULL-padding in this dialect");
            }
            var unnestSource = ParseUnnestSource();
            return new JoinClause(JoinKind.Unnest, unnestSource, null, null, startTok.Line, startTok.Column);
        }

        var source = ParseFromItem();

        TimeSpan? within = null;
        if (MatchKeyword("WITHIN")) within = ParseDuration();

        Expr? on = null;
        if (kind == JoinKind.Cross)
        {
            if (Current.IsKeyword("ON")) throw Error(Current, "CROSS JOIN may not have an ON clause");
        }
        else
        {
            ExpectKeyword("ON");
            on = ParseOr();
        }

        return new JoinClause(kind, source, within, on, startTok.Line, startTok.Column);
    }

    private TimeSpan ParseDuration()
    {
        if (Current.Kind != TokenKind.Number) throw Error(Current, $"Expected a duration amount, got '{Current.Text}'");
        var numTok = Advance();
        double n = numTok.DoubleValue ?? numTok.LongValue!.Value;
        var unitTok = ExpectIdentifierToken();
        return unitTok.Text.ToUpperInvariant() switch
        {
            "MILLISECOND" or "MILLISECONDS" => TimeSpan.FromMilliseconds(n),
            "SECOND" or "SECONDS" => TimeSpan.FromSeconds(n),
            "MINUTE" or "MINUTES" => TimeSpan.FromMinutes(n),
            "HOUR" or "HOURS" => TimeSpan.FromHours(n),
            _ => throw Error(unitTok, $"Expected a duration unit (MILLISECONDS|SECONDS|MINUTES|HOURS), got '{unitTok.Text}'"),
        };
    }

    private WindowSpec ParseWindowSpec()
    {
        if (MatchKeyword("TUMBLING"))
        {
            ExpectSymbol("(");
            ExpectKeyword("SIZE");
            var size = ParseDuration();
            ExpectSymbol(")");
            return new TumblingWindowSpec(size);
        }
        if (MatchKeyword("HOPPING"))
        {
            ExpectSymbol("(");
            ExpectKeyword("SIZE");
            var size = ParseDuration();
            ExpectSymbol(",");
            ExpectKeyword("ADVANCE");
            ExpectKeyword("BY");
            var advance = ParseDuration();
            ExpectSymbol(")");
            return new HoppingWindowSpec(size, advance);
        }
        if (MatchKeyword("SESSION"))
        {
            ExpectSymbol("(");
            ExpectKeyword("GAP");
            var gap = ParseDuration();
            ExpectSymbol(")");
            return new SessionWindowSpec(gap);
        }
        throw Error(Current, $"Expected TUMBLING, HOPPING, or SESSION, got '{Current.Text}'");
    }

    // ------------------------------------------------------------------
    // Expressions (Pratt-ish precedence chain)
    // OR < AND < NOT < comparisons < + - < * / % < unary minus < '->'/'->>'  (postfix) < primary
    // ------------------------------------------------------------------

    private Expr ParseOr()
    {
        var left = ParseAnd();
        while (Current.IsKeyword("OR"))
        {
            var tok = Advance();
            var right = ParseAnd();
            left = new BinaryExpr("OR", left, right, tok.Line, tok.Column);
        }
        return left;
    }

    private Expr ParseAnd()
    {
        var left = ParseNot();
        while (Current.IsKeyword("AND"))
        {
            var tok = Advance();
            var right = ParseNot();
            left = new BinaryExpr("AND", left, right, tok.Line, tok.Column);
        }
        return left;
    }

    private Expr ParseNot()
    {
        if (Current.IsKeyword("NOT"))
        {
            var tok = Advance();
            // 'NOT EXISTS (...)' — handled here (rather than as a generic UnaryExpr("NOT", ExistsExpr))
            // so ExistsExpr carries its own Negated flag directly, matching InSubqueryExpr's shape and
            // giving Planner one uniform (Negated: bool) field to rewrite from instead of two AST shapes.
            if (Current.IsKeyword("EXISTS"))
            {
                Advance(); // 'EXISTS'
                return ParseExistsBody(negated: true, tok.Line, tok.Column);
            }
            var operand = ParseNot();
            return new UnaryExpr("NOT", operand, tok.Line, tok.Column);
        }
        return ParseComparison();
    }

    /// <summary>Plan 004 N2: `( SELECT ... )` following an already-consumed 'EXISTS' (or 'NOT EXISTS')
    /// keyword — <paramref name="line"/>/<paramref name="col"/> is that keyword's own position (NOT's for
    /// the negated form, matching InSubqueryExpr's convention of pointing at the earliest keyword).</summary>
    private ExistsExpr ParseExistsBody(bool negated, int line, int col)
    {
        ExpectSymbol("(");
        var subquery = ParseSelectQuery();
        RejectUnionInSubqueryPosition("EXISTS (...)");
        ExpectSymbol(")");
        return new ExistsExpr(subquery, negated, line, col);
    }

    private static readonly string[] ComparisonOps = ["=", "!=", "<>", "<", "<=", ">", ">="];

    private Expr ParseComparison()
    {
        var left = ParseAdditive();

        // Plan 004 N2: '[NOT] IN ( SELECT ... )' — same precedence slot as the comparison operators below
        // (binds a left operand, produces a boolean). Only the subquery form is supported (no literal
        // list `IN (1, 2, 3)` grammar exists in this dialect yet); ExpectKeyword("SELECT") inside
        // ParseSelectQuery gives a clear, positioned diagnostic if a caller tries a literal list.
        bool inNegated = false;
        if (Current.IsKeyword("NOT") && PeekIsKeyword(1, "IN"))
        {
            inNegated = true;
            Advance(); // 'NOT'
        }
        if (Current.IsKeyword("IN"))
        {
            var tok = Advance(); // 'IN'
            ExpectSymbol("(");
            var subquery = ParseSelectQuery();
            RejectUnionInSubqueryPosition("IN (...)");
            ExpectSymbol(")");
            return new InSubqueryExpr(left, subquery, inNegated, tok.Line, tok.Column);
        }

        if (Current.Kind == TokenKind.Symbol && ComparisonOps.Contains(Current.Text))
        {
            var tok = Advance();
            var right = ParseAdditive();
            left = new BinaryExpr(tok.Text, left, right, tok.Line, tok.Column);
        }
        return left;
    }

    private Expr ParseAdditive()
    {
        var left = ParseMultiplicative();
        while (Current.IsSymbol("+") || Current.IsSymbol("-"))
        {
            var tok = Advance();
            var right = ParseMultiplicative();
            left = new BinaryExpr(tok.Text, left, right, tok.Line, tok.Column);
        }
        return left;
    }

    private Expr ParseMultiplicative()
    {
        var left = ParseUnary();
        while (Current.IsSymbol("*") || Current.IsSymbol("/") || Current.IsSymbol("%"))
        {
            var tok = Advance();
            var right = ParseUnary();
            left = new BinaryExpr(tok.Text, left, right, tok.Line, tok.Column);
        }
        return left;
    }

    private Expr ParseUnary()
    {
        if (Current.IsSymbol("-"))
        {
            var tok = Advance();
            var operand = ParseUnary();
            return new UnaryExpr("-", operand, tok.Line, tok.Column);
        }
        return ParsePostfix();
    }

    // Postgres JSON access — 'expr -> key' / 'expr ->> key', left-associative, chainable
    // (payload -> 'order' ->> 'symbol'), binding tighter than unary minus so that
    // '-a -> k' parses as '-(a -> k)', like ordinary member access.
    private Expr ParsePostfix()
    {
        var left = ParsePrimary();
        while (Current.IsSymbol("->") || Current.IsSymbol("->>"))
        {
            var tok = Advance();
            bool returnText = tok.Text == "->>";
            var key = ParseJsonKey();
            left = new JsonAccessExpr(left, returnText, key, tok.Line, tok.Column);
        }
        return left;
    }

    // Postgres allows an arbitrary expression on the right of '->'/'->>'; this dialect restricts it to a
    // string literal (object key) or an integer literal (0-based array index) — enough to express the
    // common `payload -> 'field'` / `payload -> 0` cases without needing a runtime-typed key.
    private Expr ParseJsonKey()
    {
        var tok = Current;
        if (tok.Kind == TokenKind.String)
        {
            Advance();
            return new StringLiteral(tok.StringValue!, tok.Line, tok.Column);
        }
        if (tok.Kind == TokenKind.Number && tok.LongValue is not null)
        {
            Advance();
            return new NumberLiteral(null, tok.LongValue, tok.Line, tok.Column);
        }
        throw Error(tok, $"'->'/'->>' right operand must be a string literal (object key) or integer literal (array index), got '{tok.Text}'");
    }

    private Expr ParsePrimary()
    {
        var tok = Current;
        switch (tok.Kind)
        {
            case TokenKind.Number:
                Advance();
                return new NumberLiteral(tok.DoubleValue, tok.LongValue, tok.Line, tok.Column);
            case TokenKind.String:
                Advance();
                return new StringLiteral(tok.StringValue!, tok.Line, tok.Column);
            case TokenKind.Symbol when tok.IsSymbol("("):
                Advance();
                // Plan 004 N3/N4: '( SELECT ... )' used as a value expression — a scalar subquery. Look-
                // ahead on the SELECT keyword alone disambiguates from a plain parenthesized expression
                // '(expr)': this dialect's expression grammar never starts a bare expression with SELECT.
                if (Current.IsKeyword("SELECT"))
                {
                    var subquery = ParseSelectQuery();
                    RejectUnionInSubqueryPosition("a scalar subquery");
                    ExpectSymbol(")");
                    return new ScalarSubqueryExpr(subquery, tok.Line, tok.Column);
                }
                var inner = ParseOr();
                ExpectSymbol(")");
                return inner;
            case TokenKind.Identifier:
                return ParseIdentifierPrimary();
            default:
                throw Error(tok, $"Expected an expression, got '{tok.Text}'");
        }
    }

    private Expr ParseIdentifierPrimary()
    {
        var tok = Advance();
        string name = tok.Text;

        if (string.Equals(name, "TRUE", StringComparison.OrdinalIgnoreCase)) return new BoolLiteral(true, tok.Line, tok.Column);
        if (string.Equals(name, "FALSE", StringComparison.OrdinalIgnoreCase)) return new BoolLiteral(false, tok.Line, tok.Column);
        if (string.Equals(name, "NULL", StringComparison.OrdinalIgnoreCase)) return new NullLiteral(tok.Line, tok.Column);
        if (string.Equals(name, "EXISTS", StringComparison.OrdinalIgnoreCase)) return ParseExistsBody(negated: false, tok.Line, tok.Column);

        if (Current.IsSymbol("("))
        {
            return ParseCall(name, tok.Line, tok.Column);
        }

        if (Current.IsSymbol("."))
        {
            Advance();
            var field = ExpectIdentifierToken();
            return new QualifiedIdentifier(name, field.Text, tok.Line, tok.Column);
        }

        return new Identifier(name, tok.Line, tok.Column);
    }

    private Expr ParseCall(string name, int line, int col)
    {
        ExpectSymbol("(");

        if (string.Equals(name, "COUNT", StringComparison.OrdinalIgnoreCase) && Current.IsSymbol("*"))
        {
            Advance();
            ExpectSymbol(")");
            return new AggregateCallExpr(name, null, isStar: true, line, col);
        }

        var args = new List<Expr>();
        if (!Current.IsSymbol(")"))
        {
            args.Add(ParseOr());
            while (Current.IsSymbol(","))
            {
                Advance();
                args.Add(ParseOr());
            }
        }
        ExpectSymbol(")");

        if (AggregateNames.IsAggregate(name))
        {
            var arg = args.Count > 0 ? args[0] : null;
            return new AggregateCallExpr(name, arg, isStar: false, line, col);
        }

        return new FunctionCallExpr(name, args, line, col);
    }
}
