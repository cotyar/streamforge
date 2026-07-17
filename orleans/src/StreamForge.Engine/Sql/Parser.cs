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
    };

    private readonly List<Token> _tokens;
    private int _pos;

    private Parser(List<Token> tokens) => _tokens = tokens;

    public static (SelectQuery? Query, List<SqlDiagnostic> Diagnostics) Parse(List<Token> tokens)
    {
        var parser = new Parser(tokens);
        try
        {
            var query = parser.ParseSelectQuery();
            if (parser.Current.Kind != TokenKind.EndOfInput)
            {
                throw parser.Error(parser.Current, $"Unexpected token '{parser.Current.Text}'");
            }
            return (query, []);
        }
        catch (ParseException ex)
        {
            return (null, [ex.Diagnostic]);
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
        var from = ParseSourceRef();
        var joins = new List<JoinClause>();
        while (IsJoinStart()) joins.Add(ParseJoinClause());
        var fromClause = new FromClause(from, joins);

        Expr? where = null;
        if (MatchKeyword("WHERE")) where = ParseOr();

        List<Expr>? groupBy = null;
        int? gbLine = null, gbCol = null;
        if (Current.IsKeyword("GROUP"))
        {
            var tok = Current;
            gbLine = tok.Line; gbCol = tok.Column;
            Advance();
            ExpectKeyword("BY");
            groupBy = [ParseOr()];
            while (Current.IsSymbol(",")) { Advance(); groupBy.Add(ParseOr()); }
        }

        WindowSpec? window = null;
        if (MatchKeyword("WINDOW")) window = ParseWindowSpec();

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

        return new SelectQuery(select, fromClause, where, groupBy, window, emit, emitLine, emitCol, gbLine, gbCol);
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

    private SourceRef ParseSourceRef()
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
        return new SourceRef(nameTok.Text, alias, nameTok.Line, nameTok.Column);
    }

    private bool IsJoinStart() =>
        Current.IsKeyword("JOIN") || Current.IsKeyword("INNER") || Current.IsKeyword("LEFT") ||
        Current.IsKeyword("RIGHT") || Current.IsKeyword("FULL") || Current.IsKeyword("CROSS");

    private JoinClause ParseJoinClause()
    {
        var startTok = Current;
        JoinKind kind;
        if (MatchKeyword("CROSS")) { ExpectKeyword("JOIN"); kind = JoinKind.Cross; }
        else if (MatchKeyword("INNER")) { ExpectKeyword("JOIN"); kind = JoinKind.Inner; }
        else if (MatchKeyword("LEFT")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Left; }
        else if (MatchKeyword("RIGHT")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Right; }
        else if (MatchKeyword("FULL")) { MatchKeyword("OUTER"); ExpectKeyword("JOIN"); kind = JoinKind.Full; }
        else { ExpectKeyword("JOIN"); kind = JoinKind.Inner; }

        var source = ParseSourceRef();

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
    // OR < AND < NOT < comparisons < + - < * / % < unary minus < primary
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
            var operand = ParseNot();
            return new UnaryExpr("NOT", operand, tok.Line, tok.Column);
        }
        return ParseComparison();
    }

    private static readonly string[] ComparisonOps = ["=", "!=", "<>", "<", "<=", ">", ">="];

    private Expr ParseComparison()
    {
        var left = ParseAdditive();
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
        return ParsePrimary();
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
