namespace StreamsForge.Engine.Sql;

/// <summary>Hand-rolled tokenizer: whitespace/`-- comment` skipping, identifiers, numbers, 'strings', and symbols.
/// Records accurate 1-based line/column for every token. Never throws; unrecognized characters become diagnostics
/// and are skipped so scanning can continue.</summary>
internal sealed class Tokenizer
{
    // "->>" must precede "->" — MultiCharSymbols is matched in array order (first match wins), not by
    // length, so the 3-char Postgres "returns text" JSON operator has to be tried before its 2-char prefix.
    private static readonly string[] MultiCharSymbols = ["->>", "<>", "<=", ">=", "!=", "->"];

    private readonly string _src;
    private int _pos;
    private int _line = 1;
    private int _col = 1;
    private readonly List<SqlDiagnostic> _diagnostics = [];

    public Tokenizer(string sql) => _src = sql;

    public (List<Token> Tokens, List<SqlDiagnostic> Diagnostics) Tokenize()
    {
        var tokens = new List<Token>();
        while (true)
        {
            SkipTrivia();
            var (line, col) = (_line, _col);
            if (IsAtEnd)
            {
                tokens.Add(new Token(TokenKind.EndOfInput, "", line, col));
                break;
            }

            char c = Current;

            if (char.IsDigit(c))
            {
                tokens.Add(ReadNumber(line, col));
                continue;
            }

            if (c == '\'')
            {
                tokens.Add(ReadString(line, col));
                continue;
            }

            if (char.IsLetter(c) || c == '_')
            {
                tokens.Add(ReadIdentifier(line, col));
                continue;
            }

            var multi = MultiCharSymbols.FirstOrDefault(s => Matches(s));
            if (multi != null)
            {
                Advance(multi.Length);
                tokens.Add(new Token(TokenKind.Symbol, multi, line, col));
                continue;
            }

            if ("=<>+-*/%,.()".IndexOf(c) >= 0)
            {
                Advance(1);
                tokens.Add(new Token(TokenKind.Symbol, c.ToString(), line, col));
                continue;
            }

            _diagnostics.Add(new SqlDiagnostic($"Unexpected character '{c}'", line, col));
            Advance(1);
        }

        return (tokens, _diagnostics);
    }

    private bool IsAtEnd => _pos >= _src.Length;
    private char Current => _src[_pos];
    private char Peek(int ahead = 1) => _pos + ahead < _src.Length ? _src[_pos + ahead] : '\0';

    private bool Matches(string s)
    {
        if (_pos + s.Length > _src.Length) return false;
        for (int i = 0; i < s.Length; i++)
        {
            if (_src[_pos + i] != s[i]) return false;
        }
        return true;
    }

    private void Advance(int count)
    {
        for (int i = 0; i < count; i++)
        {
            if (IsAtEnd) return;
            if (_src[_pos] == '\n') { _line++; _col = 1; }
            else { _col++; }
            _pos++;
        }
    }

    private void SkipTrivia()
    {
        while (!IsAtEnd)
        {
            char c = Current;
            if (c is ' ' or '\t' or '\r' or '\n') { Advance(1); continue; }
            if (c == '-' && Peek() == '-')
            {
                while (!IsAtEnd && Current != '\n') Advance(1);
                continue;
            }
            break;
        }
    }

    private Token ReadNumber(int line, int col)
    {
        int start = _pos;
        bool isDouble = false;
        while (!IsAtEnd && char.IsDigit(Current)) Advance(1);
        if (!IsAtEnd && Current == '.' && char.IsDigit(Peek()))
        {
            isDouble = true;
            Advance(1);
            while (!IsAtEnd && char.IsDigit(Current)) Advance(1);
        }
        string text = _src[start.._pos];
        if (isDouble)
        {
            return new Token(TokenKind.Number, text, line, col, doubleValue: double.Parse(text, System.Globalization.CultureInfo.InvariantCulture));
        }
        if (!long.TryParse(text, out var l))
        {
            // overflow: fall back to double
            return new Token(TokenKind.Number, text, line, col, doubleValue: double.Parse(text, System.Globalization.CultureInfo.InvariantCulture));
        }
        return new Token(TokenKind.Number, text, line, col, longValue: l);
    }

    private Token ReadString(int line, int col)
    {
        Advance(1); // opening quote
        var sb = new System.Text.StringBuilder();
        while (true)
        {
            if (IsAtEnd)
            {
                _diagnostics.Add(new SqlDiagnostic("Unterminated string literal", line, col));
                break;
            }
            if (Current == '\'')
            {
                if (Peek() == '\'')
                {
                    sb.Append('\'');
                    Advance(2);
                    continue;
                }
                Advance(1);
                break;
            }
            sb.Append(Current);
            Advance(1);
        }
        return new Token(TokenKind.String, sb.ToString(), line, col, stringValue: sb.ToString());
    }

    private Token ReadIdentifier(int line, int col)
    {
        int start = _pos;
        while (!IsAtEnd && (char.IsLetterOrDigit(Current) || Current == '_')) Advance(1);
        string text = _src[start.._pos];
        return new Token(TokenKind.Identifier, text, line, col);
    }
}
