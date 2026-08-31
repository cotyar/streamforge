namespace StreamsForge.Engine.Sql;

internal enum TokenKind
{
    Identifier,
    Number,
    String,
    Symbol,
    EndOfInput,
}

/// <summary>A lexical token with 1-based line/column position.</summary>
internal sealed class Token(TokenKind kind, string text, int line, int column, double? doubleValue = null, long? longValue = null, string? stringValue = null)
{
    public TokenKind Kind { get; } = kind;

    /// <summary>Raw source text (identifier name, symbol, or the number's source text).</summary>
    public string Text { get; } = text;

    public int Line { get; } = line;
    public int Column { get; } = column;

    /// <summary>Set when Kind == Number and the literal contains a decimal point.</summary>
    public double? DoubleValue { get; } = doubleValue;

    /// <summary>Set when Kind == Number and the literal is a plain integer.</summary>
    public long? LongValue { get; } = longValue;

    /// <summary>Set when Kind == String: the unescaped string contents.</summary>
    public string? StringValue { get; } = stringValue;

    public bool IsKeyword(string keyword) => Kind == TokenKind.Identifier && string.Equals(Text, keyword, StringComparison.OrdinalIgnoreCase);

    public bool IsSymbol(string symbol) => Kind == TokenKind.Symbol && Text == symbol;

    public override string ToString() => Kind == TokenKind.EndOfInput ? "<eof>" : Text;
}
