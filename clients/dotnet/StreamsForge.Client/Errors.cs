namespace StreamsForge.Client;

/// <summary>Base class for every error this client raises on purpose. Transport/network errors
/// that are NOT one of these (a raw <see cref="Grpc.Core.RpcException"/>, an <see cref="HttpRequestException"/>)
/// are allowed to propagate as-is rather than being wrapped and losing information.</summary>
public class StreamsForgeException : Exception
{
    public StreamsForgeException(string message) : base(message) { }
    public StreamsForgeException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Login failed, or a 401 survived the one-shot re-mint. Mirrors the cached-token logic
/// in <see cref="AuthHttpClient"/>: the token is discarded and re-minted exactly once on a 401;
/// if the retry also 401s, this is raised rather than looping.</summary>
public sealed class AuthException : StreamsForgeException
{
    public AuthException(string message) : base(message) { }
    public AuthException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>A <see cref="LiveTable"/> did not fill, or a <c>WaitForAsync</c> predicate never
/// matched, within its timeout. The common cause named explicitly because it is easy to
/// misdiagnose as a bug: a brand-new table gets no backfill, so subscribing to one nobody has
/// pushed to yet blocks until data arrives or this fires.</summary>
public sealed class NotReadyException : StreamsForgeException
{
    public NotReadyException(string message) : base(message) { }
}

/// <summary>An ingest push was not accepted (non-202 REST, or a non-ACCEPTED gRPC outcome).
/// <see cref="RowErrors"/> carries the per-row reasons the server gave, when it gave any.</summary>
public sealed class IngestRejectedException : StreamsForgeException
{
    public IReadOnlyList<string> RowErrors { get; }

    public IngestRejectedException(string message, IReadOnlyList<string>? rowErrors = null) : base(message)
        => RowErrors = rowErrors ?? Array.Empty<string>();
}

/// <summary>One diagnostic from the engine's SQL compiler: <c>{message, line, column, severity}</c>,
/// carried through verbatim.</summary>
public sealed record SqlDiagnostic(string Message, int Line, int Column, string Severity);

/// <summary>A SQL statement failed <c>validate</c> or the <c>config/import</c> create step.
/// <see cref="Diagnostics"/> is the engine's own list, verbatim. <see cref="Message"/> renders the
/// first diagnostic against <see cref="Sql"/> with a caret under the offending column -- the same
/// "engine explaining itself" the console's SQL editor shows, ported rather than flattened into a
/// plain message.</summary>
public sealed class SqlException : StreamsForgeException
{
    public IReadOnlyList<SqlDiagnostic> Diagnostics { get; }
    public string? Sql { get; }

    public SqlException(string message, IReadOnlyList<SqlDiagnostic> diagnostics, string? sql = null)
        : base(message)
    {
        Diagnostics = diagnostics;
        Sql = sql;
    }

    public override string Message => Render();

    private string Render()
    {
        var baseMessage = base.Message;
        if (Diagnostics.Count == 0) return baseMessage;
        var d = Diagnostics[0];
        if (Sql is null || d.Line < 1) return $"{d.Message} (line {d.Line}, column {d.Column})";

        var lines = Sql.Split('\n');
        if (d.Line > lines.Length) return $"{d.Message} (line {d.Line}, column {d.Column})";

        var sourceLine = lines[d.Line - 1];
        var caret = new string(' ', Math.Max(d.Column - 1, 0)) + "^";
        return $"{d.Message}\n{sourceLine}\n{caret}";
    }
}
