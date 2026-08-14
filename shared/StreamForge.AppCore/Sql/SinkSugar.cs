using StreamForge.Abstractions;

namespace StreamForge.AppCore.Sql;

/// <summary>The outcome of stripping an <c>INSERT INTO &lt;sink&gt;</c> prefix off a statement.
/// <paramref name="Sql"/> is what should be stored/compiled: the untouched input when there was no sugar
/// to strip (the SAME string instance, so "did this change anything?" is answerable by reference), the
/// stripped query when there was, and — deliberately — the untouched input again on any diagnostic, so a
/// half-parsed prefix never reaches the compiler as a mystery syntax error.
/// <paramref name="SinkName"/> is non-null only when the strip actually happened.</summary>
public sealed record SinkSugarResult(string Sql, string? SinkName, List<string> Diagnostics);

/// <summary>
/// Plan 014 wave K. <c>INSERT INTO &lt;sink-name&gt; SELECT …</c> as PRE-PARSE SUGAR: the target is
/// stripped here, on the write/validate path, the naked query is what gets stored and compiled, and the
/// named <see cref="SinkSpec"/> on the entity being written is switched on.
///
/// <para><b>Why sugar and not a grammar change.</b> <c>INSERT</c> is a *statement* form, not an
/// expression: supporting it in <c>StreamForge.Engine</c> means a new top-level production, a new AST
/// node, a new validator path, and — the part that actually hurts — a decision about what
/// <c>CompileResult</c>/<c>TableCompileResult</c> hand back for a statement that is not a query. Those two
/// records are the compiler's whole public shape; every consumer reads <c>OutputSchema</c>,
/// <c>PlanSummary</c>, <c>SourceNames</c>. The change would therefore spread from the parser into
/// <c>Planner</c>, <c>TablePlanner</c> and both flavours' compile call sites, and land on a public API
/// that is frozen (additive-only) and pinned by 815 tests. All of it to express a DESTINATION — which
/// this platform already models, durably, as <see cref="SinkSpec"/>. So the destination is parsed where
/// destinations already live: in AppCore, above the Engine, on the way in.</para>
///
/// <para><b>Why a sink NAME and not <c>INSERT INTO postgres.public.trades</c>.</b> A fully-qualified
/// target names a connection, and a connection named in SQL has to exist somewhere the catalog can find
/// it: a first-class named-connection entity with its own CRUD, its own secret masking, its own console
/// page, and its own import/export merge rules — plus the resolution question of what happens when the
/// SQL names one that was since deleted. That is a bigger plan than this one, and none of it is needed to
/// say "write this somewhere": the entity's own <see cref="SinkSpec"/> list already carries the
/// connection, and now (wave A) a <see cref="SinkSpec.Name"/> to address it by.</para>
///
/// <para><b>This is LOSSY on round-trip, and says so.</b> Re-opening a pipeline saved as
/// <c>INSERT INTO warehouse SELECT …</c> shows the <c>SELECT</c> plus an enabled sink row — not the text
/// that was typed. Preserving the text would mean carrying the prefix through storage and then
/// desugaring at every compile site in both flavours (grains, actors, plan endpoints, config export),
/// i.e. exactly the spread this design avoids, in exchange for round-trip cosmetics. The API is honest
/// about it rather than hiding it: the entity returned by the create/update call already carries the
/// stripped SQL, so the caller sees precisely what was stored, with no special response field to look
/// for. One further consequence, stated because it is visible: re-importing the same sugared config
/// document always plans as "updated", never "skipped" — <c>ImportPlanner</c> compares the document's
/// text against the stored text, and those two now legitimately differ.</para>
///
/// <para><b>Leading trivia.</b> The prefix is looked for after skipping exactly what
/// <c>Tokenizer.SkipTrivia</c> skips: whitespace and <c>-- line comments</c>. A leading comment therefore
/// does NOT hide the sugar, and it is dropped along with the prefix (everything before the query keyword
/// is what the strip removes) — a comment that must survive belongs inside the query. Block comments are
/// not recognized because this SQL dialect has none: the tokenizer never had a <c>/* */</c> rule, so
/// <c>/* c */ INSERT …</c> is not sugar here and goes on to fail in the tokenizer, on its own terms,
/// which is a better error than one invented here about a syntax the dialect does not have.</para>
///
/// <para><b>Nothing else is matched.</b> No column list, no <c>VALUES</c>, no second target, no
/// <c>ON CONFLICT</c> — those are row-level DML, and a streaming query has no rows to hand back. And the
/// match is anchored at the start of the statement, so an <c>insert into</c> inside a string literal or a
/// later subquery is text, not sugar. Anything that is not a leading <c>INSERT</c> is returned byte- and
/// reference-identical, which is what keeps every query authored before this existed unaffected.</para>
/// </summary>
public static class SinkSugar
{
    /// <summary>Strips a leading <c>INSERT INTO &lt;identifier&gt;</c>, or reports why it could not.
    ///
    /// <para>A statement that does not begin with the word <c>INSERT</c> is returned untouched with no
    /// diagnostics — that path does not even scan. Once the leading <c>INSERT</c> IS there the author's
    /// intent is unambiguous, so every subsequent mismatch produces a diagnostic instead of silently
    /// falling through to the compiler, which would otherwise report the failure as an unexpected token
    /// several words away from the real mistake.</para></summary>
    public static SinkSugarResult Desugar(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return Unchanged(sql);
        }

        var afterInsert = MatchWord(sql, SkipTrivia(sql, 0), "INSERT");
        if (afterInsert < 0)
        {
            return Unchanged(sql);
        }

        var afterInto = MatchWord(sql, SkipTrivia(sql, afterInsert), "INTO");
        if (afterInto < 0)
        {
            return Diagnostic(sql, "expected INTO after INSERT — 'INSERT INTO <sink> SELECT …' is the only supported form");
        }

        var nameStart = SkipTrivia(sql, afterInto);
        if (nameStart < sql.Length && sql[nameStart] is '"' or '[' or '\'')
        {
            // Pinned, rather than quietly accepted: this dialect's tokenizer knows exactly one identifier
            // shape (letter-or-underscore then letters/digits/underscores) and treats '"' and '[' as
            // unexpected characters. Inventing a quoting rule that exists nowhere else in the language, so
            // that the sugar alone could address a sink called "my warehouse", would be a second identifier
            // grammar to explain and maintain. Renaming the sink is cheaper.
            return Diagnostic(sql, "the sink target must be a bare identifier (letters, digits, underscore) — this SQL dialect has no quoted or bracketed identifiers; rename the sink");
        }

        var afterName = ReadIdentifier(sql, nameStart);
        if (afterName < 0)
        {
            return Diagnostic(sql, "INSERT INTO needs a sink name — 'INSERT INTO <sink> SELECT …'");
        }

        var name = sql[nameStart..afterName];
        if (IsQueryKeyword(name))
        {
            // 'INSERT INTO SELECT …' reads as an identifier by shape, so without this the strip would take
            // SELECT as the sink name and then complain that no query followed it — describing the second
            // symptom of a missing target rather than the missing target.
            return Diagnostic(sql, $"INSERT INTO needs a sink name — '{name.ToUpperInvariant()}' is where the query starts");
        }

        var queryStart = SkipTrivia(sql, afterName);
        if (MatchWord(sql, queryStart, "SELECT") < 0 && MatchWord(sql, queryStart, "WITH") < 0)
        {
            // WITH is accepted alongside SELECT because a CTE query is a query in this dialect (the seeded
            // "Hot symbol VWAP" pipeline is one) — refusing it would make the sugar unavailable to exactly
            // the queries most likely to want a destination.
            return Diagnostic(sql, $"expected SELECT or WITH after 'INSERT INTO {name}' — a column list, VALUES, and a second target are all unsupported");
        }

        return new SinkSugarResult(sql[queryStart..], name, []);
    }

    /// <summary>The whole write-path step in one call: desugar, then resolve the target against
    /// <paramref name="sinks"/> and switch that sink on. Returns the SQL to store plus the diagnostics the
    /// caller should turn into a 400 (endpoints) or an "error" report entry (config import).
    ///
    /// <para>An unknown name is a hard error, never a no-op and never a positional guess:
    /// <see cref="SinkSpec.Name"/> is empty on every sink authored before wave A, so "the first sink" is a
    /// coin flip dressed up as a convenience, and a silently-ignored destination is the one failure mode a
    /// destination must not have. Matching is ORDINAL — the same case-sensitive rule by which source names
    /// in the very same statement resolve against the catalog.</para>
    ///
    /// <para>The match is mutated in place (<c>Enabled = true</c>) rather than returned, so callers can
    /// keep the list they already assembled — including the one <c>SecretsMasker.MergeSinkSecrets</c>
    /// hands back — instead of rebuilding it. <paramref name="entity"/> is the noun for the error message
    /// ("pipeline" / "table"); it exists only so the message names what the operator is actually looking
    /// at.</para></summary>
    public static SinkSugarResult ApplyTo(string sql, IReadOnlyList<SinkSpec>? sinks, string entity)
    {
        var result = Desugar(sql);
        if (result.SinkName is null)
        {
            return result;
        }

        var target = (sinks ?? []).FirstOrDefault(s => string.Equals(s.Name, result.SinkName, StringComparison.Ordinal));
        if (target is null)
        {
            return Diagnostic(sql, $"no sink named '{result.SinkName}' on this {entity} — add it in Sinks first");
        }

        target.Enabled = true;
        return result;
    }

    private static SinkSugarResult Unchanged(string sql) => new(sql, null, []);

    /// <summary>Every failure hands back the ORIGINAL statement — never a partially-stripped one. The
    /// caller is about to reject the write anyway, and a half-stripped string in a 400's echo (or in a
    /// validate response) would misdescribe what the server saw.</summary>
    private static SinkSugarResult Diagnostic(string sql, string message) => new(sql, null, [message]);

    /// <summary>Whitespace and <c>--</c> line comments, mirroring <c>Tokenizer.SkipTrivia</c>. Kept a
    /// duplicate rather than shared: that method is private to the Engine's internal tokenizer, and the
    /// Engine's public surface is frozen — a nine-line mirror is cheaper than an API addition, and the
    /// two only have to agree about which characters separate words.</summary>
    private static int SkipTrivia(string sql, int i)
    {
        while (i < sql.Length)
        {
            if (char.IsWhiteSpace(sql[i]))
            {
                i++;
                continue;
            }

            if (sql[i] == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n') i++;
                continue;
            }

            break;
        }

        return i;
    }

    /// <summary>Index just past <paramref name="word"/> if the statement has it at <paramref name="i"/>,
    /// case-insensitively AND as a whole word (so <c>INSERTED</c> is not <c>INSERT</c>); -1 otherwise.</summary>
    private static int MatchWord(string sql, int i, string word)
    {
        if (i + word.Length > sql.Length) return -1;
        if (string.Compare(sql, i, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) != 0) return -1;

        var end = i + word.Length;
        return end < sql.Length && IsIdentifierChar(sql[end]) ? -1 : end;
    }

    /// <summary>Index just past the identifier starting at <paramref name="i"/>, or -1 if there isn't one.
    /// The shape is <c>Tokenizer.ReadIdentifier</c>'s, exactly.</summary>
    private static int ReadIdentifier(string sql, int i)
    {
        if (i >= sql.Length || !(char.IsLetter(sql[i]) || sql[i] == '_')) return -1;

        var end = i;
        while (end < sql.Length && IsIdentifierChar(sql[end])) end++;
        return end;
    }

    private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    /// <summary>The two words a statement in this dialect can begin with (<c>Parser.ParseTopLevel</c>:
    /// WITH, else a SELECT / UNION chain). A sink named after either is therefore unaddressable by the
    /// sugar — the same price every keyword charges an identifier in every SQL dialect.</summary>
    private static bool IsQueryKeyword(string word) =>
        word.Equals("SELECT", StringComparison.OrdinalIgnoreCase) || word.Equals("WITH", StringComparison.OrdinalIgnoreCase);
}
