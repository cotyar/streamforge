using System.Globalization;

namespace StreamForge.AppCore.Transports;

/// <summary>
/// Plan 016 wave 4 — a small, pure semver comparator, written by hand instead of taking a NuGet
/// dependency for it (the repo's house rule: zero dependencies where avoidable, and matching one
/// three-integer tuple against a handful of range shapes is avoidable). Used by
/// <c>ConfigDocument.Requires</c> to say "this document needs kind X at version Y" and by
/// <c>ConfigImportService</c> to check that at import.
///
/// <para><b>Supported grammar</b> — the subset people actually type, each SPACE-separated token being one
/// comparator, all of them ANDed together:</para>
/// <list type="bullet">
/// <item><c>""</c> or <c>"*"</c> — matches anything that parses as a version. This is also what an empty
/// token contributes when it shows up between extra spaces, so <c>"  "</c> and <c>"*"</c> behave the
/// same.</item>
/// <item><c>1.2.3</c> — exact match.</item>
/// <item><c>^1.2.3</c> — caret: compatible-with, npm's rule verbatim because it is the one people already
/// know and it correctly treats a pre-1.0 major as unstable. <c>major &gt; 0</c>: <c>&gt;=1.2.3 &lt;2.0.0</c>.
/// <c>major == 0 &amp;&amp; minor &gt; 0</c>: <c>&gt;=0.2.3 &lt;0.3.0</c> — <b>not</b> <c>&lt;1.0.0</c>, which is the
/// mistake a casual implementation makes and the reason <c>^0.x</c> gets its own boundary test.
/// <c>major == 0 &amp;&amp; minor == 0</c>: <c>&gt;=0.0.3 &lt;0.0.4</c> — every digit of a 0.0.x release is
/// considered breaking.</item>
/// <item><c>~1.2.3</c> — tilde: locks major AND minor, lets patch float: <c>&gt;=1.2.3 &lt;1.3.0</c>.</item>
/// <item><c>&gt;=1.2.3</c>, <c>&lt;=1.2.3</c>, <c>&gt;1.2.3</c>, <c>&lt;1.2.3</c>, <c>=1.2.3</c> — plain comparators,
/// combinable by spacing them: <c>"&gt;=1.2.0 &lt;2.0.0"</c> excludes the next major without needing caret's
/// bundled semantics.</item>
/// </list>
///
/// <para><b>Deliberately NOT supported</b> (fails <see cref="TryParse"/> rather than guessing):
/// OR-combinators (<c>2.0 || 3.0</c> — nobody in this codebase's config documents needs "either major"; a
/// document that does can list the requirement twice under two different profiles instead), hyphen ranges
/// (<c>1.2.3 - 2.3.4</c>), X-ranges (<c>1.2.x</c>, <c>1.2.*</c> as a partial-version wildcard — <c>*</c> is
/// only accepted as the WHOLE range, never mixed into one component), and partial versions anywhere else
/// (<c>^1.2</c>, <c>~1</c>, or a bare <c>1.2</c> as an exact match) — every version literal in this grammar
/// is a full <c>major.minor.patch</c> triple. Each is a real omission, not merely deferred: a config author
/// who needs one of these gets a clear parse failure at import (a <c>DetectUnsatisfiedPluginRequirements</c>
/// entry naming the bad range), never a silent wrong match.</para>
///
/// <para><b>Pre-release and build metadata:</b> a version string may carry a <c>-suffix</c> or
/// <c>+suffix</c> (or both) after the numeric triple; it is recognized only to be discarded before any
/// comparison. <c>1.2.3-rc.1</c> and <c>1.2.3+build.5</c> both compare as plain <c>1.2.3</c>, and a range
/// never distinguishes them. This means none of semver's actual pre-release precedence rules (an alpha
/// build sorts BELOW its release, <c>1.0.0-alpha &lt; 1.0.0</c>) are implemented — <c>ponytail:</c> the
/// ceiling is "a kind's declared version is a plain release triple, and nothing in this codebase ships a
/// pre-release connector version today"; the upgrade path, if that ever changes, is a dedicated
/// prerelease-identifier comparison per semver.org §11 before this type is trusted with it.</para>
///
/// <para>Pure. No I/O, no state, no Orleans/Dapr/ASP.NET types.</para>
/// </summary>
public sealed class SemVerRange
{
    private enum Op { Eq, Gte, Lte, Gt, Lt }

    private readonly List<(Op Op, SemVerVersion Version)> _clauses;

    private SemVerRange(List<(Op, SemVerVersion)> clauses) => _clauses = clauses;

    /// <summary>Parses a range string. Returns false (and a null range) on anything outside the grammar
    /// above — malformed tokens, an OR, a hyphen range, a partial version — rather than throwing, because
    /// the caller (an import check reading a value out of someone else's document) treats a bad range as
    /// just another diagnostic, not a crash.</summary>
    public static bool TryParse(string? range, out SemVerRange? result)
    {
        result = null;
        var tokens = (range ?? "").Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length == 0)
        {
            result = new SemVerRange([]);
            return true;
        }

        var clauses = new List<(Op, SemVerVersion)>();
        foreach (var token in tokens)
        {
            if (token == "*")
            {
                continue; // contributes nothing — "any" is the absence of a clause.
            }

            string opText;
            string rest;
            if (token.StartsWith(">=", StringComparison.Ordinal) || token.StartsWith("<=", StringComparison.Ordinal))
            {
                (opText, rest) = (token[..2], token[2..]);
            }
            else if (token[0] is '>' or '<' or '=' or '^' or '~')
            {
                (opText, rest) = (token[..1], token[1..]);
            }
            else
            {
                (opText, rest) = ("", token);
            }

            if (!SemVerVersion.TryParse(rest, out var v) || v is null)
            {
                return false;
            }

            switch (opText)
            {
                case "" or "=":
                    clauses.Add((Op.Eq, v));
                    break;
                case ">=":
                    clauses.Add((Op.Gte, v));
                    break;
                case "<=":
                    clauses.Add((Op.Lte, v));
                    break;
                case ">":
                    clauses.Add((Op.Gt, v));
                    break;
                case "<":
                    clauses.Add((Op.Lt, v));
                    break;
                case "^":
                    var (caretLo, caretHi) = v.Major > 0
                        ? (v, new SemVerVersion(v.Major + 1, 0, 0))
                        : v.Minor > 0
                            ? (v, new SemVerVersion(0, v.Minor + 1, 0))
                            : (v, new SemVerVersion(0, 0, v.Patch + 1));
                    clauses.Add((Op.Gte, caretLo));
                    clauses.Add((Op.Lt, caretHi));
                    break;
                case "~":
                    clauses.Add((Op.Gte, v));
                    clauses.Add((Op.Lt, new SemVerVersion(v.Major, v.Minor + 1, 0)));
                    break;
                default:
                    return false;
            }
        }

        result = new SemVerRange(clauses);
        return true;
    }

    /// <summary>True when <paramref name="version"/> parses AND satisfies every clause (AND, never OR —
    /// see the class doc). An unparseable candidate never matches anything, including <c>"*"</c> — a range
    /// answers "does this version qualify", and a value that isn't a version cannot.</summary>
    public bool Matches(string? version) =>
        SemVerVersion.TryParse(version, out var v) && v is not null && _clauses.All(c => c.Op switch
        {
            Op.Eq => v.CompareTo(c.Version) == 0,
            Op.Gte => v.CompareTo(c.Version) >= 0,
            Op.Lte => v.CompareTo(c.Version) <= 0,
            Op.Gt => v.CompareTo(c.Version) > 0,
            Op.Lt => v.CompareTo(c.Version) < 0,
            _ => false,
        });
}

/// <summary>A plain <c>major.minor.patch</c> release triple — the only version shape
/// <see cref="SemVerRange"/> compares against (see that type's doc for why partial versions and
/// pre-release precedence are out of scope). An optional leading <c>v</c> and an optional
/// <c>-prerelease</c>/<c>+build</c> suffix are accepted and then ignored entirely.</summary>
public sealed class SemVerVersion(int major, int minor, int patch) : IComparable<SemVerVersion>
{
    public int Major { get; } = major;
    public int Minor { get; } = minor;
    public int Patch { get; } = patch;

    public static bool TryParse(string? text, out SemVerVersion? version)
    {
        version = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var core = text.Trim();
        if (core.StartsWith('v') || core.StartsWith('V'))
        {
            core = core[1..];
        }

        // Strip build metadata first (it may itself contain '-'), then pre-release — both discarded,
        // never compared. See the class doc's "Pre-release and build metadata" paragraph.
        var plus = core.IndexOf('+');
        if (plus >= 0)
        {
            core = core[..plus];
        }

        var dash = core.IndexOf('-');
        if (dash >= 0)
        {
            core = core[..dash];
        }

        var parts = core.Split('.');
        if (parts.Length != 3)
        {
            return false; // partial versions ("1.2", "1") are out of scope — see the class doc.
        }

        if (!int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var maj) ||
            !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var min) ||
            !int.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var pat))
        {
            return false;
        }

        version = new SemVerVersion(maj, min, pat);
        return true;
    }

    public int CompareTo(SemVerVersion? other)
    {
        if (other is null)
        {
            return 1;
        }

        var byMajor = Major.CompareTo(other.Major);
        if (byMajor != 0)
        {
            return byMajor;
        }

        var byMinor = Minor.CompareTo(other.Minor);
        return byMinor != 0 ? byMinor : Patch.CompareTo(other.Patch);
    }

    public override string ToString() => $"{Major}.{Minor}.{Patch}";
}
