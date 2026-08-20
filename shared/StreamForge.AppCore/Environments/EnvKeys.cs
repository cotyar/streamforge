using System.Text.RegularExpressions;

namespace StreamForge.AppCore.Environments;

/// <summary>
/// Plan 021 wave 0 — the ONE place a runtime key is qualified with an environment, and the ONE place an
/// environment name is judged legal. Pure: no I/O, no state, no runtime types, so both flavours and the
/// REST layer compose byte-identical keys (the same reason <see cref="History.TableShardKeys"/> lives in
/// AppCore rather than in either host).
///
/// <para><b>D2, the invariant everything else is measured against.</b> The default environment is the
/// EMPTY string, and <see cref="Qualify"/> returns its input unchanged for it. So with no environment ever
/// mentioned, every grain key, actor id, stream id, storage filename and Redis key is exactly the byte
/// string it was before this plan — which is the migration strategy: there is no migration. An existing
/// <c>data/</c> directory and an existing Redis state store come up unchanged. A wave that makes
/// <c>Qualify("", k) != k</c> has broken the plan's stated acceptance criterion, not merely a test.</para>
///
/// <para><b>The separator is <c>'.'</c>, and the plan's own D3 said <c>':'</c> — overruled here, with the
/// reason.</b> <c>JsonFileGrainStorage</c> turns every character outside <c>[A-Za-z0-9_.-]</c> into
/// <c>'_'</c> when it builds a state file name, and <c>'_'</c> is a legal character in an entity name. So
/// <c>staging:orders</c> and a table named <c>staging_orders</c> in the default environment sanitize to
/// the SAME file — two distinct grains silently sharing one state file, which is precisely the
/// cross-environment leak this plan exists to prevent. <c>'.'</c> is inside that allowed set, so the
/// key→filename map stays injective. It is also the one separator that cannot collide with a usable entity
/// name: the SQL tokenizer reads an identifier as <c>[letter|digit|_]+</c>
/// (<c>Engine/Sql/Tokenizer.cs</c> ReadIdentifier), so a source or table whose name contains a dot can
/// never be named in a query and is already broken for every other reason. The write path refuses one
/// going forward regardless — see <see cref="IsQualifiableEntityName"/>.</para>
///
/// <para>It also does not collide with the shard tier's own composite key, which uses <c>'|'</c> and
/// base64url-encodes the token beside it (<see cref="History.TableShardKeys"/>): a qualified shard key
/// reads <c>{env}.{table}|{token}</c>, and <c>ParseGrainKey</c>'s <c>LastIndexOf('|')</c> still recovers
/// <c>{env}.{table}</c> intact.</para>
/// </summary>
public static partial class EnvKeys
{
    /// <summary>The default environment, spelled as the empty string EVERYWHERE internally. It renders as
    /// <see cref="DefaultDisplayName"/> in the API and the console, and nowhere else — a key composed from
    /// the literal string "default" would not be byte-identical to today's, which is the whole of D2.</summary>
    public const string Default = "";

    /// <summary>What <see cref="Default"/> is called by a human, in the API and the UI.</summary>
    public const string DefaultDisplayName = "default";

    /// <summary>See the class remarks for why this is <c>'.'</c> and not <c>':'</c>.</summary>
    public const char Separator = '.';

    /// <summary>Names an environment may not take, because each is already a singleton key in the same
    /// key space (<c>StreamConstants</c>) and qualifying it would produce an ambiguous string.</summary>
    public static readonly IReadOnlySet<string> Reserved = new HashSet<string>(StringComparer.Ordinal)
    {
        DefaultDisplayName, "catalog", "users", "access", "approvals", "audit", "events", "metrics",
    };

    [GeneratedRegex("^[a-z0-9][a-z0-9-]{0,31}$")]
    private static partial Regex NamePattern();

    /// <summary>Lower-case, digits and hyphens, 1–32 characters, not reserved. Deliberately narrower than
    /// an entity name: an environment name ends up inside a state FILE name, a Redis key and a stream id,
    /// and the one character class that survives all three untouched is this one. The empty string —
    /// <see cref="Default"/> — is not a creatable name and returns false here; it always exists.</summary>
    public static bool IsValidName(string? name) =>
        !string.IsNullOrEmpty(name) && NamePattern().IsMatch(name) && !Reserved.Contains(name);

    /// <summary>Whether an entity name can be safely qualified — i.e. does not contain the separator. A
    /// dotted source/table name is already unusable in SQL (see the class remarks), so refusing one at the
    /// write path costs nothing real and keeps <see cref="Split"/> unambiguous. Pipelines are keyed by
    /// GUID("n") and never reach this.</summary>
    public static bool IsQualifiableEntityName(string? name) =>
        !string.IsNullOrEmpty(name) && !name.Contains(Separator);

    /// <summary>The runtime key for <paramref name="key"/> inside <paramref name="env"/>. The default
    /// environment returns <paramref name="key"/> unchanged — see D2 in the class remarks.</summary>
    public static string Qualify(string? env, string key) =>
        string.IsNullOrEmpty(env) ? key : string.Concat(env, Separator.ToString(), key);

    /// <summary>The inverse of <see cref="Qualify"/>: splits a qualified key back into its environment and
    /// the bare key. A key with no separator, or whose prefix is not a legal environment name, belongs to
    /// the default environment and comes back whole — so this is safe to call on any key, including every
    /// key written before this plan existed.
    ///
    /// <para>Uses the FIRST separator, not the last: an environment name cannot contain one, so the first
    /// dot is the only candidate, and a bare key that somehow contains dots keeps all of them.</para></summary>
    public static (string Env, string Key) Split(string qualifiedKey)
    {
        var idx = qualifiedKey.IndexOf(Separator);
        if (idx <= 0)
        {
            return (Default, qualifiedKey);
        }

        var env = qualifiedKey[..idx];
        return IsValidName(env) ? (env, qualifiedKey[(idx + 1)..]) : (Default, qualifiedKey);
    }

    /// <summary>The environment half of <see cref="Split"/>, for the common case of a grain that needs to
    /// know which catalog it belongs to and already holds its bare name some other way.</summary>
    public static string EnvOf(string qualifiedKey) => Split(qualifiedKey).Env;

    /// <summary>How an environment is spelled to a human: <see cref="DefaultDisplayName"/> for the default,
    /// the name itself otherwise.</summary>
    public static string Display(string? env) => string.IsNullOrEmpty(env) ? DefaultDisplayName : env;

    /// <summary>The inverse of <see cref="Display"/>: what a caller typed, normalised to the internal
    /// spelling. <c>null</c>, empty and the literal <c>"default"</c> all mean <see cref="Default"/>.</summary>
    public static string Normalize(string? typed) =>
        string.IsNullOrWhiteSpace(typed) || string.Equals(typed, DefaultDisplayName, StringComparison.Ordinal)
            ? Default
            : typed.Trim();
}
