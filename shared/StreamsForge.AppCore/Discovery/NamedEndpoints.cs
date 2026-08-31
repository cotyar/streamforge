using System.Threading;

namespace StreamsForge.AppCore.Discovery;

/// <summary>
/// Plan 016 wave 6: named external endpoints. <c>@primary-oltp</c>, as the WHOLE value of an
/// endpoint-shaped string, resolves at CONNECT TIME to whatever this environment has configured under
/// that name.
///
/// <para><b>The whole sales pitch, in one sentence:</b> a catalog exported from prod imports
/// byte-identical into dev and connects to a different database. That only works because resolution
/// happens when a connection is opened and the resolved value is <b>never written back</b> — an export
/// still reads <c>@primary-oltp</c>.</para>
///
/// <para><b>Why a sigil inside an existing field.</b> Zero contract change: no new property on any config
/// type, no new <c>[Id(n)]</c>, nothing for a runtime to serialize differently. No URL, host or
/// connection string can begin with <c>@</c>, so the sigil is unambiguous. Only a value that is
/// ENTIRELY a reference counts — <c>nats://user@host:4222</c> contains an <c>@</c> and is left exactly
/// as authored, because the alternative (substitution anywhere in the string) would make every
/// credential-bearing URL a parsing hazard.</para>
///
/// <para><b>The map lives in configuration, never in the catalog</b> (<c>--Endpoints:primary-oltp=…</c>,
/// <c>Endpoints__PRIMARY_OLTP</c>). Per-environment by construction, which is the entire requirement: a
/// document that carried environment-specific endpoints would defeat the indirection it is asking for.
/// Static for the same reason <see cref="PeerDirectory"/> is — the connect sites that need it live in
/// grains and actors whose DI container is not the host's.</para>
///
/// <para><b>Names match ordinally</b>, like every other lookup this plan added. An unresolvable
/// reference at CONNECT time is an error (there is nothing to dial); an unresolvable reference at
/// IMPORT time is a <b>warning</b>, because importing a document destined for another environment must
/// remain possible — that is what promotion is.</para>
/// </summary>
public static class NamedEndpoints
{
    /// <summary>A value that is exactly this character followed by a name is a reference.</summary>
    public const char Sigil = '@';

    private static readonly Lock Gate = new();
    private static readonly Dictionary<string, string> Map = new(StringComparer.Ordinal);

    /// <summary>Replaces the whole map — configuration is the source of truth, so a reconfigure must be
    /// able to REMOVE a name, which a merge could not. Blank names and blank values are dropped: a name
    /// mapped to nothing would resolve to nothing and fail at dial time with a worse message than
    /// "unknown endpoint".</summary>
    public static void Configure(IEnumerable<KeyValuePair<string, string>> endpoints)
    {
        lock (Gate)
        {
            Map.Clear();
            foreach (var (name, value) in endpoints)
            {
                if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(value))
                {
                    Map[name] = value;
                }
            }
        }
    }

    /// <summary>Every configured name, in name order. The value is NOT a secret in itself — it is a host
    /// or a URL, the same class of thing already visible on a source's config — but a caller that renders
    /// it should still respect whatever gate it sits behind.</summary>
    public static IReadOnlyList<KeyValuePair<string, string>> All()
    {
        lock (Gate)
        {
            return [.. Map.OrderBy(kv => kv.Key, StringComparer.Ordinal)];
        }
    }

    /// <summary>True when <paramref name="value"/> is entirely a reference (<c>@name</c>) rather than a
    /// literal endpoint. A bare <c>"@"</c> is not: there is no name in it.</summary>
    public static bool IsReference(string? value) =>
        value is { Length: > 1 } && value[0] == Sigil;

    /// <summary>The name inside a reference, or null when <paramref name="value"/> is not one.</summary>
    public static string? NameOf(string? value) => IsReference(value) ? value![1..] : null;

    /// <summary>Resolves at CONNECT time: a literal is returned unchanged, a reference is replaced, and
    /// an unknown reference throws a sentence naming the endpoint and the names this environment does
    /// know. Throwing is right here and only here — there is no connection to open, and every transport
    /// that calls this already turns an exception into its own status-error path.</summary>
    public static string? Resolve(string? value)
    {
        if (!TryResolve(value, out var resolved, out var error))
        {
            throw new InvalidOperationException(error);
        }

        return resolved;
    }

    /// <summary>The non-throwing form, for the IMPORT path, which must be able to report an unresolvable
    /// reference as a warning and still apply the document. Returns false only for a reference this
    /// environment has no mapping for; a literal always succeeds.</summary>
    public static bool TryResolve(string? value, out string? resolved, out string? error)
    {
        error = null;
        if (!IsReference(value))
        {
            resolved = value;
            return true;
        }

        var name = value![1..];
        lock (Gate)
        {
            if (Map.TryGetValue(name, out var mapped))
            {
                resolved = mapped;
                return true;
            }

            var known = Map.Count == 0
                ? "this instance has no named endpoints configured"
                : $"known names: {string.Join(", ", Map.Keys.OrderBy(k => k, StringComparer.Ordinal))}";
            resolved = null;
            error = $"'{value}' names an endpoint that is not configured here ({known}). " +
                    "Named endpoints come from configuration (Endpoints:<name>), never from the catalog — " +
                    "which is what lets one exported catalog import into several environments.";
            return false;
        }
    }

    /// <summary>Test hook — the map is process-wide, so a test that configures it must be able to put it
    /// back.</summary>
    public static void Clear() => Configure([]);
}
