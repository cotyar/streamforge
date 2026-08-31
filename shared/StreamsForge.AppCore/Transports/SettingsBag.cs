using System.Globalization;
using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// Readers for <see cref="ConnectorConfig.Settings"/> / <see cref="SinkSpec.Settings"/> — the open,
/// string-valued config bag an OUT-OF-TREE transport uses instead of a typed class in
/// <c>StreamsForge.Contracts</c> (see that property's doc comment for why it exists).
///
/// <para>Every value is a string because that is what the console's descriptor-driven form writes and what
/// a config export round-trips. Parsing them is the transport's job, and these are the readers so each
/// out-of-tree kind doesn't re-invent "what does an absent key mean, and what does a malformed number
/// do" — <b>absent, empty and unparseable all mean "the caller's fallback"</b>, never an exception:
/// <c>Validate()</c> is where a bad value becomes a message an operator can act on, and a reader that
/// threw would turn that into a 500 from whichever call site read it first.</para>
/// </summary>
public static class SettingsBag
{
    /// <summary>Trimmed value for <paramref name="key"/>, or <paramref name="fallback"/> when absent or
    /// empty. Keys are matched ordinally — the same comparison the descriptor's own field keys use.</summary>
    public static string Get(IReadOnlyDictionary<string, string>? bag, string key, string fallback = "") =>
        bag is not null && bag.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value.Trim()
            : fallback;

    /// <summary><see cref="Get"/>, but null rather than "" for "not set" — for the fields where empty and
    /// absent must stay distinguishable (an optional override vs. a deliberate blank).</summary>
    public static string? GetOrNull(IReadOnlyDictionary<string, string>? bag, string key)
    {
        var value = Get(bag, key, "");
        return value.Length == 0 ? null : value;
    }

    /// <summary>Invariant-culture integer, or <paramref name="fallback"/> when absent/blank/unparseable.
    /// Invariant, not current-culture: a config document authored on one host must mean the same thing on
    /// another.</summary>
    public static int GetInt(IReadOnlyDictionary<string, string>? bag, string key, int fallback = 0) =>
        int.TryParse(Get(bag, key, ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;

    /// <summary><c>true</c>/<c>false</c> (case-insensitive, the spelling the console's bool field writes),
    /// else <paramref name="fallback"/>.</summary>
    public static bool GetBool(IReadOnlyDictionary<string, string>? bag, string key, bool fallback = false) =>
        bool.TryParse(Get(bag, key, ""), out var parsed) ? parsed : fallback;

    /// <summary>Appends "<paramref name="label"/> is required" to <paramref name="errors"/> when the key is
    /// absent or blank, and returns the value either way — so a transport's <c>Validate</c> reads as one
    /// line per required field. <paramref name="label"/> is what the operator sees, so pass the field's
    /// label ("Daemon"), not its key.</summary>
    public static string Require(IReadOnlyDictionary<string, string>? bag, string key, string label, List<string> errors)
    {
        var value = Get(bag, key, "");
        if (value.Length == 0)
        {
            errors.Add($"{label} is required");
        }

        return value;
    }
}
