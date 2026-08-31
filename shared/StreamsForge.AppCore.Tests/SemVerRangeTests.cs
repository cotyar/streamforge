using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 4 — <see cref="SemVerRange"/>/<see cref="SemVerVersion"/>. Covers the grammar the class
/// doc commits to (exact, caret, tilde, comparators, AND-combination, <c>*</c>) and, explicitly, the
/// three boundary cases a casual caret/comparator implementation gets wrong: a pre-1.0 caret NOT behaving
/// like a post-1.0 one, a range that must exclude the next major, and pre-release/build metadata being
/// discarded rather than compared. Also covers the grammar's stated NON-support (OR, hyphen ranges,
/// partial versions) failing <see cref="SemVerRange.TryParse"/> outright rather than silently
/// misparsing.
/// </summary>
public class SemVerRangeTests
{
    private static bool Matches(string range, string version)
    {
        Assert.True(SemVerRange.TryParse(range, out var r), $"'{range}' was expected to parse");
        return r!.Matches(version);
    }

    // ------------------------------------------------------------------
    // Exact / wildcard.
    // ------------------------------------------------------------------

    [Fact]
    public void Exact_matches_only_the_identical_triple()
    {
        Assert.True(Matches("1.2.3", "1.2.3"));
        Assert.False(Matches("1.2.3", "1.2.4"));
        Assert.False(Matches("1.2.3", "1.2.2"));
    }

    [Theory]
    [InlineData("*")]
    [InlineData("")]
    [InlineData("  ")]
    public void Wildcard_matches_any_parseable_version(string range)
    {
        Assert.True(Matches(range, "0.0.1"));
        Assert.True(Matches(range, "9.9.9"));
    }

    [Fact]
    public void Wildcard_never_matches_an_unparseable_candidate()
    {
        // A range answers "does this version qualify" — a value that isn't a version cannot, not even
        // against "*". This is what stops a broken/absent installed-version string from reading as
        // trivially satisfied.
        Assert.False(Matches("*", "not-a-version"));
        Assert.False(Matches("*", ""));
    }

    // ------------------------------------------------------------------
    // Caret — including the ^0.x boundary the plan brief calls out by name.
    // ------------------------------------------------------------------

    [Fact]
    public void Caret_on_a_stable_major_allows_minor_and_patch_drift_but_excludes_the_next_major()
    {
        Assert.True(Matches("^1.2.0", "1.2.0"));
        Assert.True(Matches("^1.2.0", "1.2.9"));
        Assert.True(Matches("^1.2.0", "1.9.9"));
        Assert.False(Matches("^1.2.0", "1.1.9")); // below the floor.
        Assert.False(Matches("^1.2.0", "2.0.0")); // the next major — must be excluded.
    }

    [Fact]
    public void Caret_0_x_with_a_nonzero_minor_locks_the_minor_unlike_a_stable_caret()
    {
        // The exact mistake a casual implementation makes: treating ^0.2.3 as "anything below 1.0.0".
        // npm's (and this type's) rule is narrower — a 0.x minor bump IS a breaking change.
        Assert.True(Matches("^0.2.3", "0.2.3"));
        Assert.True(Matches("^0.2.3", "0.2.9"));
        Assert.False(Matches("^0.2.3", "0.3.0")); // next MINOR excluded, not next major.
        Assert.False(Matches("^0.2.3", "0.9.9")); // would wrongly pass an ^1.x-style caret.
        Assert.False(Matches("^0.2.3", "1.0.0"));
    }

    [Fact]
    public void Caret_0_0_x_locks_the_patch_too()
    {
        Assert.True(Matches("^0.0.3", "0.0.3"));
        Assert.False(Matches("^0.0.3", "0.0.4"));
        Assert.False(Matches("^0.0.3", "0.0.2"));
        Assert.False(Matches("^0.0.3", "0.1.0"));
    }

    // ------------------------------------------------------------------
    // Tilde.
    // ------------------------------------------------------------------

    [Fact]
    public void Tilde_allows_patch_drift_but_locks_the_minor()
    {
        Assert.True(Matches("~1.2.3", "1.2.3"));
        Assert.True(Matches("~1.2.3", "1.2.99"));
        Assert.False(Matches("~1.2.3", "1.2.2")); // below the floor.
        Assert.False(Matches("~1.2.3", "1.3.0")); // next minor excluded.
    }

    // ------------------------------------------------------------------
    // Plain comparators and AND-combination — the "must exclude the next major" case done the
    // comparator way instead of caret's bundled way.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(">=1.2.0", "1.2.0", true)]
    [InlineData(">=1.2.0", "1.1.9", false)]
    [InlineData("<=1.2.0", "1.2.0", true)]
    [InlineData("<=1.2.0", "1.2.1", false)]
    [InlineData(">1.2.0", "1.2.1", true)]
    [InlineData(">1.2.0", "1.2.0", false)]
    [InlineData("<1.2.0", "1.1.9", true)]
    [InlineData("<1.2.0", "1.2.0", false)]
    [InlineData("=1.2.0", "1.2.0", true)]
    [InlineData("=1.2.0", "1.2.1", false)]
    public void Single_comparators(string range, string version, bool expected) =>
        Assert.Equal(expected, Matches(range, version));

    [Fact]
    public void Space_separated_comparators_AND_and_can_exclude_the_next_major()
    {
        const string range = ">=1.2.0 <2.0.0";
        Assert.True(Matches(range, "1.2.0"));
        Assert.True(Matches(range, "1.9.9"));
        Assert.False(Matches(range, "2.0.0")); // the next major — excluded by the second clause.
        Assert.False(Matches(range, "1.1.9")); // below the floor.
    }

    // ------------------------------------------------------------------
    // Pre-release / build metadata: recognized only to be discarded (documented explicitly on the
    // class, since this is the one place a real semver library would disagree with this type).
    // ------------------------------------------------------------------

    [Fact]
    public void Prerelease_and_build_metadata_on_the_CANDIDATE_are_stripped_before_comparison()
    {
        Assert.True(Matches("1.2.3", "1.2.3-rc.1"));
        Assert.True(Matches("1.2.3", "1.2.3+build.5"));
        Assert.True(Matches("1.2.3", "1.2.3-rc.1+build.5"));
        Assert.True(Matches("^1.2.0", "1.2.3-beta"));
    }

    [Fact]
    public void Prerelease_and_build_metadata_on_the_RANGE_are_also_stripped()
    {
        Assert.True(Matches(">=1.2.3-rc", "1.2.3"));
    }

    [Fact]
    public void A_leading_v_is_accepted_on_either_side()
    {
        Assert.True(Matches("v1.2.3", "1.2.3"));
        Assert.True(Matches("1.2.3", "v1.2.3"));
    }

    // ------------------------------------------------------------------
    // Deliberately unsupported grammar: TryParse fails rather than guessing.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("1.0.0 || 2.0.0")] // OR combinator.
    [InlineData("1.2.3 - 2.3.4")] // hyphen range.
    [InlineData("1.2")] // partial version as an exact match.
    [InlineData("^1.2")] // partial version under caret.
    [InlineData("~1")] // partial version under tilde.
    [InlineData("1.2.x")] // x-range.
    [InlineData("1.2.3.4")] // too many components.
    [InlineData("not-a-version")]
    public void Unsupported_grammar_fails_to_parse(string range) =>
        Assert.False(SemVerRange.TryParse(range, out _));

    // ------------------------------------------------------------------
    // SemVerVersion directly.
    // ------------------------------------------------------------------

    [Fact]
    public void SemVerVersion_orders_by_major_then_minor_then_patch()
    {
        Assert.True(SemVerVersion.TryParse("1.2.3", out var a));
        Assert.True(SemVerVersion.TryParse("1.2.4", out var b));
        Assert.True(SemVerVersion.TryParse("1.3.0", out var c));
        Assert.True(SemVerVersion.TryParse("2.0.0", out var d));

        Assert.True(a!.CompareTo(b) < 0);
        Assert.True(b!.CompareTo(c) < 0);
        Assert.True(c!.CompareTo(d) < 0);

        Assert.True(SemVerVersion.TryParse("1.2.3", out var again));
        Assert.Equal(0, a.CompareTo(again));
    }
}
