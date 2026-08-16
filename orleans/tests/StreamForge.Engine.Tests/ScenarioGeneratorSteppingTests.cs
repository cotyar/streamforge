using StreamForge.Abstractions;
using StreamForge.Host.Generators;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Wishlist #9(b): the equivalence property that is "the entire point" of <c>step: true</c> —
/// stepping a scenario run day-by-day must produce EXACTLY the same rows as generating the whole run in
/// one call. Exercises <see cref="ScenarioGenerator.BeginRun"/>/<see cref="ScenarioGenerator.GenerateDay"/>
/// directly (the same calls <c>GeneratorGrain.RunAsync</c>/<c>GeneratorActor.RunAsync</c> make for
/// <c>request.Step == true</c> — see those methods) with no grain/actor/stream machinery at all, mirroring
/// <c>ScenarioGeneratorTests.cs</c>'s own "pure, zero-setup" convention. The grain-level proof that
/// stepping ALSO publishes correctly onto the real Orleans stream lives in
/// <c>GeneratorGrainStepRunTests.cs</c> (orleans/tests/StreamForge.Host.Tests).</summary>
public class ScenarioGeneratorSteppingTests
{
    private const long FixedNowMs = 1_700_000_000_000L;

    private static SourceDefinition ScenarioSource(ScenarioSpec spec) => new()
    {
        Name = "scenario_step_test",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0,
        Enabled = true,
        Scenario = spec,
    };

    private static ScenarioSpec Spec(double rho, int paths, int days, string kind = "normal", long seed = 42) => new()
    {
        Paths = paths,
        Days = days,
        Rho = rho,
        Seed = seed,
        Distribution = new ScenarioDistributionSpec { Kind = kind },
        Instruments =
        [
            new ScenarioInstrumentSpec { Id = "A", Base = 100, Vol = 1, Group = "g" },
            new ScenarioInstrumentSpec { Id = "B", Base = 200, Vol = 1.5, Group = "g" },
            new ScenarioInstrumentSpec { Id = "C", Base = 50, Vol = 0.5, Group = "h" },
        ],
    };

    /// <summary>THE equivalence test. Generates one whole batch via <see cref="ScenarioGenerator.GenerateBatch"/>,
    /// then separately walks the SAME def/request day-by-day via BeginRun+GenerateDay (exactly what
    /// GeneratorGrain.RunAsync's Step branch does, one HTTP call at a time in production), and asserts the
    /// concatenated stepped rows are row-for-row, field-for-field identical to the whole batch — including
    /// bitwise-equal doubles, which only holds if both paths draw the identical RNG sequence in the
    /// identical order.</summary>
    [Fact]
    public void Stepping_a_run_day_by_day_produces_exactly_the_same_rows_as_generating_it_whole()
    {
        var def = ScenarioSource(Spec(rho: 0.5, paths: 12, days: 6));
        var request = new ScenarioRunRequest { RunId = "equivalence-run", Seed = 777 };

        var whole = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.Accepted, whole.Outcome);
        Assert.Equal(12 * 3 * 6, whole.Rows.Count);

        Assert.True(ScenarioGenerator.BeginRun(def, request, out var state, out var failure), $"BeginRun failed: {failure?.Errors}");
        var stepped = new List<ScenarioRow>();
        while (!state!.IsComplete)
        {
            var dayRows = ScenarioGenerator.GenerateDay(state, FixedNowMs);
            Assert.NotEmpty(dayRows); // a well-formed run's GenerateDay never returns empty before IsComplete
            stepped.AddRange(dayRows);
        }

        Assert.Equal(whole.Rows.Count, stepped.Count);
        for (var i = 0; i < whole.Rows.Count; i++)
        {
            var w = whole.Rows[i];
            var s = stepped[i];
            Assert.Equal(w.RunId, s.RunId);
            Assert.Equal(w.PathId, s.PathId);
            Assert.Equal(w.InstrumentId, s.InstrumentId);
            Assert.Equal(w.Day, s.Day);
            Assert.Equal(w.Factor, s.Factor); // bitwise — same RNG draw, same order
            Assert.Equal(w.Shock, s.Shock);
            Assert.Equal(w.Value, s.Value);
            Assert.Equal(w.TsMs, s.TsMs);
        }

        // A further step past the end is a documented no-op, not an error.
        Assert.True(state.IsComplete);
        Assert.Empty(ScenarioGenerator.GenerateDay(state, FixedNowMs));
    }

    /// <summary>Same property, single path/instrument/day-count varied and a different distribution
    /// (lognormal — exercises the multiplicative evolution path, which is exactly the code most likely to
    /// diverge between "whole" and "stepped" if per-path running-value state were reset incorrectly).</summary>
    [Theory]
    [InlineData("normal", 0.0)]
    [InlineData("normal", 1.0)]
    [InlineData("lognormal", 0.3)]
    [InlineData("student_t", 0.7)]
    public void Equivalence_holds_across_distributions_and_rho(string kind, double rho)
    {
        var def = ScenarioSource(Spec(rho: rho, paths: 8, days: 5, kind: kind, seed: 314159));
        var request = new ScenarioRunRequest { RunId = "r", Seed = 2024 };

        var whole = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.Accepted, whole.Outcome);

        Assert.True(ScenarioGenerator.BeginRun(def, request, out var state, out _));
        var stepped = new List<ScenarioRow>();
        while (!state!.IsComplete)
        {
            stepped.AddRange(ScenarioGenerator.GenerateDay(state, FixedNowMs));
        }

        Assert.Equal(whole.Rows.Count, stepped.Count);
        for (var i = 0; i < whole.Rows.Count; i++)
        {
            Assert.Equal(whole.Rows[i].Value, stepped[i].Value);
        }
    }

    /// <summary>A step sequence that recreates a FRESH state (via BeginRun) for every single day, instead
    /// of reusing one continuing state across calls the way GeneratorGrain/GeneratorActor actually do,
    /// does NOT reproduce the whole batch — it deterministically replays day 1 over and over (same seed,
    /// same RNG start state every BeginRun call). This pins that continuation (RNG position, per-path
    /// running value) is load-bearing, not incidental — the equivalence property this file is about
    /// depends on GeneratorGrain.RunAsync caching ScenarioRunState across step calls for the SAME RunId,
    /// which is exactly what it does (see its class doc).</summary>
    [Fact]
    public void Recreating_the_state_every_day_replays_day_one_instead_of_advancing()
    {
        var def = ScenarioSource(Spec(rho: 0.5, paths: 4, days: 3));
        var request = new ScenarioRunRequest { RunId = "r", Seed = 5 };

        var whole = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        var wholeDay1 = whole.Rows.Where(r => r.Day == 1).Select(r => r.Value).ToList();
        var wholeDay2 = whole.Rows.Where(r => r.Day == 2).Select(r => r.Value).ToList();
        Assert.NotEqual(wholeDay1, wholeDay2); // genuine evolution — day 2 differs from day 1

        var repeatedFirstDays = new List<List<double>>();
        for (var i = 0; i < 3; i++)
        {
            Assert.True(ScenarioGenerator.BeginRun(def, request, out var freshState, out _));
            var dayRows = ScenarioGenerator.GenerateDay(freshState!, FixedNowMs);
            Assert.All(dayRows, r => Assert.Equal(1, r.Day)); // a fresh state always starts at day 1
            repeatedFirstDays.Add(dayRows.Select(r => r.Value).ToList());
        }

        // Recreating the state every call means every "step" is bit-for-bit the SAME day 1 — proving the
        // state (not just the seed) has to be carried forward for stepping to mean anything.
        Assert.Equal(repeatedFirstDays[0], repeatedFirstDays[1]);
        Assert.Equal(repeatedFirstDays[0], repeatedFirstDays[2]);
        Assert.Equal(wholeDay1, repeatedFirstDays[0]); // and it matches the whole batch's REAL day 1
    }

    [Fact]
    public void BeginRun_runs_the_same_TOTAL_validation_as_GenerateBatch()
    {
        var spec = Spec(rho: 5.0 /* out of [0,1] */, paths: 4, days: 2);
        var def = ScenarioSource(spec);
        var request = new ScenarioRunRequest { RunId = "" }; // also blank run_id

        var whole = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, whole.Outcome);

        Assert.False(ScenarioGenerator.BeginRun(def, request, out var state, out var failure));
        Assert.Null(state);
        Assert.Equal(ScenarioRunOutcome.ValidationError, failure!.Outcome);
        Assert.Equal(whole.Errors.Count, failure.Errors.Count);
    }

    [Fact]
    public void BeginRun_rejects_a_non_scenario_source_without_throwing()
    {
        var def = new SourceDefinition { Name = "trades", GeneratorProfile = "trades" };
        var ok = ScenarioGenerator.BeginRun(def, new ScenarioRunRequest { RunId = "r" }, out var state, out var failure);

        Assert.False(ok);
        Assert.Null(state);
        Assert.Equal(ScenarioRunOutcome.WrongProfile, failure!.Outcome);
    }
}
