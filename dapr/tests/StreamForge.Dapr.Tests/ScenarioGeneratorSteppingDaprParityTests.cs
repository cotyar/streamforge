using StreamForge.Abstractions;
using StreamForge.Host.Generators;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>Wishlist #9(b): Dapr-flavor parity smoke test for scenario stepping, same convention as
/// <see cref="ScenarioGeneratorDaprParityTests"/> — the interesting logic (the whole-vs-stepped
/// equivalence property, TOTAL validation of BeginRun) is already exhaustively covered once, flavor-
/// agnostically, by orleans/tests/StreamForge.Engine.Tests/ScenarioGeneratorSteppingTests.cs;
/// <c>ScenarioGenerator</c> lives in shared/StreamForge.AppCore/Generators, referenced identically by
/// both <c>GeneratorGrain.RunAsync</c> (Orleans) and <c>GeneratorActor.RunAsync</c> (Dapr)'s Step branch.
/// This file only confirms the SAME pure calls, made from the Dapr test project, doubling as a compile-
/// time check that <c>GeneratorActor.RunAsync</c>'s exact step-mode code path (BeginRun → GenerateDay →
/// ToEventRecord) works end to end minus the actual sidecar publish — the Orleans-side proof that stepping
/// ALSO publishes correctly onto a live stream, byte-identical to a whole run, lives in
/// orleans/tests/StreamForge.Host.Tests/GeneratorGrainStepRunTests.cs (no equivalent Dapr ActorHost
/// integration test exists in this repo's test suite for GeneratorActor — see this wave's own report for
/// why: actor-host-level tests need a running Dapr sidecar, which this project's existing tests
/// consistently avoid in favor of pure-logic extraction, exactly the pattern this file follows).</summary>
public class ScenarioGeneratorSteppingDaprParityTests
{
    private const long FixedNowMs = 1_700_000_000_000L;

    private static SourceDefinition ScenarioSource(ScenarioSpec spec) => new()
    {
        Name = "dapr_scenario_step_test",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0,
        Enabled = true,
        Scenario = spec,
    };

    [Fact]
    public void Stepping_day_by_day_produces_the_identical_batch_when_called_from_the_dapr_project()
    {
        var spec = new ScenarioSpec
        {
            Paths = 5,
            Days = 4,
            Rho = 0.6,
            Seed = 21,
            Distribution = new ScenarioDistributionSpec { Kind = "normal" },
            Instruments =
            [
                new ScenarioInstrumentSpec { Id = "X", Base = 50, Vol = 1, Group = "g" },
                new ScenarioInstrumentSpec { Id = "Y", Base = 60, Vol = 1.5, Group = "g" },
            ],
        };
        var def = ScenarioSource(spec);
        var request = new ScenarioRunRequest { RunId = "dapr-step-run" };

        var whole = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.Accepted, whole.Outcome);
        Assert.Equal(5 * 2 * 4, whole.Rows.Count);

        // Exactly the sequence GeneratorActor.RunAsync's Step branch runs: BeginRun once, then
        // GenerateDay repeatedly.
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
            Assert.Equal(whole.Rows[i].Shock, stepped[i].Shock);
            Assert.Equal(whole.Rows[i].Day, stepped[i].Day);
        }

        // Stepping past the end is a no-op, not an error — same conversion GeneratorActor.RunAsync applies
        // before publishing.
        Assert.Empty(ScenarioGenerator.GenerateDay(state, FixedNowMs));
        var evt = ScenarioGenerator.ToEventRecord(stepped[0], def.Name);
        Assert.Equal(def.Name, evt.Source);
        Assert.Equal("dapr-step-run", evt["run_id"]);
    }

    [Fact]
    public void BeginRun_rejects_a_non_scenario_source_without_throwing_from_the_dapr_project_too()
    {
        var def = new SourceDefinition { Name = "trades", GeneratorProfile = "trades" };
        var ok = ScenarioGenerator.BeginRun(def, new ScenarioRunRequest { RunId = "r", Step = true }, out var state, out var failure);

        Assert.False(ok);
        Assert.Null(state);
        Assert.Equal(ScenarioRunOutcome.WrongProfile, failure!.Outcome);
    }
}
