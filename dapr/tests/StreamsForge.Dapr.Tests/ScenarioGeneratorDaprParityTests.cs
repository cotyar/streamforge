using StreamsForge.Abstractions;
using StreamsForge.Host.Generators;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>Wishlist #8: Dapr-flavor parity smoke test. The interesting logic (determinism, correlation,
/// TOTAL validation) is already exhaustively covered once, flavor-agnostically, by
/// orleans/tests/StreamsForge.Engine.Tests/ScenarioGeneratorTests.cs — <c>ScenarioGenerator</c> lives in
/// shared/StreamsForge.AppCore/Generators, referenced identically by both <c>GeneratorGrain.RunAsync</c>
/// (Orleans) and <c>GeneratorActor.RunAsync</c> (Dapr), so there is no separate Dapr-specific row-math to
/// re-test. This file only confirms the SAME pure call, made from the Dapr test project (proving the
/// AppCore reference resolves and behaves identically here too — no actor/timer/sidecar machinery, same
/// "pure logic extracted" convention as GeneratorBatchingTests/ConnectorActorLogicTests in this same
/// directory), and doubles as a compile-time check that GeneratorActor.RunAsync's exact code path (spec ->
/// GenerateBatch -> ToEventRecord) works end to end minus the actual sidecar publish.</summary>
public class ScenarioGeneratorDaprParityTests
{
    private const long FixedNowMs = 1_700_000_000_000L;

    private static SourceDefinition ScenarioSource(ScenarioSpec spec) => new()
    {
        Name = "dapr_scenario_test",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0,
        Enabled = true,
        Scenario = spec,
    };

    [Fact]
    public void Same_seed_produces_the_identical_batch_when_called_from_the_dapr_project()
    {
        var spec = new ScenarioSpec
        {
            Paths = 6,
            Days = 2,
            Rho = 0.4,
            Seed = 11,
            Distribution = new ScenarioDistributionSpec { Kind = "normal" },
            Instruments =
            [
                new ScenarioInstrumentSpec { Id = "X", Base = 50, Vol = 1, Group = "g" },
                new ScenarioInstrumentSpec { Id = "Y", Base = 60, Vol = 1.5, Group = "g" },
            ],
        };
        var def = ScenarioSource(spec);
        var request = new ScenarioRunRequest { RunId = "dapr-run" };

        var a = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        var b = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, a.Outcome);
        Assert.Equal(6 * 2 * 2, a.Rows.Count);
        for (var i = 0; i < a.Rows.Count; i++)
        {
            Assert.Equal(a.Rows[i].Value, b.Rows[i].Value);
            Assert.Equal(a.Rows[i].Shock, b.Rows[i].Shock);
        }

        // Same conversion GeneratorActor.RunAsync applies before publishing each envelope.
        var evt = ScenarioGenerator.ToEventRecord(a.Rows[0], def.Name);
        Assert.Equal(def.Name, evt.Source);
        Assert.Equal("dapr-run", evt["run_id"]);
    }

    [Fact]
    public void A_non_scenario_source_is_rejected_without_throwing_from_the_dapr_project_too()
    {
        var def = new SourceDefinition { Name = "trades", GeneratorProfile = "trades" };
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.WrongProfile, result.Outcome);
    }
}
