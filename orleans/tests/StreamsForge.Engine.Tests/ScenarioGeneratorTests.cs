using StreamsForge.Abstractions;
using StreamsForge.Host.Generators;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>Wishlist #8: pure, zero-setup coverage of ScenarioGenerator.GenerateBatch — determinism,
/// correlation structure, and TOTAL validation (a bad spec/request is a ValidationError result, never a
/// thrown exception). See shared/StreamsForge.Contracts/Models.cs's "Wishlist #8" class doc and
/// shared/StreamsForge.AppCore/Generators/ScenarioGenerator.cs's own doc comment for the exact contract
/// this exercises.</summary>
public class ScenarioGeneratorTests
{
    private const long FixedNowMs = 1_700_000_000_000L;

    private static SourceDefinition ScenarioSource(ScenarioSpec spec) => new()
    {
        Name = "scenario_test",
        GeneratorProfile = GeneratorProfiles.Scenario,
        EventsPerSecond = 0,
        Enabled = true,
        Scenario = spec,
    };

    private static ScenarioInstrumentSpec Instrument(string id, double @base = 100, double vol = 1, string group = "g") =>
        new() { Id = id, Base = @base, Vol = vol, Group = group };

    private static ScenarioSpec TwoInstrumentSpec(double rho, int paths = 1, int days = 1, string distributionKind = "normal") => new()
    {
        Paths = paths,
        Days = days,
        Rho = rho,
        Seed = 42,
        Distribution = new ScenarioDistributionSpec { Kind = distributionKind },
        Instruments = [Instrument("A"), Instrument("B")],
    };

    // ------------------------------------------------------------------
    // Determinism — the entire point of the wishlist item.
    // ------------------------------------------------------------------

    [Fact]
    public void Same_seed_and_spec_produce_byte_identical_batches()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.5, paths: 25, days: 3));
        var request = new ScenarioRunRequest { RunId = "run-1", Seed = 12345 };

        var first = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        var second = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, first.Outcome);
        Assert.Equal(first.Rows.Count, second.Rows.Count);
        Assert.Equal(25 * 2 * 3, first.Rows.Count); // N*K*D

        for (var i = 0; i < first.Rows.Count; i++)
        {
            var a = first.Rows[i];
            var b = second.Rows[i];
            Assert.Equal(a.RunId, b.RunId);
            Assert.Equal(a.PathId, b.PathId);
            Assert.Equal(a.InstrumentId, b.InstrumentId);
            Assert.Equal(a.Day, b.Day);
            Assert.Equal(a.Factor, b.Factor); // exact bitwise equality expected (same RNG sequence)
            Assert.Equal(a.Shock, b.Shock);
            Assert.Equal(a.Value, b.Value);
            Assert.Equal(a.TsMs, b.TsMs);
        }
    }

    [Fact]
    public void Different_seed_produces_a_different_batch()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.5, paths: 10, days: 2));
        var a = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "run-a", Seed = 1 }, FixedNowMs);
        var b = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "run-a", Seed = 2 }, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, a.Outcome);
        Assert.Equal(ScenarioRunOutcome.Accepted, b.Outcome);
        Assert.Equal(a.Rows.Count, b.Rows.Count);
        // At least one (Factor, Shock, Value) triple must differ — vanishingly unlikely to collide by
        // chance across 10*2*2=40 rows of continuous doubles if the seed genuinely changed the sequence.
        Assert.Contains(Enumerable.Range(0, a.Rows.Count), i =>
            a.Rows[i].Factor != b.Rows[i].Factor || a.Rows[i].Shock != b.Rows[i].Shock || a.Rows[i].Value != b.Rows[i].Value);
    }

    [Fact]
    public void Seed_override_on_the_request_wins_over_the_specs_default_seed()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.5, paths: 5, days: 1)); // spec.Seed = 42
        var viaSpecDefault = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        var viaOverrideMatchingDefault = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r", Seed = 42 }, FixedNowMs);
        var viaDifferentOverride = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r", Seed = 999 }, FixedNowMs);

        Assert.Equal(viaSpecDefault.Rows[0].Shock, viaOverrideMatchingDefault.Rows[0].Shock);
        Assert.NotEqual(viaSpecDefault.Rows[0].Shock, viaDifferentOverride.Rows[0].Shock);
    }

    // ------------------------------------------------------------------
    // Correlation structure: one common factor per group, mixed by Rho.
    // ------------------------------------------------------------------

    [Fact]
    public void Rho_1_makes_every_instrument_in_a_group_move_identically()
    {
        // shock = sqrt(rho)*factor + sqrt(1-rho)*idiosyncratic; at rho=1 the idiosyncratic term's weight
        // is exactly 0, so both instruments (same Group) get the IDENTICAL per-(path,day) factor draw —
        // this is an EXACT equality, not a statistical one, precisely because both share one Dictionary
        // entry (see ScenarioGenerator.GenerateBatch's factorByGroup).
        var def = ScenarioSource(TwoInstrumentSpec(rho: 1.0, paths: 50, days: 4));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r", Seed = 7 }, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, result.Outcome);
        var byPathDay = result.Rows.ToLookup(r => (r.PathId, r.Day));
        foreach (var group in byPathDay)
        {
            var rows = group.ToList();
            Assert.Equal(2, rows.Count); // instruments A and B
            Assert.Equal(rows[0].Shock, rows[1].Shock);
            Assert.Equal(rows[0].Factor, rows[1].Factor);
        }
    }

    [Fact]
    public void Rho_0_makes_instruments_in_a_group_statistically_independent()
    {
        // At rho=0 shock == idiosyncratic (the group factor is still drawn — see GenerateBatch's doc
        // comment on why — but contributes weight 0), so instruments A and B should show ~zero sample
        // correlation across many paths. TOLERANCE: with 4000 paths the standard error of a Pearson r
        // under the true-independence null is ~1/sqrt(n-3) ~= 0.0158; 0.08 is a ~5-sigma margin, chosen
        // to make this test not flake across CI seeds/hosts while still failing hard if rho stopped being
        // honoured (a rho=1 bug would produce |r| ~= 1.0, nowhere near this bound).
        const double correlationTolerance = 0.08;

        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.0, paths: 4000, days: 1));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r", Seed = 99 }, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, result.Outcome);
        var byInstrument = result.Rows.ToLookup(r => r.InstrumentId);
        var a = byInstrument["A"].OrderBy(r => r.PathId).Select(r => r.Shock).ToArray();
        var b = byInstrument["B"].OrderBy(r => r.PathId).Select(r => r.Shock).ToArray();

        Assert.Equal(a.Length, b.Length);
        var correlation = PearsonCorrelation(a, b);
        Assert.True(Math.Abs(correlation) < correlationTolerance, $"expected |correlation| < {correlationTolerance}, got {correlation}");
    }

    private static double PearsonCorrelation(double[] x, double[] y)
    {
        var n = x.Length;
        var meanX = x.Average();
        var meanY = y.Average();
        double cov = 0, varX = 0, varY = 0;
        for (var i = 0; i < n; i++)
        {
            var dx = x[i] - meanX;
            var dy = y[i] - meanY;
            cov += dx * dy;
            varX += dx * dx;
            varY += dy * dy;
        }

        return cov / Math.Sqrt(varX * varY);
    }

    // ------------------------------------------------------------------
    // TOTAL validation — every bad-config path returns ValidationError, never throws.
    // ------------------------------------------------------------------

    [Fact]
    public void Wrong_profile_source_is_rejected_without_throwing()
    {
        var def = new SourceDefinition { Name = "trades", GeneratorProfile = "trades" };
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.WrongProfile, result.Outcome);
        Assert.Empty(result.Rows);
    }

    [Fact]
    public void Missing_scenario_spec_on_a_scenario_profile_source_is_rejected_without_throwing()
    {
        var def = new SourceDefinition { Name = "s", GeneratorProfile = GeneratorProfiles.Scenario, Scenario = null };
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.WrongProfile, result.Outcome);
    }

    [Fact]
    public void Blank_run_id_is_a_validation_error()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.2));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("run_id"));
        Assert.Empty(result.Rows);
    }

    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    public void Rho_outside_0_1_is_a_validation_error(double badRho)
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: badRho));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("rho"));
    }

    [Fact]
    public void Empty_instrument_list_is_a_validation_error()
    {
        var spec = TwoInstrumentSpec(rho: 0.5);
        spec.Instruments = [];
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("instruments"));
    }

    [Fact]
    public void Duplicate_instrument_ids_are_a_validation_error()
    {
        var spec = TwoInstrumentSpec(rho: 0.5);
        spec.Instruments = [Instrument("A"), Instrument("A")];
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("duplicated"));
    }

    [Fact]
    public void Unknown_distribution_kind_is_a_validation_error()
    {
        var spec = TwoInstrumentSpec(rho: 0.5, distributionKind: "gumbel");
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("distribution.kind"));
    }

    [Fact]
    public void Student_t_with_df_at_or_below_2_is_a_validation_error()
    {
        var spec = TwoInstrumentSpec(rho: 0.5, distributionKind: "student_t");
        spec.Distribution.Df = 2;
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("df"));
    }

    [Fact]
    public void Lognormal_with_a_non_positive_base_is_a_validation_error()
    {
        var spec = new ScenarioSpec
        {
            Paths = 1,
            Days = 1,
            Rho = 0,
            Distribution = new ScenarioDistributionSpec { Kind = "lognormal" },
            Instruments = [Instrument("A", @base: 0)],
        };
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("lognormal"));
    }

    [Fact]
    public void Instruments_source_name_reference_is_rejected_as_a_known_gap_not_silently_ignored()
    {
        var spec = TwoInstrumentSpec(rho: 0.5);
        spec.InstrumentsSourceName = "some_other_source";
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("instrumentsSourceName"));
    }

    [Fact]
    public void Exceeding_max_batch_rows_is_a_validation_error_not_a_truncated_batch()
    {
        var spec = TwoInstrumentSpec(rho: 0.5, paths: 100, days: 1); // 100*2*1 = 200 rows
        spec.MaxBatchRows = 100;
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("maxBatchRows"));
        Assert.Empty(result.Rows); // never a partial/truncated 100-row batch
    }

    [Fact]
    public void Overrides_that_push_the_batch_past_max_batch_rows_are_also_caught()
    {
        // The stored spec is fine (2 instruments x 1 path x 1 day = 2 rows), but a run-time override
        // asking for far more paths must be validated against the SAME cap — overrides are not a way to
        // bypass MaxBatchRows.
        var spec = TwoInstrumentSpec(rho: 0.5, paths: 1, days: 1);
        spec.MaxBatchRows = 10;
        var def = ScenarioSource(spec);
        var request = new ScenarioRunRequest { RunId = "r", Overrides = new ScenarioRunOverrides { Paths = 1000 } };
        var result = ScenarioGenerator.GenerateBatch(def, request, FixedNowMs);
        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        Assert.Contains(result.Errors, e => e.Contains("maxBatchRows"));
    }

    [Fact]
    public void Multiple_independent_problems_are_all_reported_in_one_result_TOTAL()
    {
        var spec = new ScenarioSpec
        {
            Paths = -1,
            Days = 0,
            Rho = 5,
            Distribution = new ScenarioDistributionSpec { Kind = "not-a-kind" },
            Instruments = [],
        };
        var def = ScenarioSource(spec);
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "" }, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.ValidationError, result.Outcome);
        // paths, days, rho, instruments, distribution.kind, run_id — 6 independent problems, all surfaced
        // at once rather than stopping at the first one.
        Assert.True(result.Errors.Count >= 6, $"expected >= 6 independent errors, got {result.Errors.Count}: {string.Join(" | ", result.Errors)}");
    }

    // ------------------------------------------------------------------
    // Row shape / EventRecord conversion.
    // ------------------------------------------------------------------

    [Fact]
    public void GenerateBatch_produces_exactly_N_times_K_times_D_rows_with_day_starting_at_1()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.3, paths: 3, days: 4));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "r" }, FixedNowMs);

        Assert.Equal(ScenarioRunOutcome.Accepted, result.Outcome);
        Assert.Equal(3 * 2 * 4, result.Rows.Count);
        Assert.Equal(result.Rows.Count, result.Accepted);
        Assert.All(result.Rows, r => Assert.InRange(r.Day, 1, 4));
        Assert.All(result.Rows, r => Assert.InRange(r.PathId, 0, 2));
    }

    [Fact]
    public void ToEventRecord_carries_the_exact_row_contract_field_names_plus_reserved_keys()
    {
        var def = ScenarioSource(TwoInstrumentSpec(rho: 0.5, paths: 1, days: 1));
        var result = ScenarioGenerator.GenerateBatch(def, new ScenarioRunRequest { RunId = "run-x" }, FixedNowMs);
        var row = result.Rows[0];

        var evt = ScenarioGenerator.ToEventRecord(row, def.Name);

        Assert.Equal(def.Name, evt.Source);
        Assert.Equal(FixedNowMs, evt.Timestamp);
        Assert.Equal(row.RunId, evt["run_id"]);
        Assert.Equal(row.PathId, evt["path_id"]);
        Assert.Equal(row.InstrumentId, evt["instrument_id"]);
        Assert.Equal(row.Day, evt["day"]);
        Assert.Equal(row.Factor, evt["factor"]);
        Assert.Equal(row.Shock, evt["shock"]);
        Assert.Equal(row.Value, evt["value"]);
    }
}
