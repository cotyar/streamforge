using StreamsForge.Abstractions;
using StreamsForge.Engine;

namespace StreamsForge.Host.Generators;

/// <summary>Wishlist #8: pure, deterministic scenario-batch generation — see Models.cs's "Wishlist #8"
/// class doc (shared/StreamsForge.Contracts/Models.cs) for the row contract, correlation model, and TOTAL-
/// validation contract this implements.
///
/// <para><b>Determinism contract.</b> <see cref="GenerateBatch"/> seeds exactly ONE <see cref="Random"/>
/// from the run's own seed (never <see cref="Random.Shared"/>, which every OTHER
/// <see cref="MarketDataProfiles"/> profile uses on purpose — those are fine to be non-reproducible; this
/// one specifically must not be) and draws from it in a FIXED order: outer loop DAY (ascending), then path
/// (ascending PathId), then — within a (day, path) — one standardized factor per distinct correlation
/// group (in Instruments' first-appearance order), then one idiosyncratic draw per instrument (in
/// Instruments' declared order, always drawn even when Rho == 0 so the draw SEQUENCE — and therefore every
/// downstream value — is identical regardless of Rho; only the mixing weight differs). Two calls with the
/// same (def.Scenario, request.RunId, effective seed, effective overrides, nowMs) therefore draw the
/// identical sequence of uniforms off the identical Random state and produce byte-identical ScenarioRow
/// values. <paramref name="nowMs">nowMs</paramref> is a PARAMETER, never read from the clock internally,
/// precisely so a determinism test can hold it fixed without faking time process-wide.</para>
///
/// <para><b>Day-major, not path-major — this is wishlist #9(b)'s doing, not #8's.</b> The original (wave 3)
/// implementation looped path-outer/day-inner: path 0's every day was drawn before path 1's first. That
/// order cannot be sliced "give me day d for every path" without redoing path 0..N-1's ENTIRE histories
/// first, which is exactly what <c>step: true</c> (wishlist #9(b) — see <see cref="ScenarioRunRequest.Step"/>)
/// needs to do cheaply, one day at a time. Day-major — day outer, path inner — makes "the rows for day d"
/// a genuine prefix-independent slice: <see cref="GenerateDay"/> draws EXACTLY the uniforms day d needs
/// and stops, so N sequential single-day calls sharing one continuing <see cref="ScenarioRunState"/>
/// produce the IDENTICAL row values a single whole-batch call would (see <see cref="GenerateBatch"/>'s
/// body — it is now literally "<see cref="BeginRun"/> then <see cref="GenerateDay"/> in a loop until
/// done", the SAME code path <c>step: true</c> uses one call at a time). Reordering the loop nesting does
/// NOT change any statistical property the existing tests pin (same-seed reproducibility, rho=1 exact
/// group equality, rho=0 statistical independence all hold under ANY fixed deterministic order) — only the
/// specific mapping from (seed) to (raw uniform stream) changed, and nothing in this codebase depends on
/// that mapping's literal values.</para>
/// </summary>
public static class ScenarioGenerator
{
    /// <summary>Generates (or rejects) one run's whole batch. Never throws for a bad spec/request — see
    /// this file's class doc and ScenarioSpec.Validate: every failure mode returns a ScenarioRunResult
    /// with Outcome != Accepted and an empty Rows list instead.
    ///
    /// <para>Implemented as <see cref="BeginRun"/> once, then <see cref="GenerateDay"/> repeatedly until
    /// <see cref="ScenarioRunState.IsComplete"/> — see this file's class doc for why that is exactly what
    /// makes stepping day-by-day (wishlist #9(b)'s <c>step: true</c>) byte-identical to a whole run.</para>
    /// </summary>
    public static ScenarioRunResult GenerateBatch(SourceDefinition def, ScenarioRunRequest request, long nowMs)
    {
        if (!BeginRun(def, request, out var state, out var failure))
        {
            return failure!;
        }

        var rows = new List<ScenarioRow>(state!.Paths * state.Instruments.Count * state.Days);
        while (!state.IsComplete)
        {
            rows.AddRange(GenerateDay(state, nowMs));
        }

        return new ScenarioRunResult { Outcome = ScenarioRunOutcome.Accepted, Accepted = rows.Count, Rows = rows };
    }

    /// <summary>Wishlist #9(b): begins (or rejects) a CONTINUABLE run — the same TOTAL validation
    /// <see cref="GenerateBatch"/> runs (profile match, effective-overrides validation, run_id required),
    /// but produces NO rows: only a <see cref="ScenarioRunState"/> ready for repeated
    /// <see cref="GenerateDay"/> calls. This is what a <c>step: true</c> caller (GeneratorGrain/
    /// GeneratorActor's RunAsync) calls ONCE per RunId — the first <c>step: true</c> request for a RunId
    /// the caller has not seen before — and then caches the returned state, keyed by RunId, for every
    /// subsequent step. A bad spec/request fails HERE, on the first step, exactly like a whole-batch run
    /// fails on its one and only call — never partway through a step sequence.</summary>
    public static bool BeginRun(SourceDefinition def, ScenarioRunRequest request, out ScenarioRunState? state, out ScenarioRunResult? failure)
    {
        if (def.GeneratorProfile != GeneratorProfiles.Scenario || def.Scenario is null)
        {
            state = null;
            failure = new ScenarioRunResult { Outcome = ScenarioRunOutcome.WrongProfile };
            return false;
        }

        var spec = def.Scenario;
        var overrides = request.Overrides;
        var effectivePaths = overrides?.Paths ?? spec.Paths;
        var effectiveDays = overrides?.Days ?? spec.Days;
        var effectiveRho = overrides?.Rho ?? spec.Rho;
        var effectiveDistribution = overrides?.Distribution ?? spec.Distribution;

        var errors = spec.Validate(effectivePaths, effectiveDays, effectiveRho, effectiveDistribution);
        if (string.IsNullOrWhiteSpace(request.RunId))
        {
            errors.Add("run_id is required");
        }

        if (errors.Count > 0)
        {
            state = null;
            failure = new ScenarioRunResult { Outcome = ScenarioRunOutcome.ValidationError, Errors = errors };
            return false;
        }

        var instruments = spec.Instruments;
        // First-appearance order (List.Select(...).Distinct() preserves it) — part of the fixed RNG-draw
        // order this file's class doc promises, not an arbitrary implementation detail.
        var groupKeys = instruments.Select(GroupKey).Distinct().ToList();
        var kind = effectiveDistribution.Kind.Trim().ToLowerInvariant();

        var seed = request.Seed ?? spec.Seed;
        // System.Random has no long-seed constructor; fold the high/low halves together (XOR, not a
        // truncating cast alone) so two seeds differing only in their upper 32 bits don't collide.
        var rng = new Random(unchecked((int)(seed ^ (seed >>> 32))));

        // Per-PATH running level, seeded to each instrument's Base — this is the piece that has to
        // survive across GenerateDay calls (a path's Value on day d depends on day d-1's Value), which is
        // exactly why it lives on the continuable state rather than a local the whole-batch loop used to
        // reset per path.
        var currentValueByPath = new Dictionary<int, Dictionary<string, double>>(effectivePaths);
        for (var p = 0; p < effectivePaths; p++)
        {
            var cv = new Dictionary<string, double>(instruments.Count, StringComparer.Ordinal);
            foreach (var instrument in instruments)
            {
                cv[instrument.Id] = instrument.Base;
            }

            currentValueByPath[p] = cv;
        }

        state = new ScenarioRunState
        {
            RunId = request.RunId,
            Instruments = instruments,
            GroupKeys = groupKeys,
            Distribution = effectiveDistribution,
            Kind = kind,
            SqrtRho = Math.Sqrt(effectiveRho),
            SqrtOneMinusRho = Math.Sqrt(1.0 - effectiveRho),
            Paths = effectivePaths,
            Days = effectiveDays,
            Rng = rng,
            CurrentValueByPath = currentValueByPath,
        };
        failure = null;
        return true;
    }

    /// <summary>Wishlist #9(b): generates exactly the NEXT unemitted day's rows (every path, one day) for
    /// a <paramref name="state"/> created by <see cref="BeginRun"/>, advances
    /// <see cref="ScenarioRunState.NextDay"/> by one, and returns them. Returns an EMPTY list — never an
    /// error — when <see cref="ScenarioRunState.IsComplete"/> is already true (every day 1..Days has been
    /// emitted): stepping past the end of a run is a no-op, not a failure (see
    /// <see cref="ScenarioRunRequest.Step"/>'s doc comment for why).</summary>
    public static List<ScenarioRow> GenerateDay(ScenarioRunState state, long nowMs)
    {
        if (state.IsComplete)
        {
            return [];
        }

        var day = state.NextDay;
        var rows = new List<ScenarioRow>(state.Paths * state.Instruments.Count);
        var factorByGroup = new Dictionary<string, double>(state.GroupKeys.Count, StringComparer.Ordinal);

        for (var pathId = 0; pathId < state.Paths; pathId++)
        {
            foreach (var group in state.GroupKeys)
            {
                factorByGroup[group] = NextStandardized(state.Rng, state.Distribution, state.Kind);
            }

            var currentValue = state.CurrentValueByPath[pathId];
            foreach (var instrument in state.Instruments)
            {
                var factor = factorByGroup[GroupKey(instrument)];
                var idiosyncratic = NextStandardized(state.Rng, state.Distribution, state.Kind);
                var shock = state.SqrtRho * factor + state.SqrtOneMinusRho * idiosyncratic;

                var previous = currentValue[instrument.Id];
                // "lognormal": the standard GBM-discretization drift-correction (-Vol^2/2) so a Vol
                // scale alone doesn't bias E[Value] upward; every other Kind is a plain additive walk.
                var value = state.Kind == "lognormal"
                    ? previous * Math.Exp(instrument.Vol * shock - 0.5 * instrument.Vol * instrument.Vol)
                    : previous + instrument.Vol * shock;
                currentValue[instrument.Id] = value;

                rows.Add(new ScenarioRow
                {
                    RunId = state.RunId,
                    PathId = pathId,
                    InstrumentId = instrument.Id,
                    Day = day,
                    Factor = factor,
                    Shock = shock,
                    Value = value,
                    TsMs = nowMs,
                });
            }
        }

        state.NextDay = day + 1;
        return rows;
    }

    /// <summary>Converts one generated row to the EventRecord shape published onto the source's stream —
    /// the same run_id/path_id/instrument_id/day/factor/shock/value field names the wishlist's row
    /// contract specifies (row.TsMs becomes the reserved <see cref="EventRecord.TimestampField"/>, exactly
    /// as every other MarketDataProfiles profile stamps "_ts"), so a table built on a scenario source sees
    /// the identical shape it would from any other generator profile.</summary>
    public static EventRecord ToEventRecord(ScenarioRow row, string sourceName) => new()
    {
        [EventRecord.TimestampField] = row.TsMs,
        [EventRecord.SourceField] = sourceName,
        ["run_id"] = row.RunId,
        ["path_id"] = row.PathId,
        ["instrument_id"] = row.InstrumentId,
        ["day"] = row.Day,
        ["factor"] = row.Factor,
        ["shock"] = row.Shock,
        ["value"] = row.Value,
    };

    /// <summary>Blank/null Group => the instrument is its own singleton group (see ScenarioInstrumentSpec's
    /// doc comment) — the NUL prefix can never collide with a real (user-supplied, non-empty) group name.</summary>
    private static string GroupKey(ScenarioInstrumentSpec instrument) =>
        string.IsNullOrEmpty(instrument.Group) ? " singleton:" + instrument.Id : instrument.Group;

    private static double NextStandardized(Random rng, ScenarioDistributionSpec dist, string kind) => kind switch
    {
        "student_t" => NextStandardizedStudentT(rng, dist.Df),
        // "normal" and "lognormal" both draw from a standard normal — "lognormal" only changes how Value
        // evolves from the shock (see GenerateDay), not the shock's own shape.
        _ => NextStandardNormal(rng),
    };

    /// <summary>Box-Muller, consuming exactly 2 uniform draws per call and caching nothing — a cached
    /// "second" value (the usual Box-Muller optimization) would make the RNG-draw-count-per-row depend on
    /// call history, which this file's determinism contract deliberately avoids reasoning about.</summary>
    private static double NextStandardNormal(Random rng)
    {
        var u1 = 1.0 - rng.NextDouble(); // (0,1], never exactly 0 — avoids log(0)
        var u2 = rng.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
    }

    /// <summary>Classical T = Z / sqrt(V/df) construction, V ~ ChiSquare(df) built as a sum of df squared
    /// standard normals — needs an INTEGER degrees-of-freedom, so a fractional Df (validated only to be
    /// &gt; 2, not to be a whole number) is rounded UP to the next integer &gt;= 3. This is an
    /// approximation of Student-t sampling at fractional df, not exact — documented tradeoff, chosen over
    /// a full Gamma-distribution sampler for this demo-scale generator. The result is standardized to
    /// variance 1 (Var(T) = df/(df-2) for df &gt; 2) so Vol means the same thing regardless of Kind.</summary>
    private static double NextStandardizedStudentT(Random rng, double df)
    {
        var dfInt = Math.Max(3, (int)Math.Ceiling(df));
        var z = NextStandardNormal(rng);
        var chiSquare = 0.0;
        for (var i = 0; i < dfInt; i++)
        {
            var n = NextStandardNormal(rng);
            chiSquare += n * n;
        }

        var t = z / Math.Sqrt(chiSquare / dfInt);
        return t / Math.Sqrt(dfInt / (double)(dfInt - 2));
    }
}

/// <summary>Wishlist #9(b): continuable per-run generation state for <c>step: true</c>. Held in memory
/// ONLY by the owning grain/actor (see <c>GeneratorGrain.RunAsync</c> / <c>GeneratorActor.RunAsync</c>),
/// keyed by RunId — it never crosses an Orleans/Dapr RPC boundary, so it is a plain mutable class, not
/// <c>[GenerateSerializer]</c>. A step sequence does not survive the owning activation being deactivated/
/// evicted (Orleans) or the actor state store being empty on a fresh activation (Dapr) — the SAME
/// in-memory-only lifecycle <c>GeneratorGrain._def</c> itself already has; see those classes' StartAsync/
/// StopAsync for exactly when this is created and discarded.</summary>
public sealed class ScenarioRunState
{
    public required string RunId { get; init; }
    public required List<ScenarioInstrumentSpec> Instruments { get; init; }
    public required List<string> GroupKeys { get; init; }
    public required ScenarioDistributionSpec Distribution { get; init; }
    public required string Kind { get; init; }
    public required double SqrtRho { get; init; }
    public required double SqrtOneMinusRho { get; init; }
    public required int Paths { get; init; }
    public required int Days { get; init; }
    public required Random Rng { get; init; }

    /// <summary>Per-path running level, keyed by instrument id — see <see cref="ScenarioGenerator.BeginRun"/>'s
    /// doc comment for why this (not a single reused dictionary) is what day-major stepping needs.</summary>
    public required Dictionary<int, Dictionary<string, double>> CurrentValueByPath { get; init; }

    /// <summary>1-based; the day <see cref="ScenarioGenerator.GenerateDay"/> will emit NEXT.</summary>
    public int NextDay { get; set; } = 1;

    /// <summary>True once every day 1..Days has been emitted. A further <see cref="ScenarioGenerator.GenerateDay"/>
    /// call is then a no-op (empty rows), never an error — see <see cref="ScenarioRunRequest.Step"/>'s doc
    /// comment.</summary>
    public bool IsComplete => NextDay > Days;
}
