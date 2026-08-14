using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 014 (wave G): <see cref="SourceValidation.Validate"/>'s four PULL-shaped-kind edits — a sibling to
/// <see cref="SourceValidationNatsTests"/> (nats/message-transport coverage) and
/// <see cref="PolledTransportRegistryTests"/> (the SPI/registry acceptance test), kept in its own file since
/// neither of those is mine to edit (new-files-only convention). A polled kind this file has never heard of
/// otherwise — "quuxdb" — is registered from a TEST, exactly like "fizzdb"/"buzzdb" before it, and used to
/// prove: it's a known kind, its own <c>Validate</c> runs, it takes a schedule a subscriber kind would not,
/// its mapping is never forced, an unknown kind's error names it, and <c>POST /api/transports/{kind}/probe</c>
/// (via <see cref="SourceSchemaService.ProbeAsync"/>) tells "nobody registered this" apart from "registered
/// but can't probe" apart from a clean success/failure.
///
/// <para><b>Registration hygiene.</b> <see cref="PolledTransports.Register"/> is process-global and
/// permanent (see that class's own doc), so — exactly as <see cref="PolledTransportRegistryTests"/> does —
/// the fake kinds are registered exactly ONCE from this class's static constructor, with names distinctive
/// enough not to collide with any other suite's fake kind ("fizzdb", "buzzdb", "fizzdb-dapr", "fizz",
/// "fizz-sink" are all taken elsewhere in this repo).</para>
/// </summary>
public class SourceValidationPolledTests
{
    private const string QuuxDbKind = "quuxdb";

    /// <summary>A second registered kind that does NOT implement <see cref="ISchemaProbe"/> — the "known
    /// but cannot probe" half of the probe endpoint's two-way distinction.</summary>
    private const string QuuxDb2Kind = "quuxdb2";

    private static readonly QuuxDb Shared = new();
    private static readonly QuuxDbNoProbe SharedNoProbe = new();

    static SourceValidationPolledTests()
    {
        PolledTransports.Register(Shared);
        PolledTransports.Register(SharedNoProbe);
    }

    // ------------------------------------------------------------------
    // The fake transports.
    // ------------------------------------------------------------------

    private sealed class QuuxDb : IPolledTransport, ISchemaProbe
    {
        /// <summary>Set to make the next probe fail the way a real one does: a thrown exception, not a
        /// returned error code — <see cref="SourceSchemaService.ProbeAsync"/>'s contract is to turn this
        /// into a clean diagnostic rather than an unhandled 500.</summary>
        public Exception? FailNextProbe { get; set; }

        /// <summary>Set to make the next probe hang past the caller's timeout, so
        /// <see cref="SourceSchemaService.ProbeAsync"/>'s own <c>CancelAfter</c> bound is what trips —
        /// not a real network stall, which a unit test cannot wait out.</summary>
        public bool HangNextProbe { get; set; }

        public string Kind => QuuxDbKind;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            if (string.IsNullOrWhiteSpace(def.Connector?.Db?.Table))
            {
                errors.Add("kind 'quuxdb' requires connector.db.table");
            }
        }

        public Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct) =>
            Task.FromResult(new PolledBatch([], null, false));

        public async Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct)
        {
            if (FailNextProbe is not null)
            {
                var fail = FailNextProbe;
                FailNextProbe = null;
                throw fail;
            }

            if (HangNextProbe)
            {
                HangNextProbe = false;
                await Task.Delay(Timeout.InfiniteTimeSpan, ct); // only ever completes by ct firing
            }

            return new SchemaProbeResult(
                [new FieldDef("id", FieldType.String), new FieldDef("amount", FieldType.Double)],
                ["amount is numeric(18,4); it will be read as Double and lose precision"]);
        }

        public TransportDescriptor Describe() => new()
        {
            Kind = QuuxDbKind,
            Label = "QuuxDB",
            ConfigProperty = "db",
            Polled = true,
            Mapping = false,
            CanProbe = true,
        };
    }

    /// <summary>Registered and polled like <see cref="QuuxDb"/>, but deliberately does NOT implement
    /// <see cref="ISchemaProbe"/> — a kind that exists and runs sources, yet was never given schema
    /// discovery.</summary>
    private sealed class QuuxDbNoProbe : IPolledTransport
    {
        public string Kind => QuuxDb2Kind;

        public void Validate(SourceDefinition def, List<string> errors) { }

        public Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct) =>
            Task.FromResult(new PolledBatch([], null, false));

        public TransportDescriptor Describe() => new()
        {
            Kind = QuuxDb2Kind, Label = "QuuxDB2", ConfigProperty = "db", Polled = true, Mapping = false,
        };
    }

    private static SourceDefinition Def(string kind = QuuxDbKind, ConnectorConfig? connector = null, ScheduleSpec? schedule = null)
    {
        connector ??= new ConnectorConfig { Db = new DbSourceConfig { Table = "orders" } };
        connector.Schedule = schedule;
        return new SourceDefinition
        {
            Name = "s",
            Kind = kind,
            Fields = [new FieldDef("id", FieldType.String)],
            Connector = connector,
        };
    }

    // ------------------------------------------------------------------
    // 1 — Known kind, own Validate runs.
    // ------------------------------------------------------------------

    [Fact]
    public void QuuxDb_kind_is_recognized()
    {
        var errors = SourceValidation.Validate(Def());
        Assert.DoesNotContain(errors, e => e.Contains("not recognized"));
    }

    [Fact]
    public void QuuxDb_own_validate_runs_and_its_errors_surface()
    {
        var def = Def(connector: new ConnectorConfig { Db = new DbSourceConfig { Table = "" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("requires connector.db.table"));
    }

    [Fact]
    public void QuuxDb_accepts_a_well_formed_config()
    {
        Assert.Empty(SourceValidation.Validate(Def()));
    }

    // ------------------------------------------------------------------
    // 2 — Schedule: taken for a polled kind, ignored for a subscriber kind (SourceValidationNatsTests
    // pins the nats half of this contrast; this pins the polled half).
    // ------------------------------------------------------------------

    [Fact]
    public void QuuxDb_takes_a_schedule_where_a_subscriber_kind_would_not()
    {
        // Same invalid interval SourceValidationNatsTests uses to prove nats does NOT validate it. Here,
        // for a REGISTERED POLLED kind, the very same schedule DOES get validated — the registry lookup at
        // SourceValidation's schedule rule is doing real work, not always resolving to "ignore".
        var def = Def(schedule: new ScheduleSpec { IntervalMs = 10 });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("connector.schedule"));
    }

    [Fact]
    public void QuuxDb_accepts_a_well_formed_schedule()
    {
        var def = Def(schedule: new ScheduleSpec { IntervalMs = 60_000 });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // 3 — Mapping is never forced for a polled kind (the SELECT list IS the mapping — IPolledTransport's
    // own doc comment). Even a mapping document that would fail validation elsewhere is simply never
    // looked at here.
    // ------------------------------------------------------------------

    [Fact]
    public void QuuxDb_does_not_require_a_mapping()
    {
        Assert.Empty(SourceValidation.Validate(Def()));
    }

    [Fact]
    public void QuuxDb_never_validates_a_mapping_document_even_an_invalid_one()
    {
        var connector = new ConnectorConfig
        {
            Db = new DbSourceConfig { Table = "orders" },
            Mapping = new MappingSpec { ItemsPath = "$", DedupKeyField = "missing-field", Fields = [] },
        };
        Assert.Empty(SourceValidation.Validate(Def(connector: connector)));
    }

    // ------------------------------------------------------------------
    // 4 — Unknown-kind error names the registered polled kinds, and degrades sensibly when none apply.
    // ------------------------------------------------------------------

    [Fact]
    public void Unknown_kind_error_lists_the_registered_polled_kinds()
    {
        var def = Def(kind: "not-a-real-kind");
        var message = Assert.Single(SourceValidation.Validate(def), e => e.Contains("not recognized"));
        Assert.Contains(QuuxDbKind, message);
        Assert.Contains(QuuxDb2Kind, message);
    }

    [Fact]
    public void Unknown_kind_message_stays_well_formed_regardless_of_registry_population()
    {
        // On the default build PolledTransports.Kinds is EMPTY (see that class's own doc) — string.Join /
        // Concat over an empty sequence contributes nothing, so this same code path never regresses to a
        // dangling separator or a crash whether zero, one, or many polled kinds are registered. Exercised
        // here with an unrelated kind name so the assertion is about FORMAT, not which kinds happen to be
        // registered by the time this test runs in the shared test process.
        var def = Def(kind: "totally-unregistered-kind-xyz");
        var message = Assert.Single(SourceValidation.Validate(def), e => e.Contains("not recognized"));

        Assert.DoesNotContain(",,", message);
        Assert.DoesNotContain(", )", message);
        Assert.EndsWith(")", message);
    }

    // ------------------------------------------------------------------
    // 5 — POST /api/transports/{kind}/probe logic (SourceSchemaService.ProbeAsync).
    // ------------------------------------------------------------------

    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Probe_of_an_unregistered_kind_is_UnknownKind_and_names_whats_registered()
    {
        var outcome = await SourceSchemaService.ProbeAsync("no-such-kind", Def(), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.UnknownKind, outcome.Kind);
        Assert.Null(outcome.Result);
        Assert.Contains(QuuxDbKind, outcome.Message);
    }

    [Fact]
    public async Task Probe_of_a_registered_kind_that_cannot_probe_is_CannotProbe_and_says_so_distinctly()
    {
        // The distinct answer from "unregistered" above: this kind IS known (it runs sources today) — it
        // simply never implemented ISchemaProbe.
        var outcome = await SourceSchemaService.ProbeAsync(QuuxDb2Kind, Def(kind: QuuxDb2Kind), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.CannotProbe, outcome.Kind);
        Assert.Null(outcome.Result);
        Assert.Contains("does not support schema discovery", outcome.Message);
    }

    [Fact]
    public async Task Probe_success_returns_fields_and_the_precision_loss_diagnostic()
    {
        var outcome = await SourceSchemaService.ProbeAsync(QuuxDbKind, Def(), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.Ok, outcome.Kind);
        Assert.Null(outcome.Message);
        Assert.Equal(["id", "amount"], outcome.Result!.Fields.Select(f => f.Name));
        // Diagnostics matter as much as fields here: a numeric->Double coercion is a real precision loss,
        // and the probe REPORTS it rather than silently rounding — the whole point of SchemaProbeResult.
        Assert.Contains(outcome.Result.Diagnostics, d => d.Contains("lose precision"));
    }

    [Fact]
    public async Task A_throwing_probe_becomes_a_clean_diagnostic_not_a_500()
    {
        Shared.FailNextProbe = new InvalidOperationException("connection refused");

        var outcome = await SourceSchemaService.ProbeAsync(QuuxDbKind, Def(), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.Ok, outcome.Kind);       // "could not look" is a 200 with a diagnostic
        Assert.NotNull(outcome.Result);
        Assert.Empty(outcome.Result!.Fields);
        Assert.Contains(outcome.Result.Diagnostics, d => d.Contains("connection refused"));
    }

    [Fact]
    public async Task A_hanging_probe_is_cut_off_by_the_callers_timeout_bound()
    {
        Shared.HangNextProbe = true;

        var outcome = await SourceSchemaService.ProbeAsync(QuuxDbKind, Def(), TimeSpan.FromMilliseconds(50), CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.Ok, outcome.Kind);
        Assert.NotNull(outcome.Result);
        Assert.Contains(outcome.Result!.Diagnostics, d => d.Contains("timed out"));
    }

    [Fact]
    public async Task A_probe_cancelled_by_the_caller_itself_is_not_swallowed_into_a_diagnostic()
    {
        // Distinguishes the CALLER going away (request aborted — must propagate, exactly the "shutting
        // down" carve-out FileSinkClient.PublishAsync/NatsSinkClient.PublishAsync both take) from the
        // server's OWN timeout bound tripping (the case above, which IS turned into a diagnostic).
        Shared.HangNextProbe = true;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // ThrowsAnyAsync, not ThrowsAsync: Task.Delay on an already-cancelled token throws
        // TaskCanceledException, a SUBCLASS of OperationCanceledException — the rethrow preserves whichever
        // concrete type the transport's own await produced, which is the honest behaviour ("throw;", not a
        // re-wrap).
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => SourceSchemaService.ProbeAsync(QuuxDbKind, Def(), ProbeTimeout, cts.Token));
    }
}
