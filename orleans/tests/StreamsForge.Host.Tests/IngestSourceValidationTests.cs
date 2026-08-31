using StreamsForge.Abstractions;
using StreamsForge.Api;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 008 W4: unit tests for the kind='ingest' additions to <see cref="SourceValidation.Validate"/>
/// (SourceSchemaService.cs) — mirrors SourcesEndpointsLogicTests' per-kind coverage style for the
/// other connector kinds, kept in a separate file since that one is an existing test file (no edits
/// pre-approved for this wave).
/// </summary>
public class IngestSourceValidationTests
{
    private static SourceDefinition Def(string kind, IngestConfig? ingest = null) => new()
    {
        Name = "s",
        Fields = [new FieldDef("price", FieldType.Double)],
        Kind = kind,
        Ingest = ingest,
    };

    [Fact]
    public void Validate_accepts_a_well_formed_ingest_source()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig());
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_recognizes_ingest_as_a_known_kind()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig());
        Assert.DoesNotContain(SourceValidation.Validate(def), e => e.Contains("not recognized"));
    }

    [Fact]
    public void Validate_ingest_kind_does_not_require_a_connector_configuration()
    {
        // Def() sets no Connector; if Ingest wrongly fell through to the connector-required branch
        // this would fail with "kind 'ingest' requires a connector configuration".
        var def = Def(SourceKinds.Ingest, new IngestConfig());
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_rejects_ingest_kind_with_no_ingest_config()
    {
        var def = Def(SourceKinds.Ingest, ingest: null);
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("requires an ingest configuration"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Validate_rejects_non_positive_capacityRows(int capacity)
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig { CapacityRows = capacity });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("ingest.capacityRows must be > 0"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_rejects_non_positive_maxBatchRows(int maxBatch)
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig { MaxBatchRows = maxBatch });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("ingest.maxBatchRows must be > 0"));
    }

    [Fact]
    public void Validate_rejects_maxWaitMs_above_the_30s_server_cap()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig { MaxWaitMs = 30_001 });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("ingest.maxWaitMs must be > 0 and <= 30000"));
    }

    [Fact]
    public void Validate_accepts_maxWaitMs_exactly_at_the_30s_server_cap()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig { MaxWaitMs = 30_000 });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Validate_rejects_non_positive_maxWaitMs()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig { MaxWaitMs = 0 });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("ingest.maxWaitMs must be > 0"));
    }

    [Fact]
    public void Validate_rejects_ingest_config_carried_on_a_non_ingest_kind()
    {
        var def = Def(SourceKinds.Generator, new IngestConfig());
        def.EventsPerSecond = 5;
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("ingest configuration is only valid for kind 'ingest'"));
    }

    [Fact]
    public void Validate_still_requires_fields_for_ingest_kind()
    {
        var def = Def(SourceKinds.Ingest, new IngestConfig());
        def.Fields = [];
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("at least one field is required"));
    }

    // ------------------------------------------------------------------
    // SourceSchemaService.DecideStatusOutcome(bool, bool) — the generalized overload GET
    // /api/sources/{name}/ingest reuses (plan 008 W4); the pre-existing ConnectorRuntimeStatus
    // overload delegates to it, so this also covers that the delegation preserves the original
    // three-way behavior.
    // ------------------------------------------------------------------

    [Fact]
    public void DecideStatusOutcome_bool_overload_is_NotFound_when_source_does_not_exist()
    {
        Assert.Equal(SourceStatusOutcome.NotFound, SourceSchemaService.DecideStatusOutcome(sourceExists: false, statusPresent: true));
        Assert.Equal(SourceStatusOutcome.NotFound, SourceSchemaService.DecideStatusOutcome(sourceExists: false, statusPresent: false));
    }

    [Fact]
    public void DecideStatusOutcome_bool_overload_is_NoContent_when_source_exists_but_has_no_status()
    {
        Assert.Equal(SourceStatusOutcome.NoContent, SourceSchemaService.DecideStatusOutcome(sourceExists: true, statusPresent: false));
    }

    [Fact]
    public void DecideStatusOutcome_bool_overload_is_Ok_when_source_exists_and_has_a_status()
    {
        Assert.Equal(SourceStatusOutcome.Ok, SourceSchemaService.DecideStatusOutcome(sourceExists: true, statusPresent: true));
    }

    [Fact]
    public void DecideStatusOutcome_ConnectorRuntimeStatus_overload_still_behaves_identically_after_delegation()
    {
        Assert.Equal(SourceStatusOutcome.NotFound, SourceSchemaService.DecideStatusOutcome(false, (ConnectorRuntimeStatus?)null));
        Assert.Equal(SourceStatusOutcome.NoContent, SourceSchemaService.DecideStatusOutcome(true, (ConnectorRuntimeStatus?)null));
        Assert.Equal(SourceStatusOutcome.Ok, SourceSchemaService.DecideStatusOutcome(true, new ConnectorRuntimeStatus()));
    }
}
