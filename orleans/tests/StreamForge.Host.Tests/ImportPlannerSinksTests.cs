using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 009 B2: <see cref="ImportPlanner"/>'s Sinks-secret-aware compare for pipelines/tables — the
/// masked-secret-equality half of ImportPlannerTests' class doc, extended from sources to Sinks. A
/// masked doc Sink must compare equal to (not "updated" against) a stored entity whose real credential
/// it's standing in for, and the merged/"kept stored values" diagnostic must appear exactly when it's
/// needed.
/// </summary>
public class ImportPlannerSinksTests
{
    private static SinkSpec Nats(string token, string subject = "sf.out") => new()
    {
        Kind = SinkKinds.Nats,
        Enabled = true,
        Nats = new NatsPubConfig { Url = "nats://localhost:4222", Subject = subject, Token = token },
    };

    private static PipelineDefinition CatalogPipeline(string token) => new()
    {
        Id = "p1",
        Name = "pipe",
        Sql = "SELECT 1",
        Status = PipelineStatus.Running,
        Sinks = [Nats(token)],
    };

    private static TableDefinition CatalogTable(string token) => new()
    {
        Id = "t1",
        Name = "tbl",
        Sql = "TABLE AS SELECT 1",
        Status = PipelineStatus.Running,
        Sinks = [Nats(token)],
    };

    // ToConfigPipeline/ToConfigTable are `internal` to StreamForge.AppCore (no InternalsVisibleTo to
    // this test project) — FromCatalog (public) is the supported way to get a ConfigPipeline/ConfigTable
    // projection of a stored entity, masked or not, so these helpers go through it instead.
    private static ConfigPipeline AsConfigPipeline(PipelineDefinition def, bool includeSecrets) =>
        ConfigSerializer.FromCatalog([], [def], [], includeSecrets).Pipelines[0];

    private static ConfigTable AsConfigTable(TableDefinition def, bool includeSecrets) =>
        ConfigSerializer.FromCatalog([], [], [def], includeSecrets).Tables[0];

    [Fact]
    public void Pipeline_MaskedDocSinkMatchingStoredCredential_IsSkippedNotUpdated()
    {
        var stored = CatalogPipeline("real-secret");
        var maskedDoc = AsConfigPipeline(stored, includeSecrets: false); // "***" token

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [maskedDoc] }, [], [stored], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("skipped", action.Action);
        Assert.Contains("secrets: kept stored values", action.Diagnostics);
    }

    [Fact]
    public void Pipeline_MaskedDocSinkWithAnotherFieldChanged_IsUpdated_AndKeepsTheStoredCredential()
    {
        var stored = CatalogPipeline("real-secret");
        var maskedDoc = AsConfigPipeline(stored, includeSecrets: false);
        maskedDoc.Description = "changed"; // unrelated field edit — the masked secret should still merge in

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [maskedDoc] }, [], [stored], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("updated", action.Action);
        Assert.Contains("secrets: kept stored values", action.Diagnostics);
    }

    [Fact]
    public void Pipeline_UnmaskedDocWithADifferentRealCredential_IsUpdated_NoSecretsDiagnostic()
    {
        var stored = CatalogPipeline("old-secret");
        var doc = AsConfigPipeline(CatalogPipeline("new-secret"), includeSecrets: true);

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [doc] }, [], [stored], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("updated", action.Action);
        Assert.DoesNotContain("secrets: kept stored values", action.Diagnostics);
    }

    [Fact]
    public void Table_MaskedDocSinkMatchingStoredCredential_IsSkippedNotUpdated()
    {
        var stored = CatalogTable("real-secret");
        var maskedDoc = AsConfigTable(stored, includeSecrets: false);

        var actions = ImportPlanner.Plan(new ConfigDocument { Tables = [maskedDoc] }, [], [], [stored], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("skipped", action.Action);
        Assert.Contains("secrets: kept stored values", action.Diagnostics);
    }

    [Fact]
    public void Table_MaskedDocSinkWithAnotherFieldChanged_IsUpdated_AndKeepsTheStoredCredential()
    {
        var stored = CatalogTable("real-secret");
        var maskedDoc = AsConfigTable(stored, includeSecrets: false);
        maskedDoc.Description = "changed";

        var actions = ImportPlanner.Plan(new ConfigDocument { Tables = [maskedDoc] }, [], [], [stored], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("updated", action.Action);
        Assert.Contains("secrets: kept stored values", action.Diagnostics);
    }

    [Fact]
    public void NewPipelineWithASink_IsCreated()
    {
        var doc = AsConfigPipeline(CatalogPipeline("tok"), includeSecrets: true);

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [doc] }, [], [], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("created", action.Action);
    }
}
