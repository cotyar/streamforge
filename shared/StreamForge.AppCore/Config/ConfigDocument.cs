using StreamForge.Abstractions;

namespace StreamForge.AppCore.Config;

// Plan 006 (D-I/D-J): the config import/export document model. Deliberately NOT
// [GenerateSerializer] — these types never cross grain/actor transport, they live only at the
// config import/export API boundary (shared/StreamForge.Api, a future wave) and inside this
// pure, runtime-free engine (parse/compose/plan/write). See ConfigSerializer for the JSON/YAML
// wire shape and ConfigComposer/ImportPlanner for how documents combine and diff against a
// running catalog.

/// <summary>
/// The canonical config document (D-I): <c>{ version, include, sources, pipelines, tables }</c>.
/// Definitions only — no ids, no runtime status, no timestamps/CreatedBy, no users/credentials.
/// <see cref="Include"/> lists relative paths to other documents that are composed BEFORE this
/// one (see <see cref="ConfigComposer.ComposeWithIncludes"/>) — this document (the includer)
/// always wins per-entity over anything its includes declare. <see cref="Sources"/> reuses
/// <see cref="SourceDefinition"/> directly (sources are already id-less — nothing to strip);
/// <see cref="Pipelines"/>/<see cref="Tables"/> use the trimmed <see cref="ConfigPipeline"/>/
/// <see cref="ConfigTable"/> shapes below.
/// </summary>
public sealed class ConfigDocument
{
    public int Version { get; set; } = 1;
    public List<string> Include { get; set; } = [];
    public List<SourceDefinition> Sources { get; set; } = [];
    public List<ConfigPipeline> Pipelines { get; set; } = [];
    public List<ConfigTable> Tables { get; set; } = [];
}

/// <summary>
/// A PIPELINE definition inside a config document (D-I). Deliberately missing everything that's
/// runtime/catalog state rather than configuration: no id, no <see cref="PipelineStatus"/>
/// (replaced by the boolean <see cref="Running"/> — the DESIRED state), no Error, no
/// CreatedBy/CreatedAtMs/UpdatedAtMs. A catalog pipeline whose <see cref="PipelineDefinition.Status"/>
/// is <see cref="PipelineStatus.Failed"/> exports as <c>Running == true</c> — it was asked to run
/// and hasn't been asked to stop; only <see cref="PipelineStatus.Stopped"/> maps to <c>false</c>.
/// See <see cref="ConfigSerializer.FromCatalog"/> for the exact mapping.
/// </summary>
public sealed class ConfigPipeline
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public string Sql { get; set; } = "";
    public bool Running { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, string> Metadata { get; set; } = [];
    /// <summary>Plan 009 B2: mirrors <see cref="PipelineDefinition.Sinks"/> (additive). NatsPubConfig
    /// credentials are masked (<see cref="SourceKinds.SecretMask"/>) in an export unless the caller asks
    /// for <c>includeSecrets</c> — see <see cref="ConfigSerializer.FromCatalog"/> — following the exact
    /// convention Sources' secrets already use in this document.</summary>
    public List<SinkSpec> Sinks { get; set; } = [];
}

/// <summary>
/// A TABLE definition inside a config document (D-I) — same shape as <see cref="ConfigPipeline"/>
/// plus the table-only configuration knobs (search + row-history + parallelism), mirroring
/// <see cref="TableDefinition"/>'s user-configurable surface. Runtime/derived fields are dropped:
/// no id, Status/Error, CreatedBy/timestamps, OutputFields, StreamInputs/TableInputs (those are
/// products of the last successful SQL compile, not configuration — re-derived on import when the
/// SQL is recompiled). <see cref="SearchMode"/>/<see cref="HistoryMode"/> carry the source enum's
/// C# member NAME as a string (e.g. "Exact"/"Fuzzy", "All"/"LastN"/"FirstN"/"MinBy"/"MaxBy") — see
/// <see cref="ConfigSerializer"/> for the exact wire shape and round-trip rule.
/// </summary>
public sealed class ConfigTable
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public string Sql { get; set; } = "";
    public bool Running { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, string> Metadata { get; set; } = [];
    public bool SearchEnabled { get; set; }
    public string SearchMode { get; set; } = "Exact";
    public bool HistoryEnabled { get; set; }
    public string HistoryMode { get; set; } = "All";
    public int HistoryLimit { get; set; } = 10;
    public string? HistoryByField { get; set; }
    public long HistoryWindowMs { get; set; }
    public int Parallelism { get; set; } = 1;
    /// <summary>Plan 009 B2: mirrors <see cref="TableDefinition.Sinks"/> — see the identical note on
    /// <see cref="ConfigPipeline.Sinks"/>.</summary>
    public List<SinkSpec> Sinks { get; set; } = [];

    /// <summary>Plan 011 C2: mirrors <see cref="TableDefinition.RetentionMaxRows"/>. Carried through
    /// export/import — unlike the purely operational Persistence/FlushMs knobs, which this document
    /// deliberately does not carry, retention changes what ROWS the table holds, so a config round-trip
    /// that dropped it would promote a differently-behaving table.</summary>
    public int RetentionMaxRows { get; set; }

    /// <summary>Plan 011 C2: mirrors <see cref="TableDefinition.RetentionTtlMs"/>.</summary>
    public long RetentionTtlMs { get; set; }

    /// <summary>Plan 011 D1: mirrors <see cref="TableDefinition.ShardBy"/>. Carried through export/import
    /// for the same reason retention is: it is not an operational knob but part of what the table IS —
    /// where its per-key state and history live, and (via the searchEnabled refusal) what else it can be
    /// configured with. A round-trip that dropped it would promote a table whose per-key lookups silently
    /// stopped existing. Empty = not sharded, so an older document imports unchanged.</summary>
    public List<string> ShardBy { get; set; } = [];
}
