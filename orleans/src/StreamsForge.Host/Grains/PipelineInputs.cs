using Orleans;
using StreamsForge.AppCore.Environments;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Facades;

namespace StreamsForge.Host.Grains;

/// <summary>
/// Table-over-pipeline: the one place that knows a PIPELINE may stand in for a stream relation in a
/// table's SQL, and the one place that knows how to turn that pipeline's published output back into the
/// <see cref="EventRecord"/> the engine expects.
///
/// <para><b>Why the engine never learns about this.</b> <c>SqlCompiler.CompileTable</c> takes a
/// name → <see cref="SourceSchema"/> dictionary and a table-schema dictionary; it has no third category
/// and needs none, because a relation is (name, schema) and nothing more. A pipeline therefore enters the
/// compile as an ordinary entry in the STREAM-schema dictionary and comes back out in
/// <c>TableCompileResult.StreamInputs</c> exactly like a source would. Everything that distinguishes the
/// two — which stream namespace to subscribe, which payload type, which key (a source's NAME vs a
/// pipeline's ID) — is host-side, and lives here. Hard rule 2 (the Engine stays pure) is why it must.</para>
///
/// <para><b>Every consumer must build the schema dictionary the same way</b>, or the plan a grain
/// compiles at start differs from the one the registry compiled at create — same SQL, different relation
/// set, silently different dataflow. <see cref="BuildStreamSchemas"/> is that single definition; the
/// registry calls it against its own state, the grains against
/// <c>GetSourcesAsync</c>/<c>GetPipelinesAsync</c> (both on <c>RegistryGrain</c>'s
/// <c>[MayInterleave]</c> allowlist, so calling them from inside a start that the registry is itself
/// awaiting cannot deadlock).</para>
///
/// <para><b>No replay, deliberately and unavoidably.</b> A source has a replay ring and an attach
/// protocol (<c>IConnectorGrain.BeginAttachAsync</c>); an upstream table has
/// <c>ITableGrain.AttachSnapshotAsync</c>. A pipeline has neither — it is a stream transform holding no
/// materialized result to hand over (<c>_recentResults</c> is a bounded UI convenience, not a
/// consistent cut, and admitting it would double-count against live traffic with nothing to fence it
/// by). So a table attaching to a pipeline starts empty and sees only what the pipeline publishes from
/// that moment on. Callers skip the late-consumer attach protocol for pipeline inputs for this reason,
/// not by oversight.</para>
/// </summary>
internal static class PipelineInputs
{
    /// <summary>The stream-relation dictionary a TABLE compiles against: every source, plus every
    /// pipeline that has a compiled output schema. A pipeline with empty <c>OutputFields</c> (draft SQL,
    /// or a record written before that field existed and not yet backfilled) contributes no relation —
    /// naming it is then an ordinary "unknown relation" diagnostic rather than a compile against a schema
    /// that is empty because nobody filled it in.
    ///
    /// <para>Sources win a name collision here purely as a defensive tiebreak; the write paths refuse to
    /// create one in the first place (<c>RegistryGrain.ValidateUniquePipelineName</c> /
    /// <c>UpsertSourceAsync</c>), because a relation name that resolves to two different streams is not a
    /// preference to be expressed, it is a catalog that cannot be executed unambiguously.</para></summary>
    public static Dictionary<string, SourceSchema> BuildStreamSchemas(
        IEnumerable<SourceDefinition> sources, IEnumerable<PipelineDefinition> pipelines)
    {
        var schemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);

        foreach (var p in pipelines)
        {
            if (p.OutputFields.Count == 0) continue;
            schemas[p.Name] = new SourceSchema(
                p.Name, p.OutputFields.ToDictionary(f => f.Name, f => TableDataflowFactory.MapFieldKind(f.Type)));
        }

        foreach (var s in sources)
        {
            schemas[s.Name] = new SourceSchema(
                s.Name, s.Fields.ToDictionary(f => f.Name, f => TableDataflowFactory.MapFieldKind(f.Type)));
        }

        return schemas;
    }

    /// <summary>Resolves one compiled stream-input name to the pipeline that owns it, or null when the
    /// name is a source (the common case) or names nothing at all. Best-effort: a registry that throws
    /// leaves the caller on its existing source path, which is the same "losing the extra is bad, refusing
    /// to start the table is worse" rule every other lookup on this path follows.</summary>
    public static async Task<PipelineDefinition?> FindAsync(IGrainFactory grainFactory, string environment, string inputName)
    {
        try
        {
            var pipelines = await grainFactory.RegistryFor(environment).GetPipelinesAsync();
            return pipelines.FirstOrDefault(p => string.Equals(p.Name, inputName, StringComparison.Ordinal));
        }
        catch
        {
            return null;
        }
    }

    /// <summary>The stream id a pipeline publishes its results on — <c>OutputNamespace</c> keyed by the
    /// pipeline's ID (not its name: <c>PipelineGrain</c> self-publishes onto its own already-qualified
    /// primary key, which is the id). This is the whole reason a pipeline input cannot reuse the
    /// source-input subscribe path.</summary>
    public static string OutputStreamKey(string environment, PipelineDefinition pipeline) =>
        EnvKeys.Qualify(environment, pipeline.Id);

    /// <summary>One published result row as the engine's own event shape. <c>_ts</c> comes from the
    /// envelope and <c>_source</c> is the pipeline NAME (what the table's SQL wrote, and what the engine
    /// matches its relation on) — but only when the row does not already carry them: a projection that
    /// selected them explicitly has already said what they should be, and overwriting a value the query
    /// asked for would be a silent rewrite of the user's own output.</summary>
    public static EventRecord ToEventRecord(ResultEnvelope envelope, string pipelineName)
    {
        var record = new EventRecord(envelope.Row);
        if (!record.ContainsKey(EventRecord.TimestampField))
        {
            record[EventRecord.TimestampField] = envelope.TimestampMs;
        }
        if (!record.ContainsKey(EventRecord.SourceField))
        {
            record[EventRecord.SourceField] = pipelineName;
        }
        return record;
    }
}
