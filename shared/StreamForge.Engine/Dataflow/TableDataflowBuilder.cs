using StreamForge.Engine.Planning;
using StreamForge.Engine.Sql;
using static StreamForge.Engine.Dataflow.TableDataflowPlan;

namespace StreamForge.Engine.Dataflow;

/// <summary>Builds a <see cref="TableDataflowPlan"/>'s stage/edge graph from a <see cref="CompiledTablePlan"/>,
/// and constructs the per-(stage,partition) <see cref="ITableStageExecutor"/> instances. See
/// TableDataflowPlan's class doc for the supported-shapes scope. This mirrors TableExecutorImpl.EnsureInit's
/// op-selection switch 1:1 (same JoinKind → op-type mapping) so the partitioned graph and the single-
/// partition façade are provably built from the same rules — the M2 equivalence oracle exercises exactly
/// that fact.</summary>
internal static class TableDataflowBuilder
{
    public static (List<TableStageDescriptor> Stages, List<TableEdgeDescriptor> Edges,
        Dictionary<int, StageBuild> Builds, Dictionary<int, RoutingKeySpec> RoutingSpecs)
        Build(CompiledTablePlan compiled, int partitionCount)
    {
        if (compiled.Sources[0].DerivedPlan is not null)
        {
            throw new NotSupportedException(
                "Partitioned execution (Parallelism > 1) does not support a derived table/CTE in FROM position " +
                $"(source '{compiled.Sources[0].SourceName}'); only scalar-subquery and semi/anti (IN/EXISTS) joins are " +
                "supported as broadcast edges in M2. Use Parallelism = 1 for this table.");
        }
        foreach (var j in compiled.Joins)
        {
            if (j.Kind is not (JoinKind.Scalar or JoinKind.Semi or JoinKind.Anti) && j.DerivedPlan is not null)
            {
                throw new NotSupportedException(
                    $"Partitioned execution (Parallelism > 1) does not support a derived table/CTE JOIN source " +
                    $"(alias '{j.Alias}'); only scalar-subquery and semi/anti (IN/EXISTS) joins are supported as " +
                    "broadcast edges in M2. Use Parallelism = 1 for this table.");
            }
        }

        var stages = new List<TableStageDescriptor>();
        var edges = new List<TableEdgeDescriptor>();
        var builds = new Dictionary<int, StageBuild>();
        var routingSpecs = new Dictionary<int, RoutingKeySpec>();
        int nextEdgeId = 0;
        int nextStageId = 0;
        EdgeId NewEdge() => new(nextEdgeId++);

        // --- Stage: FROM ingest ---
        int fromIngestStage = nextStageId++;
        var fromAlias = compiled.Sources[0].Alias;
        var fromIngestEdge = new TableEdgeDescriptor(NewEdge(), -1, fromIngestStage, "In", TableEdgeMode.Local, [compiled.Sources[0].SourceName]);
        edges.Add(fromIngestEdge);
        var fromStageDesc = new TableStageDescriptor(fromIngestStage, TableStageKind.Ingest, fromAlias, [fromIngestEdge]);
        stages.Add(fromStageDesc);
        builds[fromIngestStage] = new StageBuild
        {
            Kind = TableStageKind.Ingest,
            Stage = fromStageDesc,
            OutEdge = fromIngestEdge, // placeholder; overwritten below once the real forward edge is known
            Compiled = compiled,
        };

        int chainStage = fromIngestStage;
        bool chainIsWire = false; // FROM ingest's raw output is a single-alias EventRecord, not wire-encoded yet
        string? chainAlias = fromAlias;

        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            var j = compiled.Joins[i];
            int stageId = nextStageId++;

            // The very first hop out of the FROM-ingest stage (partition count 1) can never be "Local"
            // (identity partition p -> p) once it feeds a P-partition stage — there is no partition p>0 on
            // the producer side to preserve. Every later hop (i>0) IS producer-partition-preserving-safe
            // (the previous join/unnest/semi-anti stage already runs at the full P). See PartitionOf's
            // UseRowContentHash doc: when no join/group key applies at this hop, spread by row content.
            bool producerIsIngest = chainStage == fromIngestStage;

            if (j.Kind == JoinKind.Unnest)
            {
                var leftMode = producerIsIngest ? TableEdgeMode.HashPartition : TableEdgeMode.Local;
                var leftEdge = new TableEdgeDescriptor(NewEdge(), chainStage, stageId, "Left", leftMode, []);
                edges.Add(leftEdge);
                builds[chainStage].OutEdge = leftEdge;
                if (producerIsIngest)
                {
                    routingSpecs[leftEdge.EdgeId.Value] = new RoutingKeySpec { IsWireEncoded = false, UseRowContentHash = true, ToStageId = stageId };
                }

                var stage = new TableStageDescriptor(stageId, TableStageKind.Unnest, j.Alias, [leftEdge]);
                stages.Add(stage);
                builds[stageId] = new StageBuild
                {
                    Kind = TableStageKind.Unnest,
                    Stage = stage,
                    OutEdge = leftEdge, // placeholder; overwritten below
                    Join = j,
                    LeftEdge = leftEdge.EdgeId,
                    LeftIsWire = chainIsWire,
                    LeftAlias = chainAlias,
                    Compiled = compiled,
                };
            }
            else
            {
                bool isDerived = j.DerivedPlan is not null; // Scalar / Semi / Anti (validated above)
                var leftMode = isDerived
                    ? (producerIsIngest ? TableEdgeMode.HashPartition : TableEdgeMode.Local)
                    : TableEdgeMode.HashPartition;

                // Plan 003 M3 arrangeability check (SCOPE — see this file's class doc addendum below): the
                // Left side of THIS hop is a candidate for a shared arrangement only at i==0 (producerIsIngest
                // — any later hop's "Left" is an already-computed multi-alias WorkingRow, not a raw input),
                // and only when LeftKey is a BARE reference to the FROM alias's own field (no transform) —
                // checked via compiled.Bindings (reference-equality keyed; a non-leaf/derived Expr node is
                // never in Bindings at all, so this check doubles as "no pre-join transform").
                IReadOnlyList<string>? leftArrangeFields = producerIsIngest && leftMode == TableEdgeMode.HashPartition
                    && IsBareOwnFieldRef(j.LeftKey, compiled, fromAlias, out var leftField)
                    ? [leftField]
                    : null;
                var leftEdge = new TableEdgeDescriptor(NewEdge(), chainStage, stageId, "Left", leftMode, [], leftArrangeFields);
                edges.Add(leftEdge);
                builds[chainStage].OutEdge = leftEdge;
                if (isDerived && producerIsIngest)
                {
                    routingSpecs[leftEdge.EdgeId.Value] = new RoutingKeySpec { IsWireEncoded = false, UseRowContentHash = true, ToStageId = stageId };
                }

                TableEdgeDescriptor rightEdge;
                CompiledTablePlan? rightDerivedPlan = null;

                if (isDerived)
                {
                    var scalarInputs = j.DerivedPlan!.StreamInputs.Concat(j.DerivedPlan.TableInputs).Distinct().ToList();
                    rightEdge = new TableEdgeDescriptor(NewEdge(), -1, stageId, "Right", TableEdgeMode.Broadcast, scalarInputs);
                    rightDerivedPlan = j.DerivedPlan;
                }
                else
                {
                    int ingestStageId = nextStageId++;
                    var ingestInEdge = new TableEdgeDescriptor(NewEdge(), -1, ingestStageId, "In", TableEdgeMode.Local, [j.SourceName]);
                    edges.Add(ingestInEdge);
                    var ingestStage = new TableStageDescriptor(ingestStageId, TableStageKind.Ingest, j.Alias, [ingestInEdge]);
                    stages.Add(ingestStage);
                    builds[ingestStageId] = new StageBuild { Kind = TableStageKind.Ingest, Stage = ingestStage, OutEdge = ingestInEdge, Compiled = compiled };

                    // Plan 003 M3: a non-derived join's Right side is ALWAYS fed by its own dedicated Ingest
                    // stage (never a chained WorkingRow) — so its own outbound edge (this rightEdge) is
                    // arrangeable whenever RightKey is a bare reference to the join alias's own field. This is
                    // the common case the M3 task targets: two tables each joining the same raw input (e.g.
                    // "trades") on the same raw field ("symbol") share ONE arrangement here.
                    IReadOnlyList<string>? rightArrangeFields = IsBareOwnFieldRef(j.RightKey, compiled, j.Alias, out var rightField)
                        ? [rightField]
                        : null;
                    rightEdge = new TableEdgeDescriptor(NewEdge(), ingestStageId, stageId, "Right", TableEdgeMode.HashPartition, [], rightArrangeFields);
                    builds[ingestStageId].OutEdge = rightEdge;
                }
                edges.Add(rightEdge);

                var kind = j.Kind is JoinKind.Semi or JoinKind.Anti ? TableStageKind.SemiAnti : TableStageKind.Join;
                var stage = new TableStageDescriptor(stageId, kind, j.Alias, [leftEdge, rightEdge]);
                stages.Add(stage);
                builds[stageId] = new StageBuild
                {
                    Kind = kind,
                    Stage = stage,
                    OutEdge = leftEdge, // placeholder; overwritten below
                    Join = j,
                    LeftEdge = leftEdge.EdgeId,
                    LeftIsWire = chainIsWire,
                    LeftAlias = chainAlias,
                    RightEdge = rightEdge.EdgeId,
                    RightAlias = j.Alias,
                    RightDerivedPlan = rightDerivedPlan,
                    Compiled = compiled,
                };

                if (leftMode == TableEdgeMode.HashPartition)
                {
                    // A non-derived join's Left edge is always a real key hash (never the content-hash
                    // fan-out — see producerIsIngest above: a non-derived join's leftMode is HashPartition
                    // unconditionally, i==0 included, since LeftKey IS the natural co-partitioning key).
                    routingSpecs[leftEdge.EdgeId.Value] = new RoutingKeySpec
                    {
                        IsWireEncoded = chainIsWire,
                        UseRowContentHash = false,
                        Alias = chainAlias,
                        KeyExprs = [j.LeftKey ?? throw new NotSupportedException($"Join alias '{j.Alias}' has no left key; only equi-joins are supported in table mode.")],
                        Bindings = compiled.Bindings,
                        ToStageId = stageId,
                    };
                }
                if (rightEdge.Mode == TableEdgeMode.HashPartition)
                {
                    routingSpecs[rightEdge.EdgeId.Value] = new RoutingKeySpec
                    {
                        IsWireEncoded = false,
                        UseRowContentHash = false,
                        Alias = j.Alias,
                        KeyExprs = [j.RightKey ?? throw new NotSupportedException($"Join alias '{j.Alias}' has no right key; only equi-joins are supported in table mode.")],
                        Bindings = compiled.Bindings,
                        ToStageId = stageId,
                    };
                }
            }

            chainStage = stageId;
            chainIsWire = true; // every join-chain stage's output is a (possibly multi-alias) WorkingRow, wire-encoded from here on
            chainAlias = null;
        }

        // --- Stage: FilterProject ---
        // Same producer-partition-count caveat as the join chain's first hop: if there were no JOINs at
        // all, FilterProject is fed directly by the 1-partition FROM-ingest stage and needs a real fan-out
        // (content hash — FilterProject is stateless/row-local, so which partition a row lands on here
        // doesn't affect correctness, only balance); otherwise it's fed by an already-P-partition join
        // stage and Local (p -> p) is correct.
        bool fpProducerIsIngest = chainStage == fromIngestStage;
        int fpStage = nextStageId++;
        var fpInMode = fpProducerIsIngest ? TableEdgeMode.HashPartition : TableEdgeMode.Local;
        var fpInEdge = new TableEdgeDescriptor(NewEdge(), chainStage, fpStage, "In", fpInMode, []);
        edges.Add(fpInEdge);
        builds[chainStage].OutEdge = fpInEdge;
        if (fpProducerIsIngest)
        {
            routingSpecs[fpInEdge.EdgeId.Value] = new RoutingKeySpec { IsWireEncoded = false, UseRowContentHash = true, ToStageId = fpStage };
        }

        bool grouped = compiled.GroupBy is not null || compiled.HasAggregates;
        bool latestBy = !grouped && compiled.LatestBy is not null;
        bool terminalHere = !grouped && !latestBy;

        var fpStageDesc = new TableStageDescriptor(fpStage, TableStageKind.FilterProject, "", [fpInEdge]);
        stages.Add(fpStageDesc);
        var fpBuild = new StageBuild
        {
            Kind = TableStageKind.FilterProject,
            Stage = fpStageDesc,
            OutEdge = fpInEdge, // placeholder; overwritten below
            InEdge = fpInEdge.EdgeId,
            InIsWire = chainIsWire,
            InAlias = chainAlias,
            Terminal = terminalHere,
            Compiled = compiled,
        };
        builds[fpStage] = fpBuild;

        if (terminalHere)
        {
            var termEdge = new TableEdgeDescriptor(NewEdge(), fpStage, -1, "Out", TableEdgeMode.Gather, []);
            edges.Add(termEdge);
            fpBuild.OutEdge = termEdge;
        }
        else
        {
            int aggStage = nextStageId++;
            var aggInEdge = new TableEdgeDescriptor(NewEdge(), fpStage, aggStage, "In", TableEdgeMode.HashPartition, []);
            edges.Add(aggInEdge);
            fpBuild.OutEdge = aggInEdge;

            var kind = grouped ? TableStageKind.Reduce : TableStageKind.LatestBy;
            var aggKeys = grouped ? compiled.GroupBy : compiled.LatestBy;
            var aggStageDesc = new TableStageDescriptor(aggStage, kind, "", [aggInEdge]);
            stages.Add(aggStageDesc);
            var aggBuild = new StageBuild
            {
                Kind = kind,
                Stage = aggStageDesc,
                OutEdge = aggInEdge, // placeholder; overwritten below
                InEdge = aggInEdge.EdgeId,
                InIsWire = true,
                Compiled = compiled,
                ReduceOrLatestKeys = aggKeys,
            };
            builds[aggStage] = aggBuild;

            routingSpecs[aggInEdge.EdgeId.Value] = new RoutingKeySpec
            {
                IsWireEncoded = true,
                UseRowContentHash = false,
                Alias = null,
                KeyExprs = aggKeys ?? [],
                Bindings = compiled.Bindings,
                ToStageId = aggStage,
            };

            var termEdge = new TableEdgeDescriptor(NewEdge(), aggStage, -1, "Out", TableEdgeMode.Gather, []);
            edges.Add(termEdge);
            aggBuild.OutEdge = termEdge;
        }

        return (stages, edges, builds, routingSpecs);
    }

    public static ITableStageExecutor CreateExecutor(StageBuild build, int partition)
    {
        return build.Kind switch
        {
            TableStageKind.Ingest => new IngestStageExecutor(build, partition),
            TableStageKind.Join or TableStageKind.SemiAnti or TableStageKind.Unnest => new JoinChainStageExecutor(build, partition),
            TableStageKind.FilterProject => new FilterProjectStageExecutor(build, partition),
            TableStageKind.Reduce => new ReduceStageExecutor(build, partition),
            TableStageKind.LatestBy => new LatestByStageExecutor(build, partition),
            _ => throw new NotSupportedException($"Unknown stage kind {build.Kind}."),
        };
    }

    /// <summary>
    /// Plan 003 M3 arrangeability rule: <paramref name="expr"/> qualifies as "a bare reference to
    /// <paramref name="alias"/>'s own raw field, with no pre-join transform" iff it is a leaf identifier node
    /// (<see cref="Identifier"/> or <see cref="QualifiedIdentifier"/> — every OTHER Expr subtype represents a
    /// computation over its children, e.g. BinaryExpr/FunctionCallExpr/JsonAccessExpr/UnaryExpr/a literal)
    /// AND it is present in <paramref name="compiled"/>'s Bindings (a reference-equality-keyed dictionary the
    /// validator populates ONLY for identifier leaves it resolves — see Sql/Validator.cs's
    /// ResolveBareIdentifier/ResolveQualifiedIdentifier) resolving to exactly <paramref name="alias"/>. This
    /// is the SCOPE boundary the M3 task calls for: "keyed directly by the join key (no pre-join transforms
    /// other than ingest normalization) — anything fancier keeps the private per-table path" — e.g.
    /// `t.symbol = q.symbol` qualifies on both sides, `UPPER(t.symbol) = q.symbol` does not (the Left key is
    /// a FunctionCallExpr, never added to Bindings as a whole node), and `t.symbol = q.symbol` joined at hop
    /// i&gt;1 (chained past the first join) does not qualify on the Left side either — not because of this
    /// check, but because the caller only invokes it for the Left edge when producerIsIngest (i==0).
    /// </summary>
    private static bool IsBareOwnFieldRef(Expr? expr, CompiledTablePlan compiled, string alias, out string field)
    {
        field = "";
        if (expr is not (Identifier or QualifiedIdentifier)) return false;
        if (!compiled.Bindings.TryGetValue(expr, out var binding)) return false;
        if (!string.Equals(binding.Alias, alias, StringComparison.Ordinal)) return false;
        field = binding.Field;
        return true;
    }
}
