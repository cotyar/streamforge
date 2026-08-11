using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Planning;

public static class Planner
{
    public static CompileResult Compile(string sql, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        try
        {
            return CompileCore(sql, schemas);
        }
        catch (Exception ex)
        {
            return new CompileResult
            {
                Ok = false,
                Diagnostics = [new SqlDiagnostic($"Internal compiler error: {ex.Message}", 1, 1)],
            };
        }
    }

    private static CompileResult CompileCore(string sql, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        var diagnostics = new List<SqlDiagnostic>();

        var (tokens, tokenDiags) = new Tokenizer(sql).Tokenize();
        diagnostics.AddRange(tokenDiags);

        var (query, setOp, parseDiags) = Parser.ParseStatement(tokens);
        diagnostics.AddRange(parseDiags);

        if (query is null && setOp is null)
        {
            return new CompileResult { Ok = false, Diagnostics = diagnostics };
        }

        // Plan 008 W3: a top-level (or WITH-wrapped) set operation takes its own compile path — see
        // CompileSetOperation. Everything below is the pre-008 single-query path, unchanged.
        if (setOp is not null)
        {
            return CompileSetOperation(setOp, schemas, diagnostics);
        }

        var validation = Validator.Validate(query!, schemas);
        diagnostics.AddRange(validation.Diagnostics);

        if (diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error))
        {
            return new CompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var compiled = BuildCompiledPlan(query!, validation);
        var plan = new PipelinePlan(compiled);

        return new CompileResult
        {
            Ok = true,
            Diagnostics = diagnostics,
            PlanSummary = compiled.PlanSummary,
            SourceNames = compiled.SourceNames,
            OutputSchema = compiled.OutputSchema,
            Plan = plan,
        };
    }

    /// <summary>Plan 008 W3: compiles a top-level (or WITH-wrapped) `SELECT ... UNION [ALL] SELECT ...`
    /// chain — validates every branch (independently, uncorrelated) plus branch compatibility via
    /// Validator.ValidateSetOperation, then builds each branch's own CompiledPlan (the SAME
    /// BuildCompiledPlan every ordinary query goes through) and wraps them in a union-root CompiledPlan —
    /// see BuildCompiledUnionPlan and CompiledPlan.UnionBranches's doc comment.</summary>
    private static CompileResult CompileSetOperation(SetOperationQuery setOp, IReadOnlyDictionary<string, SourceSchema> schemas, List<SqlDiagnostic> diagnostics)
    {
        var v = Validator.ValidateSetOperation(setOp, schemas);
        diagnostics.AddRange(v.Diagnostics);

        if (diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error))
        {
            return new CompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var compiled = BuildCompiledUnionPlan(setOp, v.BranchValidations, v.OutputSchema!);
        var plan = new PipelinePlan(compiled);

        return new CompileResult
        {
            Ok = true,
            Diagnostics = diagnostics,
            PlanSummary = compiled.PlanSummary,
            SourceNames = compiled.SourceNames,
            OutputSchema = compiled.OutputSchema,
            Plan = plan,
        };
    }

    /// <summary>Plan 008 W3: builds a union-root CompiledPlan — every branch compiled via the ordinary,
    /// recursive BuildCompiledPlan, then EVERY branch (0 included) normalized to the SAME output column
    /// names (branch 0's own — a no-op for branch 0 itself) AND the SAME `_source` stamp (the union's own
    /// joined label, not each branch's own alias-based one) — see NormalizeOutputColumns's doc comment for
    /// why `_source` matters here too: table mode's consolidation/dedup depend on it (this pipeline-mode
    /// copy has no consolidation of its own, but keeps the SAME normalization for a consistent, non-
    /// surprising `_source` value regardless of which branch a row came from).</summary>
    private static CompiledPlan BuildCompiledUnionPlan(SetOperationQuery setOp, List<ValidationResult> branchValidations, SourceSchema unifiedSchema)
    {
        var branchNames = unifiedSchema.Fields.Keys.ToList();
        var rawBranchPlans = setOp.Branches.Select((b, i) => BuildCompiledPlan(b, branchValidations[i])).ToList();

        var sourceNames = rawBranchPlans.SelectMany(b => b.SourceNames).Distinct().ToList();
        var sourceLabel = string.Join(",", rawBranchPlans.Select(b => b.SourceLabel));
        string opText = setOp.All ? "UNION ALL" : "UNION";
        var summary = string.Join($" {opText} ", rawBranchPlans.Select(b => b.SourceLabel)) + $" → SELECT {unifiedSchema.Fields.Count} cols";

        var branchPlans = rawBranchPlans.Select(bp => NormalizeOutputColumns(bp, branchNames, sourceLabel)).ToList();

        return new CompiledPlan
        {
            Sources = [],
            Joins = [],
            Where = null,
            GroupBy = null,
            Window = null,
            Emit = EmitMode.Final,
            Output = [],
            AggregateNodes = [],
            AggregateIndex = new Dictionary<AggregateCallExpr, int>(ReferenceEqualityComparer.Instance),
            Bindings = new Dictionary<Expr, (string Alias, string Field)>(ReferenceEqualityComparer.Instance),
            HasAggregates = false,
            PlanSummary = summary,
            OutputSchema = unifiedSchema,
            SourceNames = sourceNames,
            SourceLabel = sourceLabel,
            UnionBranches = branchPlans,
        };
    }

    /// <summary>Plan 008 W3: returns a copy of <paramref name="plan"/> with its Output items' Names replaced
    /// positionally by <paramref name="names"/> (same Expression/GroupByIndex — only the column NAME
    /// changes), its OutputSchema rebuilt to match (same kinds, new names, same order), and its SourceLabel
    /// replaced by <paramref name="sourceLabel"/> — see BuildCompiledUnionPlan's doc comment on why every
    /// branch (0 included) needs this.</summary>
    private static CompiledPlan NormalizeOutputColumns(CompiledPlan plan, List<string> names, string sourceLabel)
    {
        var newOutput = new List<OutputItem>();
        for (int i = 0; i < plan.Output.Count; i++)
        {
            var item = plan.Output[i];
            newOutput.Add(new OutputItem { Name = i < names.Count ? names[i] : item.Name, Expression = item.Expression, GroupByIndex = item.GroupByIndex });
        }

        var oldKinds = plan.OutputSchema.Fields.Values.ToList();
        var newFields = new Dictionary<string, FieldKind>();
        for (int i = 0; i < newOutput.Count; i++)
        {
            newFields[newOutput[i].Name] = i < oldKinds.Count ? oldKinds[i] : FieldKind.String;
        }

        return new CompiledPlan
        {
            Sources = plan.Sources,
            Joins = plan.Joins,
            Where = plan.Where,
            GroupBy = plan.GroupBy,
            Window = plan.Window,
            Emit = plan.Emit,
            Output = newOutput,
            AggregateNodes = plan.AggregateNodes,
            AggregateIndex = plan.AggregateIndex,
            Bindings = plan.Bindings,
            HasAggregates = plan.HasAggregates,
            PlanSummary = plan.PlanSummary,
            OutputSchema = new SourceSchema(plan.OutputSchema.Name, newFields),
            SourceNames = plan.SourceNames,
            SourceLabel = sourceLabel,
            UnionBranches = plan.UnionBranches,
        };
    }

    /// <summary>Builds the executable plan from a validated query — recursive: plan 004 N1's derived
    /// tables/CTEs each carry their own (already-validated, see Validator.ResolveFromItem) inner
    /// SelectQuery + ValidationResult, so a derived source's nested CompiledPlan is built by calling right
    /// back into this same method — no re-tokenizing/re-parsing/re-validating, and inner diagnostics were
    /// already folded into the outer diagnostics list during validation (Compile() bails out before ever
    /// reaching here if any of them are errors).</summary>
    private static CompiledPlan BuildCompiledPlan(SelectQuery q, ValidationResult v)
    {
        var sources = v.Sources.Select(s => new CompiledSource
        {
            Alias = s.Alias,
            SourceName = s.SourceName,
            Schema = s.Schema,
            // Plan 008 W3: a derived-table-position set operation (`FROM ( ... UNION ... ) alias`) reuses
            // this EXACT nesting seam — its compiled form IS a union-root CompiledPlan (see
            // BuildCompiledUnionPlan), which slots into DerivedPlan just like a plain derived table's.
            DerivedPlan = s.Derived is not null
                ? BuildCompiledPlan(s.Derived.Query, s.Derived.Validation)
                : s.UnionDerived is not null
                    ? BuildCompiledUnionPlan(s.UnionDerived.SetOp, s.UnionDerived.BranchValidations, s.Schema)
                    : null,
        }).ToList();
        var joins = v.Joins.Select(j =>
        {
            var srcEntry = v.Sources.First(s => s.Alias == j.Alias);
            // Plan 008: pipeline mode has no composite-key-aware op yet (PipelineJoinOp handles every
            // kind, including Left/Right/Full, via its existing single-key + WITHIN eviction path) — so,
            // unlike table mode's TablePlanner, the fold is UNCONDITIONAL here: every equi-key component
            // past the first always becomes an extra Residual conjunct, regardless of JoinKind. See
            // JoinKeyFolding's doc comment; a no-op for every single-key join (the common case, and every
            // join this wave's regression tests — ExecutorJoinTests, PipelineOpsUnitTests — cover).
            var residual = JoinKeyFolding.FoldExtraKeysIntoResidual(j.LeftKeys, j.RightKeys, j.Residual);
            return new CompiledJoin
            {
                Kind = j.Kind,
                Alias = j.Alias,
                SourceName = j.SourceName,
                Schema = srcEntry.Schema,
                Within = j.Within ?? TimeSpan.Zero,
                LeftKey = j.LeftKeys?[0],
                RightKey = j.RightKeys?[0],
                Residual = residual,
                LeftKeys = j.LeftKeys,
                RightKeys = j.RightKeys,
                DerivedPlan = sources.First(s => s.Alias == j.Alias).DerivedPlan,
                UnnestExpr = j.UnnestExpr,
            };
        }).ToList();

        var bindings = v.Bindings;

        // Plan 004 N2/N3/N4: rewrite WHERE (extracting IN/EXISTS predicates into semi/anti join stages,
        // substituting scalar subquery occurrences) and the projection (substituting scalar subquery
        // occurrences) — each substitution appends one Semi/Anti/Scalar CompiledJoin to `joins`, AFTER the
        // real joins, so a synthesized join's key expressions (which may reference already-joined outer
        // columns — N4 correlation, or an IN's left operand from a joined alias) resolve against the fully
        // accumulated row. See SubqueryRewriter.RewriteWhere/RewriteSelectItems below.
        var where = RewriteWhereForSubqueryPredicates(q.Where, v, bindings, joins);
        var selectItems = q.Select.Items.Select(item => new SelectItem(RewriteScalarSubqueries(item.Expression, v, bindings, joins), item.Alias)).ToList();
        var qForOutput = new SelectQuery(new SelectClause(q.Select.IsStar, selectItems), q.From, q.Where, q.GroupBy, q.Window, q.Emit,
            q.EmitLine, q.EmitColumn, q.GroupByLine, q.GroupByColumn, q.WindowLine, q.WindowColumn);

        var output = BuildOutput(qForOutput, sources, bindings);

        if (q.Window is not null)
        {
            AssignGroupByIndexes(output, q.GroupBy, bindings);
        }

        var aggregateNodes = CollectAggregateNodes(output.Select(o => o.Expression));
        var aggregateIndex = new Dictionary<AggregateCallExpr, int>(ReferenceEqualityComparer.Instance);
        for (int i = 0; i < aggregateNodes.Count; i++) aggregateIndex[aggregateNodes[i]] = i;

        // Real, externally-feedable leaf source names: a derived source contributes its own (already
        // transitively flattened) child SourceNames instead of its synthetic "(derived)" marker, so
        // CompileResult.SourceNames always names actual streams a caller can OnEvent() — see
        // ExecutorImpl's role table, which dispatches derived nodes by these same leaf names. Plan 004
        // N2/N3/N4's synthesized Semi/Anti/Scalar joins are ALSO derived-plan-backed (never a plain named
        // source — see SubqueryRewriter) and must contribute their own leaf names the exact same way, or a
        // subquery's real stream inputs would silently never receive events. Plan 002 L2: an UNNEST alias's
        // synthetic "(unnest)" SourceName is EXCLUDED here — it has no external driving source at all (it's
        // derived purely from an expression over the already-accumulated row; see PipelineUnnestOp/
        // ExecutorImpl's role-registration skip) and would otherwise pollute CompileResult.SourceNames with
        // a name no caller can ever meaningfully OnEvent().
        var unnestAliases = joins.Where(j => j.Kind == JoinKind.Unnest).Select(j => j.Alias).ToHashSet();
        var sourceNames = sources.Where(s => !unnestAliases.Contains(s.Alias))
            .SelectMany(s => (IEnumerable<string>)(s.DerivedPlan?.SourceNames ?? [s.SourceName]))
            .Concat(joins.Where(j => j.Kind is JoinKind.Semi or JoinKind.Anti or JoinKind.Scalar).SelectMany(j => j.DerivedPlan!.SourceNames))
            .Distinct().ToList();
        var sourceLabel = string.Join(",", sources.Select(s => s.Alias));

        return new CompiledPlan
        {
            Sources = sources,
            Joins = joins,
            Where = where,
            GroupBy = q.GroupBy,
            Window = q.Window,
            Emit = q.Emit ?? EmitMode.Final,
            Output = output,
            AggregateNodes = aggregateNodes,
            AggregateIndex = aggregateIndex,
            Bindings = bindings,
            HasAggregates = aggregateNodes.Count > 0,
            PlanSummary = BuildPlanSummary(q, sources, joins, output, bindings),
            OutputSchema = BuildOutputSchema(output, sources, v.ExprKinds),
            SourceNames = sourceNames,
            SourceLabel = sourceLabel,
        };
    }

    /// <summary>Derives the pipeline's output row schema (column name → kind) from the projection —
    /// mirrors TablePlanner.BuildOutputSchema so pipelines and tables expose the same shape.</summary>
    private static SourceSchema BuildOutputSchema(List<OutputItem> output, List<CompiledSource> sources, Dictionary<Expr, FieldKind> exprKinds)
    {
        var fields = new Dictionary<string, FieldKind>();
        foreach (var item in output)
        {
            FieldKind kind;
            if (item.Expression is QualifiedIdentifier qid && sources.Any(s => s.Alias == qid.Qualifier))
            {
                // Star-expansion synthetic node: not part of the validated AST, so exprKinds won't have it.
                var src = sources.First(s => s.Alias == qid.Qualifier);
                kind = src.Schema.Fields.TryGetValue(qid.Name, out var k) ? k : FieldKind.String;
            }
            else if (exprKinds.TryGetValue(item.Expression, out var k))
            {
                kind = k;
            }
            else
            {
                kind = FieldKind.String;
            }
            fields[item.Name] = kind;
        }
        return new SourceSchema("(pipeline)", fields);
    }

    private static List<OutputItem> BuildOutput(SelectQuery q, List<CompiledSource> sources, Dictionary<Expr, (string Alias, string Field)> bindings)
    {
        var output = new List<OutputItem>();

        if (q.Select.IsStar)
        {
            bool prefixed = sources.Count > 1;
            foreach (var src in sources)
            {
                foreach (var field in src.Schema.Fields.Keys)
                {
                    var node = new QualifiedIdentifier(src.Alias, field, 0, 0);
                    bindings[node] = (src.Alias, field);
                    var name = prefixed ? $"{src.Alias}_{field}" : field;
                    output.Add(new OutputItem { Name = name, Expression = node });
                }
            }
            return output;
        }

        for (int i = 0; i < q.Select.Items.Count; i++)
        {
            var item = q.Select.Items[i];
            if (item.Expression is QualifiedStarExpr qs)
            {
                ExpandQualifiedStar(qs, sources, bindings, output);
                continue;
            }
            var name = item.Alias ?? DefaultName(item.Expression, i);
            output.Add(new OutputItem { Name = name, Expression = item.Expression });
        }
        return output;
    }

    /// <summary>Expands `alias.*` into one OutputItem per field of that alias's source schema — mirrors
    /// bare `SELECT *`'s expansion (same alias-prefixing rule: prefixed whenever the query has more than
    /// one FROM/JOIN source, regardless of how many other select items surround the star).</summary>
    private static void ExpandQualifiedStar(QualifiedStarExpr qs, List<CompiledSource> sources, Dictionary<Expr, (string Alias, string Field)> bindings, List<OutputItem> output)
    {
        var src = sources.First(s => s.Alias == qs.Alias);
        bool prefixed = sources.Count > 1;
        foreach (var field in src.Schema.Fields.Keys)
        {
            var node = new QualifiedIdentifier(src.Alias, field, qs.Line, qs.Column);
            bindings[node] = (src.Alias, field);
            var name = prefixed ? $"{src.Alias}_{field}" : field;
            output.Add(new OutputItem { Name = name, Expression = node });
        }
    }

    private static string DefaultName(Expr e, int index) => e switch
    {
        Identifier id => id.Name,
        QualifiedIdentifier q => q.Name,
        AggregateCallExpr { IsStar: true } => "count_star",
        AggregateCallExpr agg => agg.Name.ToLowerInvariant(),
        FunctionCallExpr f => f.Name.ToLowerInvariant(),
        JsonAccessExpr j => JsonKeyText(j.Key),
        _ => $"col{index + 1}",
    };

    private static string JsonKeyText(Expr key) => key switch
    {
        StringLiteral s => s.Value,
        NumberLiteral { LongValue: { } n } => n.ToString(System.Globalization.CultureInfo.InvariantCulture),
        _ => "json",
    };

    private static void AssignGroupByIndexes(List<OutputItem> output, List<Expr>? groupBy, Dictionary<Expr, (string Alias, string Field)> bindings)
    {
        if (groupBy is null) return;
        foreach (var item in output)
        {
            if (Validator.ContainsAggregate(item.Expression)) continue;
            int idx = groupBy.FindIndex(g => Validator.StructurallyEqual(item.Expression, g, bindings));
            if (idx >= 0) item.GroupByIndex = idx;
        }
    }

    private static List<AggregateCallExpr> CollectAggregateNodes(IEnumerable<Expr> roots)
    {
        var list = new List<AggregateCallExpr>();
        void Walk(Expr e)
        {
            switch (e)
            {
                case AggregateCallExpr agg:
                    list.Add(agg);
                    break;
                case UnaryExpr u:
                    Walk(u.Operand);
                    break;
                case BinaryExpr b:
                    Walk(b.Left);
                    Walk(b.Right);
                    break;
                case FunctionCallExpr f:
                    foreach (var a in f.Args) Walk(a);
                    break;
                case JsonAccessExpr j:
                    Walk(j.Left);
                    break;
            }
        }
        foreach (var root in roots) Walk(root);
        return list;
    }

    private static string FormatDuration(TimeSpan d)
    {
        if (d.TotalMilliseconds % 1000 == 0) return $"{(long)d.TotalSeconds}s";
        return $"{(long)d.TotalMilliseconds}ms";
    }

    private static string ColumnText(Expr e, Dictionary<Expr, (string Alias, string Field)> bindings) => e switch
    {
        Identifier or QualifiedIdentifier when bindings.TryGetValue(e, out var b) => $"{b.Alias}_{b.Field}",
        Identifier id => id.Name,
        QualifiedIdentifier q => $"{q.Qualifier}_{q.Name}",
        JsonAccessExpr j => $"{ColumnText(j.Left, bindings)}{(j.ReturnText ? "->>" : "->")}{JsonKeyText(j.Key)}",
        _ => "expr",
    };

    private static string BuildPlanSummary(SelectQuery q, List<CompiledSource> sources, List<CompiledJoin> joins, List<OutputItem> output, Dictionary<Expr, (string Alias, string Field)> bindings)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append(sources[0].SourceName).Append(" AS ").Append(sources[0].Alias);

        foreach (var j in joins)
        {
            if (j.Kind == JoinKind.Unnest)
            {
                // Plan 002 L2: UNNEST has no WITHIN/duration and no separate SourceName — print the
                // unnested expression's column text instead of the ⋈[label,duration] shape every other
                // join kind uses.
                sb.Append($" ⇶[UNNEST] {ColumnText(j.UnnestExpr!, bindings)} AS {j.Alias}");
                continue;
            }
            string symbol = j.Kind == JoinKind.Cross ? "×" : "⋈";
            string label = j.Kind switch
            {
                JoinKind.Inner => "INNER",
                JoinKind.Left => "LEFT",
                JoinKind.Right => "RIGHT",
                JoinKind.Full => "FULL",
                JoinKind.Cross => "CROSS",
                _ => j.Kind.ToString(),
            };
            sb.Append($" {symbol}[{label},{FormatDuration(j.Within)}] {j.SourceName} AS {j.Alias}");
        }

        var parts = new List<string> { sb.ToString() };

        if (q.Where is not null) parts.Add("WHERE");

        if (q.Window is not null)
        {
            string windowText = q.Window switch
            {
                TumblingWindowSpec t => $"TUMBLING({FormatDuration(t.Size)})",
                HoppingWindowSpec h => $"HOPPING({FormatDuration(h.Size)},{FormatDuration(h.Advance)})",
                SessionWindowSpec s => $"SESSION({FormatDuration(s.Gap)})",
                _ => "WINDOW",
            };
            if (q.GroupBy is not null)
            {
                windowText += " GROUP BY " + string.Join(", ", q.GroupBy.Select(g => ColumnText(g, bindings)));
            }
            parts.Add(windowText);
        }

        // output.Count already equals q.Select.Items.Count for the non-star, no-qualified-star case (one
        // OutputItem per select item) and equals the fully expanded column count for bare `*`/`alias.*` —
        // using it unconditionally keeps the summary accurate for a mix like `t.*, q.bid`.
        parts.Add($"SELECT {output.Count}");

        return string.Join(" → ", parts);
    }

    // ------------------------------------------------------------------
    // Plan 004 N2/N3/N4 — subquery predicate/expression rewriting. See BuildCompiledPlan's call site doc
    // comment for the overall shape; CompiledTablePlan's TablePlanner.cs carries a near-identical copy
    // (different Compiled*/Join DTOs — same "necessarily duplicated" reasoning as
    // Validator.BuildDerivedOutputSchema's doc comment).
    // ------------------------------------------------------------------

    private static IEnumerable<Expr> FlattenAndForRewrite(Expr e)
    {
        if (e is BinaryExpr { Op: "AND" } b)
        {
            foreach (var x in FlattenAndForRewrite(b.Left)) yield return x;
            foreach (var x in FlattenAndForRewrite(b.Right)) yield return x;
        }
        else
        {
            yield return e;
        }
    }

    /// <summary>Plan 004 N2: extracts every top-level (AND-connected) IN/EXISTS predicate out of
    /// <paramref name="where"/> into its own synthesized Semi (IN/EXISTS) or Anti (NOT IN/NOT EXISTS) join
    /// appended to <paramref name="joins"/>, leaving the remaining conjuncts — each also scanned for a
    /// nested scalar subquery (plan 004 N3/N4) — as the residual WHERE. Validator already confirmed every
    /// InSubqueryExpr/ExistsExpr reaching here is a top-level conjunct (see its `_whereTopLevelConjuncts`
    /// gate) and has a corresponding entry in v.SubqueryPredicates (compilation would have already bailed
    /// out on a diagnostic otherwise).</summary>
    private static Expr? RewriteWhereForSubqueryPredicates(Expr? where, ValidationResult v, Dictionary<Expr, (string Alias, string Field)> bindings, List<CompiledJoin> joins)
    {
        if (where is null) return null;

        var residual = new List<Expr>();
        foreach (var conjunct in FlattenAndForRewrite(where))
        {
            if (conjunct is InSubqueryExpr ins && v.SubqueryPredicates.TryGetValue(ins, out var inInfo))
            {
                joins.Add(BuildSemiAntiJoin(inInfo, ins.Left, bindings, joins.Count));
                continue;
            }
            if (conjunct is ExistsExpr existsExpr && v.SubqueryPredicates.TryGetValue(existsExpr, out var existsInfo))
            {
                joins.Add(BuildSemiAntiJoin(existsInfo, insLeft: null, bindings, joins.Count));
                continue;
            }
            residual.Add(RewriteScalarSubqueries(conjunct, v, bindings, joins));
        }

        if (residual.Count == 0) return null;
        return residual.Aggregate((a, b) => new BinaryExpr("AND", a, b, a.Line, a.Column));
    }

    private static CompiledJoin BuildSemiAntiJoin(SubqueryPredicateInfo info, Expr? insLeft, Dictionary<Expr, (string Alias, string Field)> bindings, int index)
    {
        string alias = $"__sq{index}";
        var derivedPlan = BuildCompiledPlan(info.Query, info.Validation);

        Expr leftKey, rightKey;
        if (insLeft is not null)
        {
            // IN: leftKey is the subquery's own left operand — already resolved/bound by the Validator
            // against the OUTER scope (see Validator.ResolveInSubquery), so it's already present in
            // `bindings` (== v.Bindings, the SAME dictionary CompiledPlan.Bindings ends up as).
            leftKey = insLeft;
            var rightNode = new QualifiedIdentifier(alias, info.KeyColumnName!, 0, 0);
            bindings[rightNode] = (alias, info.KeyColumnName!);
            rightKey = rightNode;
        }
        else
        {
            // EXISTS: existence of ANY row, not a specific key match — both sides use the SAME constant so
            // presence never depends on a column value (see Runtime/Ops/TableSemiAntiOp.cs's class doc).
            leftKey = new NumberLiteral(null, 0L, 0, 0);
            rightKey = new NumberLiteral(null, 0L, 0, 0);
        }

        return new CompiledJoin
        {
            Kind = info.Negated ? JoinKind.Anti : JoinKind.Semi,
            Alias = alias,
            SourceName = "(derived)",
            Schema = derivedPlan.OutputSchema,
            Within = TimeSpan.Zero,
            LeftKey = leftKey,
            RightKey = rightKey,
            Residual = null,
            LeftKeys = [leftKey],
            RightKeys = [rightKey],
            DerivedPlan = derivedPlan,
        };
    }

    /// <summary>Plan 004 N3 (info.Correlations empty)/N4 (non-empty): builds the Scalar-kind join stage a
    /// resolved scalar subquery rewrites into. N3 joins on a constant key both sides (singleton cross-join
    /// — "reuse TableJoinOp/an equivalent pipeline op with a constant key"). N4 joins on the FIRST
    /// correlation component's key; any ADDITIONAL correlation components become residual equality
    /// conjuncts against their own `__key{i}` projection — the same "one hash key + residual" shape this
    /// engine already uses for any other multi-condition equi-join (see Validator.ExtractEquiKey).</summary>
    private static CompiledJoin BuildScalarJoin(ScalarSubqueryInfo info, Dictionary<Expr, (string Alias, string Field)> bindings, int index)
    {
        string alias = $"__sq{index}";
        var derivedPlan = BuildCompiledPlan(info.ResidualQuery, info.Validation);

        Expr leftKey, rightKey;
        Expr? residual = null;

        if (info.Correlations.Count == 0)
        {
            leftKey = new NumberLiteral(null, 0L, 0, 0);
            rightKey = new NumberLiteral(null, 0L, 0, 0);
        }
        else
        {
            var first = info.Correlations[0];
            var outerNode = new QualifiedIdentifier(first.OuterAlias, first.OuterField, 0, 0);
            bindings[outerNode] = (first.OuterAlias, first.OuterField);
            leftKey = outerNode;

            var innerNode = new QualifiedIdentifier(alias, info.KeyColumnNames[0], 0, 0);
            bindings[innerNode] = (alias, info.KeyColumnNames[0]);
            rightKey = innerNode;

            for (int i = 1; i < info.Correlations.Count; i++)
            {
                var c = info.Correlations[i];
                var outerN = new QualifiedIdentifier(c.OuterAlias, c.OuterField, 0, 0);
                bindings[outerN] = (c.OuterAlias, c.OuterField);
                var innerN = new QualifiedIdentifier(alias, info.KeyColumnNames[i], 0, 0);
                bindings[innerN] = (alias, info.KeyColumnNames[i]);
                var eq = new BinaryExpr("=", outerN, innerN, 0, 0);
                residual = residual is null ? eq : new BinaryExpr("AND", residual, eq, 0, 0);
            }
        }

        return new CompiledJoin
        {
            Kind = JoinKind.Scalar,
            Alias = alias,
            SourceName = "(derived)",
            Schema = derivedPlan.OutputSchema,
            Within = TimeSpan.Zero,
            LeftKey = leftKey,
            RightKey = rightKey,
            Residual = residual,
            LeftKeys = [leftKey],
            RightKeys = [rightKey],
            DerivedPlan = derivedPlan,
        };
    }

    /// <summary>Plan 004 N3/N4: replaces every ScalarSubqueryExpr in <paramref name="e"/> with a bound
    /// reference to its synthesized Scalar join stage's output column, appending one CompiledJoin per
    /// occurrence to <paramref name="joins"/>. Reference-preserving: a subtree with no scalar subquery
    /// inside it is returned UNCHANGED (same object) rather than rebuilt, so v.Bindings/v.ExprKinds lookups
    /// keyed by the original node identity keep working for everything this rewrite doesn't touch; where a
    /// node genuinely IS rebuilt (something inside it changed), its own recorded ExprKind (if any) is
    /// copied forward onto the new node for the same reason.</summary>
    private static Expr RewriteScalarSubqueries(Expr e, ValidationResult v, Dictionary<Expr, (string Alias, string Field)> bindings, List<CompiledJoin> joins)
    {
        switch (e)
        {
            case ScalarSubqueryExpr sse when v.ScalarSubqueries.TryGetValue(sse, out var info):
            {
                var join = BuildScalarJoin(info, bindings, joins.Count);
                joins.Add(join);
                var node = new QualifiedIdentifier(join.Alias, info.ValueColumnName, sse.Line, sse.Column);
                bindings[node] = (join.Alias, info.ValueColumnName);
                if (v.ExprKinds.TryGetValue(sse, out var k)) v.ExprKinds[node] = k;
                return node;
            }
            case UnaryExpr u:
            {
                var operand = RewriteScalarSubqueries(u.Operand, v, bindings, joins);
                if (ReferenceEquals(operand, u.Operand)) return e;
                var rebuilt = new UnaryExpr(u.Op, operand, u.Line, u.Column);
                CopyExprKind(v, e, rebuilt);
                return rebuilt;
            }
            case BinaryExpr b:
            {
                var left = RewriteScalarSubqueries(b.Left, v, bindings, joins);
                var right = RewriteScalarSubqueries(b.Right, v, bindings, joins);
                if (ReferenceEquals(left, b.Left) && ReferenceEquals(right, b.Right)) return e;
                var rebuilt = new BinaryExpr(b.Op, left, right, b.Line, b.Column);
                CopyExprKind(v, e, rebuilt);
                return rebuilt;
            }
            case FunctionCallExpr f:
            {
                var args = f.Args.Select(a => RewriteScalarSubqueries(a, v, bindings, joins)).ToList();
                if (args.Zip(f.Args).All(p => ReferenceEquals(p.First, p.Second))) return e;
                var rebuilt = new FunctionCallExpr(f.Name, args, f.Line, f.Column);
                CopyExprKind(v, e, rebuilt);
                return rebuilt;
            }
            case AggregateCallExpr agg:
            {
                if (agg.Arg is null) return e;
                var arg = RewriteScalarSubqueries(agg.Arg, v, bindings, joins);
                if (ReferenceEquals(arg, agg.Arg)) return e;
                var rebuilt = new AggregateCallExpr(agg.Name, arg, agg.IsStar, agg.Line, agg.Column);
                CopyExprKind(v, e, rebuilt);
                return rebuilt;
            }
            case JsonAccessExpr j:
            {
                var left = RewriteScalarSubqueries(j.Left, v, bindings, joins);
                if (ReferenceEquals(left, j.Left)) return e;
                var rebuilt = new JsonAccessExpr(left, j.ReturnText, j.Key, j.Line, j.Column);
                CopyExprKind(v, e, rebuilt);
                return rebuilt;
            }
            default:
                return e;
        }
    }

    private static void CopyExprKind(ValidationResult v, Expr from, Expr to)
    {
        if (v.ExprKinds.TryGetValue(from, out var k)) v.ExprKinds[to] = k;
    }
}
