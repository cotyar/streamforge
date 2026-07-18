using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Planning;

public static class TablePlanner
{
    public static TableCompileResult Compile(string sql, IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        try
        {
            return CompileCore(sql, streamSchemas, tableSchemas);
        }
        catch (Exception ex)
        {
            return new TableCompileResult
            {
                Ok = false,
                Diagnostics = [new SqlDiagnostic($"Internal compiler error: {ex.Message}", 1, 1)],
            };
        }
    }

    private static TableCompileResult CompileCore(string sql, IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        var diagnostics = new List<SqlDiagnostic>();

        var (tokens, tokenDiags) = new Tokenizer(sql).Tokenize();
        diagnostics.AddRange(tokenDiags);

        var (query, parseDiags) = Parser.Parse(tokens);
        diagnostics.AddRange(parseDiags);

        if (query is null)
        {
            return new TableCompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var validation = Validator.ValidateTable(query, streamSchemas, tableSchemas);
        diagnostics.AddRange(validation.Diagnostics);

        if (diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error))
        {
            return new TableCompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var compiled = BuildCompiledTablePlan(query, validation);
        var plan = new TablePlan(compiled);

        return new TableCompileResult
        {
            Ok = true,
            Diagnostics = diagnostics,
            PlanSummary = compiled.PlanSummary,
            StreamInputs = compiled.StreamInputs,
            TableInputs = compiled.TableInputs,
            OutputSchema = compiled.OutputSchema,
            Plan = plan,
        };
    }

    /// <summary>Recursive for the same reason Planner.BuildCompiledPlan is — see its doc comment. Plan 004
    /// N1's "table mode: an inline intermediate Z-set operator (same machinery as named table-over-table
    /// chaining)" is realized at the runtime layer (TableExecutorImpl nests a full child TableExecutor per
    /// derived source, wired exactly like table-over-table OnTableDelta chaining) — here, planning only
    /// needs to build that child's CompiledTablePlan.</summary>
    private static CompiledTablePlan BuildCompiledTablePlan(SelectQuery q, ValidationResult v)
    {
        var sources = v.Sources.Select(s => new CompiledTableSource
        {
            Alias = s.Alias,
            SourceName = s.SourceName,
            Schema = s.Schema,
            IsTable = s.IsTable,
            DerivedPlan = s.Derived is null ? null : BuildCompiledTablePlan(s.Derived.Query, s.Derived.Validation),
        }).ToList();
        var joins = v.Joins.Select(j =>
        {
            var srcEntry = v.Sources.First(s => s.Alias == j.Alias);
            return new CompiledTableJoin
            {
                Kind = j.Kind,
                Alias = j.Alias,
                SourceName = j.SourceName,
                Schema = srcEntry.Schema,
                IsTable = j.IsTable,
                LeftKey = j.LeftKey,
                RightKey = j.RightKey,
                Residual = j.Residual,
                DerivedPlan = sources.First(s => s.Alias == j.Alias).DerivedPlan,
            };
        }).ToList();

        var bindings = v.Bindings;
        var output = BuildOutput(q, sources, bindings);

        if (q.GroupBy is not null)
        {
            AssignGroupByIndexes(output, q.GroupBy, bindings);
        }

        var aggregateNodes = CollectAggregateNodes(output.Select(o => o.Expression));
        var aggregateIndex = new Dictionary<AggregateCallExpr, int>(ReferenceEqualityComparer.Instance);
        for (int i = 0; i < aggregateNodes.Count; i++) aggregateIndex[aggregateNodes[i]] = i;

        var sourceLabel = string.Join(",", sources.Select(s => s.Alias));
        var outputSchema = BuildOutputSchema(output, sources, v.ExprKinds);

        // Flattened, transitively-real leaf StreamInputs/TableInputs: Validator.ResolveFromItem already
        // folded a derived source's own inner StreamInputs/TableInputs into v.StreamInputs/v.TableInputs
        // (see its doc comment), so v.StreamInputs/v.TableInputs are already the right (leaf, feedable)
        // set here — no extra flattening needed at this layer, unlike pipeline mode's SourceNames (which
        // is computed from the Sources list directly, not from a Validator-tracked accumulator).
        return new CompiledTablePlan
        {
            Sources = sources,
            Joins = joins,
            Where = q.Where,
            GroupBy = q.GroupBy,
            Output = output,
            AggregateNodes = aggregateNodes,
            AggregateIndex = aggregateIndex,
            Bindings = bindings,
            HasAggregates = aggregateNodes.Count > 0,
            PlanSummary = BuildPlanSummary(q, sources, joins, output, bindings),
            StreamInputs = v.StreamInputs,
            TableInputs = v.TableInputs,
            SourceLabel = sourceLabel,
            OutputSchema = outputSchema,
        };
    }

    private static List<OutputItem> BuildOutput(SelectQuery q, List<CompiledTableSource> sources, Dictionary<Expr, (string Alias, string Field)> bindings)
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

    /// <summary>Expands `alias.*` into one OutputItem per field of that alias's source schema — table-mode
    /// mirror of Planner.ExpandQualifiedStar (same alias-prefixing rule as bare `SELECT *`).</summary>
    private static void ExpandQualifiedStar(QualifiedStarExpr qs, List<CompiledTableSource> sources, Dictionary<Expr, (string Alias, string Field)> bindings, List<OutputItem> output)
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

    private static void AssignGroupByIndexes(List<OutputItem> output, List<Expr> groupBy, Dictionary<Expr, (string Alias, string Field)> bindings)
    {
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

    /// <summary>Derives the table's output row schema (column name → kind) from the projection, using the
    /// validator's per-node kind tracking (falls back to String for anything unrecognized).</summary>
    private static SourceSchema BuildOutputSchema(List<OutputItem> output, List<CompiledTableSource> sources, Dictionary<Expr, FieldKind> exprKinds)
    {
        var fields = new Dictionary<string, FieldKind>();
        foreach (var item in output)
        {
            FieldKind kind;
            if (item.Expression is QualifiedIdentifier qid && sources.Any(s => s.Alias == qid.Qualifier))
            {
                // Star-expansion synthetic node: look the kind up directly from the source schema (these
                // nodes are freshly created here, not part of the validated AST, so exprKinds won't have them).
                var src = sources.First(s => s.Alias == qid.Qualifier);
                kind = src.Schema.Fields.TryGetValue(qid.Name, out var k) ? k : FieldKind.String;
            }
            else if (exprKinds.TryGetValue(item.Expression, out var k))
            {
                kind = k == FieldKind.Json ? FieldKind.Json : k;
            }
            else
            {
                kind = FieldKind.String;
            }
            fields[item.Name] = kind;
        }
        return new SourceSchema("(table)", fields);
    }

    private static string FormatColumnName(Expr e, Dictionary<Expr, (string Alias, string Field)> bindings) => e switch
    {
        Identifier or QualifiedIdentifier when bindings.TryGetValue(e, out var b) => $"{b.Alias}_{b.Field}",
        Identifier id => id.Name,
        QualifiedIdentifier q => $"{q.Qualifier}_{q.Name}",
        JsonAccessExpr j => $"{FormatColumnName(j.Left, bindings)}{(j.ReturnText ? "->>" : "->")}{JsonKeyText(j.Key)}",
        _ => "expr",
    };

    private static string BuildPlanSummary(SelectQuery q, List<CompiledTableSource> sources, List<CompiledTableJoin> joins, List<OutputItem> output, Dictionary<Expr, (string Alias, string Field)> bindings)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append(sources[0].SourceName).Append(" AS ").Append(sources[0].Alias);

        foreach (var j in joins)
        {
            sb.Append($" ⋈[INNER] {j.SourceName} AS {j.Alias}");
        }

        var parts = new List<string> { sb.ToString() };

        if (q.Where is not null) parts.Add("WHERE");

        if (q.GroupBy is not null)
        {
            parts.Add("GROUP BY " + string.Join(", ", q.GroupBy.Select(g => FormatColumnName(g, bindings))));
        }

        // See Planner.BuildPlanSummary's comment: output.Count is accurate for star, qualified-star, and
        // plain-item cases alike.
        parts.Add($"SELECT {output.Count}");

        return string.Join(" → ", parts);
    }
}
