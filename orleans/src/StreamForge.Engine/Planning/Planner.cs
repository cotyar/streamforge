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

        var (query, parseDiags) = Parser.Parse(tokens);
        diagnostics.AddRange(parseDiags);

        if (query is null)
        {
            return new CompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var validation = Validator.Validate(query, schemas);
        diagnostics.AddRange(validation.Diagnostics);

        if (diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error))
        {
            return new CompileResult { Ok = false, Diagnostics = diagnostics };
        }

        var compiled = BuildCompiledPlan(query, validation);
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

    private static CompiledPlan BuildCompiledPlan(SelectQuery q, ValidationResult v)
    {
        var sources = v.Sources.Select(s => new CompiledSource { Alias = s.Alias, SourceName = s.SourceName, Schema = s.Schema }).ToList();
        var joins = v.Joins.Select(j => new CompiledJoin
        {
            Kind = j.Kind,
            Alias = j.Alias,
            SourceName = j.SourceName,
            Schema = v.Sources.First(s => s.Alias == j.Alias).Schema,
            Within = j.Within ?? TimeSpan.Zero,
            LeftKey = j.LeftKey,
            RightKey = j.RightKey,
            Residual = j.Residual,
        }).ToList();

        var bindings = v.Bindings;
        var output = BuildOutput(q, sources, bindings);

        if (q.Window is not null)
        {
            AssignGroupByIndexes(output, q.GroupBy, bindings);
        }

        var aggregateNodes = CollectAggregateNodes(output.Select(o => o.Expression));
        var aggregateIndex = new Dictionary<AggregateCallExpr, int>(ReferenceEqualityComparer.Instance);
        for (int i = 0; i < aggregateNodes.Count; i++) aggregateIndex[aggregateNodes[i]] = i;

        var sourceNames = sources.Select(s => s.SourceName).Distinct().ToList();
        var sourceLabel = string.Join(",", sources.Select(s => s.Alias));

        return new CompiledPlan
        {
            Sources = sources,
            Joins = joins,
            Where = q.Where,
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
}
