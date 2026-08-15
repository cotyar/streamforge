using System.Collections.Generic;

namespace StreamForge.Engine.Sql;

/// <summary>A resolved, validated JOIN: its equi-key component lists (null only when validation already
/// failed, or this kind has no equi-key concept — CROSS, UNNEST) and residual filter.</summary>
internal sealed class JoinBinding(JoinKind kind, string alias, string sourceName, TimeSpan? within, IReadOnlyList<Expr>? leftKeys, IReadOnlyList<Expr>? rightKeys, Expr? residual, bool isTable = false, Expr? unnestExpr = null)
{
    public JoinKind Kind { get; } = kind;
    public string Alias { get; } = alias;
    public string SourceName { get; } = sourceName;
    public TimeSpan? Within { get; } = within;
    /// <summary>Plan 008: every equi-conjunct's left/right operand, in ON-clause order — composite keys
    /// (e.g. `ON a.x=b.x AND a.y=b.y` yields two components on each side, not one key plus a residual).
    /// Non-null is always &gt;= 1 element. <see cref="Residual"/> holds only conjuncts that are NOT an
    /// equi-comparison between the two sides at all — see Validator.ExtractEquiKey. Consumed directly
    /// (as full composite keys) only by table-mode LEFT/RIGHT/FULL's TableOuterJoinOp; every single-key
    /// op (TableJoinOp, TableSemiAntiOp, PipelineJoinOp, PipelineSubqueryOp) is unaffected — see
    /// Planning/TablePlanner.cs and Planning/Planner.cs's JoinKeyFolding.FoldExtraKeysIntoResidual call,
    /// which reconstructs their pre-008 "first key + residual" view from these lists.</summary>
    public IReadOnlyList<Expr>? LeftKeys { get; } = leftKeys;
    public IReadOnlyList<Expr>? RightKeys { get; } = rightKeys;
    public Expr? Residual { get; } = residual;
    /// <summary>Table mode only: whether SourceName resolved against the table namespace (vs. streams).</summary>
    public bool IsTable { get; } = isTable;
    /// <summary>Plan 002 L2: set only when Kind == Unnest — the expression to unnest (evaluated against
    /// whatever real sources precede this join). Null for every other join kind.</summary>
    public Expr? UnnestExpr { get; } = unnestExpr;
}

/// <summary>Plan 004 N1: set on a <see cref="ResolvedSource"/> when it came from `FROM (SELECT ...) alias`
/// or a desugared WITH-list CTE. Carries the (already parser-desugared) inner query plus its own,
/// independently-computed <see cref="ValidationResult"/> — Planner reuses both directly (no re-validation)
/// to build the nested child CompiledPlan/CompiledTablePlan this source wraps at plan time.</summary>
internal sealed class DerivedInfo
{
    public required SelectQuery Query { get; init; }
    public required ValidationResult Validation { get; init; }
}

/// <summary>Plan 008 W3: set on a <see cref="ResolvedSource"/> when it came from a derived-table-position
/// set operation — `FROM ( SELECT ... UNION [ALL] SELECT ... ) alias`. Parallels <see cref="DerivedInfo"/>
/// (kept as its own type rather than widening DerivedInfo — see DerivedSetOperationSource's own doc comment
/// for why). Carries every branch's own independently-computed ValidationResult, in branch order, so
/// Planner can build each branch's CompiledPlan/CompiledTablePlan the exact same recursive way a plain
/// derived table's does (Planner.BuildCompiledPlan/BuildCompiledTablePlan already take a SelectQuery +
/// ValidationResult pair per call).</summary>
internal sealed class UnionDerivedInfo
{
    public required SetOperationQuery SetOp { get; init; }
    public required List<ValidationResult> BranchValidations { get; init; }
}

/// <summary>One resolved FROM/JOIN source — a plain named stream/table (Derived/UnionDerived both null), a
/// derived table/CTE (Derived is set; Schema is then the inner query's synthesized output schema, "one
/// level up" per plan 004 N1), or a derived-table-position set operation (UnionDerived is set — plan 008
/// W3; Schema is then the branches' unified output schema). Derived and UnionDerived are mutually
/// exclusive.</summary>
internal sealed class ResolvedSource
{
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required bool IsTable { get; init; }
    public DerivedInfo? Derived { get; init; }
    public UnionDerivedInfo? UnionDerived { get; init; }
    /// <summary>Plan 002 L2: true when this source is an UNNEST alias (its Schema is the synthetic
    /// one-field element schema — see Validator.ResolveUnnestJoin) rather than a real named/derived
    /// source.</summary>
    public bool IsUnnest { get; init; }
}

/// <summary>Plan 008 W3: outcome of validating a top-level (or WITH-wrapped) set operation — the
/// SetOperationQuery analogue of <see cref="ValidationResult"/>. <see cref="OutputSchema"/> is null only
/// when branch validation/compatibility failed (Diagnostics then holds the reason); StreamInputs/
/// TableInputs are each branch's own already-folded leaf inputs, unioned (distinct) across every branch —
/// mirrors how Planner.BuildCompiledPlan unions SourceNames across branches for pipeline mode.</summary>
internal sealed class SetOperationValidationResult
{
    public required List<SqlDiagnostic> Diagnostics { get; init; }
    public required List<ValidationResult> BranchValidations { get; init; }
    public required SourceSchema? OutputSchema { get; init; }
    public required List<string> StreamInputs { get; init; }
    public required List<string> TableInputs { get; init; }
    public bool HasErrors => Diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error);
}

/// <summary>Plan 004 N2: one resolved `[NOT] IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)` WHERE predicate
/// — uncorrelated in this tier. Planner rewrites the predicate's own AST node into a Semi (Negated=false)
/// or Anti (Negated=true) join stage appended to the query's join chain, and drops the predicate out of
/// the residual WHERE — see Planner.RewriteWhereForSubqueryPredicates.</summary>
internal sealed class SubqueryPredicateInfo
{
    public required SelectQuery Query { get; init; }
    public required ValidationResult Validation { get; init; }
    public required bool Negated { get; init; }
    /// <summary>Null for EXISTS (existence-only — no specific key column; Planner uses a constant key on
    /// both sides of the synthesized join instead). Set for IN: the subquery's single projected output
    /// column's name, matching the naming DerivedDefaultName/Planner.DefaultName would assign it.</summary>
    public required string? KeyColumnName { get; init; }
}

/// <summary>Plan 004 N4: one component of a scalar subquery's equality correlation —
/// `<paramref name="InnerExpr"/> = <paramref name="OuterAlias"/>.<paramref name="OuterField"/>` (or the
/// reverse) found as a top-level WHERE conjunct of the subquery. Planner uses <see cref="InnerExpr"/> as
/// (one component of) the decorrelated join's inner-side key and <see cref="OuterAlias"/>/
/// <see cref="OuterField"/> to build the outer-side key.</summary>
internal sealed class CorrelationKey
{
    public required Expr InnerExpr { get; init; }
    public required string OuterAlias { get; init; }
    public required string OuterField { get; init; }
}

/// <summary>Plan 004 N3 (Correlations empty) / N4 (Correlations non-empty): one resolved scalar subquery
/// expression. <see cref="ResidualQuery"/> is <see cref="ScalarSubqueryExpr.Query"/> with its correlation
/// conjuncts stripped out of WHERE and — for N4 — a synthesized GROUP BY + `__key{i}` projections added
/// (the decorrelation itself; see Validator.ResolveScalarSubquery). Planner compiles ResidualQuery exactly
/// like a derived table (BuildCompiledPlan/BuildCompiledTablePlan) and wires it as a Scalar-kind join
/// stage, then rewrites the original ScalarSubqueryExpr node (everywhere it appears in Output/WHERE) into
/// a bound reference to that join's <see cref="ValueColumnName"/> output column.</summary>
internal sealed class ScalarSubqueryInfo
{
    public required SelectQuery ResidualQuery { get; init; }
    public required ValidationResult Validation { get; init; }
    /// <summary>Empty for N3 (uncorrelated — Planner joins on a constant key instead).</summary>
    public required List<CorrelationKey> Correlations { get; init; }
    public required string ValueColumnName { get; init; }
    /// <summary>N4 only: ResidualQuery's synthesized `__key{i}` output column names, same order as
    /// <see cref="Correlations"/>.</summary>
    public required List<string> KeyColumnNames { get; init; }
}

internal sealed class ValidationResult
{
    public required List<SqlDiagnostic> Diagnostics { get; init; }
    public required List<ResolvedSource> Sources { get; init; }
    public required Dictionary<Expr, (string Alias, string Field)> Bindings { get; init; }
    public required List<JoinBinding> Joins { get; init; }
    public required List<AggregateCallExpr> UsedAggregates { get; init; }
    /// <summary>Every expression node's inferred FieldKind (column refs, JSON access results, literals,
    /// arithmetic/aggregate results, ...) — populated in both modes, consumed by table-mode OutputSchema
    /// derivation and by the JSON bare-operand checks.</summary>
    public required Dictionary<Expr, FieldKind> ExprKinds { get; init; }
    /// <summary>Table mode only: distinct stream source names referenced (FROM/JOIN).</summary>
    public required List<string> StreamInputs { get; init; }
    /// <summary>Table mode only: distinct other-table names referenced (FROM/JOIN).</summary>
    public required List<string> TableInputs { get; init; }
    /// <summary>Plan 004 N2: every resolved IN/EXISTS WHERE predicate, keyed by its own InSubqueryExpr or
    /// ExistsExpr AST node.</summary>
    public required Dictionary<Expr, SubqueryPredicateInfo> SubqueryPredicates { get; init; }
    /// <summary>Plan 004 N3/N4: every resolved scalar subquery expression, keyed by its own AST node.</summary>
    public required Dictionary<ScalarSubqueryExpr, ScalarSubqueryInfo> ScalarSubqueries { get; init; }
    /// <summary>Plan 002 L3: the query's LATEST BY key expressions (table mode only — see the
    /// "LATEST BY is table-mode only" diagnostic), carried through unchanged from SelectQuery.LatestBy.</summary>
    public required List<Expr>? LatestBy { get; init; }
    public bool HasAggregates => UsedAggregates.Count > 0;
    public bool HasErrors => Diagnostics.Exists(d => d.Severity == DiagnosticSeverity.Error);
}

internal enum ValidationMode { Stream, Table }

/// <summary>Semantic analysis: source/column/alias resolution, aggregate/window/emit structural rules,
/// and equi-join key extraction. Never throws — every problem becomes a <see cref="SqlDiagnostic"/>.
/// Two modes: Stream (windowed pipelines — the original dialect) and Table (persistent materialized
/// tables — unwindowed running aggregates over a combined streams+tables namespace).</summary>
internal sealed class Validator
{
    private readonly ValidationMode _mode;
    private readonly IReadOnlyDictionary<string, SourceSchema> _schemas;
    private readonly HashSet<string> _tableNames = new(StringComparer.Ordinal);
    private readonly HashSet<string> _ambiguousNames = new(StringComparer.Ordinal);
    private readonly List<string> _streamInputs = [];
    private readonly List<string> _tableInputs = [];

    // Table mode only: the original (pre-merge) stream/table schema dicts, retained so a derived table's
    // recursive Validator.ValidateTable call gets the exact same streams-vs-tables split as the outer query
    // (the merged _schemas dict alone can't distinguish which side a name came from).
    private readonly IReadOnlyDictionary<string, SourceSchema>? _streamSchemasForRecursion;
    private readonly IReadOnlyDictionary<string, SourceSchema>? _tableSchemasForRecursion;

    private readonly List<SqlDiagnostic> _diags = [];
    private readonly Dictionary<Expr, (string Alias, string Field)> _bindings = new(ReferenceEqualityComparer.Instance);
    private readonly List<AggregateCallExpr> _usedAggregates = [];

    // Plan 008 W3: `GROUP BY ALL` deliberately reuses the SAME Expr instances as the matching select-list
    // items (see Parser.ParseSelectQuery), so StructurallyEqual/AssignGroupByIndexes match trivially. That
    // sharing means ResolveExpr can be asked to resolve the exact same node twice (once walking GROUP BY,
    // once walking SELECT — Run() always does both, in that order). Re-resolving a node that already
    // resolved successfully is harmless (the same _bindings/_exprKind entry is written again), but
    // re-resolving one that FAILED (unknown/ambiguous column) would otherwise append the same diagnostic a
    // second time — so every node is only ever walked once, tracked here by reference identity.
    private readonly HashSet<Expr> _resolvedNodes = new(ReferenceEqualityComparer.Instance);

    // Plan 004 N2: WHERE-position gating for IN/EXISTS subquery predicates. Set only while resolving this
    // query's own WHERE clause (Run() toggles _resolvingWhere around that one ResolveExpr call);
    // _whereTopLevelConjuncts is FlattenAnd(q.Where)'s yield, by reference — the exact set of nodes an
    // IN/EXISTS predicate is allowed to BE (not just appear inside): a predicate nested inside OR/NOT/
    // another operator isn't in this set even while _resolvingWhere is true, since pushing it down into a
    // semi/anti join would change the query's meaning (see ResolveInSubquery/ResolveExists).
    private bool _resolvingWhere;
    private HashSet<Expr>? _whereTopLevelConjuncts;

    // Plan 004 N2/N3/N4 resolved-subquery side tables — see SubqueryPredicateInfo/ScalarSubqueryInfo docs.
    private readonly Dictionary<Expr, SubqueryPredicateInfo> _subqueryPredicates = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<ScalarSubqueryExpr, ScalarSubqueryInfo> _scalarSubqueries = new(ReferenceEqualityComparer.Instance);

    // Tracks the resolved FieldKind of expression nodes: a column reference gets its schema kind, a
    // JsonAccessExpr gets Json ('->') or String ('->>'), and (for table-mode OutputSchema derivation)
    // literals/arithmetic/aggregate/function results get their inferred kind too. Nodes absent from this
    // map (StarExpr, NullLiteral, ...) simply have no meaningful kind for our purposes.
    private readonly Dictionary<Expr, FieldKind> _exprKind = new(ReferenceEqualityComparer.Instance);

    // Plan 002 L2: every UNNEST alias resolved so far, in this query's own FROM/JOIN chain — populated as
    // each UnnestSource resolves (see ResolveUnnestJoin), consulted (a) to keep a LATER UNNEST's own expr
    // from referencing an EARLIER unnest alias (only real FROM sources are allowed — see
    // FindUnnestAliasReference) and (b) to give `unnestAlias.field` a tailored diagnostic instead of the
    // generic "unknown column" one (see ResolveQualifiedIdentifier).
    private readonly HashSet<string> _unnestAliases = new(StringComparer.Ordinal);

    private Validator(IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        _mode = ValidationMode.Stream;
        _schemas = schemas;
    }

    private Validator(IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        _mode = ValidationMode.Table;
        _tableNames = new HashSet<string>(tableSchemas.Keys, StringComparer.Ordinal);
        _ambiguousNames = new HashSet<string>(streamSchemas.Keys.Intersect(tableSchemas.Keys, StringComparer.Ordinal), StringComparer.Ordinal);

        var merged = new Dictionary<string, SourceSchema>(streamSchemas, StringComparer.Ordinal);
        foreach (var kv in tableSchemas) merged[kv.Key] = kv.Value;
        _schemas = merged;

        _streamSchemasForRecursion = streamSchemas;
        _tableSchemasForRecursion = tableSchemas;
    }

    public static ValidationResult Validate(SelectQuery query, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        var v = new Validator(schemas);
        return v.Run(query);
    }

    /// <summary>Table-mode validation: sql is a SELECT over streams AND/OR other tables, without windows.
    /// A FROM/JOIN identifier must exist in exactly one of streamSchemas/tableSchemas — present in both
    /// is an "ambiguous name" diagnostic.</summary>
    public static ValidationResult ValidateTable(SelectQuery query, IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        var v = new Validator(streamSchemas, tableSchemas);
        return v.Run(query);
    }

    /// <summary>Plan 008 W3: message for "UNION (distinct) used in pipeline mode" — shared by the top-level
    /// set-operation path (<see cref="FinishSetOperationValidation"/>) and the derived-table-position path
    /// (ResolveFromItem's DerivedSetOperationSource case), so both surfaces report byte-identical wording.
    /// Names UNION ALL as the fix per plan 008's D-F decision (pipeline mode has no Z-set weights to dedup
    /// with, so weight-clamping is meaningless there, and an unbounded distinct over an unbounded stream is
    /// unbounded state — see DESIGN.md §D11).</summary>
    private const string PipelineUnionDistinctMessage =
        "UNION (distinct) is not supported in pipeline mode — pipeline mode has no Z-set weights to dedup " +
        "with, and an unbounded distinct over an unbounded stream would be unbounded state; use UNION ALL instead";

    /// <summary>Plan 008 W3: top-level (or WITH-wrapped) pipeline-mode set operation. Every branch validates
    /// independently and uncorrelated (own Validator instance per branch, same schemas) — see
    /// FinishSetOperationValidation for the shared branch-compatibility/ALL-ness checks.</summary>
    public static SetOperationValidationResult ValidateSetOperation(SetOperationQuery setOp, IReadOnlyDictionary<string, SourceSchema> schemas)
    {
        var branchValidations = setOp.Branches.Select(b => Validate(b, schemas)).ToList();
        return FinishSetOperationValidation(setOp, branchValidations, isTable: false);
    }

    /// <summary>Plan 008 W3: top-level (or WITH-wrapped) table-mode set operation — the ValidateTable
    /// analogue of <see cref="ValidateSetOperation"/>. UNION (distinct) is allowed here (table mode only).</summary>
    public static SetOperationValidationResult ValidateSetOperationTable(SetOperationQuery setOp, IReadOnlyDictionary<string, SourceSchema> streamSchemas, IReadOnlyDictionary<string, SourceSchema> tableSchemas)
    {
        var branchValidations = setOp.Branches.Select(b => ValidateTable(b, streamSchemas, tableSchemas)).ToList();
        return FinishSetOperationValidation(setOp, branchValidations, isTable: true);
    }

    private static SetOperationValidationResult FinishSetOperationValidation(SetOperationQuery setOp, List<ValidationResult> branchValidations, bool isTable)
    {
        var diags = new List<SqlDiagnostic>();
        foreach (var bv in branchValidations) diags.AddRange(bv.Diagnostics);

        if (!isTable && !setOp.All)
        {
            diags.Add(new SqlDiagnostic(PipelineUnionDistinctMessage, setOp.Line, setOp.Column));
        }

        SourceSchema? unified = null;
        var streamInputs = new List<string>();
        var tableInputs = new List<string>();

        if (!diags.Exists(d => d.Severity == DiagnosticSeverity.Error))
        {
            var branchSchemas = setOp.Branches.Zip(branchValidations, (b, bv) => BuildDerivedOutputSchema(b, bv)).ToList();
            var (compatDiags, u) = CheckSetOperationBranchCompatibility(branchSchemas, setOp.Line, setOp.Column);
            diags.AddRange(compatDiags);
            unified = u;

            if (isTable)
            {
                streamInputs = branchValidations.SelectMany(bv => bv.StreamInputs).Distinct(StringComparer.Ordinal).ToList();
                tableInputs = branchValidations.SelectMany(bv => bv.TableInputs).Distinct(StringComparer.Ordinal).ToList();
            }
        }

        return new SetOperationValidationResult
        {
            Diagnostics = diags,
            BranchValidations = branchValidations,
            OutputSchema = unified,
            StreamInputs = streamInputs,
            TableInputs = tableInputs,
        };
    }

    private ValidationResult Run(SelectQuery q)
    {
        var sources = new List<ResolvedSource>();
        var aliasSeen = new HashSet<string>(StringComparer.Ordinal);
        var joins = new List<JoinBinding>();

        // FROM
        RegisterSource(q.From.Source, sources, aliasSeen);

        // JOINs, left-to-right, each ON scoped to (aliases so far) ∪ (this join's alias).
        foreach (var j in q.From.Joins)
        {
            // Plan 002 L2: UNNEST has no ON/WITHIN/equi-key concept at all — a completely different shape
            // from every other JoinKind, so it's resolved by its own dedicated method rather than falling
            // through the WITHIN/ON machinery below (which would reject it for missing both).
            if (j.Source is UnnestSource unnestSrc)
            {
                ResolveUnnestJoin(unnestSrc, sources, aliasSeen, joins);
                continue;
            }

            var leftAliasesBefore = sources.Select(s => s.Alias).ToHashSet(StringComparer.Ordinal);
            var resolved = ResolveFromItem(j.Source, aliasSeen);
            bool sourceOk = resolved.Ok;
            var jSchema = resolved.Schema;
            bool jIsTable = resolved.IsTable;

            if (_mode == ValidationMode.Stream)
            {
                if (j.Within is null)
                {
                    _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN requires a WITHIN clause", j.Line, j.Column));
                }
            }
            else
            {
                if (j.Within is not null)
                {
                    _diags.Add(new SqlDiagnostic(
                        $"{JoinLabel(j.Kind)} JOIN may not have a WITHIN clause in table mode — state is unbounded/consolidated, not time-bounded",
                        j.Line, j.Column));
                }
                // Plan 008: the outer-kind rejection that used to live here is gone — LEFT/RIGHT/FULL
                // (and CROSS, already supported) now compile in table mode too. WITHIN stays banned
                // (above) and the ON/equi-comparison requirements below still apply to every kind.
            }

            List<Expr>? leftKeys = null, rightKeys = null;
            Expr? residual = null;

            if (j.Kind == JoinKind.Cross)
            {
                // Grammar already forbids ON for CROSS; nothing further to resolve.
            }
            else if (j.On is null)
            {
                _diags.Add(new SqlDiagnostic($"{JoinLabel(j.Kind)} JOIN requires an ON clause", j.Line, j.Column));
            }
            else
            {
                var scope = sources.Select(s => (s.Alias, s.Schema)).ToList();
                if (sourceOk) scope.Add((j.Source.Alias, jSchema!));
                ResolveExpr(j.On, scope, aggDepth: 0);

                var extracted = ExtractEquiKey(j.On, leftAliasesBefore, j.Source.Alias);
                if (extracted is null)
                {
                    _diags.Add(new SqlDiagnostic(
                        $"{JoinLabel(j.Kind)} JOIN's ON clause must contain an equi-comparison between {string.Join(", ", leftAliasesBefore)} and {j.Source.Alias}",
                        j.On.Line, j.On.Column));
                }
                else
                {
                    (leftKeys, rightKeys, residual) = extracted.Value;
                }
            }

            if (sourceOk)
            {
                sources.Add(new ResolvedSource { Alias = j.Source.Alias, SourceName = resolved.SourceName, Schema = jSchema!, IsTable = jIsTable, Derived = resolved.Derived, UnionDerived = resolved.UnionDerived });
            }

            joins.Add(new JoinBinding(j.Kind, j.Source.Alias, resolved.SourceName, j.Within, leftKeys, rightKeys, residual, jIsTable));
        }

        var fullScope = sources.Select(s => (s.Alias, s.Schema)).ToList();

        if (q.Where is not null)
        {
            // Plan 004 N2: only a top-level (AND-connected) conjunct of WHERE may be an IN/EXISTS
            // predicate — see the _whereTopLevelConjuncts field doc.
            _whereTopLevelConjuncts = new HashSet<Expr>(FlattenAnd(q.Where), ReferenceEqualityComparer.Instance);
            _resolvingWhere = true;
            ResolveExpr(q.Where, fullScope, aggDepth: 0);
            _resolvingWhere = false;
        }

        if (q.GroupBy is not null)
        {
            foreach (var g in q.GroupBy) ResolveExpr(g, fullScope, aggDepth: 0);
        }

        // Plan 002 L3: LATEST BY key expressions resolve against the same fullScope GROUP BY does — any
        // aggregate used inside one (like an aggregate used inside GROUP BY) folds into _usedAggregates the
        // same way, so the "combined with aggregates" exclusivity check below sees it without extra plumbing.
        if (q.LatestBy is not null)
        {
            foreach (var k in q.LatestBy) ResolveExpr(k, fullScope, aggDepth: 0);
        }

        if (!q.Select.IsStar)
        {
            foreach (var item in q.Select.Items) ResolveExpr(item.Expression, fullScope, aggDepth: 0);
        }

        bool hasAggregates = _usedAggregates.Count > 0;

        if (_mode == ValidationMode.Stream)
        {
            if (hasAggregates && q.Window is null)
            {
                var first = _usedAggregates[0];
                _diags.Add(new SqlDiagnostic("Aggregate functions require a WINDOW clause", first.Line, first.Column));
            }
            if (q.GroupBy is not null && q.Window is null)
            {
                _diags.Add(new SqlDiagnostic("GROUP BY requires a WINDOW clause", q.GroupByLine ?? 1, q.GroupByColumn ?? 1));
            }
            if (q.Emit is not null && q.Window is null)
            {
                _diags.Add(new SqlDiagnostic("EMIT requires a WINDOW clause", q.EmitLine ?? 1, q.EmitColumn ?? 1));
            }
            if (q.LatestBy is not null)
            {
                _diags.Add(new SqlDiagnostic("LATEST BY is table-mode only", q.LatestByLine ?? 1, q.LatestByColumn ?? 1));
            }
        }
        else
        {
            // Table mode: aggregates + GROUP BY are allowed WITHOUT a window (running aggregates —
            // that's the point of a table). WINDOW and EMIT are structurally forbidden outright.
            if (q.Window is not null)
            {
                _diags.Add(new SqlDiagnostic(
                    "WINDOW clause not allowed in table mode — tables maintain state continuously; windows belong to stream pipelines",
                    q.WindowLine ?? 1, q.WindowColumn ?? 1));
            }
            if (q.Emit is not null)
            {
                _diags.Add(new SqlDiagnostic("EMIT not allowed in table mode", q.EmitLine ?? 1, q.EmitColumn ?? 1));
            }
            // Plan 002 L3: LATEST BY is table-mode's running-argmax-by-_ts sugar (see TableLatestByOp) —
            // mutually exclusive with GROUP BY/aggregates (WINDOW is already structurally forbidden above
            // regardless of LATEST BY, so no extra diagnostic is needed for that combination).
            if (q.LatestBy is not null)
            {
                if (q.GroupBy is not null)
                {
                    _diags.Add(new SqlDiagnostic("LATEST BY may not be combined with GROUP BY", q.LatestByLine ?? 1, q.LatestByColumn ?? 1));
                }
                if (hasAggregates)
                {
                    _diags.Add(new SqlDiagnostic("LATEST BY may not be combined with aggregate functions", q.LatestByLine ?? 1, q.LatestByColumn ?? 1));
                }
            }
        }

        if ((q.Window is not null || q.GroupBy is not null || hasAggregates) && !q.Select.IsStar)
        {
            // Every non-aggregate select item must be one of the GROUP BY columns (or there is nothing to
            // group by at all, in which case only aggregate expressions are meaningful).
            var groupByList = q.GroupBy ?? [];
            foreach (var item in q.Select.Items)
            {
                if (item.Expression is QualifiedStarExpr qStar)
                {
                    // A qualified star expands to N columns at plan time; validating "every expanded column
                    // is a grouping column" would require re-resolving the alias's schema here too. Simplest
                    // correct rule instead: disallow alias.* outright alongside GROUP BY/aggregates.
                    _diags.Add(new SqlDiagnostic("star is not allowed with GROUP BY/aggregates", qStar.Line, qStar.Column));
                    continue;
                }
                // Plan 004 N3/N4: a scalar subquery outside any aggregate's own argument can't sit in a
                // grouped/windowed/aggregated query's SELECT list in this engine — TableReduceOp/
                // PipelineWindowOp evaluate non-GROUP-BY select items post-aggregation against a synthetic
                // all-empty row (see their BuildRow), which has none of the joined fields a rewritten
                // scalar-subquery reference needs; silently evaluating to NULL there would be a correctness
                // bug, not a supported case, so this is a diagnostic instead. A scalar subquery used as an
                // AGGREGATE's own argument (e.g. `SUM(price - (SELECT AVG(price) FROM trades))`) is fine —
                // aggregate arguments ARE evaluated against the real per-row WorkingRow pre-aggregation.
                var badScalarSubquery = FindScalarSubqueryOutsideAggregate(item.Expression, insideAggregate: false);
                if (badScalarSubquery is not null)
                {
                    _diags.Add(new SqlDiagnostic(
                        "Scalar subquery in the SELECT list of a grouped/windowed/aggregated query is not supported in this tier — move it to WHERE, or restructure without GROUP BY/WINDOW/aggregates (plan 004 N3/N4)",
                        badScalarSubquery.Line, badScalarSubquery.Column));
                }

                if (ContainsAggregate(item.Expression)) continue;
                bool matchesGroupBy = groupByList.Any(g => StructurallyEqual(item.Expression, g));
                if (!matchesGroupBy)
                {
                    _diags.Add(new SqlDiagnostic(
                        "Non-aggregate select item must appear in GROUP BY",
                        item.Expression.Line, item.Expression.Column));
                }
            }
        }

        return new ValidationResult
        {
            Diagnostics = _diags,
            Sources = sources,
            Bindings = _bindings,
            Joins = joins,
            UsedAggregates = _usedAggregates,
            ExprKinds = _exprKind,
            StreamInputs = _streamInputs.Distinct(StringComparer.Ordinal).ToList(),
            TableInputs = _tableInputs.Distinct(StringComparer.Ordinal).ToList(),
            SubqueryPredicates = _subqueryPredicates,
            ScalarSubqueries = _scalarSubqueries,
            LatestBy = q.LatestBy,
        };
    }

    private void RegisterSource(FromItem item, List<ResolvedSource> sources, HashSet<string> aliasSeen)
    {
        var r = ResolveFromItem(item, aliasSeen);
        if (r.Ok)
        {
            sources.Add(new ResolvedSource { Alias = item.Alias, SourceName = r.SourceName, Schema = r.Schema!, IsTable = r.IsTable, Derived = r.Derived, UnionDerived = r.UnionDerived });
        }
    }

    /// <summary>Resolves one FROM/JOIN item — a plain named source (existing lookup rules unchanged), a
    /// derived table/CTE (plan 004 N1: recursively validates the inner query against the SAME
    /// stream/table namespace — uncorrelated, no outer-alias visibility — then synthesizes this alias's
    /// schema from the inner query's own output), or (plan 008 W3) a derived-table-position set operation
    /// (same uncorrelated-recursion idea, applied to every branch, plus a branch-compatibility check for
    /// the unified schema). Does NOT add to <paramref name="sources"/> itself (callers differ: FROM adds
    /// unconditionally via <see cref="RegisterSource"/>, JOIN adds after also resolving its ON clause) —
    /// but DOES perform the duplicate-alias check every FROM/JOIN item needs, same as the pre-N1
    /// RegisterSource did regardless of call site.</summary>
    private (bool Ok, string SourceName, SourceSchema? Schema, bool IsTable, DerivedInfo? Derived, UnionDerivedInfo? UnionDerived) ResolveFromItem(FromItem item, HashSet<string> aliasSeen)
    {
        if (!aliasSeen.Add(item.Alias))
        {
            _diags.Add(new SqlDiagnostic($"Duplicate alias '{item.Alias}'", item.Line, item.Column));
        }

        if (item is DerivedSource ds)
        {
            ValidationResult inner = ValidateNestedUncorrelated(ds.Query);
            _diags.AddRange(inner.Diagnostics);
            FoldNestedInputs(inner);
            var schema = BuildDerivedOutputSchema(ds.Query, inner);
            return (true, "(derived)", schema, false, new DerivedInfo { Query = ds.Query, Validation = inner }, null);
        }

        if (item is DerivedSetOperationSource dus)
        {
            var branchValidations = dus.SetOp.Branches.Select(ValidateNestedUncorrelated).ToList();
            foreach (var bv in branchValidations)
            {
                _diags.AddRange(bv.Diagnostics);
                FoldNestedInputs(bv);
            }

            if (_mode == ValidationMode.Stream && !dus.SetOp.All)
            {
                _diags.Add(new SqlDiagnostic(PipelineUnionDistinctMessage, dus.SetOp.Line, dus.SetOp.Column));
            }

            var branchSchemas = dus.SetOp.Branches.Zip(branchValidations, (b, bv) => BuildDerivedOutputSchema(b, bv)).ToList();
            var (compatDiags, unified) = CheckSetOperationBranchCompatibility(branchSchemas, dus.SetOp.Line, dus.SetOp.Column);
            _diags.AddRange(compatDiags);

            var schema = unified ?? new SourceSchema("(derived)", new Dictionary<string, FieldKind>());
            return (true, "(derived)", schema, false, null, new UnionDerivedInfo { SetOp = dus.SetOp, BranchValidations = branchValidations });
        }

        var sref = (NamedSource)item;

        if (_mode == ValidationMode.Table && _ambiguousNames.Contains(sref.Name))
        {
            _diags.Add(new SqlDiagnostic($"Ambiguous name '{sref.Name}' — present in both streams and tables", sref.Line, sref.Column));
            return (false, sref.Name, null, false, null, null);
        }

        if (!_schemas.TryGetValue(sref.Name, out var namedSchema))
        {
            var available = string.Join(", ", _schemas.Keys.OrderBy(k => k, StringComparer.Ordinal));
            _diags.Add(new SqlDiagnostic($"Unknown source '{sref.Name}' — available: {available}", sref.Line, sref.Column));
            return (false, sref.Name, null, false, null, null);
        }

        bool isTable = _mode == ValidationMode.Table && _tableNames.Contains(sref.Name);
        if (_mode == ValidationMode.Table)
        {
            if (isTable) _tableInputs.Add(sref.Name); else _streamInputs.Add(sref.Name);
        }

        return (true, sref.Name, namedSchema, isTable, null, null);
    }

    // ------------------------------------------------------------------
    // Plan 002 L2 — UNNEST(expr) AS alias
    // ------------------------------------------------------------------

    /// <summary>Resolves one UNNEST join item (see UnnestSource's doc comment for the two syntactic forms
    /// that both land here). Unlike every other JoinKind, there's no ON/WITHIN/equi-key to extract: the
    /// alias's "schema" is a synthetic one-field element schema (field name == alias name, kind Json —
    /// element typing is dynamic; see the class-level design note this mirrors in WorkingRow/
    /// ExpressionEvaluator), and <paramref name="unnestSrc"/>'s own Expr is resolved against only the REAL
    /// (non-UNNEST) sources registered so far — see FindUnnestAliasReference for why another UNNEST alias
    /// specifically is rejected rather than silently falling through to a generic "unknown column".</summary>
    private void ResolveUnnestJoin(UnnestSource unnestSrc, List<ResolvedSource> sources, HashSet<string> aliasSeen, List<JoinBinding> joins)
    {
        if (!aliasSeen.Add(unnestSrc.Alias))
        {
            _diags.Add(new SqlDiagnostic($"Duplicate alias '{unnestSrc.Alias}'", unnestSrc.Line, unnestSrc.Column));
        }

        var referencedUnnestAlias = FindUnnestAliasReference(unnestSrc.Expr);
        if (referencedUnnestAlias is not null)
        {
            _diags.Add(new SqlDiagnostic(
                $"UNNEST argument may not reference another UNNEST alias '{referencedUnnestAlias}' — only real FROM sources are allowed (UNNEST-of-UNNEST is not supported; plan 002 L2)",
                unnestSrc.Expr.Line, unnestSrc.Expr.Column));
        }
        else
        {
            var realScope = sources.Where(s => !s.IsUnnest).Select(s => (s.Alias, s.Schema)).ToList();
            ResolveExpr(unnestSrc.Expr, realScope, aggDepth: 0);

            var exprKind = GetExprKind(unnestSrc.Expr);
            if (exprKind is not null && exprKind != FieldKind.Json)
            {
                _diags.Add(new SqlDiagnostic(
                    $"UNNEST argument must be a JSON value (a JSON column, or a '->' JSON access expression) — got {DescribeKind(exprKind)}, which can never be a JSON array",
                    unnestSrc.Expr.Line, unnestSrc.Expr.Column));
            }
        }

        // Synthetic one-field element schema: the field NAME equals the alias itself, which is what makes a
        // bare `Identifier(alias)` in SELECT ("the element itself") resolve through the EXACT SAME
        // ResolveBareIdentifier/RecordColumnKind path every other bare column reference already uses — no
        // separate "pseudo-column" special case needed anywhere else in the validator, planner, or runtime
        // (WorkingRow keys this "{alias}_{alias}", same convention as any other alias-qualified field).
        var elementSchema = new SourceSchema("(unnest)", new Dictionary<string, FieldKind> { [unnestSrc.Alias] = FieldKind.Json });
        sources.Add(new ResolvedSource { Alias = unnestSrc.Alias, SourceName = "(unnest)", Schema = elementSchema, IsTable = false, Derived = null, IsUnnest = true });
        _unnestAliases.Add(unnestSrc.Alias);

        joins.Add(new JoinBinding(JoinKind.Unnest, unnestSrc.Alias, "(unnest)", within: null, leftKeys: null, rightKeys: null, residual: null, isTable: false, unnestExpr: unnestSrc.Expr));
    }

    /// <summary>Walks <paramref name="e"/> for the first Identifier/QualifiedIdentifier that names an
    /// already-resolved UNNEST alias (see <see cref="_unnestAliases"/>) — used to reject "UNNEST of an
    /// UNNEST alias" with a specific, helpful diagnostic instead of letting it fall through to whatever
    /// generic "unknown source/column" message an intentionally-narrowed scope would otherwise produce.
    /// Doesn't descend into a nested subquery expression's own SelectQuery — UNNEST's grammar (ParseOr) can
    /// syntactically admit a ScalarSubqueryExpr/InSubqueryExpr/ExistsExpr as its argument, but those are
    /// independently scoped and not what this check is about.</summary>
    private string? FindUnnestAliasReference(Expr e) => e switch
    {
        Identifier id when _unnestAliases.Contains(id.Name) => id.Name,
        QualifiedIdentifier qid when _unnestAliases.Contains(qid.Qualifier) => qid.Qualifier,
        UnaryExpr u => FindUnnestAliasReference(u.Operand),
        BinaryExpr b => FindUnnestAliasReference(b.Left) ?? FindUnnestAliasReference(b.Right),
        FunctionCallExpr f => f.Args.Select(FindUnnestAliasReference).FirstOrDefault(r => r is not null),
        JsonAccessExpr j => FindUnnestAliasReference(j.Left),
        _ => null,
    };

    /// <summary>Validates <paramref name="q"/> as a fully independent, uncorrelated query against this
    /// Validator's own namespace — the one mechanism N1's derived tables, N2's IN/EXISTS subqueries, and
    /// (for the residual, correlation-conjuncts-stripped query) N3/N4's scalar subqueries all share. "Same
    /// mode, same schemas" is what makes it uncorrelated: no outer alias is ever added to the scope the
    /// recursive Validate/ValidateTable call resolves against, so a reference to one fails exactly like any
    /// other unknown source would.</summary>
    private ValidationResult ValidateNestedUncorrelated(SelectQuery q) =>
        _mode == ValidationMode.Table ? ValidateTable(q, _streamSchemasForRecursion!, _tableSchemasForRecursion!) : Validate(q, _schemas);

    /// <summary>Table mode only: folds a nested (uncorrelated) validation's own leaf StreamInputs/
    /// TableInputs into this query's — see ResolveFromItem's original doc comment on why this must be
    /// transitive rather than reporting the synthetic "(derived)" marker.</summary>
    private void FoldNestedInputs(ValidationResult inner)
    {
        if (_mode != ValidationMode.Table) return;
        _streamInputs.AddRange(inner.StreamInputs);
        _tableInputs.AddRange(inner.TableInputs);
    }

    /// <summary>Plan 008 W3: checks that every branch of a set operation has the same output arity and
    /// positionally-compatible column kinds, and computes the unified output schema — branch 0's column
    /// NAMES, with each column's KIND unified pairwise across every branch (Long+Double unify to Double;
    /// every other pair must be identical; Json and Timestamp only unify with themselves, matching how
    /// nothing else in this dialect implicitly widens a JSON or timestamp value). An arity mismatch is
    /// reported once (comparing every branch to branch 0's own count) and short-circuits — a schema of the
    /// WRONG column count would be worse than none, and a kind mismatch on a column that doesn't even line
    /// up positionally is not a meaningful diagnostic. Returns (Diagnostics, null) on any incompatibility;
    /// (empty, non-null) on success. There is no schema-equality/merge helper anywhere else in the repo —
    /// this is new code for plan 008 W3, homed next to BuildDerivedOutputSchema (the other "compute a
    /// derived alias's schema from an inner validation" helper) rather than in Planning/, since both the
    /// top-level set-operation path and the derived-table-position path need it at validation time, before
    /// any CompiledPlan exists.</summary>
    internal static (List<SqlDiagnostic> Diagnostics, SourceSchema? Unified) CheckSetOperationBranchCompatibility(
        IReadOnlyList<SourceSchema> branchSchemas, int line, int column)
    {
        var diags = new List<SqlDiagnostic>();
        if (branchSchemas.Count == 0) return (diags, null);

        var firstNames = branchSchemas[0].Fields.Keys.ToList();
        var firstKinds = branchSchemas[0].Fields.Values.ToList();
        int arity = firstNames.Count;

        for (int b = 1; b < branchSchemas.Count; b++)
        {
            if (branchSchemas[b].Fields.Count != arity)
            {
                diags.Add(new SqlDiagnostic(
                    $"UNION branch {b + 1} has {branchSchemas[b].Fields.Count} output column(s), branch 1 has {arity} — every branch of a set operation must have the same number of columns",
                    line, column));
            }
        }
        if (diags.Count > 0) return (diags, null);

        var unifiedKinds = firstKinds.ToArray();
        for (int b = 1; b < branchSchemas.Count; b++)
        {
            var kinds = branchSchemas[b].Fields.Values.ToList();
            for (int i = 0; i < arity; i++)
            {
                var u = unifiedKinds[i];
                var k = kinds[i];
                if (u == k) continue;
                if ((u == FieldKind.Long && k == FieldKind.Double) || (u == FieldKind.Double && k == FieldKind.Long))
                {
                    unifiedKinds[i] = FieldKind.Double;
                    continue;
                }
                diags.Add(new SqlDiagnostic(
                    $"UNION branch {b + 1}'s column {i + 1} ('{firstNames[i]}') has kind {k}, which is not compatible with branch 1's kind {u}",
                    line, column));
            }
        }
        if (diags.Count > 0) return (diags, null);

        var fields = new Dictionary<string, FieldKind>();
        for (int i = 0; i < arity; i++) fields[firstNames[i]] = unifiedKinds[i];
        return (diags, new SourceSchema("(union)", fields));
    }

    /// <summary>Plan 004 N1: "a derived table's output schema (existing BuildOutputSchema) is the
    /// synthetic source schema one level up." Mirrors Planning.Planner/TablePlanner's BuildOutputSchema —
    /// necessarily duplicated (not shared) here: Sql/ sits below Planning/ in this codebase's layering
    /// (Planner references Validator, not the reverse), and Planner's version works off its own
    /// Planning-specific OutputItem/CompiledSource DTOs the Validator doesn't have yet at this point in the
    /// pipeline. Same star/qualified-star/default-naming rules as both those methods.</summary>
    private static SourceSchema BuildDerivedOutputSchema(SelectQuery q, ValidationResult inner)
    {
        var fields = new Dictionary<string, FieldKind>();
        bool prefixed = inner.Sources.Count > 1;

        void AddSourceFields(ResolvedSource src)
        {
            foreach (var (fname, fkind) in src.Schema.Fields)
            {
                fields[prefixed ? $"{src.Alias}_{fname}" : fname] = fkind;
            }
        }

        if (q.Select.IsStar)
        {
            foreach (var src in inner.Sources) AddSourceFields(src);
            return new SourceSchema("(derived)", fields);
        }

        for (int i = 0; i < q.Select.Items.Count; i++)
        {
            var item = q.Select.Items[i];
            if (item.Expression is QualifiedStarExpr qs)
            {
                var src = inner.Sources.FirstOrDefault(s => string.Equals(s.Alias, qs.Alias, StringComparison.Ordinal));
                if (src is not null) AddSourceFields(src);
                continue;
            }
            var name = item.Alias ?? DerivedDefaultName(item.Expression, i);
            var kind = inner.ExprKinds.TryGetValue(item.Expression, out var k) ? k : FieldKind.String;
            fields[name] = kind;
        }
        return new SourceSchema("(derived)", fields);
    }

    private static string DerivedDefaultName(Expr e, int index) => e switch
    {
        Identifier id => id.Name,
        QualifiedIdentifier q => q.Name,
        AggregateCallExpr { IsStar: true } => "count_star",
        AggregateCallExpr agg => agg.Name.ToLowerInvariant(),
        FunctionCallExpr f => f.Name.ToLowerInvariant(),
        JsonAccessExpr j => j.Key switch
        {
            StringLiteral s => s.Value,
            NumberLiteral { LongValue: { } n } => n.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => "json",
        },
        _ => $"col{index + 1}",
    };

    private static string JoinLabel(JoinKind kind) => kind switch
    {
        JoinKind.Inner => "INNER",
        JoinKind.Left => "LEFT",
        JoinKind.Right => "RIGHT",
        JoinKind.Full => "FULL",
        JoinKind.Cross => "CROSS",
        _ => kind.ToString(),
    };

    private static bool IsReservedName(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(name, EventRecord.SourceField, StringComparison.OrdinalIgnoreCase);

    // ------------------------------------------------------------------
    // Column / alias resolution
    // ------------------------------------------------------------------

    private void ResolveExpr(Expr e, List<(string Alias, SourceSchema Schema)> scope, int aggDepth)
    {
        if (!_resolvedNodes.Add(e)) return;

        switch (e)
        {
            case NumberLiteral nl:
                _exprKind[e] = nl.IsDouble ? FieldKind.Double : FieldKind.Long;
                return;
            case StringLiteral:
                _exprKind[e] = FieldKind.String;
                return;
            case BoolLiteral:
                _exprKind[e] = FieldKind.Bool;
                return;
            case NullLiteral or StarExpr:
                return;
            case Identifier id:
                ResolveBareIdentifier(id, scope);
                return;
            case QualifiedIdentifier qid:
                ResolveQualifiedIdentifier(qid, scope);
                return;
            case QualifiedStarExpr qs:
                ResolveQualifiedStar(qs, scope);
                return;
            case UnaryExpr u:
                ResolveExpr(u.Operand, scope, aggDepth);
                RecordUnaryKind(e, u);
                return;
            case BinaryExpr b:
                ResolveExpr(b.Left, scope, aggDepth);
                ResolveExpr(b.Right, scope, aggDepth);
                if (ArithmeticOrComparisonOps.Contains(b.Op))
                {
                    CheckBareJsonOperand(b.Left, b.Op, b.Right);
                    CheckBareJsonOperand(b.Right, b.Op, b.Left);
                }
                RecordBinaryKind(e, b);
                return;
            case JsonAccessExpr j:
                ResolveJsonAccess(j, scope, aggDepth);
                return;
            case FunctionCallExpr f:
                ValidateFunctionArity(f);
                foreach (var a in f.Args) ResolveExpr(a, scope, aggDepth);
                RecordFunctionKind(e, f);
                return;
            case AggregateCallExpr agg:
                if (aggDepth > 0)
                {
                    _diags.Add(new SqlDiagnostic("Aggregate functions cannot be nested", agg.Line, agg.Column));
                }
                _usedAggregates.Add(agg);
                if (!agg.IsStar && agg.Arg is not null) ResolveExpr(agg.Arg, scope, aggDepth + 1);
                RecordAggregateKind(e, agg);
                return;
            case InSubqueryExpr ins:
                ResolveInSubquery(ins, scope, aggDepth);
                return;
            case ExistsExpr ex:
                ResolveExists(ex);
                return;
            case ScalarSubqueryExpr sse:
                ResolveScalarSubquery(sse, scope);
                return;
            default:
                return;
        }
    }

    private void RecordUnaryKind(Expr node, UnaryExpr u)
    {
        if (u.Op == "NOT") { _exprKind[node] = FieldKind.Bool; return; }
        if (GetExprKind(u.Operand) is { } k && k != FieldKind.Json) _exprKind[node] = k;
    }

    private static readonly HashSet<string> ComparisonOps = new(StringComparer.Ordinal)
    {
        "=", "!=", "<>", "<", "<=", ">", ">=", "AND", "OR",
    };

    private void RecordBinaryKind(Expr node, BinaryExpr b)
    {
        if (ComparisonOps.Contains(b.Op)) { _exprKind[node] = FieldKind.Bool; return; }
        if (b.Op == "/") { _exprKind[node] = FieldKind.Double; return; }
        var lk = GetExprKind(b.Left);
        var rk = GetExprKind(b.Right);
        if (lk == FieldKind.Long && rk == FieldKind.Long) _exprKind[node] = FieldKind.Long;
        else if ((lk is FieldKind.Long or FieldKind.Double) && (rk is FieldKind.Long or FieldKind.Double)) _exprKind[node] = FieldKind.Double;
    }

    private void RecordFunctionKind(Expr node, FunctionCallExpr f)
    {
        switch (f.Name.ToUpperInvariant())
        {
            case "ABS" or "ROUND":
                _exprKind[node] = f.Args.Count > 0 && GetExprKind(f.Args[0]) is FieldKind.Long ? FieldKind.Long : FieldKind.Double;
                break;
            case "UPPER" or "LOWER":
                _exprKind[node] = FieldKind.String;
                break;
            case "COALESCE":
                if (f.Args.Count > 0 && GetExprKind(f.Args[0]) is { } k) _exprKind[node] = k;
                break;
            // Plan 009 Round C wave C1: type-conversion functions — result kind is fixed by the
            // function name, independent of the argument's kind (unlike ABS/ROUND/COALESCE above).
            case "TO_LONG":
                _exprKind[node] = FieldKind.Long;
                break;
            case "TO_DOUBLE":
                _exprKind[node] = FieldKind.Double;
                break;
            case "TO_BOOL":
                _exprKind[node] = FieldKind.Bool;
                break;
            case "TO_TIMESTAMP":
                _exprKind[node] = FieldKind.Timestamp;
                break;
            case "TO_STRING":
                _exprKind[node] = FieldKind.String;
                break;
            case "IF":
                RecordIfKind(node, f);
                break;
            default:
                // A registered scalar (SqlFunctions) — reached only for a name none of the built-in arms
                // above claimed, because registration refuses a built-in's name.
                if (SqlFunctions.FindScalar(f.Name) is { } ext)
                {
                    var argKinds = f.Args.Select(GetExprKind).ToArray();
                    if (ext.ResultKind(argKinds) is { } extKind) _exprKind[node] = extKind;
                }
                break;
        }
    }

    /// <summary>`IF(cond, then, else)` — and therefore every searched CASE, which desugars to it. The
    /// result kind is the branches' common kind, so the two branches have to agree: a table column whose
    /// type depends on which row it came from is not something the rest of the planner can represent.
    /// Long/Double mix widens to Double, matching what <see cref="RecordBinaryKind"/> already does for
    /// mixed arithmetic; anything else that disagrees is a diagnostic, positioned on the call so a
    /// nested CASE points at the branch that broke it. An unknown branch kind (NULL literal, an
    /// unresolved column that already produced its own diagnostic) is deferred to, not guessed at —
    /// same tolerance COALESCE has.</summary>
    private void RecordIfKind(Expr node, FunctionCallExpr f)
    {
        if (f.Args.Count != 3) return; // arity already diagnosed

        // A non-Bool condition is never true at runtime (truthiness here is `value is true`, nothing
        // wider), so it would silently always take the else-branch. Loud beats silent.
        if (GetExprKind(f.Args[0]) is { } condKind && condKind != FieldKind.Bool)
        {
            _diags.Add(new SqlDiagnostic(
                $"CASE/IF condition must be a boolean expression, got {condKind}", f.Args[0].Line, f.Args[0].Column));
        }

        var thenKind = GetExprKind(f.Args[1]);
        var elseKind = GetExprKind(f.Args[2]);
        if (thenKind is null || elseKind is null)
        {
            if ((thenKind ?? elseKind) is { } known) _exprKind[node] = known;
            return;
        }
        if (thenKind == elseKind)
        {
            _exprKind[node] = thenKind.Value;
            return;
        }
        if (thenKind is FieldKind.Long or FieldKind.Double && elseKind is FieldKind.Long or FieldKind.Double)
        {
            _exprKind[node] = FieldKind.Double;
            return;
        }
        _diags.Add(new SqlDiagnostic(
            $"CASE/IF branches must produce the same type, got {thenKind} and {elseKind}", f.Line, f.Column));
    }

    private void RecordAggregateKind(Expr node, AggregateCallExpr agg)
    {
        var argKind = agg.Arg is not null ? GetExprKind(agg.Arg) : null;
        _exprKind[node] = agg.Name switch
        {
            "COUNT" => FieldKind.Long,
            "SUM" => argKind == FieldKind.Long ? FieldKind.Long : FieldKind.Double,
            "AVG" => FieldKind.Double,
            "MIN" or "MAX" => argKind ?? FieldKind.Double,
            // A registered aggregate states its own; Double stays the fallback for anything that
            // declines to (same "unknown means don't guess narrower" rule the scalars use).
            _ => SqlFunctions.FindAggregate(agg.Name)?.ResultKind(argKind) ?? FieldKind.Double,
        };
    }

    private void ResolveBareIdentifier(Identifier id, List<(string Alias, SourceSchema Schema)> scope)
    {
        bool reserved = IsReservedName(id.Name);
        var matches = new List<(string Alias, string Field)>();
        foreach (var (alias, schema) in scope)
        {
            if (reserved)
            {
                matches.Add((alias, CanonicalReserved(id.Name)));
                continue;
            }
            var field = schema.Fields.Keys.FirstOrDefault(k => string.Equals(k, id.Name, StringComparison.OrdinalIgnoreCase));
            if (field is not null) matches.Add((alias, field));
        }

        if (matches.Count == 0)
        {
            _diags.Add(new SqlDiagnostic($"Unknown column '{id.Name}'", id.Line, id.Column));
            return;
        }
        if (matches.Count > 1)
        {
            _diags.Add(new SqlDiagnostic(
                $"Ambiguous column '{id.Name}' — present in: {string.Join(", ", matches.Select(m => m.Alias))}",
                id.Line, id.Column));
            return;
        }
        _bindings[id] = matches[0];
        if (reserved) _exprKind[id] = ReservedKind(id.Name);
        else RecordColumnKind(id, scope, matches[0]);
    }

    private void ResolveQualifiedIdentifier(QualifiedIdentifier qid, List<(string Alias, SourceSchema Schema)> scope)
    {
        var entryIndex = scope.FindIndex(s => string.Equals(s.Alias, qid.Qualifier, StringComparison.Ordinal));
        if (entryIndex < 0)
        {
            var available = string.Join(", ", scope.Select(s => s.Alias));
            _diags.Add(new SqlDiagnostic($"Unknown source '{qid.Qualifier}' — available: {available}", qid.Line, qid.Column));
            return;
        }
        var (alias, schema) = scope[entryIndex];
        if (IsReservedName(qid.Name))
        {
            _bindings[qid] = (alias, CanonicalReserved(qid.Name));
            _exprKind[qid] = ReservedKind(qid.Name);
            return;
        }
        // Plan 002 L2: `unnestAlias.field` — the alias's value is a single JSON element, not a row with
        // named fields (its own field name is a synthetic key equal to the alias, not "field" — see
        // ResolveUnnestJoin), so this is an error unconditionally, with a message pointing at the '->>'
        // syntax it should have used instead of the generic "unknown column on alias" wording.
        if (_unnestAliases.Contains(alias))
        {
            _diags.Add(new SqlDiagnostic(
                $"UNNEST alias '{alias}' has no field '{qid.Name}' — its value is a single JSON array element, not a row; use a JSON operator instead, e.g. {alias} ->> '{qid.Name}'",
                qid.Line, qid.Column));
            return;
        }
        var field = schema.Fields.Keys.FirstOrDefault(k => string.Equals(k, qid.Name, StringComparison.OrdinalIgnoreCase));
        if (field is null)
        {
            _diags.Add(new SqlDiagnostic($"Unknown column '{qid.Name}' on '{alias}'", qid.Line, qid.Column));
            return;
        }
        _bindings[qid] = (alias, field);
        if (schema.Fields.TryGetValue(field, out var kind)) _exprKind[qid] = kind;
    }

    /// <summary>Resolves `alias.*`'s qualifier against the query's sources — same "unknown alias" shape as
    /// ResolveQualifiedIdentifier's qualifier check, but there's no single field/kind to record: the
    /// Planner re-walks the (now-known-valid) alias at plan time to expand it into one output column per
    /// field of that source's schema.</summary>
    private void ResolveQualifiedStar(QualifiedStarExpr qs, List<(string Alias, SourceSchema Schema)> scope)
    {
        bool found = scope.Any(s => string.Equals(s.Alias, qs.Alias, StringComparison.Ordinal));
        if (!found)
        {
            var available = string.Join(", ", scope.Select(s => s.Alias));
            _diags.Add(new SqlDiagnostic($"Unknown source/alias '{qs.Alias}' — available: {available}", qs.Line, qs.Column));
        }
    }

    private void RecordColumnKind(Expr node, List<(string Alias, SourceSchema Schema)> scope, (string Alias, string Field) resolved)
    {
        var schema = scope.First(s => string.Equals(s.Alias, resolved.Alias, StringComparison.Ordinal)).Schema;
        if (schema.Fields.TryGetValue(resolved.Field, out var kind)) _exprKind[node] = kind;
    }

    private static FieldKind ReservedKind(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ? FieldKind.Long : FieldKind.String;

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') validation
    // ------------------------------------------------------------------

    private static readonly HashSet<string> ArithmeticOrComparisonOps = new(StringComparer.Ordinal)
    {
        "+", "-", "*", "/", "%", "=", "!=", "<>", "<", "<=", ">", ">=",
    };

    private FieldKind? GetExprKind(Expr e) => _exprKind.TryGetValue(e, out var k) ? k : null;

    /// <summary>Bare-JSON guard for comparisons/arithmetic: a Json-kind operand (a raw Json column, or the
    /// Json-kind result of a terminal '->') can't be compared/computed on directly — except `= NULL` /
    /// `!= NULL`, which is how NULL-ness of a JSON column is legitimately tested. Fires for both a plain
    /// Json column used bare and for a `->` chain that never got its final '->>'; the hint applies to both.</summary>
    private void CheckBareJsonOperand(Expr operand, string op, Expr otherOperand)
    {
        if (GetExprKind(operand) != FieldKind.Json) return;
        bool isNullEquality = op is "=" or "!=" or "<>" && otherOperand is NullLiteral;
        if (isNullEquality) return;

        _diags.Add(new SqlDiagnostic(
            $"JSON value used directly in '{op}' — extract a value first with '->>' (e.g. col ->> 'key' {op} ...); " +
            "a bare JSON column (or a '->' chain without a final '->>') can only be compared to NULL",
            operand.Line, operand.Column));
    }

    private void ResolveJsonAccess(JsonAccessExpr j, List<(string Alias, SourceSchema Schema)> scope, int aggDepth)
    {
        ResolveExpr(j.Left, scope, aggDepth);

        var leftKind = GetExprKind(j.Left);
        if (leftKind != FieldKind.Json)
        {
            string op = j.ReturnText ? "->>" : "->";
            _diags.Add(new SqlDiagnostic(
                $"'{op}' left operand must be a JSON column (or another '->' result), not {DescribeKind(leftKind)}",
                j.Line, j.Column));
        }

        // Result kind: '->>' always yields TEXT; a terminal '->' yields Json (so chains keep validating,
        // and — via CheckBareJsonOperand above — using that Json result directly in a comparison is
        // itself an error that nudges the caller toward '->>').
        _exprKind[j] = j.ReturnText ? FieldKind.String : FieldKind.Json;
    }

    private static string DescribeKind(FieldKind? kind) => kind switch
    {
        null => "a non-JSON value",
        FieldKind.Json => "JSON", // unreachable (Json left operands never fail the check) — kept for completeness
        _ => kind.Value.ToString(),
    };

    private static string CanonicalReserved(string name) =>
        string.Equals(name, EventRecord.TimestampField, StringComparison.OrdinalIgnoreCase) ? EventRecord.TimestampField : EventRecord.SourceField;

    // Plan 009 Round C wave C1: TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/TO_STRING — total, never-
    // throwing type-conversion functions (unconvertible/NULL input yields NULL); CAST(expr AS type) in
    // Sql/Parser.cs desugars to these same names, so there is exactly one implementation.
    private static readonly HashSet<string> KnownFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "ABS", "ROUND", "UPPER", "LOWER", "COALESCE",
        "TO_LONG", "TO_DOUBLE", "TO_BOOL", "TO_TIMESTAMP", "TO_STRING",
        // The searched-CASE desugar target (Sql/Parser.cs ParseCaseBody). Callable directly as
        // IF(cond, then, else) too — same node, same rules, nothing extra to implement either way.
        "IF",
    };

    private void ValidateFunctionArity(FunctionCallExpr f)
    {
        if (!KnownFunctions.Contains(f.Name))
        {
            if (SqlFunctions.FindScalar(f.Name) is { } ext)
            {
                if (!ext.IsValidArity(f.Args.Count))
                {
                    _diags.Add(new SqlDiagnostic($"Function '{f.Name}' called with wrong number of arguments", f.Line, f.Column));
                }
                return;
            }
            _diags.Add(new SqlDiagnostic($"Unknown function '{f.Name}'", f.Line, f.Column));
            return;
        }
        int n = f.Args.Count;
        bool ok = f.Name.ToUpperInvariant() switch
        {
            "ABS" or "UPPER" or "LOWER" => n == 1,
            "ROUND" => n is 1 or 2,
            "COALESCE" => n >= 1,
            "TO_LONG" or "TO_DOUBLE" or "TO_BOOL" or "TO_TIMESTAMP" or "TO_STRING" => n == 1,
            "IF" => n == 3,
            _ => true,
        };
        if (!ok)
        {
            _diags.Add(new SqlDiagnostic($"Function '{f.Name}' called with wrong number of arguments", f.Line, f.Column));
        }
    }

    // ------------------------------------------------------------------
    // Plan 004 N2 — IN / EXISTS subquery predicates (WHERE, top-level AND-conjunct only)
    // ------------------------------------------------------------------

    private bool IsAllowedSubqueryPredicatePosition(Expr e) =>
        _resolvingWhere && _whereTopLevelConjuncts is not null && _whereTopLevelConjuncts.Contains(e);

    private string SubqueryPredicatePositionMessage() =>
        _resolvingWhere
            ? "IN/EXISTS subquery predicates must be a top-level AND-connected condition in WHERE (not nested inside OR/NOT/another expression) — plan 004 N2"
            : "IN/EXISTS subquery predicates are only allowed in WHERE (not SELECT list, ON, or GROUP BY) — plan 004 N2";

    /// <summary>Plan 004 N2, pipelines only: "the subquery side must be a windowed derived query" — true
    /// when <paramref name="q"/> itself windows, OR (recursively) any of its FROM/JOIN sources is a
    /// derived table/CTE whose own inner query is transitively windowed. This is exactly what makes "the
    /// most recent inner window close" well-defined for the rolling-snapshot membership/value rule the
    /// runtime implements (see Runtime/Ops/PipelineSubqueryOp.cs) — an unwindowed subquery has no notion of
    /// "close" to snapshot from at all.</summary>
    private static bool IsTransitivelyWindowed(SelectQuery q, ValidationResult v) =>
        q.Window is not null || v.Sources.Any(s => s.Derived is not null && IsTransitivelyWindowed(s.Derived.Query, s.Derived.Validation));

    private void ResolveExists(ExistsExpr ex)
    {
        if (!IsAllowedSubqueryPredicatePosition(ex))
        {
            _diags.Add(new SqlDiagnostic(SubqueryPredicatePositionMessage(), ex.Line, ex.Column));
            return;
        }

        var inner = ValidateNestedUncorrelated(ex.Subquery);
        _diags.AddRange(inner.Diagnostics);
        FoldNestedInputs(inner);

        // EXISTS/NOT EXISTS is uncorrelated in this tier (plan 004 N2: "the subquery is uncorrelated for
        // IN/EXISTS in this tier"): the inner query above was validated against ONLY this query's own
        // namespace, with no outer alias in scope, so a reference to one already surfaced as the SAME
        // "Unknown source"/"Unknown column" diagnostic an uncorrelated N1 derived table would produce — no
        // separate correlation-detection pass needed here (unlike N4's scalar subqueries, which DO support
        // equality correlation and so need to tell "correlated" apart from "just wrong").
        if (_mode == ValidationMode.Stream && !IsTransitivelyWindowed(ex.Subquery, inner))
        {
            _diags.Add(new SqlDiagnostic(
                "EXISTS subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its membership is well-defined — plan 004 N2",
                ex.Line, ex.Column));
        }

        _exprKind[ex] = FieldKind.Bool;
        _subqueryPredicates[ex] = new SubqueryPredicateInfo { Query = ex.Subquery, Validation = inner, Negated = ex.Negated, KeyColumnName = null };
    }

    private void ResolveInSubquery(InSubqueryExpr ie, List<(string Alias, SourceSchema Schema)> scope, int aggDepth)
    {
        ResolveExpr(ie.Left, scope, aggDepth);

        if (!IsAllowedSubqueryPredicatePosition(ie))
        {
            _diags.Add(new SqlDiagnostic(SubqueryPredicatePositionMessage(), ie.Line, ie.Column));
            return;
        }

        var inner = ValidateNestedUncorrelated(ie.Subquery);
        _diags.AddRange(inner.Diagnostics);
        FoldNestedInputs(inner);

        bool singleColumn = !ie.Subquery.Select.IsStar && ie.Subquery.Select.Items.Count == 1 && ie.Subquery.Select.Items[0].Expression is not QualifiedStarExpr;
        if (!singleColumn)
        {
            _diags.Add(new SqlDiagnostic("IN subquery must select exactly one column", ie.Line, ie.Column));
        }

        if (_mode == ValidationMode.Stream && !IsTransitivelyWindowed(ie.Subquery, inner))
        {
            _diags.Add(new SqlDiagnostic(
                "IN subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its membership set is well-defined — plan 004 N2",
                ie.Line, ie.Column));
        }

        _exprKind[ie] = FieldKind.Bool;
        string? keyColumn = singleColumn ? (ie.Subquery.Select.Items[0].Alias ?? DerivedDefaultName(ie.Subquery.Select.Items[0].Expression, 0)) : null;
        _subqueryPredicates[ie] = new SubqueryPredicateInfo { Query = ie.Subquery, Validation = inner, Negated = ie.Negated, KeyColumnName = keyColumn };
    }

    // ------------------------------------------------------------------
    // Plan 004 N3/N4 — scalar subquery expressions
    // ------------------------------------------------------------------

    private static HashSet<string> CollectFromAliases(SelectQuery q)
    {
        var result = new HashSet<string>(StringComparer.Ordinal) { q.From.Source.Alias };
        foreach (var j in q.From.Joins) result.Add(j.Source.Alias);
        return result;
    }

    /// <summary>True when <paramref name="e"/> references an alias that resolves against
    /// <paramref name="outerAliases"/> and is NOT shadowed by <paramref name="innerAliases"/> (standard SQL
    /// scoping: an inner FROM/JOIN alias of the same name always shadows the outer one). Only qualified
    /// references (`alias.field`, `alias.*`) can be correlation refs in this dialect — a bare identifier is
    /// always resolved against the subquery's own scope only (see ResolveScalarSubquery's doc on this
    /// descope). Does not descend into a nested subquery expression (ScalarSubqueryExpr/InSubqueryExpr's
    /// own subquery/ExistsExpr) — those are independently scoped and validated on their own.</summary>
    private static bool ContainsOuterRef(Expr e, HashSet<string> innerAliases, HashSet<string> outerAliases) => e switch
    {
        QualifiedIdentifier qid => !innerAliases.Contains(qid.Qualifier) && outerAliases.Contains(qid.Qualifier),
        QualifiedStarExpr qs => !innerAliases.Contains(qs.Alias) && outerAliases.Contains(qs.Alias),
        UnaryExpr u => ContainsOuterRef(u.Operand, innerAliases, outerAliases),
        BinaryExpr b => ContainsOuterRef(b.Left, innerAliases, outerAliases) || ContainsOuterRef(b.Right, innerAliases, outerAliases),
        FunctionCallExpr f => f.Args.Any(a => ContainsOuterRef(a, innerAliases, outerAliases)),
        AggregateCallExpr agg => agg.Arg is not null && ContainsOuterRef(agg.Arg, innerAliases, outerAliases),
        JsonAccessExpr j => ContainsOuterRef(j.Left, innerAliases, outerAliases),
        InSubqueryExpr ins => ContainsOuterRef(ins.Left, innerAliases, outerAliases), // .Subquery is independently scoped
        _ => false,
    };

    private static bool QueryHasOuterRefOutsideWhere(SelectQuery q, HashSet<string> innerAliases, HashSet<string> outerAliases)
    {
        if (!q.Select.IsStar && q.Select.Items.Any(i => ContainsOuterRef(i.Expression, innerAliases, outerAliases))) return true;
        if (q.GroupBy is not null && q.GroupBy.Any(g => ContainsOuterRef(g, innerAliases, outerAliases))) return true;
        return false;
    }

    /// <summary>If <paramref name="eq"/> is exactly `innerExpr = outerAlias.outerField` or the reverse (one
    /// side a single qualified reference to an outer-scope alias, the other side an inner-only expression),
    /// resolves the outer field against <paramref name="outerScope"/> and returns the split — this IS
    /// plan 004 N4's "single-level equality correlation" shape. Returns false (not this shape) for anything
    /// looser — e.g. an outer ref buried inside a larger expression on either side — which the caller then
    /// reports as "beyond equality".</summary>
    private bool TrySplitCorrelationEquality(
        BinaryExpr eq, HashSet<string> innerAliases, HashSet<string> outerAliases, List<(string Alias, SourceSchema Schema)> outerScope,
        out Expr innerExpr, out string outerAlias, out string outerField)
    {
        innerExpr = null!; outerAlias = ""; outerField = "";

        bool TryOuterSide(Expr outerSide, Expr otherSide, out Expr resolvedInner, out string resolvedAlias, out string resolvedField)
        {
            resolvedInner = null!; resolvedAlias = ""; resolvedField = "";
            if (outerSide is not QualifiedIdentifier q || innerAliases.Contains(q.Qualifier) || !outerAliases.Contains(q.Qualifier)) return false;
            if (ContainsOuterRef(otherSide, innerAliases, outerAliases)) return false; // both sides reference outer — not a clean split

            var outerSourceSchema = outerScope.First(s => string.Equals(s.Alias, q.Qualifier, StringComparison.Ordinal)).Schema;
            var canonicalField = outerSourceSchema.Fields.Keys.FirstOrDefault(k => string.Equals(k, q.Name, StringComparison.OrdinalIgnoreCase));
            if (canonicalField is null)
            {
                _diags.Add(new SqlDiagnostic($"Unknown column '{q.Name}' on '{q.Qualifier}'", q.Line, q.Column));
                return false;
            }

            resolvedInner = otherSide; resolvedAlias = q.Qualifier; resolvedField = canonicalField;
            return true;
        }

        if (TryOuterSide(eq.Left, eq.Right, out innerExpr, out outerAlias, out outerField)) return true;
        if (TryOuterSide(eq.Right, eq.Left, out innerExpr, out outerAlias, out outerField)) return true;
        return false;
    }

    /// <summary>Resolves a scalar subquery expression (plan 004 N3 uncorrelated / N4 equality-correlated —
    /// the parser doesn't distinguish them; this method does, based on whether the inner WHERE references
    /// an outer-scope alias). N4's decorrelation happens right here: correlation conjuncts are stripped out
    /// of a residual copy of the inner query's WHERE, and (when any correlation was found) a GROUP BY over
    /// every correlation's inner expression is synthesized, with each one ALSO projected under a synthetic
    /// `__key{i}` name so Planner's decorrelated join can bind against it as an ordinary output column —
    /// exactly the "GROUP-BY-k aggregate joined on k" plan 004 N4 specifies. The residual query (N3: an
    /// unchanged copy; N4: WHERE-stripped + GROUP BY/`__key{i}`-augmented) is then validated exactly like an
    /// N1 derived table — see ValidateNestedUncorrelated.</summary>
    private void ResolveScalarSubquery(ScalarSubqueryExpr sse, List<(string Alias, SourceSchema Schema)> outerScope)
    {
        var innerAliases = CollectFromAliases(sse.Query);
        var outerAliases = outerScope.Select(s => s.Alias).ToHashSet(StringComparer.Ordinal);

        var pureInnerConjuncts = new List<Expr>();
        var correlations = new List<(Expr InnerExpr, string OuterAlias, string OuterField)>();
        bool invalidCorrelation = false;

        if (sse.Query.Where is not null)
        {
            foreach (var conjunct in FlattenAnd(sse.Query.Where))
            {
                if (!ContainsOuterRef(conjunct, innerAliases, outerAliases))
                {
                    pureInnerConjuncts.Add(conjunct);
                    continue;
                }
                if (conjunct is BinaryExpr { Op: "=" } eq &&
                    TrySplitCorrelationEquality(eq, innerAliases, outerAliases, outerScope, out var innerExpr, out var outerAlias, out var outerField))
                {
                    correlations.Add((innerExpr, outerAlias, outerField));
                    continue;
                }
                invalidCorrelation = true;
            }
        }

        if (!invalidCorrelation && QueryHasOuterRefOutsideWhere(sse.Query, innerAliases, outerAliases))
        {
            invalidCorrelation = true;
        }

        if (invalidCorrelation)
        {
            _diags.Add(new SqlDiagnostic("Correlated subqueries beyond equality are not supported — rewrite as a JOIN", sse.Line, sse.Column));
            return;
        }

        if (sse.Query.GroupBy is not null)
        {
            _diags.Add(new SqlDiagnostic(
                "Scalar subquery may not have its own GROUP BY — use equality correlation instead (plan 004 N4) or drop it for an uncorrelated single-row aggregate (N3)",
                sse.Line, sse.Column));
            return;
        }
        if (sse.Query.Select.IsStar || sse.Query.Select.Items.Count != 1 || sse.Query.Select.Items[0].Expression is QualifiedStarExpr)
        {
            _diags.Add(new SqlDiagnostic("Scalar subquery must select exactly one column", sse.Line, sse.Column));
            return;
        }
        var valueItem = sse.Query.Select.Items[0];
        if (!ContainsAggregate(valueItem.Expression))
        {
            _diags.Add(new SqlDiagnostic(
                "Scalar subquery must be a single-row aggregate query (an aggregate with no GROUP BY) — anything else is not provably single-row",
                sse.Line, sse.Column));
            return;
        }

        Expr? residualWhere = pureInnerConjuncts.Count == 0 ? null : pureInnerConjuncts.Aggregate((a, b) => new BinaryExpr("AND", a, b, a.Line, a.Column));

        var selectItems = new List<SelectItem>(sse.Query.Select.Items);
        var keyColumnNames = new List<string>();
        List<Expr>? groupBy = null;
        if (correlations.Count > 0)
        {
            groupBy = [];
            for (int i = 0; i < correlations.Count; i++)
            {
                var name = $"__key{i}";
                keyColumnNames.Add(name);
                groupBy.Add(correlations[i].InnerExpr);
                selectItems.Add(new SelectItem(correlations[i].InnerExpr, name));
            }
        }

        var residualQuery = new SelectQuery(
            new SelectClause(isStar: false, selectItems),
            sse.Query.From,
            residualWhere,
            groupBy,
            sse.Query.Window,
            sse.Query.Emit,
            sse.Query.EmitLine, sse.Query.EmitColumn, sse.Query.GroupByLine, sse.Query.GroupByColumn, sse.Query.WindowLine, sse.Query.WindowColumn);

        var inner = ValidateNestedUncorrelated(residualQuery);
        _diags.AddRange(inner.Diagnostics);
        FoldNestedInputs(inner);

        if (_mode == ValidationMode.Stream && !IsTransitivelyWindowed(residualQuery, inner))
        {
            _diags.Add(new SqlDiagnostic(
                "Scalar subquery in a pipeline must be windowed (directly, or via a windowed derived source/CTE) so its value is well-defined — plan 004 N3/N4",
                sse.Line, sse.Column));
        }

        var valueColumnName = valueItem.Alias ?? DerivedDefaultName(valueItem.Expression, 0);
        var kind = inner.ExprKinds.TryGetValue(valueItem.Expression, out var k) ? k : FieldKind.Double;
        _exprKind[sse] = kind;
        _scalarSubqueries[sse] = new ScalarSubqueryInfo
        {
            ResidualQuery = residualQuery,
            Validation = inner,
            Correlations = correlations.Select(c => new CorrelationKey { InnerExpr = c.InnerExpr, OuterAlias = c.OuterAlias, OuterField = c.OuterField }).ToList(),
            ValueColumnName = valueColumnName,
            KeyColumnNames = keyColumnNames,
        };
    }

    // ------------------------------------------------------------------
    // Equi-join key extraction
    // ------------------------------------------------------------------

    private static IEnumerable<Expr> FlattenAnd(Expr e)
    {
        if (e is BinaryExpr { Op: "AND" } b)
        {
            foreach (var x in FlattenAnd(b.Left)) yield return x;
            foreach (var x in FlattenAnd(b.Right)) yield return x;
        }
        else
        {
            yield return e;
        }
    }

    private HashSet<string> CollectAliases(Expr e)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        void Walk(Expr node)
        {
            switch (node)
            {
                case Identifier or QualifiedIdentifier:
                    if (_bindings.TryGetValue(node, out var b)) result.Add(b.Alias);
                    break;
                case UnaryExpr u: Walk(u.Operand); break;
                case BinaryExpr bin: Walk(bin.Left); Walk(bin.Right); break;
                case FunctionCallExpr f: foreach (var a in f.Args) Walk(a); break;
                case AggregateCallExpr agg: if (agg.Arg is not null) Walk(agg.Arg); break;
                case JsonAccessExpr j: Walk(j.Left); break; // Key is always a literal — no aliases there
            }
        }
        Walk(e);
        return result;
    }

    /// <summary>Plan 008: collects EVERY equi-conjunct spanning the two sides into an ordered pair of key
    /// lists (composite keys) — not just the first one (pre-008: only the first `=` conjunct became the
    /// key; every other conjunct, equi-shaped or not, folded into the residual — see JoinKeyFolding for
    /// how single-key consumers get that exact view reconstructed). Only a conjunct that is NOT an
    /// equi-comparison between exactly these two sides remains in <c>Residual</c>.</summary>
    private (List<Expr> LeftKeys, List<Expr> RightKeys, Expr? Residual)? ExtractEquiKey(Expr onExpr, HashSet<string> leftAliases, string rightAlias)
    {
        var conjuncts = FlattenAnd(onExpr).ToList();
        var leftKeys = new List<Expr>();
        var rightKeys = new List<Expr>();
        var residuals = new List<Expr>();

        foreach (var c in conjuncts)
        {
            if (c is BinaryExpr { Op: "=" } be)
            {
                var la = CollectAliases(be.Left);
                var ra = CollectAliases(be.Right);
                if (la.Count > 0 && la.IsSubsetOf(leftAliases) && ra.Count == 1 && ra.Contains(rightAlias))
                {
                    leftKeys.Add(be.Left);
                    rightKeys.Add(be.Right);
                    continue;
                }
                if (ra.Count > 0 && ra.IsSubsetOf(leftAliases) && la.Count == 1 && la.Contains(rightAlias))
                {
                    leftKeys.Add(be.Right);
                    rightKeys.Add(be.Left);
                    continue;
                }
            }
            residuals.Add(c);
        }

        if (leftKeys.Count == 0) return null;
        Expr? residual = residuals.Count == 0 ? null : residuals.Aggregate((a, b) => new BinaryExpr("AND", a, b, a.Line, a.Column));
        return (leftKeys, rightKeys, residual);
    }

    // ------------------------------------------------------------------
    // Structural helpers
    // ------------------------------------------------------------------

    internal static bool ContainsAggregate(Expr e) => e switch
    {
        AggregateCallExpr => true,
        // Plan 004 N3/N4: a scalar subquery's value doesn't vary across an outer GROUP BY group any
        // differently than an aggregate's would (N3: truly constant; N4: constant per the correlation
        // key) — exempt it from "non-aggregate select item must appear in GROUP BY" the same way an
        // aggregate is exempt, rather than forcing it to structurally match a GROUP BY expression.
        ScalarSubqueryExpr => true,
        UnaryExpr u => ContainsAggregate(u.Operand),
        BinaryExpr b => ContainsAggregate(b.Left) || ContainsAggregate(b.Right),
        FunctionCallExpr f => f.Args.Any(ContainsAggregate),
        JsonAccessExpr j => ContainsAggregate(j.Left),
        _ => false,
    };

    /// <summary>Plan 004 N3/N4: finds the first ScalarSubqueryExpr in <paramref name="e"/> that is NOT
    /// nested inside an aggregate call's own argument (see the caller's doc comment for why that
    /// distinction matters) — returns its node (for position), or null if none. Doesn't descend into
    /// InSubqueryExpr/ExistsExpr/ScalarSubqueryExpr's own subqueries — those are independently-scoped
    /// nested queries with their own validation, not part of this expression tree's evaluation.</summary>
    private static Expr? FindScalarSubqueryOutsideAggregate(Expr e, bool insideAggregate) => e switch
    {
        ScalarSubqueryExpr when !insideAggregate => e,
        AggregateCallExpr agg => agg.Arg is not null ? FindScalarSubqueryOutsideAggregate(agg.Arg, insideAggregate: true) : null,
        UnaryExpr u => FindScalarSubqueryOutsideAggregate(u.Operand, insideAggregate),
        BinaryExpr b => FindScalarSubqueryOutsideAggregate(b.Left, insideAggregate) ?? FindScalarSubqueryOutsideAggregate(b.Right, insideAggregate),
        FunctionCallExpr f => f.Args.Select(a => FindScalarSubqueryOutsideAggregate(a, insideAggregate)).FirstOrDefault(r => r is not null),
        JsonAccessExpr j => FindScalarSubqueryOutsideAggregate(j.Left, insideAggregate),
        _ => null,
    };

    private bool StructurallyEqual(Expr a, Expr b) => StructurallyEqual(a, b, _bindings);

    internal static bool StructurallyEqual(Expr a, Expr b, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings)
    {
        if (a is Identifier or QualifiedIdentifier)
        {
            if (b is not (Identifier or QualifiedIdentifier)) return false;
            return bindings.TryGetValue(a, out var ba) && bindings.TryGetValue(b, out var bb) && ba == bb;
        }
        return (a, b) switch
        {
            (NumberLiteral na, NumberLiteral nb) => na.IsDouble == nb.IsDouble && Equals(na.Value, nb.Value),
            (StringLiteral sa, StringLiteral sb) => sa.Value == sb.Value,
            (BoolLiteral ba, BoolLiteral bb) => ba.Value == bb.Value,
            (NullLiteral, NullLiteral) => true,
            (StarExpr, StarExpr) => true,
            (UnaryExpr ua, UnaryExpr ub) => ua.Op == ub.Op && StructurallyEqual(ua.Operand, ub.Operand, bindings),
            (BinaryExpr xa, BinaryExpr xb) => xa.Op == xb.Op && StructurallyEqual(xa.Left, xb.Left, bindings) && StructurallyEqual(xa.Right, xb.Right, bindings),
            (FunctionCallExpr fa, FunctionCallExpr fb) => string.Equals(fa.Name, fb.Name, StringComparison.OrdinalIgnoreCase) &&
                fa.Args.Count == fb.Args.Count && fa.Args.Zip(fb.Args).All(p => StructurallyEqual(p.First, p.Second, bindings)),
            (AggregateCallExpr aa, AggregateCallExpr ab) => string.Equals(aa.Name, ab.Name, StringComparison.OrdinalIgnoreCase) &&
                aa.IsStar == ab.IsStar && (aa.Arg is null && ab.Arg is null || aa.Arg is not null && ab.Arg is not null && StructurallyEqual(aa.Arg, ab.Arg, bindings)),
            (JsonAccessExpr ja, JsonAccessExpr jb) => ja.ReturnText == jb.ReturnText &&
                StructurallyEqual(ja.Left, jb.Left, bindings) && StructurallyEqual(ja.Key, jb.Key, bindings),
            _ => false,
        };
    }
}

/// <summary>Plan 008: bridges JoinBinding's new composite key LISTS back to the single-key shape every op
/// this wave does NOT touch (TableJoinOp, TableSemiAntiOp, PipelineJoinOp, PipelineSubqueryOp) still
/// expects — used by Planning/TablePlanner.cs and Planning/Planner.cs when building CompiledTableJoin/
/// CompiledJoin's pre-008 <c>LeftKey</c>/<c>RightKey</c>/<c>Residual</c> fields (kept, unchanged in name
/// and meaning, since existing tests — TableOpsUnitTests, PipelineOpsUnitTests — construct those ops
/// directly off exactly those fields). Only table-mode LEFT/RIGHT/FULL (TableOuterJoinOp) consumes the
/// full <c>LeftKeys</c>/<c>RightKeys</c> lists directly instead and skips this fold entirely — see
/// TablePlanner's join builder.</summary>
internal static class JoinKeyFolding
{
    /// <summary>Reconstructs the pre-008 "first equi-key + residual" shape: every key component AFTER the
    /// first becomes an extra `leftKeys[i] = rightKeys[i]` conjunct ANDed onto <paramref name="residual"/>,
    /// in list order. A no-op whenever there's nothing to fold — <paramref name="leftKeys"/> is null (no
    /// equi-key at all: CROSS/UNNEST) or has exactly one element (the overwhelming common case, and every
    /// single-key join this wave's regression tests cover) — which is exactly what keeps every existing
    /// single-key join's compiled Residual byte-for-byte identical to before this refactor.</summary>
    public static Expr? FoldExtraKeysIntoResidual(IReadOnlyList<Expr>? leftKeys, IReadOnlyList<Expr>? rightKeys, Expr? residual)
    {
        if (leftKeys is null) return residual;
        for (int i = 1; i < leftKeys.Count; i++)
        {
            var eq = new BinaryExpr("=", leftKeys[i], rightKeys![i], leftKeys[i].Line, leftKeys[i].Column);
            residual = residual is null ? eq : new BinaryExpr("AND", residual, eq, residual.Line, residual.Column);
        }
        return residual;
    }
}
