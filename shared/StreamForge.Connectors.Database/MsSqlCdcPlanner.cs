using System.Globalization;
using System.Text.RegularExpressions;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>Which of the four starting points <see cref="MsSqlCdcPlanner.PlanFrom"/> chose, in the order
/// the plan's own doc comment lists them.</summary>
public enum MsSqlCdcFromKind
{
    /// <summary>A persisted cursor exists — <c>from</c> is one past it, via <c>sys.fn_cdc_increment_lsn</c>.</summary>
    Cursor,
    /// <summary>No cursor, but <see cref="DbSourceConfig.InitialCursor"/> names a starting LSN directly.</summary>
    InitialCursor,
    /// <summary>No cursor, no initial, <see cref="DbSourceConfig.Snapshot"/> — start at the retention
    /// floor, <c>sys.fn_cdc_get_min_lsn</c>.</summary>
    Snapshot,
    /// <summary>No cursor, no initial, no snapshot — tail-only: <c>sys.fn_cdc_get_max_lsn</c>, a seed
    /// cycle that persists the cursor and emits nothing.</summary>
    Tail,
}

/// <summary>What <see cref="MsSqlCdcPlanner.PlanFrom"/> needs the caller to do to resolve <c>from</c>.
/// <see cref="Sql"/> is null exactly when <see cref="ResolvedFrom"/> is already known without a round
/// trip (<see cref="MsSqlCdcFromKind.InitialCursor"/>) — the caller runs <see cref="Sql"/> otherwise and
/// encodes the returned <c>binary(10)</c> scalar with <see cref="CdcLsn.EncodeMsSql"/> to get the same
/// shape <see cref="ResolvedFrom"/> would have carried.</summary>
public sealed record MsSqlCdcFromStep(MsSqlCdcFromKind Kind, string? Sql, IReadOnlyList<KeyValuePair<string, object?>> Parameters, string? ResolvedFrom);

/// <summary>The main change-read: SQL text plus its bound parameters, built once <c>from</c> and
/// <c>to</c> are both known encoded LSNs.</summary>
public sealed record MsSqlCdcReadPlan(string Sql, IReadOnlyList<KeyValuePair<string, object?>> Parameters);

/// <summary>What <see cref="MsSqlCdcPlanner.Complete"/> produced: either a batch ready to hand back from
/// <c>PollAsync</c> (<see cref="Batch"/> set, <see cref="RereadBoundLsn"/> null), or a signal that the
/// caller must re-read once more, bounded exactly at <see cref="RereadBoundLsn"/>, before this cycle can
/// produce anything (<see cref="Batch"/> null). Exactly one of the two is set — see
/// <see cref="MsSqlCdcPlanner.Complete"/>'s doc for why this split exists.</summary>
public sealed record MsSqlCdcCompleteResult(PolledBatch? Batch, string? RereadBoundLsn)
{
    /// <summary>True when the caller must re-read via <see cref="MsSqlCdcPlanner.PlanBoundedRead"/>,
    /// bounded at <see cref="RereadBoundLsn"/>, before a batch exists for this cycle.</summary>
    public bool NeedsReread => RereadBoundLsn is not null;
}

/// <summary>
/// The pure half of the SQL Server CDC source: given the config, the persisted cursor, and — once the
/// driver has run the small scalar queries this class asks for — the resolved LSN boundaries, what SQL
/// runs next and what the resulting rows mean. Split from <see cref="MsSqlCdcSource"/> for the same reason
/// <see cref="DbPollPlanner"/> is split from <see cref="DbSource"/>: this is the part of the connector that
/// can lose data silently, so it is the part covered by tests that need no SQL Server at all.
///
/// <para><b>Why this can't be one pure function the way <see cref="DbPollPlanner.Plan"/> is.</b> A polled
/// source's cursor is a value from the table itself — the planner can hand back one SQL statement and be
/// done. CDC's cursor is a log position, and SQL Server exposes the operations that move it
/// (<c>fn_cdc_increment_lsn</c>, <c>fn_cdc_get_min_lsn</c>, <c>fn_cdc_get_max_lsn</c>) only as scalar
/// functions that themselves need a round trip — there is no way to compute "one past this LSN" or "the
/// current tail" without asking the server. So this class hands back one step at a time
/// (<see cref="PlanFrom"/>, then — once <c>from</c> is known — <see cref="MsSqlCdcSource"/> asks for
/// <see cref="MinLsnSql"/>/<see cref="MaxLsnSql"/> directly), and everything that genuinely needs no I/O
/// (retention comparison, empty-range detection, the read SQL, and turning raw rows into a
/// <see cref="PolledBatch"/>) is a standalone pure method below.</para>
///
/// <para><b>The four states of "where do I start", mirroring <see cref="DbPollPlanner"/>'s doc structure:</b></para>
/// <list type="number">
/// <item>A persisted cursor exists → <c>from = fn_cdc_increment_lsn(cursor)</c>. Never re-reads the last
/// transaction, unlike re-reading a table's cursor column with <c>&gt;=</c>.</item>
/// <item>No cursor, <see cref="DbSourceConfig.InitialCursor"/> set → decode it directly as an LSN. No round
/// trip needed — the operator supplied the exact starting point.</item>
/// <item>No cursor, no initial, <see cref="DbSourceConfig.Snapshot"/> → <c>from = fn_cdc_get_min_lsn</c>,
/// the oldest LSN CDC retention still has. This is NOT a full-table snapshot — see
/// <see cref="MsSqlCdcSource.Describe"/>'s Help text for the honest distinction.</item>
/// <item>No cursor, no initial, no snapshot → <c>from = fn_cdc_get_max_lsn</c> (tail-only). A SEED cycle:
/// the caller persists this as the cursor and emits nothing, exactly like <see cref="DbPollPlanner"/>'s own
/// branch 4 seeds from <c>MAX(cursorColumn)</c> without reading a row.</item>
/// </list>
///
/// <para><b>Retention breach is loud, on purpose.</b> <see cref="CheckRetention"/> throws rather than
/// clamping <c>from</c> up to <c>min</c> — clamping is indistinguishable from "everything is fine" to an
/// operator watching the source's status, and it is exactly the kind of silent skip this whole connector
/// exists to avoid. It is skipped for the <see cref="MsSqlCdcFromKind.Snapshot"/> branch only, because
/// there <c>from</c> IS <c>min</c> by construction — the comparison can only ever come back equal.</para>
/// </summary>
public static class MsSqlCdcPlanner
{
    /// <summary>Bound parameter name for the resolved starting LSN.</summary>
    public const string FromParameterName = "from";

    /// <summary>Bound parameter name for the resolved ending LSN.</summary>
    public const string ToParameterName = "to";

    /// <summary>Bound parameter name for the capture instance, where it is a genuine scalar-function
    /// parameter (<see cref="MinLsnSql"/>) rather than part of an identifier.</summary>
    public const string CaptureParameterName = "capture";

    /// <summary>Bound parameter name for the row cap on the main read.</summary>
    public const string BatchParameterName = "batch";

    /// <summary>The oldest LSN CDC retention still has for one capture instance. <c>@capture</c> is a
    /// genuine <c>nvarchar</c> parameter of this scalar function — unlike <c>fn_cdc_get_all_changes_*</c>,
    /// the capture instance is never part of this function's name, so it needs no identifier validation.</summary>
    public const string MinLsnSql = "SELECT sys.fn_cdc_get_min_lsn(@capture)";

    /// <summary>The current tail of the capture tables — no parameters.</summary>
    public const string MaxLsnSql = "SELECT sys.fn_cdc_get_max_lsn()";

    /// <summary>One LSN past <c>@from</c> — NOT a re-read of the transaction at <c>@from</c> itself.</summary>
    public const string IncrementLsnSql = "SELECT sys.fn_cdc_increment_lsn(@from)";

    /// <summary>The <c>__$*</c> column carrying the transaction's commit LSN — the group key for the
    /// transaction-boundary cut.</summary>
    private const string StartLsnColumn = "__$start_lsn";

    /// <summary>The <c>__$*</c> column carrying the numeric change-type code.</summary>
    private const string OperationColumn = "__$operation";

    /// <summary>The helper column this connector's own SELECT adds via <c>fn_cdc_map_lsn_to_time</c> — not
    /// a <c>__$*</c> column, so it needs its own name in the strip list.</summary>
    private const string TsAliasColumn = "__ts";

    /// <summary>A SQL Server CDC capture instance name is interpolated directly into
    /// <c>cdc.fn_cdc_get_all_changes_&lt;capture&gt;</c> — there is no bound-parameter form for part of a
    /// function name — so this is the injection guard: a plain identifier, nothing else.</summary>
    private static readonly Regex CaptureInstancePattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    /// <summary>True when <paramref name="name"/> is safe to interpolate into
    /// <c>cdc.fn_cdc_get_all_changes_&lt;name&gt;</c>.</summary>
    public static bool IsValidCaptureInstance(string? name) => !string.IsNullOrEmpty(name) && CaptureInstancePattern.IsMatch(name);

    /// <summary>Step 1: decide the starting point and what, if anything, the caller must run to resolve
    /// it. See this class's doc for the four branches.</summary>
    public static MsSqlCdcFromStep PlanFrom(DbSourceConfig config, string? cursor)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (!string.IsNullOrWhiteSpace(cursor))
        {
            var bytes = CdcLsn.DecodeMsSql(cursor.Trim());
            return new MsSqlCdcFromStep(MsSqlCdcFromKind.Cursor, IncrementLsnSql, [new(FromParameterName, bytes)], null);
        }

        if (!string.IsNullOrWhiteSpace(config.InitialCursor))
        {
            var text = config.InitialCursor.Trim();
            // Validates the shape (throws FormatException naming the offending text on bad input) without
            // needing a round trip — the operator supplied the exact starting point.
            CdcLsn.DecodeMsSql(text);
            return new MsSqlCdcFromStep(MsSqlCdcFromKind.InitialCursor, null, [], text);
        }

        if (config.Snapshot)
        {
            return new MsSqlCdcFromStep(MsSqlCdcFromKind.Snapshot, MinLsnSql, [new(CaptureParameterName, config.CaptureInstance.Trim())], null);
        }

        return new MsSqlCdcFromStep(MsSqlCdcFromKind.Tail, MaxLsnSql, [], null);
    }

    /// <summary>Step 2: throws when <paramref name="from"/> is older than the oldest LSN CDC retention
    /// still has — the range has already been discarded and there is no way to recover it. Never clamps;
    /// see this class's doc for why.</summary>
    public static void CheckRetention(string captureInstance, string from, string min)
    {
        if (CdcLsn.CompareMsSql(from, min) < 0)
        {
            throw new InvalidOperationException(
                $"CDC capture instance '{captureInstance}': requested LSN {from} is older than the minimum " +
                $"retained LSN {min} — CDC retention (default 3 days) has already discarded this range");
        }
    }

    /// <summary>Step 3: true when the window is empty (<paramref name="to"/> older than
    /// <paramref name="from"/>) — happens when nothing has committed since the last cycle. The caller
    /// returns an empty batch with a null cursor (leave it unchanged), never a reset.</summary>
    public static bool IsEmptyRange(string from, string to) => CdcLsn.CompareMsSql(to, from) < 0;

    /// <summary>Step 4: the main change-read. <c>'all'</c>, not <c>'all update old'</c> — so
    /// <c>__$operation</c> is only ever 1 (delete), 2 (insert) or 4 (update-after), and there is no
    /// before-image row to reconcile against the after-image one. Capped with <c>TOP (@batch)</c> — which
    /// is exactly why a lone transaction bigger than the cap needs <see cref="PlanBoundedRead"/> to
    /// resolve, since this statement alone can never prove it read that transaction in full.</summary>
    public static MsSqlCdcReadPlan PlanRead(DbSourceConfig config, string from, string to)
    {
        ArgumentNullException.ThrowIfNull(config);

        var capture = ValidatedCaptureInstance(config);
        var batch = EffectiveBatchSize(config);
        var sql =
            $"SELECT TOP (@{BatchParameterName}) *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __ts " +
            $"FROM cdc.fn_cdc_get_all_changes_{capture}(@{FromParameterName}, @{ToParameterName}, 'all') " +
            "ORDER BY __$start_lsn, __$seqval";

        return new MsSqlCdcReadPlan(sql,
        [
            new(BatchParameterName, batch),
            new(FromParameterName, CdcLsn.DecodeMsSql(from)),
            new(ToParameterName, CdcLsn.DecodeMsSql(to)),
        ]);
    }

    /// <summary>The re-read <see cref="MsSqlCdcSource"/> issues when <see cref="Complete"/> reports
    /// <see cref="MsSqlCdcCompleteResult.NeedsReread"/>: the SAME <paramref name="from"/> as the original
    /// capped attempt, but <c>to</c> pinned exactly at <paramref name="boundaryStartLsn"/> — the single
    /// transaction's own <c>__$start_lsn</c> — and NO <c>TOP</c> at all. Bounding <c>to</c> there means the
    /// result can only ever be that one transaction, however many rows it has, so this read is safe to run
    /// uncapped: it is bounded by construction (a single LSN), not by a row limit. Deliberately over
    /// budget — see <see cref="Complete"/>'s doc for why that is the accepted cost, in those words.</summary>
    public static MsSqlCdcReadPlan PlanBoundedRead(DbSourceConfig config, string from, string boundaryStartLsn)
    {
        ArgumentNullException.ThrowIfNull(config);

        var capture = ValidatedCaptureInstance(config);
        var sql =
            "SELECT *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __ts " +
            $"FROM cdc.fn_cdc_get_all_changes_{capture}(@{FromParameterName}, @{ToParameterName}, 'all') " +
            "ORDER BY __$start_lsn, __$seqval";

        return new MsSqlCdcReadPlan(sql,
        [
            new(FromParameterName, CdcLsn.DecodeMsSql(from)),
            new(ToParameterName, CdcLsn.DecodeMsSql(boundaryStartLsn)),
        ]);
    }

    private static string ValidatedCaptureInstance(DbSourceConfig config)
    {
        var capture = config.CaptureInstance.Trim();
        if (!IsValidCaptureInstance(capture))
        {
            // Defense in depth: Validate() rejects this shape too, but a plan built by hand (as every
            // test here does) must not be able to smuggle SQL into a function name by skipping Validate.
            throw new InvalidOperationException($"'{capture}' is not a valid CDC capture instance name");
        }

        return capture;
    }

    private static int EffectiveBatchSize(DbSourceConfig config) => config.BatchSize > 0 ? config.BatchSize : 1000;

    /// <summary>
    /// Steps 5–7: turns one cycle's raw rows — straight off the reader, still carrying every
    /// <c>__$*</c> helper column and the <c>__ts</c> alias this connector's own SELECT added — into either
    /// the batch the driver persists against, or a signal that one more bounded read is needed first. See
    /// <see cref="MsSqlCdcCompleteResult"/>. Mutates each emitted row dictionary in place (same contract
    /// <see cref="CdcStamp.Apply"/> itself follows).
    ///
    /// <para><b>The cut, precisely.</b> Rows arrive ordered by <c>__$start_lsn, __$seqval</c> — the read's
    /// own ORDER BY — so consecutive rows sharing one <c>__$start_lsn</c> are exactly one transaction. Only
    /// the LAST group can possibly be incomplete: <c>TOP (@batch)</c> can only have cut the read off at the
    /// very end, never in the middle, because every earlier group is proven complete by a DIFFERENT
    /// <c>__$start_lsn</c> appearing after it. So the rule is: drop the last group, keep the rest — UNLESS
    /// the last group is also the ONLY group, in which case dropping it would emit nothing and stall the
    /// source on the same truncated read forever.</para>
    ///
    /// <para><b><paramref name="capped"/> is what decides the only-group case, and it is the CALLER's
    /// signal, not an inference made here.</b> <c>rawRows.Count</c> alone cannot distinguish "this
    /// transaction had exactly this many rows" from "<c>TOP</c> cut it off here" — both look identical from
    /// inside this method — so the caller states which read produced <paramref name="rawRows"/>:</para>
    /// <list type="bullet">
    /// <item><b><paramref name="capped"/> is true and there is exactly one group:</b> completeness cannot
    /// be established. This method emits NOTHING and does NOT advance the cursor — advancing past a group
    /// whose completeness is unproven is exactly the silent skip this connector exists to prevent, the one
    /// outcome <c>PolledSourceCore</c>'s "a failed cycle keeps the old cursor" rule is there to stop. Instead
    /// it returns a <see cref="MsSqlCdcCompleteResult"/> with <see cref="MsSqlCdcCompleteResult.RereadBoundLsn"/>
    /// set to that group's own <c>__$start_lsn</c>. The caller re-reads via <see cref="PlanBoundedRead"/> —
    /// same <c>from</c>, <c>to</c> pinned at that exact LSN, no <c>TOP</c> — which can only ever return that
    /// one transaction in full, then calls this method again with <c>capped: false</c>.</item>
    /// <item><b>Otherwise</b> (uncapped, or two or more groups): the batch is built normally. A transaction
    /// larger than <see cref="DbSourceConfig.BatchSize"/> is therefore always delivered WHOLE, in one
    /// over-budget batch, once the bounded re-read resolves it — <see cref="DbSourceConfig.BatchSize"/> is a
    /// TARGET for a cycle's read, not a hard ceiling on what one batch can emit. Nothing is ever silently
    /// dropped; the cost is one extra round trip on the cycle that hits an oversized transaction.</item>
    /// </list>
    ///
    /// <para><b><see cref="DbSourceConfig.Tables"/> is applied per row, not per group</b> — the cursor
    /// still advances past a group even when every row in it is filtered out, because the cursor tracks how
    /// far the CDC log has been read, not how much was emitted.</para>
    /// </summary>
    public static MsSqlCdcCompleteResult Complete(DbSourceConfig config, IReadOnlyList<Dictionary<string, object?>> rawRows, bool capped)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(rawRows);

        if (rawRows.Count == 0)
        {
            return new MsSqlCdcCompleteResult(new PolledBatch([], null, HasMore: false), null);
        }

        List<(string StartLsn, List<Dictionary<string, object?>> Rows)> groups = [];
        foreach (var row in rawRows)
        {
            var raw = row.TryGetValue(StartLsnColumn, out var value) ? value : null;
            if (raw is not byte[] lsnBytes)
            {
                throw new InvalidOperationException($"row has no usable '{StartLsnColumn}' column");
            }

            var startLsn = CdcLsn.EncodeMsSql(lsnBytes);
            if (groups.Count > 0 && string.Equals(groups[^1].StartLsn, startLsn, StringComparison.Ordinal))
            {
                groups[^1].Rows.Add(row);
            }
            else
            {
                groups.Add((startLsn, [row]));
            }
        }

        if (capped && groups.Count == 1)
        {
            // Completeness of the only group cannot be established — see this method's doc. Emit nothing,
            // advance nothing; the caller re-reads bounded at this exact LSN.
            return new MsSqlCdcCompleteResult(null, groups[0].StartLsn);
        }

        // Drop the trailing group unless it is the only one (handled above when that one group is also
        // unproven). An uncapped single group reaches here and is emitted whole — it is either the true
        // end of a normal read, or the bounded re-read's own result, which is complete by construction.
        var emitted = groups.Count > 1 ? groups.GetRange(0, groups.Count - 1) : groups;

        var qualifiedTable = QualifiedTable(config);
        var tableFilter = ParseTables(config.Tables);

        List<Dictionary<string, object?>> outRows = [];
        foreach (var (_, groupRows) in emitted)
        {
            foreach (var row in groupRows)
            {
                if (tableFilter is not null && (qualifiedTable is null || !tableFilter.Contains(qualifiedTable)))
                {
                    continue;
                }

                var op = OpLetter(Convert.ToInt32(row[OperationColumn], CultureInfo.InvariantCulture));
                var tsMs = row.TryGetValue(TsAliasColumn, out var tsValue) && tsValue is DateTime ts
                    ? new DateTimeOffset(DateTime.SpecifyKind(ts, DateTimeKind.Utc)).ToUnixTimeMilliseconds()
                    : (long?)null;

                foreach (var key in row.Keys.Where(IsHelperColumn).ToList())
                {
                    row.Remove(key);
                }

                CdcStamp.Apply(row, op, qualifiedTable, tsMs);
                outRows.Add(row);
            }
        }

        var cursor = emitted[^1].StartLsn;
        var hasMore = capped || groups.Count > 1;

        return new MsSqlCdcCompleteResult(new PolledBatch(outRows, cursor, hasMore), null);
    }

    /// <summary><c>__$operation</c> → <see cref="CdcStamp"/>'s op letters. Only 1/2/4 can appear because
    /// the read uses <c>'all'</c>, never <c>'all update old'</c> (which would also emit 3, the
    /// before-image row this connector never asks for).</summary>
    private static string OpLetter(int operation) => operation switch
    {
        1 => CdcStamp.OpDelete,
        2 => CdcStamp.OpCreate,
        4 => CdcStamp.OpUpdate,
        _ => throw new InvalidOperationException(
            $"unexpected CDC __$operation value {operation} — expected 1 (delete), 2 (insert) or 4 (update); " +
            "3 (update before-image) should never appear because this reader uses the 'all' row filter, not 'all update old'"),
    };

    private static bool IsHelperColumn(string name) =>
        name.StartsWith("__$", StringComparison.Ordinal) || string.Equals(name, TsAliasColumn, StringComparison.Ordinal);

    /// <summary><c>"&lt;schema&gt;.&lt;table&gt;"</c> for <see cref="CdcStamp.TableColumn"/> and the
    /// <see cref="DbSourceConfig.Tables"/> filter — built from the same general <see cref="DbSourceConfig.Schema"/>
    /// / <see cref="DbSourceConfig.Table"/> fields the polled kind uses, since a capture instance name
    /// carries no reliable schema/table split of its own (the "&lt;schema&gt;_&lt;table&gt;" convention is
    /// exactly that, a convention, not a rule an operator is required to follow). Null when
    /// <see cref="DbSourceConfig.Table"/> is empty — <see cref="CdcStamp.Apply"/> already knows to skip
    /// stamping <c>_table</c> rather than fabricate one.</summary>
    private static string? QualifiedTable(DbSourceConfig config)
    {
        var table = config.Table.Trim();
        if (table.Length == 0)
        {
            return null;
        }

        var schema = string.IsNullOrWhiteSpace(config.Schema) ? "dbo" : config.Schema.Trim();
        return $"{schema}.{table}";
    }

    private static HashSet<string>? ParseTables(string tables)
    {
        if (string.IsNullOrWhiteSpace(tables))
        {
            return null;
        }

        return tables.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }
}
