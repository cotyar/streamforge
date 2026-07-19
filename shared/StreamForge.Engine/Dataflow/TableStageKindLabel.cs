namespace StreamForge.Engine.Dataflow;

/// <summary>
/// Plan 003 M4 — additive seam alongside <see cref="TableDataflowPlan"/> (see that file's header comment:
/// "ADDITIVE public seam ... nothing here changes an existing signature"). A pure, exhaustive
/// <see cref="TableStageKind"/> -&gt; human-facing operator name mapping, so the Host can label
/// TablePartitionMetrics.Kind for the M5 dataflow observability panel without either project needing to
/// duplicate the enum's string values by hand (DataflowPanel.tsx's own doc comment flagged this exact gap:
/// "TablePartitionMetrics does not carry the stage's operator kind ... candid gap for a future M3/M4
/// metrics pass").
///
/// The switch is deliberately exhaustive (no `_ => kind.ToString()` fallback) — a future
/// <see cref="TableStageKind"/> addition that forgets to extend this mapping fails LOUDLY here (a thrown
/// exception surfaced through GetMetricsAsync) rather than silently shipping a blank/wrong column to the
/// UI. <see cref="TableStageKindLabelTests"/> (Engine.Tests) asserts every current enum value is covered.
/// </summary>
public static class TableStageKindLabel
{
    public static string Of(TableStageKind kind) => kind switch
    {
        TableStageKind.Ingest => "Ingest",
        TableStageKind.Join => "Join",
        TableStageKind.SemiAnti => "SemiAnti",
        TableStageKind.Unnest => "Unnest",
        TableStageKind.FilterProject => "FilterProject",
        TableStageKind.Reduce => "Reduce",
        TableStageKind.LatestBy => "LatestBy",
        _ => throw new System.ArgumentOutOfRangeException(nameof(kind), kind, "Unmapped TableStageKind — add a label above (see this type's doc comment)."),
    };
}
