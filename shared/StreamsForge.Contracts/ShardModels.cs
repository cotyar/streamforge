namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 011 wave D1 — SHARDED TABLES: the transport shapes.
//
// WHAT THE TIER IS. A table with a non-empty TableDefinition.ShardBy gains a second materialization of
// its own output, fed by the SAME (StreamConstants.TableDeltaNamespace, tableName) delta stream the
// row-history tier and every downstream table already consume. Nothing about how the table is COMPUTED
// changes — the SQL path, the planner and the partitioned dataflow are untouched, and a shard consumer
// works identically at Parallelism == 1 and Parallelism >= 2 because TableOutputGrain republishes onto
// that same stream.
//
// WHAT IT BUYS. One grain per shard key, holding only that key's rows and that key's version history,
// and — the actual point — NOT pinned alive: an idle key deactivates and its state waits on disk until
// something asks for it again. The financial-instrument case this was built for ("give me everything for
// this instrument, with its full history") becomes one small grain load instead of a slice of one
// enormous resident structure.
//
// WHAT IT IS NOT. It is not row retention (plan 011 C2). Retention DELETES rows to bound a table;
// sharding KEEPS everything and bounds what is RESIDENT. They compose — see TableShardConfig's own note.
// ============================================================================

/// <summary>Plan 011 D1: everything a <c>TableShardGrain</c> needs to apply a batch and to answer reads
/// after it reactivates from cold storage, carried on every routed batch rather than looked up.
///
/// WHY CARRIED, NOT LOOKED UP: a shard that has just reactivated must not have to call the registry to
/// learn its own configuration. Beyond the extra hop on every cold read, a shard → registry call would
/// introduce exactly the orchestrator↔worker cycle <c>RegistryGrain</c>'s <c>[MayInterleave]</c> allowlist
/// exists to guard (the registry is the thing that calls the router, which calls the shard). The router
/// stamps this onto every batch and the shard persists the last one it saw, so a cold read answers from
/// its own state alone.</summary>
[GenerateSerializer]
public sealed class TableShardConfig
{
    /// <summary>The owning table's name — which is also what the whole shard tier is KEYED by (router,
    /// directory and every shard grain; see TableShardKeys.GrainKey). Plan 011 D2 therefore refuses to
    /// RENAME a sharded table: a rename would leave every existing shard filed under a key nothing would
    /// ever look up again, silently, with nothing failing. See RegistryGrain.UpdateTableAsync's own note
    /// for why the alternative (re-keying the tier on the immutable id) was not taken here.</summary>
    [Id(0)] public string TableName { get; set; } = "";

    /// <summary>The output columns whose values form the shard key — <see cref="TableDefinition.ShardBy"/>.</summary>
    [Id(1)] public List<string> ShardBy { get; set; } = [];

    /// <summary>Row-identity columns WITHIN a shard, from the same best-effort textual extraction the
    /// row-history tier uses (<c>TableGroupKeyExtractor.ExtractIdentityColumns</c>). Null = whole-row
    /// identity. This is only ever used to group VERSIONS of the same logical row inside a shard that has
    /// already been chosen by <see cref="ShardBy"/> — never to decide which grain owns a row, which is the
    /// distinction that makes best-effort matching acceptable here and not there.</summary>
    [Id(2)] public List<string>? IdentityColumns { get; set; }

    [Id(3)] public bool HistoryEnabled { get; set; }
    [Id(4)] public TableHistoryMode HistoryMode { get; set; } = TableHistoryMode.All;
    [Id(5)] public int HistoryLimit { get; set; } = 10;
    [Id(6)] public string? HistoryByField { get; set; }
    [Id(7)] public long HistoryWindowMs { get; set; }

    /// <summary>The owning table's own durability policy — a shard writes under the same rules the table
    /// does. <see cref="TablePersistenceMode.MemoryOnly"/> is honored literally and is the one combination
    /// that throws the memory win away in the other direction: a shard that never writes has nothing on
    /// disk to reactivate from, so deactivating it LOSES its rows and history rather than swapping them
    /// out. Documented, not blocked — the mode's contract already says exactly that.</summary>
    [Id(8)] public TablePersistenceMode Persistence { get; set; } = TablePersistenceMode.Batched;

    [Id(9)] public int FlushMs { get; set; }
}

/// <summary>Plan 011 D1: one logical row's retained version trail inside a shard. The row-identity key is
/// derived exactly as the table-wide history tier derives it (<c>RowKeyCodec.EncodeIdentity</c> over
/// <see cref="TableShardConfig.IdentityColumns"/>), so a shard's history for a key is the same data the
/// unsharded tier would have held for it — just held per key, and swappable.</summary>
[GenerateSerializer]
public sealed class TableShardHistoryEntry
{
    [Id(0)] public string RowKey { get; set; } = "";
    /// <summary>Newest-first, capped by the caller's requested limit.</summary>
    [Id(1)] public List<HistoryVersion> Versions { get; set; } = [];
    [Id(2)] public long RetractionCount { get; set; }
    /// <summary>Retained version count BEFORE the caller's limit was applied.</summary>
    [Id(3)] public int TotalVersions { get; set; }
}

/// <summary>Plan 011 D1: "give me everything for this key" — the query the whole tier exists for. One
/// grain call, one grain, strictly consistent by construction: the shard applies routed batches and
/// serves reads on the same single-threaded turn queue, so a read observes a whole prefix of the delta
/// stream and never a half-applied batch. No fence, no epoch negotiation, nothing to configure.</summary>
[GenerateSerializer]
public sealed class TableShardView
{
    /// <summary>False when nothing has ever been routed to this shard key (as opposed to a key whose rows
    /// have all been retracted, which is Found with an empty <see cref="Rows"/>).</summary>
    [Id(0)] public bool Found { get; set; }

    [Id(1)] public string ShardKey { get; set; } = "";
    [Id(2)] public List<TableRowDto> Rows { get; set; } = [];
    [Id(3)] public List<TableShardHistoryEntry> History { get; set; } = [];

    /// <summary>Highest router sequence number this shard has applied — see
    /// <see cref="TableShardingInfo.RouterSeq"/> for what the number is and why it is not the dataflow
    /// epoch. -1 = nothing applied yet.</summary>
    [Id(4)] public long AppliedSeq { get; set; } = -1;

    [Id(5)] public long DeltasApplied { get; set; }
    [Id(6)] public bool HistoryEnabled { get; set; }
}

/// <summary>Plan 011 D1: one shard's cheap summary. Reading it ACTIVATES the shard — see
/// <see cref="TableShardingInfo"/> for the numbers that don't.</summary>
[GenerateSerializer]
public sealed class TableShardStats
{
    [Id(0)] public string ShardKey { get; set; } = "";
    [Id(1)] public int RowCount { get; set; }
    [Id(2)] public int HistoryKeyCount { get; set; }
    [Id(3)] public long TotalVersions { get; set; }
    [Id(4)] public long AppliedSeq { get; set; } = -1;

    /// <summary>Plan 011 D2: total deltas this shard has ever applied, persisted across deactivation. It
    /// is what makes a FENCED scan CHECKABLE rather than merely asserted: summed over every shard of a
    /// table it must equal the router's own <see cref="TableShardScanResult.RoutedDeltasAtFence"/> — no
    /// delta forwarded at or before the fence missing, none from after it counted.</summary>
    [Id(5)] public long DeltasApplied { get; set; }
}

/// <summary>Plan 011 D2 — THE FENCED SCAN's result: a genuine consistent cut across every shard of one
/// table, at a named router sequence.
///
/// HOW THE CUT IS TAKEN, and why it is not "each shard waits until AppliedSeq &gt;= S". That obvious
/// design is wrong twice over. It DEADLOCKS on the common case: the sequence is per TABLE and stamped per
/// forwarded batch, so a shard whose key has seen no traffic since sequence 12 sits at AppliedSeq 12 while
/// the table is at 9000, will never reach 9000, and would hang the scan forever — and its state at 12 IS
/// its state at 9000, so there was never anything to wait for. And it does not even give a cut in the
/// other direction: a shard that raced ahead to S+5 before being read would report deltas from AFTER the
/// fence.
///
/// What is actually true is stronger and needs no waiting anywhere. The router forwards one batch at a
/// time and AWAITS every shard's apply before returning (see TableShardRouterGrain.OnDeltaBatchAsync), and
/// the scan runs as a call ON THE ROUTER, which is non-reentrant — so no batch can be forwarded while the
/// scan is in flight, on either stream transport (memory streams and the push bus both deliver as real
/// Orleans messages, subject to the activation's normal concurrency rules). Every shard therefore holds
/// exactly the deltas of batches &lt;= S at the moment it is read, and nothing beyond. The cost is
/// latency: the shard tier's ingest is paused for the duration of the scan, which is why this is opt-in
/// and why the unfenced scan remains the default. No per-sequence versions are retained, so it costs no
/// memory at all.
///
/// SCOPE: the cut is per REQUEST. Paging through a large table with several fenced calls gives several
/// cuts, each honest about its own <see cref="FenceSeq"/> — it does not give one cut across the pages.</summary>
[GenerateSerializer]
public sealed class TableShardScanResult
{
    [Id(0)] public List<TableShardStats> Shards { get; set; } = [];

    /// <summary>The router sequence this scan is a cut AT: the highest batch the router had fully
    /// forwarded when the scan began. -1 = nothing has been routed yet.</summary>
    [Id(1)] public long FenceSeq { get; set; } = -1;

    /// <summary>Deltas the router had forwarded up to and including <see cref="FenceSeq"/>. The
    /// verifiable half of the fence — see <see cref="TableShardStats.DeltasApplied"/>.</summary>
    [Id(2)] public long RoutedDeltasAtFence { get; set; }

    /// <summary>Live shard keys at the fence, from the directory — so a caller can tell "this page is all
    /// of them" (and the DeltasApplied sum is therefore comparable) from "this page is a page".</summary>
    [Id(3)] public int ShardCount { get; set; }
}

/// <summary>Plan 011 D1: table-level sharding metrics, answerable WITHOUT touching a single shard — which
/// is the only reason they are safe to poll. <see cref="ShardCount"/> comes from the directory grain and
/// <see cref="ResidentShardCount"/>/<see cref="Activations"/>/<see cref="Deactivations"/> from an
/// in-process activation counter, so nothing here can wake an idle key.</summary>
[GenerateSerializer]
public sealed class TableShardingInfo
{
    [Id(0)] public bool Enabled { get; set; }
    [Id(1)] public List<string> ShardBy { get; set; } = [];

    /// <summary>Distinct live shard keys, from the shard directory. HONEST LIMIT, stated because it is the
    /// one structure this design does not bound: the directory is O(distinct keys) of strings and is
    /// itself resident. It holds keys, not rows or versions — kilobytes where the shards hold megabytes —
    /// but it is a real ceiling and it is not swapped out.</summary>
    [Id(2)] public int ShardCount { get; set; }

    /// <summary>Shards ACTIVATED right now, in this process. The number that proves the feature works: on
    /// a high-cardinality table it should sit far below <see cref="ShardCount"/> and stay there. Per-silo
    /// by construction (an in-process counter) — accurate for the single-silo deployments this repo
    /// runs, and a per-replica figure rather than a cluster total if that ever changes.</summary>
    [Id(3)] public int ResidentShardCount { get; set; }

    /// <summary>Cumulative shard activations / deactivations in this process. Activations far exceeding
    /// <see cref="ResidentShardCount"/> is the direct evidence that shards are being swapped out and
    /// faithfully reactivated from storage, rather than merely never having been created.</summary>
    [Id(4)] public long Activations { get; set; }
    [Id(5)] public long Deactivations { get; set; }

    /// <summary>The highest per-table sequence number the router has stamped onto a forwarded batch.
    ///
    /// WHY A ROUTER-ASSIGNED SEQUENCE AND NOT THE DATAFLOW EPOCH: <c>SnapshotFrontierEpoch</c> is null for
    /// every Parallelism == 1 table (there is no partitioned frontier to report), so an epoch-based fence
    /// would only ever work for half the tables. The router stamps a monotonically increasing number on
    /// every batch it forwards and each shard records the highest it has applied, which gives both modes
    /// the same ordering primitive. Wave D1 uses it for observability only; it is the mechanism a fenced
    /// consistent whole-table scan (wave D2) needs, built now because retrofitting an ordering stamp
    /// after the fact means reprocessing history.</summary>
    [Id(6)] public long RouterSeq { get; set; } = -1;

    [Id(7)] public long RoutedBatches { get; set; }
    [Id(8)] public long RoutedDeltas { get; set; }

    /// <summary>False when the table is sharded but its router is not currently subscribed (table never
    /// created the tier, or the tier was disabled) — distinguishes "no keys yet" from "not running".</summary>
    [Id(9)] public bool RouterActive { get; set; }
}
