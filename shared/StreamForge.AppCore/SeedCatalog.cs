using StreamForge.Abstractions;
using StreamForge.Host.Generators;

namespace StreamForge.AppCore;

/// <summary>
/// The demo world both runtime flavors seed on first boot (plan 005 W2): stream sources, pipelines,
/// tables, and the three demo users. Extracted from Orleans' <c>RegistryGrain</c>/<c>UserStoreGrain</c>
/// so the Dapr flavor's <c>RegistryActor</c>/<c>UserStoreActor</c> seed byte-identical data without
/// depending on Orleans grain types. Pure data — no I/O, no compilation, no password hashing (the
/// caller hashes seed passwords with its own <see cref="Auth.PasswordHasher"/>, exactly like the grain
/// used to).
/// </summary>
public static class SeedCatalog
{
    /// <summary>Demo stream sources. Delegates to <see cref="MarketDataProfiles.SeedSources"/> — the
    /// generator-profile data lives there since it's paired with <see cref="MarketDataProfiles.GenerateEvent"/>;
    /// exposed here too so both runtimes have one seed-catalog entry point.</summary>
    public static List<SourceDefinition> Sources() => MarketDataProfiles.SeedSources();

    /// <summary>Demo pipelines seeded on first run. The first three (VWAP, spread, hot-symbol VWAP) and
    /// the fill-rate pipeline are marked Running here — the caller's resume-on-boot logic turns that into
    /// a real start against the seeded sources, exactly like a normal restart.</summary>
    public static List<PipelineDefinition> Pipelines()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        PipelineDefinition Make(string name, string description, string sql, PipelineStatus status) => new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = name,
            Description = description,
            Sql = sql,
            Status = status,
            CreatedBy = "system",
            CreatedAtMs = now,
            UpdatedAtMs = now,
        };

        return
        [
            Make(
                "VWAP by symbol (5s)",
                "Volume-weighted average price per symbol over 5-second tumbling windows.",
                "SELECT symbol, SUM(price * qty) / SUM(qty) AS vwap, COUNT(*) AS trades FROM trades " +
                "GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
                PipelineStatus.Running),
            Make(
                "Trade vs quote spread",
                "Joins BUY trades against the prevailing quote to compare trade price with the bid.",
                "SELECT t.symbol, t.price, q.bid, q.ask, t.price - q.bid AS above_bid FROM trades t " +
                "JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol WHERE t.side = 'BUY'",
                PipelineStatus.Running),
            Make(
                "Hot symbol VWAP (nested)",
                "Plan 004 showcase: a WITH CTE finds busy symbols per 10s window; the outer query keeps " +
                "only trades whose symbol is IN that rolling set, then computes 5s VWAP.",
                "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                "SELECT t.symbol, SUM(t.price * t.qty) / SUM(t.qty) AS vwap, COUNT(*) AS trades FROM trades t " +
                "WHERE t.symbol IN (SELECT symbol FROM hot) " +
                "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
                PipelineStatus.Running),
            Make(
                "Order bursts (session)",
                "Groups order activity per symbol into session windows to spot bursts.",
                "SELECT symbol, COUNT(*) AS orders, SUM(qty) AS total_qty FROM orders " +
                "GROUP BY symbol WINDOW SESSION(GAP 3 SECONDS)",
                PipelineStatus.Stopped),
            Make(
                "Unfilled orders (LEFT JOIN)",
                "New orders left-joined against recent trades to surface ones that haven't filled yet.",
                "SELECT o.orderId, o.symbol, o.qty, t.price FROM orders o " +
                "LEFT JOIN trades t WITHIN 10 SECONDS ON o.symbol = t.symbol WHERE o.status = 'NEW'",
                PipelineStatus.Stopped),
            Make(
                "JSON payload join",
                "Extracts user tier and order symbol from app_events' nested JSON payload via '->'/'->>' " +
                "and joins on the extracted symbol to attach the prevailing trade price.",
                "SELECT e.eventType, e.payload -> 'user' ->> 'tier' AS tier, e.payload -> 'order' ->> 'symbol' AS symbol, t.price FROM app_events e " +
                "JOIN trades t WITHIN 10 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol",
                PipelineStatus.Stopped),
            Make(
                "fill-rate-5s",
                "Per-symbol fill activity over 5-second tumbling windows (Phase L3): count of PART_FILL/FILLED " +
                "order_events and their filled_qty. Note: filled_qty is order_events' cumulative-fill field, " +
                "not a per-fill delta, so SUM(filled_qty) here is a windowed sum of cumulative snapshots — " +
                "useful as an activity/volume-scale signal, not a literal 'shares filled in this window' count.",
                "SELECT symbol, COUNT(*) AS fills, SUM(filled_qty) AS filled FROM order_events " +
                "WHERE stage = 'PART_FILL' OR stage = 'FILLED' GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
                PipelineStatus.Running),
        ];
    }

    /// <summary>Demo tables seeded on first run: "positions" is Running (a plain running aggregate over
    /// "trades"), "gold_tier_orders" demonstrates JSON expressions in table mode, and "hot_symbols"
    /// demonstrates table-over-table chaining (FROM "positions"). The latter two are seeded Stopped so
    /// dependency-order start is a deliberate user action, not implicit at boot. Returned raw (SQL only,
    /// no compiled OutputFields/StreamInputs/TableInputs) — the caller compiles each entry against its
    /// own freshly-seeded sources, exactly like the Orleans grain used to.</summary>
    public static List<TableDefinition> Tables()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        TableDefinition Make(string name, string description, string sql, PipelineStatus status, bool searchEnabled = false, TableSearchMode searchMode = TableSearchMode.Exact) => new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = name,
            Description = description,
            Sql = sql,
            Status = status,
            CreatedBy = "system",
            CreatedAtMs = now,
            UpdatedAtMs = now,
            SearchEnabled = searchEnabled,
            SearchMode = searchMode,
        };

        // "order_states" (Phase L3): current state per order_id via LATEST BY (plan 002) — keeps the
        // latest event row per key by _ts and emits retract/assert pairs as orders progress. The actual
        // stage STRING rides along (no monotone-MAX workaround needed anymore).
        var orderStates = Make(
            "order_states",
            "Current state per live order (Phase L3): the latest order_events row per order_id via LATEST BY.",
            "SELECT order_id, symbol, side, stage, stage_rank, stage_ts, qty, filled_qty, px " +
            "FROM order_events LATEST BY (order_id)",
            PipelineStatus.Running);
        // Row history mode: LastN(8), not MinBy/MaxBy — the demo goal is the STAGE TRAIL (NEW, ACK,
        // PART_FILL, PART_FILL, ..., FILLED/CANCELED) for a clicked order, i.e. the recent-versions trail,
        // not a peak+latest pair. MaxBy(stage_rank) would only ever retain 2 entries (the FILLED/CANCELED
        // extreme + itself as latest) and lose the PART_FILL steps in between — LastN(8) keeps the walk.
        //
        // PLAN 011 WAVE C2 — THIS TABLE IS BOUNDED, AND HERE IS EXACTLY WHAT THAT MEANS. It is the one
        // seeded entry whose key space is unbounded: `order_id` is a fresh GUID-derived string per order
        // (MarketDataProfiles.SpawnOrder), an order goes terminal and is never re-emitted
        // (LifecycleGeneratorTests asserts exactly that finality), and `LATEST BY (order_id)` retains one
        // row per key forever. Seeded RUNNING at 5 order_events/s (~1 new order/s), it used to gain roughly
        // one PERMANENT row per second for as long as the host lived, with up to 8 history versions behind
        // each — a host left running overnight on the stock seed would eventually exhaust memory. That was
        // not a hypothetical: it is the reproduction of the reported failure. Wave C removed the AMPLIFIER
        // (the whole-table snapshot rebuild every FlushMs); wave C2's per-table ROW RETENTION policy is
        // what bounds the row set itself, and this table is its first customer.
        //
        // RetentionMaxRows = 2000 is roughly the last half hour of orders at the seeded rate. Past that,
        // the oldest rows (by EVENT timestamp — see TableDefinition.RetentionMaxRows) are evicted with real
        // retractions, so the delta stream, any downstream table, the search index and this table's own row
        // history all follow along; the evicted key's history is reclaimed with it. The honest consequence,
        // which is the point of the policy being opt-in everywhere else: `order_states` is now a BOUNDED
        // VIEW of "current state per order", not the full one. An order that went terminal ~2000 orders ago
        // is no longer listed. For the demo that is the intended reading anyway — the table's own
        // description says "per LIVE order" — and it is strictly better than the alternative, which was a
        // demo that eats the machine.
        //
        // WHY THIS AND NOT THE TWO OBVIOUS ALTERNATIVES, both of which remain blocked: seeding it Stopped
        // contradicts LifecycleSeedClusterTests / StreamBridgeServiceStartupRaceTests, which assert it
        // seeds Running; recycling order ids to bound the key space contradicts LifecycleGeneratorTests'
        // terminal-finality invariant. Retention touches neither assertion — all three still pass
        // unmodified, which is exactly why the policy was built rather than the seed rewritten.
        orderStates.HistoryEnabled = true;
        orderStates.HistoryMode = TableHistoryMode.LastN;
        orderStates.HistoryLimit = 8;
        orderStates.RetentionMaxRows = 2000;

        return
        [
            Make(
                "positions",
                "Running per-symbol trade aggregates: count, total quantity, and price stats.",
                "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty, AVG(price) AS avg_price, MIN(price) AS low, MAX(price) AS high " +
                "FROM trades GROUP BY symbol",
                PipelineStatus.Running,
                searchEnabled: true,
                searchMode: TableSearchMode.Fuzzy),
            Make(
                "gold_tier_orders",
                "Order counts per symbol for gold-tier users, extracted from app_events' nested JSON payload via '->'/'->>'.",
                "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
                "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'",
                PipelineStatus.Stopped),
            Make(
                "hot_symbols",
                "Symbols from 'positions' with more than 50 trades — table-over-table chaining demo.",
                "SELECT p.symbol, p.trades, p.avg_price FROM positions p WHERE p.trades > 50",
                PipelineStatus.Stopped),
            Make(
                "leg_exposure",
                "Per-currency notional across all structure legs (plan 002 L2): UNNEST flattens each " +
                "multileg instrument's legs array; SUM uses '->' (raw numeric node — '->>' is text and " +
                "would not accumulate).",
                "SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS notional, COUNT(*) AS legs " +
                "FROM structures s, UNNEST(s.legs) AS l GROUP BY l ->> 'ccy'",
                PipelineStatus.Running),
            orderStates,
        ];
    }

    /// <summary>One seed demo user: plaintext <paramref name="Password"/> — the caller hashes it with its
    /// own <see cref="Auth.PasswordHasher"/> (PBKDF2) before persisting, exactly like the Orleans grain
    /// used to. Never persisted or logged as-is.</summary>
    public readonly record struct SeedUser(string Username, string DisplayName, string Role, string Password);

    /// <summary>Demo users seeded on first run: admin/editor/viewer, one per role.</summary>
    public static IReadOnlyList<SeedUser> Users { get; } =
    [
        new("admin", "Administrator", "Admin", "admin123!"),
        new("editor", "Editor", "Editor", "editor123!"),
        new("viewer", "Viewer", "Viewer", "viewer123!"),
    ];
}
