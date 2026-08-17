/**
 * Fallback-only map of table name -> logical key columns, ported from
 * clients/python/src/streamforge/_keyfields.py, itself lifted from otc-terms'
 * lib/streamforge/catalog.ts `keyFields`. `client.table(name, {key: [...]})` always overrides;
 * this only fills in when a caller omits `key` for a table this map happens to know about.
 * Unknown-and-omitted falls back to whole-row identity in zset.ts's groupKeyOf, never a guessed
 * first column.
 *
 * This is yet another copy of the same hand-maintained list (the web console's per-call-site
 * keyFields, the Excel add-in's KEY_FIELDS, the Python client's copy) -- wishlist #18 (surface
 * TableGroupKeyExtractor.Describe's result on TableDefinition) is the fix that deletes all of
 * them. Until then, this file is demo-shaped: it only knows the otc-terms reference catalog's
 * tables. A StreamForge instance running a different SQL catalog gets no fallback here and falls
 * through to whole-row identity, which is correct (if occasionally deduplication-free) rather
 * than silently wrong.
 */

export const KEY_FIELDS: Readonly<Record<string, readonly string[]>> = Object.freeze({
  effective_terms: ["agreement_id"],
  latest_ratings: ["counterparty_id"],
  trigger_monitor: ["counterparty_id", "agreement_id"],
  otc_positions: ["trade_id"],
  strategy_exposure: ["desk", "strategy", "counterparty_id"],
  counterparty_exposure: ["counterparty_id"],
  desk_exposure: ["desk"],
  fund_exposure: [],
  otc_priced_positions: ["trade_id"],
  scenario_registry_latest: ["scenario_id"],
  scenarios: ["scenario_id"],
  scenario_overrides: ["scenario_id", "param", "scope_kind", "scope_id"],
  scenario_positions_repriced: ["scenario_id", "trade_id"],
  scenario_positions_base: ["scenario_id", "trade_id"],
  scenario_block_trades: ["scenario_id", "counterparty_id"],
  scenario_positions: ["scenario_id", "trade_id", "counterparty_id"],
  scenario_strategy_exposure: ["scenario_id", "desk", "strategy", "counterparty_id"],
  scenario_counterparty_exposure: ["scenario_id", "counterparty_id"],
  scenario_desk_exposure: ["scenario_id", "desk"],
  scenario_fund_exposure: ["scenario_id"],
  scenario_ratings: ["scenario_id", "counterparty_id"],
  scenario_trigger_monitor: ["scenario_id", "counterparty_id", "agreement_id"],
  mc_trades: ["trade_id"],
  mc_run_thresholds: ["run_id", "counterparty_id"],
  mc_run_status: ["run_id"],
  mc_positions: ["path_id", "trade_id"],
  mc_path_pnl: ["run_id", "path_id"],
  mc_var: ["run_id"],
  mc_es: ["run_id"],
  mc_cp_exposure: ["run_id", "path_id", "counterparty_id"],
  mc_buckets: ["run_id", "bucket"],
  mc_breach: ["run_id", "counterparty_id", "agreement_id"],
  mc_breach_static: ["run_id", "counterparty_id"],
  mc_day_cp_exposure: ["run_id", "day", "path_id", "counterparty_id"],
  mc_margin_calls: ["run_id", "day", "counterparty_id"],
});
