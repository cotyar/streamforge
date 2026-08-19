/**
 * LiveTablePanel -- the one-line "lego" block: `useLiveTable` (data) wired straight into
 * `LiveTableView` (markup). Reach for the hook and the view separately instead when a caller needs
 * to interleave other UI with the fetch (a toolbar, a row-click handler, a second view over the
 * same rows) -- this component exists purely to collapse the common case, "show me table X", to
 * one JSX line.
 */
import type { ReactElement } from "react";
import { useLiveTable } from "../hooks.js";
import { LiveTableView, type LiveTableViewProps } from "./LiveTableView.js";

export interface LiveTablePanelProps extends Omit<LiveTableViewProps, "rows" | "loading" | "error" | "flashKeys"> {
  name: string;
  /** Forwarded as useLiveTable's opts.key -- the row-identity columns for supersession. */
  tableKey?: string[];
  timeoutMs?: number;
  /** Coalescing window in ms -- see useLiveTable's own flushMs. Default 16 (one frame). */
  flushMs?: number;
}

export function LiveTablePanel(props: LiveTablePanelProps): ReactElement {
  const { name, tableKey, timeoutMs, flushMs, ...viewProps } = props;
  // flashKeys comes from the hook, not from the caller (hence its Omit above): the keys are the
  // Z-set's own, and only this hook's LiveTable knows which ones its last batch touched.
  const { rows, loading, error, flashKeys } = useLiveTable(name, { key: tableKey, timeoutMs, flushMs });
  return <LiveTableView {...viewProps} rows={rows} loading={loading} error={error} flashKeys={flashKeys} />;
}
