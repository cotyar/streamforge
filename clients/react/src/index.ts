/**
 * @streamsforge/react -- React bindings for @streamsforge/client.
 *
 *   <StreamsForgeProvider url="http://localhost:5199" user="admin" password="admin123!">
 *     <LiveTablePanel name="trigger_monitor" />
 *   </StreamsForgeProvider>
 *
 * Nothing here re-implements the wire protocol: the provider owns one `connect()`ed `Client`,
 * the hooks own one `LiveTable` each (which already does subscribe -> buffer -> snapshot ->
 * replay and the Z-set reduction), and the components are unstyled markup over the rows those
 * hooks return. See README.md for the split, and clients/typescript for the client itself.
 */

export { StreamsForgeProvider, useStreamsForge, useStreamsForgeStatus } from "./provider.js";
export type { StreamsForgeProviderProps, StreamsForgeStatus } from "./provider.js";

export { useLiveTable, useLiveSql, useTables } from "./hooks.js";
export type { LiveTableState, UseLiveTableOptions, UseLiveSqlOptions, TablesState } from "./hooks.js";

export { LiveTableView } from "./components/LiveTableView.js";
export type { LiveTableViewProps } from "./components/LiveTableView.js";
export { LiveTablePanel } from "./components/LiveTablePanel.js";
export type { LiveTablePanelProps } from "./components/LiveTablePanel.js";
export { Sparkline } from "./components/Sparkline.js";
export type { SparklineProps } from "./components/Sparkline.js";

export { StreamView } from "./components/StreamView.js";
export type { StreamViewProps } from "./components/StreamView.js";
