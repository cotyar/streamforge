// Ambient typings for a StreamsForge console UI plugin, apiVersion 3.
//
// SCRIPT MODE ON PURPOSE: no `import`/`export` statement anywhere in this file. A plugin is one file
// loaded by `import()` at runtime with no bundler and no module graph of its own (see TRANSPORTS.md's
// "Console UI plugins" section) — everything it needs comes off `window.streamsforge`, not from
// importing `web/src/...` modules it has no access to. This file is a hand-written MIRROR of the real
// (module-mode) types in `web/src/plugins/registry.tsx` / `web/src/plugins/suggest.ts` / `web/src/api/
// types.ts`, kept in sync by hand rather than generated, so it can be copied out of this repo and used
// standalone by an out-of-tree plugin author.
//
// Usage (see example-nats.tsx alongside this file):
//   /// <reference path="./streamsforge-plugin.d.ts" />
//   const { react, registerTransportEditor } = window.streamsforge
//
// `web/tsconfig.json` deliberately excludes this directory (`include: ["src"]`) — a script-mode
// ambient file with no import/export would otherwise pollute the console's own type-checked build.

type FieldType = 'String' | 'Double' | 'Long' | 'Bool' | 'Timestamp' | 'Json'

interface FieldDef {
  name: string
  type: FieldType
  /** Declared nested shape of a `Json` field (drill-down schema). Absent for scalar fields. */
  children?: FieldDef[]
  /** The field holds a JSON array rather than a single value. */
  isArray?: boolean
}

type Tags = string[]

type TransportFieldType = 'string' | 'secret' | 'number' | 'bool' | 'select' | 'text'

interface TransportField {
  key: string
  label: string
  type: TransportFieldType
  group?: string | null
  required: boolean
  mono: boolean
  placeholder?: string | null
  help?: string | null
  options?: string[] | null
  /** Initial value for a NEW entity, as a string coerced by `type`. */
  default?: string | null
}

interface TransportGroup {
  key: string
  label: string
  help?: string | null
  /** Rendered with an on/off switch; when off, `objectKey` is written as null. */
  optional: boolean
  /** When set, this group's fields live in a nested object under this property. */
  objectKey?: string | null
}

/** What the console needs to render a transport's config form, served by GET /api/transports. */
interface TransportDescriptor {
  kind: string
  label: string
  help?: string | null
  /** Which property of ConnectorConfig / SinkSpec holds this transport's config object. */
  configProperty: string
  fields: TransportField[]
  groups: TransportGroup[]
  polled: boolean
  mapping: boolean
  canProbe: boolean
  duplex?: boolean
}

/** The transport's own config object as it goes on the wire. A plugin editor reads and writes this by
 *  descriptor key, exactly like the built-in generic form it replaces. */
type TransportConfigValue = Record<string, unknown>

type TransportDirection = 'inbound' | 'outbound'

/**
 * apiVersion 3: the rest of the source form a transport editor is embedded in, and the one hook to
 * propose a patch to it. See `web/src/plugins/suggest.ts`'s `applySuggestion` for the exact merge
 * rules (name/description only while blank, fields replace when non-empty, tags union, `{}` on no-op).
 */
interface EditorDraft {
  name: string
  description: string
  fields: FieldDef[]
  tags: Tags
}

type EditorSuggestion = Partial<EditorDraft>

interface TransportEditorProps {
  descriptor: TransportDescriptor
  value: TransportConfigValue
  onChange: (next: TransportConfigValue) => void
  /** True while editing an existing entity: a secret field reads back as `***` and sending it keeps the
   *  stored value. A plugin that renders secrets must honor that or it will wipe them. */
  isEdit: boolean
  disabled: boolean
  idPrefix: string
  direction: TransportDirection
  /** apiVersion 3: read-only snapshot of the rest of the source form. Absent on the sinks editor. */
  draft?: EditorDraft
  /** apiVersion 3: propose a patch to the draft above. Absent wherever `draft` is absent. */
  onSuggest?: (patch: EditorSuggestion) => void
}

type TransportEditor = (props: TransportEditorProps) => import('react').ReactElement | null

// -- The console's own live feed and REST client, handed to a plugin instead of it opening its own. --

type RowValue = string | number | boolean | null | { [key: string]: RowValue } | RowValue[]
type ResultRow = Record<string, RowValue>

interface ResultEnvelope {
  pipelineId: string
  seq: number
  timestampMs: number
  row: ResultRow
}

interface TableRowDto {
  row: ResultRow
  weight: number
}

type PipelineStatusValue = 'Stopped' | 'Running' | 'Failed'

type Unsubscribe = () => void
type TableUnsubscribe = Unsubscribe & { ready: Promise<void> }

interface StreamsForgeApi {
  get: <T>(path: string) => Promise<T>
  post: <T>(path: string, body?: unknown) => Promise<T>
  put: <T>(path: string, body?: unknown) => Promise<T>
  del: <T>(path: string) => Promise<T>
}

interface StreamsForgeLive {
  subscribeTable: (name: string, onDeltas: (deltas: TableRowDto[], seq: number) => void) => TableUnsubscribe
  subscribeSource: (name: string, onEvent: (row: ResultRow) => void) => Unsubscribe
  subscribePipeline: (
    id: string,
    onRows: (rows: ResultEnvelope[]) => void,
    onStatus?: (status: PipelineStatusValue) => void,
  ) => Unsubscribe
}

/** Resolved by `loadLiveTables()` — TanStack DB wired to this console's own connection. Kept loosely
 *  typed here on purpose: a plugin using it already depends on `@tanstack/db`'s own types directly. */
interface StreamsForgeLiveTables {
  createCollection: (...args: unknown[]) => unknown
  createLiveQueryCollection: (...args: unknown[]) => unknown
  streamsForgeCollectionOptions: (...args: unknown[]) => unknown
  connect: () => Promise<unknown>
}

/** The plugin-facing API installed on `window.streamsforge` before any plugin module is imported. */
interface StreamsForgeHost {
  /** Bumped when this object or TransportEditorProps changes shape. 2 → 3 added `draft`/`onSuggest`. */
  apiVersion: number
  react: typeof import('react')
  registerTransportEditor: (kind: string, component: TransportEditor, direction?: TransportDirection) => void
  api: StreamsForgeApi
  live: StreamsForgeLive
  loadLiveTables: () => Promise<StreamsForgeLiveTables>
}

interface Window {
  streamsforge: StreamsForgeHost
}
