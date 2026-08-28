import * as react from 'react'
import { api, getStoredToken } from '@/api/client'
import { subscribePipeline, subscribeSource, subscribeTable } from '@/realtime/hub'
import type { TransportDescriptor } from '@/api/types'
import type { TransportConfigValue } from '@/components/sources/TransportConfigEditor'

/**
 * UI plugins: a third-party library that adds a source/sink KIND can also add a specialized editor for it,
 * without a change anywhere in `web/`.
 *
 * The backend already made a new transport configurable with zero console changes (plan 010 — the
 * descriptor IS the form). This is the escape hatch for the cases that generic form can't express: a
 * topic browser, a connection tester, a query builder. A plugin is one ES module served from the host's
 * `ui-plugins/` directory (GET /api/ui-plugins), loaded at boot, which calls
 * `window.streamforge.registerTransportEditor(kind, Component)` — and from then on that kind's config
 * panel is the plugin's component instead of the descriptor-driven one, in the source modal and in the
 * sinks editor alike.
 *
 * ponytail: keyed by kind, nothing else is pluggable. A plugin gets the same props the built-in editor
 * gets — plain data plus one `onChange` — so it needs no knowledge of this console beyond them.
 */
export interface TransportEditorProps {
  descriptor: TransportDescriptor
  value: TransportConfigValue
  onChange: (next: TransportConfigValue) => void
  /** True while editing an existing entity: a secret field reads back as `***` and sending it keeps the
   *  stored value. A plugin that renders secrets must honor that or it will wipe them. */
  isEdit: boolean
  disabled: boolean
  idPrefix: string
  direction: TransportDirection
}

export type TransportDirection = 'inbound' | 'outbound'
export type TransportEditor = react.ComponentType<TransportEditorProps>

const editors = new Map<string, TransportEditor>()

const key = (direction: TransportDirection | '*', kind: string) => `${direction}:${kind.toLowerCase()}`

/** Exposed to plugins as `window.streamforge.registerTransportEditor`. Omit `direction` to serve both
 *  halves of a kind that exists as a source AND a sink (nats, fix-duplex, …). */
export function registerTransportEditor(kind: string, component: TransportEditor, direction?: TransportDirection): void {
  editors.set(key(direction ?? '*', kind), component)
}

/** A direction-specific registration wins over a both-directions one. Undefined = render the built-in
 *  descriptor form. */
export function findTransportEditor(kind: string, direction: TransportDirection): TransportEditor | undefined {
  return editors.get(key(direction, kind)) ?? editors.get(key('*', kind))
}

/** Test/HMR seam — the registry is otherwise write-once at boot. */
export function clearTransportEditors(): void {
  editors.clear()
}

/**
 * The plugin-facing API, installed on `window` before any plugin module is imported.
 *
 * Everything here is something the console ALREADY has and a plugin cannot get for itself without paying
 * twice: `react` (a second copy breaks hooks), `api` (an authenticated fetch that carries the session
 * token AND the selected environment header), and `live` (the console's ONE SignalR connection — a plugin
 * that opened its own would mean a second socket, a second auth handshake and a second subscription for
 * the same rows). `loadLiveTables` is the heavy path, behind a dynamic import so a console that never
 * loads a plugin never downloads it.
 */
export const pluginHost = {
  /** Bumped when this object or TransportEditorProps changes shape, so a plugin can feature-detect
   *  (`if ((window.streamforge?.apiVersion ?? 0) >= 2)`) instead of assuming. 1 → 2 added `api`, `live`
   *  and `loadLiveTables`. */
  apiVersion: 2,
  react,
  registerTransportEditor,

  /** The console's own REST client: `get`/`post`/`put`/`del`, bearer token and environment header
   *  included, `ApiError` (with `.status`) on failure. Paths are absolute — `api.get('/api/tables')`. */
  api,

  /** The console's own live feed, off the connection it already holds. Each returns an unsubscribe
   *  function; `subscribeTable`'s also carries `.ready`, a promise resolved once the server has confirmed
   *  the subscription (await it before reading a snapshot you need to be complete). */
  live: {
    subscribeTable,
    subscribeSource,
    subscribePipeline,
  },

  /**
   * TanStack DB against a StreamForge table, loaded on demand: resolves
   * `{ createCollection, createLiveQueryCollection, streamForgeCollectionOptions, connect }` — enough for
   *
   *   const { createCollection, streamForgeCollectionOptions, connect } = await sf.loadLiveTables()
   *   const client = await connect()            // this console's URL + session token, SignalR transport
   *   const rows = createCollection(streamForgeCollectionOptions({ client, table: 'orders' }))
   *
   * `connect()` is memoized per page: one client, however many plugins ask. It is a SECOND connection
   * from the console's SignalR hub above — use `live.subscribeTable` when plain deltas are enough, and
   * this when the plugin wants TanStack DB's own query/join layer on top.
   */
  loadLiveTables,
}

let clientPromise: Promise<unknown> | null = null

async function loadLiveTables() {
  // Three dynamic imports, so @tanstack/db and the client's transport stack are their own chunks — the
  // console itself uses none of them.
  const [db, bridge, client] = await Promise.all([
    import('@tanstack/db'),
    import('@streamforge/tanstack-db'),
    import('@streamforge/client'),
  ])

  return {
    createCollection: db.createCollection,
    createLiveQueryCollection: db.createLiveQueryCollection,
    streamForgeCollectionOptions: bridge.streamForgeCollectionOptions,
    /** Connects (once) with this console's origin and stored session token. `transport: 'signalr'`
     *  because the client's gRPC transport is Node-only. */
    connect: () => {
      clientPromise ??= client.connect({
        url: window.location.origin,
        token: getStoredToken() ?? undefined,
        transport: 'signalr',
      })
      return clientPromise as ReturnType<typeof client.connect>
    },
  }
}

declare global {
  interface Window {
    streamforge?: typeof pluginHost
  }
}

/** A plugin that throws takes down its own panel, not the page around it. */
export class PluginErrorBoundary extends react.Component<
  { kind: string; children: react.ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  render() {
    if (!this.state.error) return this.props.children
    return (
      <p className="text-[11px] text-destructive">
        The UI plugin for <span className="font-mono">{this.props.kind}</span> failed to render (
        {this.state.error.message}). Its configuration is unchanged.
      </p>
    )
  }
}
