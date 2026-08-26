import * as react from 'react'
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

/** The plugin-facing API, installed on `window` before any plugin module is imported. `react` is handed
 *  over so a plugin doesn't bundle (and break on) a second copy of it. */
export const pluginHost = {
  /** Bump only for a breaking change to TransportEditorProps; a plugin can refuse to register below it. */
  apiVersion: 1,
  react,
  registerTransportEditor,
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
