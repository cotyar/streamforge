/// <reference path="./streamsforge-plugin.d.ts" />

// A UI plugin, in TypeScript-plus-JSX form: a single `.tsx` file, transpiled in the browser by sucrase
// (see `web/src/plugins/transpile.ts`) — no build step, no bundled React, same "drop one file in
// ui-plugins/" story as example-nats.js (which stays a plain `.js` example alongside this one). This
// one also exercises apiVersion 3's `onSuggest` hook: pressing "Suggest name" proposes a name (and a
// `nats` tag) for the rest of the source form — but only while the draft doesn't already have a name;
// `applySuggestion` (web/src/plugins/suggest.ts) enforces that rule, this plugin only has to ask.
//
// Single-file contract: NO import/export statements (the transpiled output becomes a `blob:` module
// with no module graph of its own) and classic JSX only — no automatic runtime, which would emit
// `import "react/jsx-runtime"`, unresolvable from that blob module. Hence `const React = window.
// streamsforge.react` below instead of a normal `import React from 'react'`.

const { react, registerTransportEditor } = window.streamsforge
const React = react

function NatsEditor({ value, onChange, disabled, idPrefix, draft, onSuggest }: TransportEditorProps) {
  const field = (key: string, label: string, placeholder: string) => (
    <label key={key} className="flex flex-col gap-1 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <input
        id={`${idPrefix}-${key}`}
        className="h-8 rounded-md border border-border bg-transparent px-2 font-mono text-xs"
        value={(value[key] as string) ?? ''}
        placeholder={placeholder}
        disabled={disabled}
        // Always spread the previous value: the config object holds fields this editor never shows
        // (credentials, the optional JetStream group) and a bare `{[key]: v}` would drop them.
        onChange={(e: { target: { value: string } }) => onChange({ ...value, [key]: e.target.value })}
      />
    </label>
  )

  const nameAlreadySet = !!draft?.name?.trim()

  return (
    <div className="flex flex-col gap-2 rounded-lg border border-border p-3">
      <p className="text-[11px] text-muted-foreground">Rendered by the example TypeScript UI plugin.</p>
      {field('url', 'Server', 'nats://localhost:4222')}
      {field('subject', 'Subject', 'orders.>')}
      {onSuggest && (
        <button
          type="button"
          disabled={disabled || nameAlreadySet}
          onClick={() => onSuggest({ name: 'nats-source', tags: ['nats'] })}
          className="self-start rounded-md border border-border px-2 py-1 text-[11px] disabled:opacity-50"
        >
          Suggest name
        </button>
      )}
    </div>
  )
}

registerTransportEditor('nats', NatsEditor, 'inbound')
