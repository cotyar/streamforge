// A UI plugin, in the form the console loads it: one plain ES module, no build step, no bundled React.
// Drop it (or your own) into the host's `ui-plugins/` directory — `<host binaries>/ui-plugins/`, or
// wherever `Ui:PluginsPath` points — and reload the console.
//
// This one replaces the generic descriptor form for the `nats` SOURCE with a hand-written editor. Delete
// the third argument to `registerTransportEditor` to serve the sink half too.
const { react, registerTransportEditor } = window.streamforge
const h = react.createElement

function NatsEditor({ value, onChange, disabled, idPrefix }) {
  const field = (key, label, placeholder) =>
    h('label', { key, className: 'flex flex-col gap-1 text-xs' },
      h('span', { className: 'text-muted-foreground' }, label),
      h('input', {
        id: `${idPrefix}-${key}`,
        className: 'h-8 rounded-md border border-border bg-transparent px-2 font-mono text-xs',
        value: value[key] ?? '',
        placeholder,
        disabled,
        // Always spread the previous value: the config object holds fields this editor never shows
        // (credentials, the optional JetStream group) and a bare `{[key]: v}` would drop them.
        onChange: (e) => onChange({ ...value, [key]: e.target.value }),
      }))

  return h('div', { className: 'flex flex-col gap-2 rounded-lg border border-border p-3' },
    h('p', { className: 'text-[11px] text-muted-foreground' }, 'Rendered by the example UI plugin.'),
    field('url', 'Server', 'nats://localhost:4222'),
    field('subject', 'Subject', 'orders.>'))
}

registerTransportEditor('nats', NatsEditor, 'inbound')
