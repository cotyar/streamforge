// Pins `web/src/plugins/transpile.ts` — how a `.ts`/`.tsx` UI plugin becomes plain JS the browser can
// `import()` from a blob URL. No DOM: the last test evaluates the transpiled example plugin directly
// against a stubbed `window`, the same shape `load.ts` sets up before importing any plugin module.
import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { pluginTransforms, transpilePlugin } from '../src/plugins/transpile'

describe('pluginTransforms', () => {
  test('a plain ES module needs no transform', () => {
    expect(pluginTransforms('/api/ui-plugins/example-nats.js')).toBeNull()
    expect(pluginTransforms('/api/ui-plugins/example-nats.mjs')).toBeNull()
  })

  test('a cache-busting query is stripped before matching', () => {
    expect(pluginTransforms('/api/ui-plugins/example.ts?v=12345')).toEqual(['typescript'])
  })

  test('.tsx gets the jsx transform too', () => {
    expect(pluginTransforms('/api/ui-plugins/example-nats.tsx')).toEqual(['typescript', 'jsx'])
  })

  test('extension matching is case-insensitive', () => {
    expect(pluginTransforms('/api/ui-plugins/EXAMPLE.TSX')).toEqual(['typescript', 'jsx'])
  })
})

describe('transpilePlugin', () => {
  test('transpiles the example .tsx plugin to plain JS with classic JSX (React.createElement)', async () => {
    const source = readFileSync(join(import.meta.dir, '..', 'plugins-example', 'example-nats.tsx'), 'utf8')
    const code = await transpilePlugin(source, '/api/ui-plugins/example-nats.tsx?v=1')

    expect(code).toContain('React.createElement(')
    // No TypeScript syntax should survive — the ambient-typed destructure and prop annotations are gone.
    expect(code).not.toContain('interface')
    expect(code).not.toContain(': TransportEditorProps')

    // Evaluate the transpiled plugin against a stubbed host, exactly like the real loader (load.ts)
    // does after transpiling: `new Function(code)()` runs it as a plain script, no module system
    // involved (there must be no import/export left in the output for that to even parse).
    const registrations: unknown[][] = []
    const originalWindow = (globalThis as { window?: unknown }).window
    ;(globalThis as { window?: unknown }).window = {
      streamsforge: {
        react: { createElement: (...args: unknown[]) => args },
        registerTransportEditor: (...args: unknown[]) => registrations.push(args),
      },
    }
    try {
      new Function(code)()
    } finally {
      ;(globalThis as { window?: unknown }).window = originalWindow
    }

    expect(registrations.length).toBe(1)
    const [kind, component, direction] = registrations[0]!
    expect(kind).toBe('nats')
    expect(typeof component).toBe('function')
    expect(direction).toBe('inbound')
  })
})
