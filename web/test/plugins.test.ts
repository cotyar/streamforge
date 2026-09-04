// Pins the UI-plugin registry's lookup rules (web/src/plugins/registry.tsx). Pure map logic — bun's
// runner has no DOM, so the components themselves are exercised only by `bun run build`'s typecheck.
import { beforeEach, describe, expect, test } from 'bun:test'
import { clearTransportEditors, findTransportEditor, pluginHost, registerTransportEditor } from '../src/plugins/registry'

const A = (() => null) as never
const B = (() => null) as never

describe('transport editor registry', () => {
  beforeEach(clearTransportEditors)

  test('no plugin registered means the built-in descriptor form', () => {
    expect(findTransportEditor('nats', 'inbound')).toBeUndefined()
  })

  test('a registration without a direction serves both halves', () => {
    registerTransportEditor('nats', A)
    expect(findTransportEditor('nats', 'inbound')).toBe(A)
    expect(findTransportEditor('nats', 'outbound')).toBe(A)
  })

  test('a direction-specific registration wins, and does not leak to the other half', () => {
    registerTransportEditor('nats', A)
    registerTransportEditor('nats', B, 'outbound')
    expect(findTransportEditor('nats', 'outbound')).toBe(B)
    expect(findTransportEditor('nats', 'inbound')).toBe(A)
  })

  test('kind matching is case-insensitive, like findDescriptor', () => {
    registerTransportEditor('Postgres-CDC', A)
    expect(findTransportEditor('postgres-cdc', 'inbound')).toBe(A)
  })

  test('apiVersion is 3', () => {
    expect(pluginHost.apiVersion).toBe(3)
  })
})
