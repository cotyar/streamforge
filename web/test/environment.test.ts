// Plan 021 wave 2 (021-F) — pins the SPA's pure environment-selection logic. Run with `bun test web/test`.
//
// WHY ONLY THE PURE FUNCTIONS. bun's runner (see permissions.test.ts's header comment) has no DOM: no
// `window`, no `localStorage`. getStoredEnvironment()/setStoredEnvironment() defensively try/catch
// those globals (see lib/environment.ts), so under bun test they exercise their "no storage available"
// fallback rather than the browser path — which is itself worth pinning (a server with no localStorage
// must behave like the default environment, not throw), but the useEnvironment() REACT HOOK and the
// dropdown component are exercised only by `bun run build`'s typecheck, not here.
import { describe, expect, test } from 'bun:test'
import {
  DEFAULT_ENVIRONMENT,
  getStoredEnvironment,
  isValidEnvironmentName,
  needsEnvironmentSelector,
  setStoredEnvironment,
} from '../src/lib/environment'
import { environmentHeader, ENVIRONMENT_HEADER } from '../src/api/client'

describe('isValidEnvironmentName', () => {
  // Mirrors EnvKeys.IsValidName's NamePattern (shared/StreamForge.AppCore/Environments/EnvKeys.cs):
  // lower-case, digits, hyphens, 1-32 chars, starting with a letter or digit.
  test('accepts the server-legal shape', () => {
    expect(isValidEnvironmentName('staging')).toBe(true)
    expect(isValidEnvironmentName('prod-eu')).toBe(true)
    expect(isValidEnvironmentName('a')).toBe(true)
    expect(isValidEnvironmentName('a'.repeat(32))).toBe(true)
  })

  test('rejects what the server rejects', () => {
    expect(isValidEnvironmentName('')).toBe(false)
    expect(isValidEnvironmentName('-staging')).toBe(false) // must start with letter/digit
    expect(isValidEnvironmentName('Staging')).toBe(false) // lower-case only
    expect(isValidEnvironmentName('staging_eu')).toBe(false) // no underscores
    expect(isValidEnvironmentName('a'.repeat(33))).toBe(false) // over 32 chars
    expect(isValidEnvironmentName('staging ')).toBe(false) // no trailing whitespace
  })
})

describe('needsEnvironmentSelector', () => {
  // D2 in both EnvKeys.cs and EnvironmentSelectionMiddleware.cs: the default environment costs
  // nothing — no header, no ?env=. This is the client-side mirror of that contract.
  test('false for default, empty, true for anything else', () => {
    expect(needsEnvironmentSelector(DEFAULT_ENVIRONMENT)).toBe(false)
    expect(needsEnvironmentSelector('')).toBe(false)
    expect(needsEnvironmentSelector('staging')).toBe(true)
    expect(needsEnvironmentSelector('prod-eu')).toBe(true)
  })
})

describe('getStoredEnvironment / setStoredEnvironment without a DOM', () => {
  test('falls back to default when localStorage is unavailable', () => {
    expect(getStoredEnvironment()).toBe(DEFAULT_ENVIRONMENT)
  })

  test('setStoredEnvironment does not throw when window/localStorage are absent', () => {
    expect(() => setStoredEnvironment('staging')).not.toThrow()
    // No storage to persist to, so the read-back is still the default — pins that this is a graceful
    // no-op rather than a silent throw swallowed somewhere upstream.
    expect(getStoredEnvironment()).toBe(DEFAULT_ENVIRONMENT)
  })
})

describe('client.ts environmentHeader()', () => {
  test('is empty with no environment selected (no DOM ⇒ always default here)', () => {
    expect(environmentHeader()).toEqual({})
  })

  test('the header name matches the server constant', () => {
    // EnvironmentSelectionMiddleware.HeaderName in shared/StreamForge.Api/Environments/ — pinned as a
    // literal here (the SPA cannot import C#) so a rename on either side breaks a test instead of
    // silently talking past each other.
    expect(ENVIRONMENT_HEADER).toBe('X-StreamForge-Environment')
  })
})
