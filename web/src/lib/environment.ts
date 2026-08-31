import { useCallback, useEffect, useState } from 'react'

// Plan 021 wave 2 (021-F) — the SPA half of environment isolation. Mirrors lib/theme.ts's shape on
// purpose: state lives in localStorage, not in React context, so every call site (the picker, the
// Topbar badge, client.ts's header injection, the SignalR hub) re-derives it independently and stays
// in sync via a same-tab custom event plus the cross-tab `storage` event. No provider needed even with
// several consumers.
//
// This file is deliberately DUMB: it only stores/reads/broadcasts a name. It does NOT know about the
// REST client, the hub connection, or cache invalidation — those live in api/environments.ts,
// realtime/hub.ts and EnvironmentPicker.tsx respectively, each importing FROM here, so this file never
// has to import any of them back (no cycle).

const STORAGE_KEY = 'sf.environment'
const ENV_CHANGE_EVENT = 'sf-environment-change'

/** How the default environment is spelled everywhere a human or the API sees it — matches
 * EnvKeys.DefaultDisplayName / EnvironmentRecord.Name server-side (shared/StreamsForge.Contracts/EnvironmentModels.cs,
 * shared/StreamsForge.AppCore/Environments/EnvKeys.cs). The SPA never stores the server's internal empty-string
 * spelling; localStorage either holds a real environment name or nothing at all. */
export const DEFAULT_ENVIRONMENT = 'default'

/** Same character class the server enforces at creation (EnvKeys.IsValidName's NamePattern) — checked
 * here only for immediate form feedback; the server remains the authority; a reserved name or a
 * duplicate still comes back as a 400/409 from POST /api/environments for the caller to toast. */
const NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,31}$/

export function isValidEnvironmentName(name: string): boolean {
  return NAME_PATTERN.test(name)
}

/** Whether `env` needs to ride on the wire at all — the header (client.ts) and the hub URL's `?env=`
 * (realtime/hub.ts) both skip themselves for the default environment, matching the server's D2 "the
 * default environment costs nothing" contract (EnvironmentSelectionMiddleware.cs). */
export function needsEnvironmentSelector(env: string): boolean {
  return !!env && env !== DEFAULT_ENVIRONMENT
}

export function getStoredEnvironment(): string {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw && raw.trim() ? raw.trim() : DEFAULT_ENVIRONMENT
  } catch {
    return DEFAULT_ENVIRONMENT
  }
}

/** Persists the selection and notifies same-tab listeners. Deliberately has NO side effects beyond
 * storage + the event — no hub teardown, no cache clear. EnvironmentPicker.tsx orchestrates those
 * around this call, in the order that matters (see its switchTo()). */
export function setStoredEnvironment(name: string): void {
  try {
    if (name === DEFAULT_ENVIRONMENT || !name) {
      localStorage.removeItem(STORAGE_KEY)
    } else {
      localStorage.setItem(STORAGE_KEY, name)
    }
  } catch {
    // ignore (private browsing / storage full) — the in-memory event below still syncs this tab
  }
  try {
    window.dispatchEvent(new Event(ENV_CHANGE_EVENT))
  } catch {
    // SSR/tests
  }
}

/** Reads/writes the selected environment, re-deriving from localStorage on every same-tab change event
 * and the cross-tab `storage` event — exactly useTheme()'s pattern in lib/theme.ts. */
export function useEnvironment() {
  const [environment, setEnvironmentState] = useState<string>(getStoredEnvironment)

  useEffect(() => {
    const sync = () => setEnvironmentState(getStoredEnvironment())
    window.addEventListener(ENV_CHANGE_EVENT, sync)
    window.addEventListener('storage', sync)
    return () => {
      window.removeEventListener(ENV_CHANGE_EVENT, sync)
      window.removeEventListener('storage', sync)
    }
  }, [])

  const setEnvironment = useCallback((name: string) => {
    setStoredEnvironment(name)
    setEnvironmentState(name === DEFAULT_ENVIRONMENT || !name ? DEFAULT_ENVIRONMENT : name)
  }, [])

  return { environment, setEnvironment }
}
