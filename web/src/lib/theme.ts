import { useCallback, useEffect, useState } from 'react'

export type Theme = 'light' | 'dark'

const STORAGE_KEY = 'sf.theme'
const THEME_CHANGE_EVENT = 'sf-theme-change'

function isTheme(value: string | null): value is Theme {
  return value === 'light' || value === 'dark'
}

function currentTheme(): Theme {
  if (typeof document === 'undefined') return 'light'
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light'
}

function applyTheme(theme: Theme) {
  document.documentElement.classList.toggle('dark', theme === 'dark')
  localStorage.setItem(STORAGE_KEY, theme)
  window.dispatchEvent(new Event(THEME_CHANGE_EVENT))
}

/**
 * Reads/writes the app theme. State lives in localStorage + the `dark` class
 * on <html> (set synchronously by the inline script in index.html to avoid a
 * flash), not in React context — every call site re-derives it from the DOM
 * and stays in sync via a same-tab custom event plus the cross-tab `storage`
 * event, so no provider is needed even with multiple consumers (e.g. the
 * theme toggle and the Sonner toaster).
 */
export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(currentTheme)

  useEffect(() => {
    const sync = () => setThemeState(currentTheme())
    window.addEventListener(THEME_CHANGE_EVENT, sync)
    window.addEventListener('storage', sync)
    return () => {
      window.removeEventListener(THEME_CHANGE_EVENT, sync)
      window.removeEventListener('storage', sync)
    }
  }, [])

  const setTheme = useCallback((next: Theme) => {
    applyTheme(next)
    setThemeState(next)
  }, [])

  const toggleTheme = useCallback(() => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }, [theme, setTheme])

  return { theme, setTheme, toggleTheme }
}

export function getStoredTheme(): Theme {
  const stored = typeof localStorage !== 'undefined' ? localStorage.getItem(STORAGE_KEY) : null
  return isTheme(stored) ? stored : 'light'
}
