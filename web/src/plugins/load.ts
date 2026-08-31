import { pluginHost } from './registry'

/**
 * Loads every UI plugin the host serves (see registry.tsx for what a plugin is). Called once from
 * main.tsx before the first render, so an editor is registered before anything can ask for it.
 *
 * ponytail: plain fetch, not `api.get` — the listing is anonymous on purpose (plugins load before login)
 * and this way a stale token can't turn "no plugins" into a redirect. A plugin that 404s, fails to parse
 * or throws on import is logged and skipped; a broken third-party module must not keep the console from
 * starting.
 */
export async function loadUiPlugins(): Promise<void> {
  window.streamsforge = pluginHost

  let urls: string[] = []
  try {
    const res = await fetch('/api/ui-plugins')
    if (!res.ok) return
    urls = await res.json()
  } catch {
    return // no host, no plugins — the console works exactly as it did before
  }

  await Promise.all(
    urls.map((url) =>
      import(/* @vite-ignore */ url).catch((e: unknown) => {
        console.error(`[streamsforge] UI plugin failed to load: ${url}`, e)
      }),
    ),
  )
}
