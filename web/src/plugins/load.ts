import { pluginHost } from './registry'
import { pluginTransforms, transpilePlugin } from './transpile'

/**
 * Loads every UI plugin the host serves (see registry.tsx for what a plugin is). Called once from
 * main.tsx before the first render, so an editor is registered before anything can ask for it.
 *
 * ponytail: plain fetch, not `api.get` — the listing is anonymous on purpose (plugins load before login)
 * and this way a stale token can't turn "no plugins" into a redirect. A plugin that 404s, fails to parse
 * or throws on import is logged and skipped; a broken third-party module must not keep the console from
 * starting.
 *
 * `cache: 'no-store'` on the LISTING only: the host versions each plugin's own URL (`?v=...`), so a
 * stale cached listing is the one thing that can serve a stale plugin forever (the versioned file URLs
 * beneath it are safe to cache normally — the version IS the cache key).
 */
export async function loadUiPlugins(): Promise<void> {
  window.streamsforge = pluginHost

  let urls: string[] = []
  try {
    const res = await fetch('/api/ui-plugins', { cache: 'no-store' })
    if (!res.ok) return
    urls = await res.json()
  } catch {
    return // no host, no plugins — the console works exactly as it did before
  }

  await Promise.all(
    urls.map((url) =>
      importPlugin(url).catch((e: unknown) => {
        console.error(`[streamsforge] UI plugin failed to load: ${url}`, e)
      }),
    ),
  )
}

/**
 * Imports one plugin module, transpiling it first when it is TypeScript (`.ts`/`.tsx`, sniffed from the
 * URL by `pluginTransforms`). A plain `.js`/`.mjs` module imports directly — the pre-existing path,
 * unchanged. The transpiled-to-JS case goes through a `blob:` URL because `import()` needs a URL, not a
 * string of source; the blob is revoked once the import settles (success or failure) so it doesn't leak.
 */
export async function importPlugin(url: string): Promise<unknown> {
  if (!pluginTransforms(url)) {
    return import(/* @vite-ignore */ url)
  }

  // The versioned URL (`?v=...`) IS the cache key for a file response — no `no-store` needed here,
  // unlike the listing fetch above.
  const res = await fetch(url)
  const source = await res.text()
  const js = await transpilePlugin(source, url)
  const blobUrl = URL.createObjectURL(new Blob([js], { type: 'text/javascript' }))
  try {
    return await import(/* @vite-ignore */ blobUrl)
  } finally {
    URL.revokeObjectURL(blobUrl)
  }
}
