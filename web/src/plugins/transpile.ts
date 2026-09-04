import type { Transform } from 'sucrase'

/**
 * TypeScript UI plugins (integrator #7): a plugin can be a single `.ts`/`.tsx` file, served verbatim
 * (`text/plain`) by the host and transpiled in the browser — no build step, no bundler, matching the
 * existing "drop one file in ui-plugins/" story for plain `.js`/`.mjs`.
 *
 * `sucrase` is the transpiler (~620 KB ESM) — small enough to ship, but still only worth paying for
 * when a plugin actually needs it, hence the lazy `import('sucrase')` in `transpilePlugin` below. This
 * module itself imports only sucrase's TYPES (erased at compile time), so importing `transpile.ts`
 * never pulls the package into a console that has no TS plugin.
 */

/** `null` = serve/import as-is (a plain ES module); otherwise the sucrase transforms this file needs.
 *  Strips a cache-busting `?v=...` query before matching, and matches extensions case-insensitively. */
export function pluginTransforms(url: string): Transform[] | null {
  const pathname = new URL(url, 'http://x').pathname.toLowerCase()
  if (pathname.endsWith('.tsx')) return ['typescript', 'jsx']
  if (pathname.endsWith('.ts')) return ['typescript']
  return null
}

/**
 * Transpiles one plugin's source to plain JS. `production: true` + `disableESTransforms: true` keep
 * sucrase's output as close to the input as sucrase allows (no dev-mode JSX helpers); no `jsxPragma`
 * option is passed, so its default JSX runtime applies — classic (`React.createElement`), NOT the
 * automatic runtime, which emits `import "react/jsx-runtime"` and would be unresolvable from a `blob:`
 * module. A plugin author writes `const React = window.streamsforge.react` for exactly that reason.
 */
export async function transpilePlugin(source: string, url: string): Promise<string> {
  const transforms = pluginTransforms(url)
  if (!transforms) return source
  const { transform } = await import('sucrase')
  return transform(source, {
    transforms,
    production: true,
    disableESTransforms: true,
  }).code
}
