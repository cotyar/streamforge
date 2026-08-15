import { api } from './client'

export interface SqlFunctionCatalog {
  scalars: string[]
  aggregates: string[]
  registeredScalars: string[]
  registeredAggregates: string[]
}

/**
 * Which functions this deployment's SQL dialect actually has. The editor's own lists stay as the
 * offline fallback (they cover every built-in, so completion and highlighting work before this
 * resolves, or if it fails) — what this adds is the ones registered at host startup, which the
 * console cannot know statically.
 *
 * Cached for the page's lifetime, same as the transport catalog and for the same reason: the registry
 * is fixed at host startup and cannot change without a restart.
 */
let cached: Promise<SqlFunctionCatalog> | null = null

export const sqlFunctionsApi = {
  catalog(): Promise<SqlFunctionCatalog> {
    cached ??= api.get<SqlFunctionCatalog>('/api/sql/functions').catch((e: unknown) => {
      cached = null // a failed fetch must not poison the cache — the next caller retries
      throw e
    })
    return cached
  },

  clearCache() {
    cached = null
  },
}
