import { api } from './client'
import type { TransportCatalog, TransportDescriptor } from './types'

/**
 * Plan 010: the registered message transports and their form descriptors. The console renders config
 * editors from these, so a transport added to the backend needs no change in `web/` at all.
 *
 * Cached for the lifetime of the page: the registry is fixed at host startup and cannot change without a
 * restart, and this is fetched from two unrelated places (the source modal, the sinks editor) that would
 * otherwise each pay for it.
 */
let cached: Promise<TransportCatalog> | null = null

export const transportsApi = {
  catalog(): Promise<TransportCatalog> {
    cached ??= api.get<TransportCatalog>('/api/transports').catch((e: unknown) => {
      cached = null // a failed fetch must not poison the cache — the next caller retries
      throw e
    })
    return cached
  },

  /** Test/HMR seam — the cache is otherwise process-lifetime by design. */
  clearCache() {
    cached = null
  },
}

export function findDescriptor(list: TransportDescriptor[], kind: string | undefined): TransportDescriptor | undefined {
  return kind ? list.find((d) => d.kind.toLowerCase() === kind.toLowerCase()) : undefined
}
