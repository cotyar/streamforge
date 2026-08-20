import { useEffect, useState } from 'react'
import type { NamedEndpoint } from '@/api/types'
import { api } from '@/api/client'

// Fetched once per page load and shared by every hint on screen — the map is configuration, not
// catalog (see NamedEndpoints.cs's class doc), so it cannot change without a restart, and there is no
// reason for two fields on the same form to each pay for their own round trip.
let cached: NamedEndpoint[] | null = null
let inflight: Promise<NamedEndpoint[]> | null = null

function loadEndpoints(): Promise<NamedEndpoint[]> {
  if (cached) return Promise.resolve(cached)
  inflight ??= api
    .get<NamedEndpoint[]>('/api/meta/endpoints')
    .then((list) => {
      cached = list
      return list
    })
    .catch(() => [] as NamedEndpoint[]) // degrade silently — see GrpcConfigEditor's identical peers fetch
  return inflight
}

/**
 * Plan 016 wave 6 — the SPA half of named external endpoints (see `NamedEndpoints.cs`'s class doc for
 * the whole feature). A value that is ENTIRELY `@name` is a reference; this renders, right under the
 * field that carries it, whether THIS instance's `Endpoints:<name>` configuration knows that name —
 * the same "will this land here" question `POST /api/config/import?mode=validate` answers for a whole
 * document, answered inline while a human is still typing one field. Renders nothing for a literal
 * value (the overwhelmingly common case) and nothing before the one-time fetch resolves.
 *
 * <p>ponytail: wired into `sources/UrlConfigEditor.tsx`'s URL field only — the plan's own first
 * example (`UrlPollConfig.url`) and the field most likely to carry a
 * `@name` in practice. Every other endpoint-shaped field the wave 6 import walk also covers —
 * `GrpcConfigEditor`'s address/restAddress, `TransportConfigEditor`'s nats/db/fix host fields,
 * `SinksEditor`'s nats/http/db fields — gets the identical one-line addition
 * (`<EndpointRefHint value={...} />` under the `Input`) when it earns the screen space; the
 * fetch-once cache above already covers all of them for free, so wiring in the rest costs nothing
 * beyond the JSX line itself. Deliberately not a management page: the list rendered here is read-only,
 * because the map itself is (see `NamedEndpoints`' own class doc — configuration, never the catalog,
 * so there is nothing for a console to edit).</p>
 */
export function EndpointRefHint({ value }: { value: string }) {
  const [endpoints, setEndpoints] = useState<NamedEndpoint[] | null>(cached)

  useEffect(() => {
    if (endpoints) return
    let cancelled = false
    loadEndpoints().then((list) => {
      if (!cancelled) setEndpoints(list)
    })
    return () => {
      cancelled = true
    }
  }, [endpoints])

  const trimmed = value.trim()
  const isReference = trimmed.length > 1 && trimmed[0] === '@'
  if (!isReference || endpoints === null) return null

  const name = trimmed.slice(1)
  const known = endpoints.find((e) => e.name === name)

  if (known) {
    return (
      <p className="mt-1 text-[11px] text-primary">
        Resolves here to <span className="font-mono">{known.value}</span>.
      </p>
    )
  }

  return (
    <p className="mt-1 text-[11px] text-destructive">
      This instance has no endpoint named <span className="font-mono">{name}</span>
      {endpoints.length > 0 ? <> (known: {endpoints.map((e) => e.name).join(', ')})</> : null}. Fine
      to import — resolution happens per environment; only a live connect attempt here would fail.
    </p>
  )
}
