import type { AuditEntry } from '../../api/types'

/** Pretty-print if it parses, hand back verbatim if it does not — an over-cap row degrades to the
 *  changed field *names* rather than a truncated blob, and that string is worth showing as-is. */
function pretty(json: string | null | undefined): string | null {
  if (!json) return null
  try {
    return JSON.stringify(JSON.parse(json), null, 2)
  } catch {
    return json
  }
}

/** Top-level keys whose serialization moved. The server already recorded only the properties that
 *  changed on an update, so this is normally every key present; it earns its place on a create/delete,
 *  where one side is the whole document.
 *
 *  A rotated credential shows up here even though both sides render `***`: wave 5 decides WHICH
 *  properties moved on the unmasked pair and only then masks, so the key's presence is the signal —
 *  the values deliberately are not. */
function changedKeys(before: string | null, after: string | null): string[] {
  const parse = (s: string | null): Record<string, unknown> | null => {
    if (!s) return null
    try {
      const v: unknown = JSON.parse(s)
      return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : null
    } catch {
      return null
    }
  }
  const b = parse(before)
  const a = parse(after)
  if (!b && !a) return []
  const keys = new Set([...Object.keys(b ?? {}), ...Object.keys(a ?? {})])
  return [...keys].filter((k) => JSON.stringify(b?.[k]) !== JSON.stringify(a?.[k])).sort()
}

function Side({ label, body, empty }: { label: string; body: string | null; empty: string }) {
  return (
    <div className="min-w-0 flex-1">
      <div className="mb-1 text-xs font-medium text-muted-foreground">{label}</div>
      {body === null ? (
        <div className="rounded-md border border-dashed border-border px-3 py-2 text-xs text-muted-foreground">{empty}</div>
      ) : (
        <pre className="max-h-96 overflow-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs whitespace-pre text-foreground/90">
          {body}
        </pre>
      )}
    </div>
  )
}

/**
 * The before/after pair for one audit row.
 *
 * ponytail: a formatted-JSON pair plus the list of top-level keys that moved — no diff library, no
 * per-line alignment. Ceiling: a large nested `config` shows as two blocks and the reader does the
 * character-level comparison themselves. Upgrade path is a line-level LCS in this file if that ever
 * stops being good enough; nothing outside it would change.
 */
export function ChangeDiff({ entry, included, withheld }: { entry: AuditEntry; included: boolean; withheld: number }) {
  const before = pretty(entry.beforeJson)
  const after = pretty(entry.afterJson)

  if (!before && !after) {
    // The distinction the contract insists on: "this row never had a diff" is not the same statement
    // as "this row had one and you were not given it". Rendering an empty pair for the second would be
    // exactly the silence the withheld counter exists to break.
    return (
      <p className="text-xs text-muted-foreground">
        {!included && withheld > 0
          ? 'This response withheld before/after payloads. If this row carried one, it is not shown — enable "include changes" (needs access.read).'
          : 'No before/after payload was recorded for this row. Denied decisions and non-catalog actions carry none.'}
      </p>
    )
  }

  const keys = changedKeys(entry.beforeJson ?? null, entry.afterJson ?? null)

  return (
    <div className="flex flex-col gap-3">
      {keys.length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
          <span>Changed:</span>
          {keys.map((k) => (
            <code key={k} className="rounded bg-muted px-1.5 py-0.5 font-mono text-foreground/80">
              {k}
            </code>
          ))}
        </div>
      )}
      <div className="flex flex-col gap-4 md:flex-row">
        <Side label="Before" body={before} empty="Nothing — this row created the entity." />
        <Side label="After" body={after} empty="Nothing — this row deleted the entity." />
      </div>
      <p className="text-xs text-muted-foreground">
        Credentials are masked to <code className="font-mono">***</code> before the row is written, so two identical
        masks can still be a rotation — the property appearing above is the signal, not its value.
      </p>
    </div>
  )
}
