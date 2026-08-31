import { Plus, Trash2 } from 'lucide-react'
import type { PermissionEffect, PermissionGrant } from '../../api/types'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

/** The permission grammar's vocabulary, mirrored from `Actions` in StreamsForge.Contracts/AccessModels.cs.
 *  Offered as a datalist, never enforced: `action` is a free string with `*` wildcards on the server and
 *  a picker that refused `pipeline.*` would be lying about the grammar.
 *
 *  ponytail: hand-copied. Ceiling: a new action constant added server-side does not appear here until
 *  someone copies it. Upgrade path is a `GET /api/access/actions` route returning the vocabulary — worth
 *  it when the list stops being one screen long, not before. */
export const KNOWN_ACTIONS = [
  '*',
  'source.read', 'source.write', 'source.delete', 'source.ingest', 'source.run',
  'pipeline.read', 'pipeline.write', 'pipeline.delete', 'pipeline.control',
  'table.read', 'table.write', 'table.delete', 'table.control',
  'config.export', 'config.replace',
  'user.read', 'user.write',
  'access.read', 'access.write',
  'audit.read',
  'approval.request', 'approval.decide', 'approval.bypass',
  'chat.use',
  'catalog.read', 'catalog.write',
] as const

const ACTIONS_DATALIST_ID = 'sf-access-actions'

/** Rendered once per page so every action input on it can share one datalist. */
export function ActionsDatalist() {
  return (
    <datalist id={ACTIONS_DATALIST_ID}>
      {KNOWN_ACTIONS.map((a) => (
        <option key={a} value={a} />
      ))}
    </datalist>
  )
}

export function emptyGrant(): PermissionGrant {
  return { action: '', scope: '*', effect: 'Allow', requiresApproval: false, note: null }
}

/**
 * The `action` / `scope` / `effect` / `requiresApproval` form, which roles, groups and per-user entries
 * all need identically.
 *
 * Two rules the server owns and this editor only surfaces:
 * - `effect: 'Deny'` wins over any Allow anywhere (deny-overrides), so a Deny row is styled as the loud
 *   one it is.
 * - `requiresApproval` on an *Allow* is the tri-state's third answer and reaches a button label
 *   ("Request approval…"). On a Deny it is inert — the decision is already no — so the switch is
 *   disabled there rather than silently ignored.
 */
export function GrantEditor({
  grants,
  onChange,
  disabled = false,
}: {
  grants: PermissionGrant[]
  onChange: (next: PermissionGrant[]) => void
  disabled?: boolean
}) {
  const patch = (i: number, fields: Partial<PermissionGrant>) =>
    onChange(grants.map((g, idx) => (idx === i ? { ...g, ...fields } : g)))

  return (
    <div className="flex flex-col gap-2">
      <div className="grid grid-cols-[1fr_1fr_7rem_5.5rem_2rem] items-center gap-2 px-1 text-xs font-medium text-muted-foreground">
        <span>Action</span>
        <span>Scope</span>
        <span>Effect</span>
        <span title="Allowed, but only after a second pair of eyes">Approval</span>
        <span />
      </div>

      {grants.length === 0 && (
        <p className="rounded-md border border-dashed border-border px-3 py-4 text-center text-sm text-muted-foreground">
          No grants — this confers nothing on its own.
        </p>
      )}

      {grants.map((g, i) => (
        <div key={i} className="grid grid-cols-[1fr_1fr_7rem_5.5rem_2rem] items-center gap-2">
          <Input
            value={g.action}
            list={ACTIONS_DATALIST_ID}
            disabled={disabled}
            placeholder="pipeline.write"
            aria-label="Action"
            onChange={(e) => patch(i, { action: e.target.value })}
          />
          <Input
            value={g.scope}
            disabled={disabled}
            placeholder="* | name | prod-* | tag:finance"
            aria-label="Scope"
            onChange={(e) => patch(i, { scope: e.target.value })}
          />
          <Select
            value={g.effect}
            disabled={disabled}
            onValueChange={(v) =>
              patch(i, {
                effect: v as PermissionEffect,
                // A Deny that "requires approval" is a contradiction the evaluator resolves as Deny;
                // clearing it here keeps the stored document from carrying the contradiction at all.
                requiresApproval: v === 'Deny' ? false : g.requiresApproval,
              })
            }
          >
            <SelectTrigger className="w-full" aria-label="Effect">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="Allow">Allow</SelectItem>
              <SelectItem value="Deny">Deny</SelectItem>
            </SelectContent>
          </Select>
          <div className="flex justify-center">
            <Switch
              checked={g.requiresApproval}
              disabled={disabled || g.effect === 'Deny'}
              aria-label="Requires approval"
              title={
                g.effect === 'Deny'
                  ? 'A Deny is already a no — approval does not apply'
                  : 'Allowed only after an approval request is granted'
              }
              onCheckedChange={(v) => patch(i, { requiresApproval: v })}
            />
          </div>
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            disabled={disabled}
            title="Remove grant"
            className="hover:text-destructive"
            onClick={() => onChange(grants.filter((_, idx) => idx !== i))}
          >
            <Trash2 />
          </Button>
        </div>
      ))}

      {!disabled && (
        <div>
          <Button type="button" variant="outline" size="sm" onClick={() => onChange([...grants, emptyGrant()])}>
            <Plus data-icon="inline-start" /> Add grant
          </Button>
        </div>
      )}

      {grants.some((g) => g.effect === 'Deny') && (
        <p className="text-xs text-muted-foreground">
          Deny overrides: a matching Deny anywhere — role, group or user — beats every Allow.
        </p>
      )}
    </div>
  )
}

/** Read-only, for tables. Compact enough to sit in a cell and honest about what it truncates. */
export function GrantSummary({ grants, max = 3 }: { grants: PermissionGrant[]; max?: number }) {
  if (grants.length === 0) {
    return <span className="text-xs text-muted-foreground">none</span>
  }
  const shown = grants.slice(0, max)
  return (
    <div className="flex flex-wrap items-center gap-1">
      {shown.map((g, i) => (
        <Badge
          key={i}
          variant={g.effect === 'Deny' ? 'destructive' : g.requiresApproval ? 'outline' : 'secondary'}
          title={`${g.effect} ${g.action} on ${g.scope}${g.requiresApproval ? ' (requires approval)' : ''}${g.note ? ` — ${g.note}` : ''}`}
        >
          {g.effect === 'Deny' ? '!' : g.requiresApproval ? '?' : ''}
          {g.action}
          {g.scope !== '*' && <span className="opacity-70">@{g.scope}</span>}
        </Badge>
      ))}
      {grants.length > shown.length && (
        <span className="text-xs text-muted-foreground">+{grants.length - shown.length}</span>
      )}
    </div>
  )
}
