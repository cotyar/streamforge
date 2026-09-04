import type { FieldDef, Tags } from '@/api/types'

/**
 * The plugin-facing "richer editor" contract (integrator #5/#6): a UI plugin for a source's transport
 * can see the rest of the source form as it stands (`draft`) and propose a patch to it (`onSuggest`) —
 * e.g. a NATS editor that infers a sensible name/description/fields from the subject it just probed.
 *
 * Deliberately ONE hook rather than N callbacks (`onNameSuggested`, `onFieldsSuggested`, …): a plugin
 * calls `onSuggest({ name, fields })` with whatever it has an opinion about, and `applySuggestion`
 * below is the single, testable place the merge rules live.
 */
export interface EditorDraft {
  name: string
  description: string
  fields: FieldDef[]
  tags: Tags
}

/** Whatever a plugin has an opinion about — every key optional, nothing required to agree on. */
export type EditorSuggestion = Partial<EditorDraft>

/**
 * Pure merge: what of `patch` actually applies against the current `draft`, as a patch to apply (not
 * the full merged draft — the caller decides how to fold it in, e.g. re-syncing mapping source paths
 * when `fields` changes).
 *
 * Rules (ponytail — a plugin that fires from an effect on every render must not loop):
 * - `name`/`description`: only while the draft's own value is blank after trimming (a plugin proposing
 *   a name never clobbers one the user already typed, or another plugin already suggested).
 * - `fields`: replaces wholesale, but only when the patch actually has some (an empty array is a no-op,
 *   not "clear the fields").
 * - `tags`: unioned with the draft's tags, and only included in the result when that union is actually
 *   longer than what's there — so re-suggesting the same tags twice is a no-op.
 * - Nothing that applies → `{}` (an empty object, not `undefined`, so `Object.keys(...).length` is the
 *   caller's one no-op check).
 */
export function applySuggestion(draft: EditorDraft, patch: EditorSuggestion): EditorSuggestion {
  const out: EditorSuggestion = {}

  if (patch.name?.trim() && !draft.name.trim()) {
    out.name = patch.name
  }

  if (patch.description?.trim() && !draft.description.trim()) {
    out.description = patch.description
  }

  if (patch.fields && patch.fields.length > 0) {
    out.fields = patch.fields
  }

  if (patch.tags && patch.tags.length > 0) {
    const union = [...new Set([...draft.tags, ...patch.tags])]
    if (union.length > draft.tags.length) {
      out.tags = union
    }
  }

  return out
}
