// Pins the merge rules of `web/src/plugins/suggest.ts` — the ONE hook a UI plugin uses to propose a
// patch to the rest of the source form it's embedded in. Pure logic, no DOM.
import { describe, expect, test } from 'bun:test'
import { applySuggestion, type EditorDraft } from '../src/plugins/suggest'

function draft(overrides: Partial<EditorDraft> = {}): EditorDraft {
  return { name: '', description: '', fields: [], tags: [], ...overrides }
}

describe('applySuggestion', () => {
  test('a name is applied only while the draft is blank', () => {
    expect(applySuggestion(draft(), { name: 'orders' })).toEqual({ name: 'orders' })
    expect(applySuggestion(draft({ name: 'existing' }), { name: 'orders' })).toEqual({})
  })

  test('whitespace-only counts as blank, both ways', () => {
    expect(applySuggestion(draft({ name: '   ' }), { name: 'orders' })).toEqual({ name: 'orders' })
    expect(applySuggestion(draft(), { name: '   ' })).toEqual({})
  })

  test('description follows the identical blank-only rule', () => {
    expect(applySuggestion(draft(), { description: 'from nats' })).toEqual({ description: 'from nats' })
    expect(applySuggestion(draft({ description: 'already typed' }), { description: 'from nats' })).toEqual({})
  })

  test('fields replace wholesale when the patch has some', () => {
    const fields = [{ name: 'id', type: 'Long' as const }]
    expect(applySuggestion(draft({ fields: [{ name: 'old', type: 'String' as const }] }), { fields })).toEqual({
      fields,
    })
  })

  test('an empty fields array in the patch is a no-op, not "clear the fields"', () => {
    const existing = [{ name: 'id', type: 'Long' as const }]
    expect(applySuggestion(draft({ fields: existing }), { fields: [] })).toEqual({})
  })

  test('tags union with the draft, deduped', () => {
    expect(applySuggestion(draft({ tags: ['a'] }), { tags: ['a', 'b'] })).toEqual({ tags: ['a', 'b'] })
  })

  test('re-suggesting the same tags is a no-op (union is not longer)', () => {
    expect(applySuggestion(draft({ tags: ['a', 'b'] }), { tags: ['b'] })).toEqual({})
    expect(applySuggestion(draft({ tags: ['a', 'b'] }), { tags: [] })).toEqual({})
  })

  test('nothing in the patch applies against a filled-in draft: {}', () => {
    const full = draft({ name: 'orders', description: 'desc', tags: ['x'] })
    expect(applySuggestion(full, { name: 'other', description: 'other', tags: ['x'] })).toEqual({})
  })

  test('a patch with nothing set at all is a no-op', () => {
    expect(applySuggestion(draft(), {})).toEqual({})
  })
})
