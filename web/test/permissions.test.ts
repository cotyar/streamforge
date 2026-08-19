// Plan 015 wave 6-A — the client evaluator's tests, run with `bun test web/test`.
//
// WHY THIS FILE LIVES OUTSIDE src/. web/tsconfig.json includes only `src`, so a test file here is
// neither typechecked by `tsc -b` nor pulled into the vite bundle — the SPA ships without it and
// without a test runner. Bun's runner is built in, so this costs zero dependencies; the repo adds none
// for it.
//
// WHAT IS BEING PINNED. web/src/api/permissions.ts is the THIRD implementation of one authorization
// grammar (PermissionEvaluator.cs is the first, and ApprovalStateMachine shares its ScopeMatches since
// wave 4). Neither .NET suite can see a divergence in this one, so the cases below are deliberately
// the same cases the C# tests pin: deny-overrides beating a broader allow, a disabled user denied even
// `*`, prefix and `tag:` scopes, the most-SPECIFIC-Allow rule on the approval axis (wave 8), and the
// dot-crossing `*`. The last block pins the OTHER half — the ordinal fallback for a server that sends
// no permissions[] at all, which must reproduce today's Viewer/Editor/Admin answer exactly or a
// rolling deploy locks people out of buttons that still work.

import { describe, expect, test } from 'bun:test'
import { can, decide, globMatch, scopeMatches, specificity } from '../src/api/permissions'
import { __testing } from '../src/api/auth'
import type { PermissionGrant } from '../src/api/types'

function allow(action: string, scope = '*', requiresApproval = false): PermissionGrant {
  return { action, scope, effect: 'Allow', requiresApproval }
}

function deny(action: string, scope = '*'): PermissionGrant {
  return { action, scope, effect: 'Deny', requiresApproval: false }
}

describe('glob matching', () => {
  test('* covers any run of characters, including dots', () => {
    expect(globMatch('*', 'pipeline.write')).toBe(true)
    expect(globMatch('pipeline.*', 'pipeline.write')).toBe(true)
    // The deliberate dot-crossing: a future third segment stays covered rather than silently falling
    // out of every existing `x.*` entitlement.
    expect(globMatch('pipeline.*', 'pipeline.write.sql')).toBe(true)
    // …and the boundary the C# pins: the pattern demands the dot.
    expect(globMatch('pipeline.*', 'pipeline')).toBe(false)
  })

  test('matching is exact and case-sensitive, and an empty pattern matches nothing', () => {
    expect(globMatch('pipeline.write', 'pipeline.write')).toBe(true)
    expect(globMatch('pipeline.write', 'pipeline.read')).toBe(false)
    expect(globMatch('Pipeline.*', 'pipeline.write')).toBe(false)
    expect(globMatch('', 'pipeline.write')).toBe(false)
    expect(globMatch('', '')).toBe(true)
  })

  test('backtracking: multiple stars', () => {
    expect(globMatch('*.write.*', 'pipeline.write.sql')).toBe(true)
    expect(globMatch('a*b*c', 'axxbyyc')).toBe(true)
    expect(globMatch('a*b*c', 'axxbyy')).toBe(false)
  })
})

describe('scope grammar', () => {
  test('* | exact | prefix', () => {
    expect(scopeMatches('*', 'prod-orders')).toBe(true)
    expect(scopeMatches('prod-orders', 'prod-orders')).toBe(true)
    expect(scopeMatches('prod-orders', 'prod-orders-eu')).toBe(false)
    expect(scopeMatches('prod-*', 'prod-orders')).toBe(true)
    expect(scopeMatches('prod-*', 'dev-orders')).toBe(false)
    // Case-sensitive on purpose: an entitlement that widened itself is the one direction of surprise
    // authorization must not have.
    expect(scopeMatches('prod-*', 'PROD-Sandbox')).toBe(false)
  })

  test('tag: matches on the resource tags, and misses when none were passed', () => {
    expect(scopeMatches('tag:finance', 'anything', ['finance', 'eu'])).toBe(true)
    expect(scopeMatches('tag:finance', 'anything', ['eu'])).toBe(false)
    expect(scopeMatches('tag:pii-*', 'anything', ['pii-names'])).toBe(true)
    // No tags supplied is a MISS, not "unknown, so allow" — otherwise every call site that forgot to
    // pass tags would silently widen every tag entitlement.
    expect(scopeMatches('tag:finance', 'anything')).toBe(false)
    expect(scopeMatches('tag:finance', 'anything', [])).toBe(false)
  })

  test('asking at * is answered only by a *-scoped grant', () => {
    // A caller holding prod-* cannot do the global thing.
    expect(decide([allow('config.replace', 'prod-*')], 'config.replace', '*')).toBe('Denied')
    expect(decide([allow('config.replace', '*')], 'config.replace', '*')).toBe('Allowed')
  })
})

describe('decide', () => {
  test('a matching allow allows; nothing matching denies', () => {
    expect(decide([allow('pipeline.write')], 'pipeline.write', 'orders')).toBe('Allowed')
    expect(decide([allow('pipeline.read')], 'pipeline.write', 'orders')).toBe('Denied')
    expect(decide([], 'pipeline.write', 'orders')).toBe('Denied')
    expect(decide(null, 'pipeline.write', 'orders')).toBe('Denied')
  })

  test('deny overrides a broader allow, whichever order they are in', () => {
    const broadThenNarrow = [allow('*', '*'), deny('pipeline.write', 'prod-*')]
    expect(decide(broadThenNarrow, 'pipeline.write', 'prod-orders')).toBe('Denied')
    expect(decide(broadThenNarrow, 'pipeline.write', 'dev-orders')).toBe('Allowed')

    // The Allow does not win by coming second either: the whole list is walked.
    const narrowThenBroad = [deny('pipeline.write', 'prod-*'), allow('*', '*')]
    expect(decide(narrowThenBroad, 'pipeline.write', 'prod-orders')).toBe('Denied')
  })

  test('a deny on a tag beats an allow on the name', () => {
    const grants = [allow('table.write', '*'), deny('table.write', 'tag:finance')]
    expect(decide(grants, 'table.write', 'ledger', ['finance'])).toBe('Denied')
    // …and does not fire when the resource does not carry the tag.
    expect(decide(grants, 'table.write', 'ledger', ['ops'])).toBe('Allowed')
  })

  test('a disabled user is denied everything, including a * on *', () => {
    const admin = { grants: [allow('*', '*')], disabled: true }
    expect(decide(admin, 'pipeline.write', 'orders')).toBe('Denied')
    expect(decide(admin, 'source.read', '*')).toBe('Denied')
    // The same grants without the flag are the Admin answer, so the flag is what did it.
    expect(decide({ grants: [allow('*', '*')] }, 'pipeline.write', 'orders')).toBe('Allowed')
  })

  test('alice: her narrow prod-* Allow beats her broad approval-gated one', () => {
    // Renamed in wave 8 (was "the approval axis takes the MOST permissive matching Allow"); every
    // assertion is UNCHANGED. The answer is the same, but the reason is now that `prod-*` is MORE
    // SPECIFIC than `*`, not that it is unconditional.
    //
    // "alice may deploy to prod-*, and separately alice may deploy anywhere with an approval" must not
    // force alice through an approval for prod.
    const grants = [allow('pipeline.control', '*', true), allow('pipeline.control', 'prod-*')]
    expect(decide(grants, 'pipeline.control', 'prod-orders')).toBe('Allowed')
    expect(decide(grants, 'pipeline.control', 'dev-orders')).toBe('RequiresApproval')

    // Order-independent.
    expect(decide([...grants].reverse(), 'pipeline.control', 'prod-orders')).toBe('Allowed')

    // A Deny still beats both.
    expect(decide([...grants, deny('pipeline.control', 'prod-*')], 'pipeline.control', 'prod-orders')).toBe('Denied')
  })

  // ---------------------------------------------------------- specificity on the approval axis (wave 8)
  //
  // These are the same cases shared/StreamForge.AppCore.Tests/Access/PermissionEvaluatorTests.cs pins
  // for PermissionEvaluator.Specificity. A divergence here is a security bug no .NET suite can see —
  // the SPA would offer a plain "Delete" button for an action the server will answer with
  // "requires approval", or hide one the operator is entitled to use outright.

  test('operator: a narrow approval grant beats a role blanket Allow (015 finding 1)', () => {
    const grants = [allow('pipeline.delete', '*'), allow('pipeline.delete', 'dev-*', true)]
    expect(decide(grants, 'pipeline.delete', 'dev-thing')).toBe('RequiresApproval')
    expect(decide(grants, 'pipeline.delete', 'prod-thing')).toBe('Allowed')
    // Order-independent: the score decides, not the position in the list.
    expect(decide([...grants].reverse(), 'pipeline.delete', 'dev-thing')).toBe('RequiresApproval')
  })

  test('a narrower prefix beats a broader one, and an exact name beats both', () => {
    const nested = [allow('table.delete', 'prod-*', true), allow('table.delete', 'prod-sandbox-*')]
    expect(decide(nested, 'table.delete', 'prod-sandbox-1')).toBe('Allowed')
    expect(decide(nested, 'table.delete', 'prod-orders')).toBe('RequiresApproval')

    expect(decide([allow('table.delete', 'prod-*'), allow('table.delete', 'prod-orders', true)], 'table.delete', 'prod-orders')).toBe('RequiresApproval')
    expect(decide([allow('table.delete', 'prod-*', true), allow('table.delete', 'prod-orders')], 'table.delete', 'prod-orders')).toBe('Allowed')
  })

  test('tag: outranks * and loses to a name scope', () => {
    const tagged = allow('table.write', 'tag:finance', true)
    expect(decide([allow('table.write', '*'), tagged], 'table.write', 'ledger', ['finance'])).toBe('RequiresApproval')
    // The documented cost of that placement: a name-scoped Allow beats the tag gate.
    expect(decide([allow('table.write', 'prod-*'), tagged], 'table.write', 'prod-ledger', ['finance'])).toBe('Allowed')
  })

  test('a more specific action beats a broader one on the same scope', () => {
    const grants = [allow('table.*', '*'), allow('table.delete', '*', true)]
    expect(decide(grants, 'table.delete', 't1')).toBe('RequiresApproval')
    expect(decide(grants, 'table.write', 't1')).toBe('Allowed')
  })

  test('an equal-specificity tie goes to RequiresApproval, either order', () => {
    // `table.*` (6 literals) on the exact `prod-orders` (11) scores exactly what `table.delete` (12)
    // on the prefix `prod-*` (5) does.
    const gated = allow('table.*', 'prod-orders', true)
    const plain = allow('table.delete', 'prod-*')
    expect(specificity(gated)).toBe(specificity(plain))
    expect(decide([gated, plain], 'table.delete', 'prod-orders')).toBe('RequiresApproval')
    expect(decide([plain, gated], 'table.delete', 'prod-orders')).toBe('RequiresApproval')
  })

  test('specificity never outranks a Deny, however specific the Allow is', () => {
    // The deliberate narrowing: the ladder applies ONLY to the approval axis, so an exact-scope Allow
    // cannot punch a hole in a guardrail Deny.
    const grants = [allow('pipeline.delete', 'prod-orders'), deny('pipeline.*', 'prod-*')]
    expect(decide(grants, 'pipeline.delete', 'prod-orders')).toBe('Denied')
  })

  test('specificity is the documented ladder: * < tag: < prefix < exact, on both axes', () => {
    const score = (action: string, scope: string) => specificity(allow(action, scope))
    expect(score('*', '*')).toBe(0)
    expect(score('*', 'tag:finance')).toBeLessThan(score('*', 'prod-*'))
    expect(score('*', 'prod-*')).toBeLessThan(score('*', 'prod-orders'))
    expect(score('*', 'prod-*')).toBeLessThan(score('*', 'prod-eu-*'))
    expect(score('table.*', '*')).toBeLessThan(score('table.delete', '*'))
    // `**` names nothing either, so it scores as the bare wildcard rather than as a prefix.
    expect(score('**', '**')).toBe(0)
  })

  test('can() is Allowed only — RequiresApproval is not permission', () => {
    expect(can([allow('table.delete', '*')], 'table.delete', 'ledger')).toBe(true)
    expect(can([allow('table.delete', '*', true)], 'table.delete', 'ledger')).toBe(false)
    expect(decide([allow('table.delete', '*', true)], 'table.delete', 'ledger')).toBe('RequiresApproval')
  })

  test('scope defaults to *', () => {
    expect(decide([allow('config.replace', '*')], 'config.replace')).toBe('Allowed')
    expect(decide([allow('config.replace', 'prod-*')], 'config.replace')).toBe('Denied')
  })
})

describe('old-server fallback (no permissions[] at all)', () => {
  // AuthProvider does the falling back; what is pinned here is the TABLE it falls back to, which must
  // reproduce the legacy ASP.NET policies exactly — the same route-by-route mapping
  // shared/StreamForge.AppCore.Tests/Access/LegacyEquivalenceMatrixTests.cs pins on the server, and the
  // same one BuiltInRoleCatalog seeds the three built-in roles from.
  const { minRoleFor, ROLE_ORDER } = __testing
  const admits = (role: 'Viewer' | 'Editor' | 'Admin', action: string) =>
    ROLE_ORDER[role] >= ROLE_ORDER[minRoleFor(action)]

  const viewerActions = ['source.read', 'pipeline.read', 'table.read', 'config.export', 'catalog.read', 'approval.request']
  const editorActions = [
    'source.write', 'source.delete', 'source.ingest', 'source.run',
    'pipeline.write', 'pipeline.delete', 'pipeline.control',
    'table.write', 'table.delete', 'table.control',
    'config.replace', 'catalog.write', 'chat.use',
  ]
  const adminActions = ['user.read', 'user.write', 'access.read', 'access.write', 'audit.read', 'approval.decide', 'approval.bypass']

  test('a Viewer gets the read surface and nothing else', () => {
    for (const action of viewerActions) expect(admits('Viewer', action)).toBe(true)
    for (const action of [...editorActions, ...adminActions]) expect(admits('Viewer', action)).toBe(false)
  })

  test('an Editor gets the read surface plus the mutating half, and no privileged action', () => {
    for (const action of [...viewerActions, ...editorActions]) expect(admits('Editor', action)).toBe(true)
    for (const action of adminActions) expect(admits('Editor', action)).toBe(false)
  })

  test('an Admin gets everything', () => {
    for (const action of [...viewerActions, ...editorActions, ...adminActions]) {
      expect(admits('Admin', action)).toBe(true)
    }
  })

  test('an unnamed action floors at Editor, and at Admin under a privileged family', () => {
    expect(minRoleFor('pipeline.somethingNew')).toBe('Editor')
    expect(minRoleFor('access.somethingNew')).toBe('Admin')
    expect(minRoleFor('audit.export')).toBe('Admin')
    expect(minRoleFor('user.impersonate')).toBe('Admin')
    // approval.request is named Viewer above; the prefix rule only catches the deciding half.
    expect(minRoleFor('approval.request')).toBe('Viewer')
    expect(minRoleFor('approval.somethingNew')).toBe('Admin')
  })
})

describe('the two answers agree for the seeded roles', () => {
  // The grant sets below are the ones a live instance actually serves — copied from `GET /api/auth/me`
  // on a freshly seeded host (admin / editor / viewer), not from anybody's reading of the seeds. This
  // is the "no flag day" claim stated as a test: for every action the platform has, the evaluator fed
  // a real snapshot answers what the ordinal fallback answers, so a screen renders the same way either
  // side of the upgrade — and a drift between BuiltInRoleCatalog.cs and ACTION_ROLE_FLOOR shows up
  // here rather than as a missing button.
  const { minRoleFor, ROLE_ORDER } = __testing
  const viewerGrants = ['source.read', 'pipeline.read', 'table.read', 'config.export', 'catalog.read', 'approval.request'].map((a) => allow(a))
  const editorGrants = [
    ...viewerGrants,
    ...['source.write', 'source.delete', 'source.ingest', 'source.run', 'pipeline.write', 'pipeline.delete', 'pipeline.control', 'table.write', 'table.delete', 'table.control', 'config.replace', 'catalog.write', 'chat.use'].map((a) => allow(a)),
  ]
  const seeded = {
    Viewer: viewerGrants,
    Editor: editorGrants,
    Admin: [allow('*', '*')],
  } as const

  const everyAction = [
    'source.read', 'source.write', 'source.delete', 'source.ingest', 'source.run',
    'pipeline.read', 'pipeline.write', 'pipeline.delete', 'pipeline.control',
    'table.read', 'table.write', 'table.delete', 'table.control',
    'config.export', 'config.replace', 'catalog.read', 'catalog.write', 'chat.use',
    'user.read', 'user.write', 'access.read', 'access.write', 'audit.read',
    'approval.request', 'approval.decide', 'approval.bypass',
  ]

  for (const role of ['Viewer', 'Editor', 'Admin'] as const) {
    test(`${role}: snapshot and fallback answer identically for every action`, () => {
      for (const action of everyAction) {
        const fromSnapshot = decide(seeded[role], action, '*') === 'Allowed'
        const fromFallback = ROLE_ORDER[role] >= ROLE_ORDER[minRoleFor(action)]
        expect(`${action}=${fromSnapshot}`).toBe(`${action}=${fromFallback}`)
      }
    })
  }
})
