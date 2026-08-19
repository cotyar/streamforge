// Plan 015 wave 6 — the client-side twin of shared/StreamForge.AppCore/Access/PermissionEvaluator.cs.
//
// WHY A TWIN AT ALL. The SPA has to decide what to render before it makes a request, and asking the
// server "may I?" per button would be a round trip per button. So the *same* decision runs twice: once
// here to shape the UI, and once on the server where it is actually enforced. This file is advisory —
// nothing here keeps anybody out of anything. The server's AccessGuard does that, on every route, and
// it is the only opinion that counts.
//
// WHY IT IS A MIRROR AND NOT AN APPROXIMATION. There is exactly ONE scope grammar in this system
// (`*` | exact | prefix `prod-*` | `tag:finance`) and, since wave 4 promoted `ScopeMatches` to
// internal so ApprovalStateMachine could share it, exactly one implementation of it per runtime. This
// is its third implementation and its second language. A divergence here is invisible to both .NET
// suites — it shows up as a button the operator can see and cannot use, or (worse) one they cannot see
// and are entitled to. So every rule below is a transliteration of the C# with the same ordering, the
// same ordinal case-SENSITIVE comparisons and the same dot-crossing `*`, and web/test/permissions.test.ts
// pins the boundary cases the C# tests pin.
//
// THE THREE RULES, IN THE ORDER THEY APPLY (PermissionEvaluator.Evaluate):
//   1. a disabled user is denied everything, before a single grant is read;
//   2. deny overrides — any matching Deny wins outright, flat, ABSOLUTE, and deliberately outside the
//      specificity ladder in rule 3;
//   3. among matching Allows the MOST SPECIFIC one decides the approval axis (see `specificity`), and
//      on a tie 'RequiresApproval' wins.

import type { AccessDecision, PermissionGrant } from './types'

/** What the evaluator needs about a principal. The array form is the common one (`UserInfo.permissions`
 *  straight off `/api/auth/me`); the object form exists so the disabled-first rule can be exercised —
 *  see the note on {@link decide}. */
export interface ClientPermissions {
  grants: readonly PermissionGrant[]
  disabled?: boolean
}

/**
 * The one matcher behind both axes. `*` stands for any run of characters, **including dots**: so
 * `pipeline.*` covers `pipeline.write` and would also cover a future `pipeline.write.sql`, while
 * `pipeline` alone is NOT covered by `pipeline.*` (the pattern demands the dot). Dot-crossing is the
 * deliberate choice on the server — a segment-bounded `*` would mean every existing `x.*` entitlement
 * silently stops covering the day somebody adds a third segment — and a client that folded segments
 * would disagree with it.
 *
 * An empty pattern matches nothing: `PermissionGrant.action` defaults to `""`, and a half-filled grant
 * must grant nothing.
 *
 * Iterative glob with backtracking, transliterated from `PermissionEvaluator.GlobMatch`: no RegExp, no
 * allocation, no escaping question, and — the actual reason — no chance of the two implementations
 * disagreeing about what a character class or an anchor means, because neither has any.
 */
export function globMatch(pattern: string, value: string): boolean {
  let p = 0
  let v = 0
  let lastStar = -1
  let resumeAt = 0

  while (v < value.length) {
    if (p < pattern.length && pattern[p] === '*') {
      lastStar = p++
      resumeAt = v
    } else if (p < pattern.length && pattern[p] === value[v]) {
      p++
      v++
    } else if (lastStar >= 0) {
      p = lastStar + 1
      v = ++resumeAt
    } else {
      return false
    }
  }

  while (p < pattern.length && pattern[p] === '*') {
    p++
  }

  return p === pattern.length
}

const TAG_PREFIX = 'tag:'

/**
 * The four scope forms. `tag:finance` matches when the resource carries that tag (globbed, so
 * `tag:pii-*` falls out for free); `*`, an exact id/name and a prefix like `prod-*` are all one glob
 * against the resource's id or name.
 *
 * A tag-scoped grant against a call that passed no tags is a MISS, not a match — same as the server.
 * Treating "no tags supplied" as "unknown, so allow" would make every call site that forgot to pass
 * tags silently widen every tag entitlement.
 */
export function scopeMatches(
  pattern: string,
  scope: string,
  resourceTags?: readonly string[],
): boolean {
  if (pattern.startsWith(TAG_PREFIX)) {
    if (!resourceTags || resourceTags.length === 0) return false
    const tagPattern = pattern.slice(TAG_PREFIX.length)
    return resourceTags.some((tag) => globMatch(tagPattern, tag))
  }

  return globMatch(pattern, scope)
}

/** Does one grant cover this (action, scope) pair? Both axes are case-SENSITIVE, as on the server: an
 *  entitlement written `prod-*` must not silently start covering a table somebody named `PROD-Sandbox`,
 *  because an entitlement widening itself is the one direction of surprise authorization must not have. */
export function grantMatches(
  grant: PermissionGrant,
  action: string,
  scope: string,
  resourceTags?: readonly string[],
): boolean {
  return globMatch(grant.action, action) && scopeMatches(grant.scope, scope, resourceTags)
}

/** One tier outranks any literal count, so tiers dominate and literals only break ties inside one. */
const TIER_STEP = 1000

function axisScore(pattern: string, tagsAllowed: boolean): number {
  let literals = 0
  for (const c of pattern) if (c !== '*') literals++
  // `*`, `**` or "" — nothing was named at all. Tier 0, no tiebreak.
  if (literals === 0) return 0

  const tier =
    tagsAllowed && pattern.startsWith(TAG_PREFIX) ? 1 : pattern.includes('*') ? 2 : 3

  return tier * TIER_STEP + Math.min(literals, TIER_STEP - 1)
}

/**
 * How specific a grant is — transliterated from `PermissionEvaluator.Specificity`, and the reason the
 * whole doc comment there matters here too: this is the score that decides, among grants that ALL
 * match, which one says whether the action needs an approval. Higher is more specific.
 *
 * Each axis is scored `tier * 1000 + literalCount` and the two are summed:
 *   tier 0 — no literals at all (`*`, or ""): the grant names nothing;
 *   tier 1 — a `tag:` scope (scope axis only; there is no tag form on the action axis);
 *   tier 2 — a literal part plus a `*` (`prod-*`, `pipeline.*`);
 *   tier 3 — an exact literal, no wildcard.
 * Within a tier the longer literal wins, so `prod-eu-*` beats `prod-*` — nested prefixes are the
 * commonest way an operator carves a narrower area out of a broader one.
 *
 * `tag:` sits BELOW both name forms and above `*`: a tag scope matches a set its author neither
 * enumerated nor can see the boundary of (anyone with catalog write can add the tag later), so it must
 * not outrank the forms whose membership the grant's author wrote down. The axes are summed rather
 * than ranked because neither is obviously senior — a tie is a defined answer, and `RequiresApproval`
 * wins it.
 */
export function specificity(grant: PermissionGrant): number {
  return axisScore(grant.action, false) + axisScore(grant.scope, true)
}

/**
 * Evaluate one action against one resource, and answer the tri-state.
 *
 * `RequiresApproval` is returned rather than folded into a boolean because it is load-bearing all the
 * way to a button label ("Request approval…") in a later wave; retrofitting it would mean touching
 * every call site twice. {@link can} is the boolean on top, for the call sites that only ever wanted
 * show-or-hide.
 *
 * @param perms The caller's grants — either the raw `UserInfo.permissions` array, or the object form
 *   when the disabled flag matters. **`null`/`undefined` answers `'Denied'`**, which is NOT the same
 *   thing as "an old server": a server that sends no `permissions[]` at all is handled one level up,
 *   in AuthProvider, by falling back to role ordering. Nothing should reach here with null.
 * @param scope The resource's name or id, or `'*'` to ask "…anywhere?". Asking with `'*'` is answered
 *   only by a `*`-scoped grant: a caller holding `prod-*` cannot do the global thing, which is the
 *   correct reading of a scoped entitlement and exactly what the server does.
 * @param resourceTags The resource's tags, so `tag:finance` scopes can match. Almost no client call
 *   site has them — see the ponytail note below.
 */
export function decide(
  perms: readonly PermissionGrant[] | ClientPermissions | null | undefined,
  action: string,
  scope: string = '*',
  resourceTags?: readonly string[],
): AccessDecision {
  if (!perms) return 'Denied'

  const grants: readonly PermissionGrant[] = Array.isArray(perms)
    ? perms
    : (perms as ClientPermissions).grants
  const disabled = !Array.isArray(perms) && (perms as ClientPermissions).disabled === true

  // Rule 1, before any grant is consulted. The server empties the grant list for a disabled user, so
  // this is the second lock on the same door — and the only one a hand-built permission set trips.
  if (disabled) return 'Denied'
  if (!grants) return 'Denied'

  // Rules 2 and 3. The whole list is walked even after an Allow matches, because a Deny later in the
  // list still overrides it. Grant lists are tens of entries at most.
  //
  // Deny is ABSOLUTE and stays outside the specificity ladder, exactly as on the server: a specific
  // Allow must not be able to punch a hole in a guardrail `Deny pipeline.* on prod-*`. The cost is that
  // an Allow cannot be carved out of a broad Deny — narrow the Deny's scope instead.
  let best: PermissionGrant | null = null
  let bestScore = -1

  for (const grant of grants) {
    if (!grantMatches(grant, action, scope, resourceTags)) continue

    // requiresApproval on a Deny is meaningless and is ignored here rather than inventing a fourth
    // state — AccessModels.cs says so on the field itself.
    if (grant.effect === 'Deny') return 'Denied'

    const score = specificity(grant)
    // Strictly greater wins; on an exact tie the approval-gated grant does, which is the safer answer
    // and the one an operator adding a grant is likelier to have meant. The tie-break also takes
    // document order out of the decision entirely.
    if (score > bestScore || (score === bestScore && grant.requiresApproval && best !== null && !best.requiresApproval)) {
      best = grant
      bestScore = score
    }
  }

  if (best) return best.requiresApproval ? 'RequiresApproval' : 'Allowed'
  return 'Denied'
}

/** The boolean on top of {@link decide}: strictly `'Allowed'`. `'RequiresApproval'` is deliberately
 *  NOT true — the caller may not do the thing, they may ask to. Until a wave renders that as its own
 *  button, such a grant hides the control, which is the same answer the pre-015 SPA gave (nobody had
 *  approval-gated grants) and the safe direction of the two. */
export function can(
  perms: readonly PermissionGrant[] | ClientPermissions | null | undefined,
  action: string,
  scope: string = '*',
  resourceTags?: readonly string[],
): boolean {
  return decide(perms, action, scope, resourceTags) === 'Allowed'
}

// ponytail: no reason string, and no matched grant, on this side. Ceiling: the SPA can say "you may
// not" but not "…because of the Deny somebody wrote on prod-*" — which is exactly the diagnostic
// AccessResult.Reason exists to give. Upgrade path when a screen wants it: return the matched grant
// alongside the decision (the loop above already holds it) and render `note`. Nothing today reads a
// reason, and every 403 the server returns carries its own.
//
// ponytail: resourceTags is threaded through but almost nothing passes it, because a client call site
// generally has the entity in hand only on a detail page. Ceiling: a user entitled ONLY via a
// `tag:finance` grant sees the buttons hidden on list pages and gets them back on the detail page —
// the request would have succeeded either way, so this errs toward hiding, never toward showing.
// Upgrade path: pass `definition.tags` at the call sites that already have the definition loaded; the
// argument is there and the matcher already handles it.
