import { api } from './client'
import type {
  AccessPolicyDocument,
  ApprovalTemplate,
  EffectivePermissions,
  GroupDefinition,
  RoleDefinition,
  SetAccessDisabledRequest,
  UserAccessEntry,
} from './types'

const enc = encodeURIComponent

/** Plan 015 wave 6-B. The `/api/access` surface, one method per route and nothing else — the shape of
 *  `users.ts`. Every route is Admin-gated at the group and `access.read`/`access.write`-gated in the
 *  handler; the 403s and the 409 on deleting a built-in role carry a server-written sentence, which is
 *  why nothing here catches or rewrites an error. */
export const accessApi = {
  /** The whole document — roles, groups, user entries, approval templates and the version stamp. Not
   *  four list calls: the server deliberately answers all four at once. */
  get: () => api.get<AccessPolicyDocument>('/api/access'),

  /** What this user can actually do, flattened the way an authorization decision flattens it, and
   *  stamped with the resolver snapshot version that answered. */
  effective: (username: string) =>
    api.get<EffectivePermissions>(`/api/access/effective/${enc(username)}`),

  upsertRole: (name: string, body: RoleDefinition) =>
    api.put<RoleDefinition>(`/api/access/roles/${enc(name)}`, body),
  deleteRole: (name: string) => api.del<void>(`/api/access/roles/${enc(name)}`),

  upsertGroup: (name: string, body: GroupDefinition) =>
    api.put<GroupDefinition>(`/api/access/groups/${enc(name)}`, body),
  deleteGroup: (name: string) => api.del<void>(`/api/access/groups/${enc(name)}`),

  upsertUser: (username: string, body: UserAccessEntry) =>
    api.put<UserAccessEntry>(`/api/access/users/${enc(username)}`, body),
  deleteUser: (username: string) => api.del<void>(`/api/access/users/${enc(username)}`),

  /** Its own route on purpose: disabling a login under pressure must never mean re-sending grants the
   *  caller did not know about. Never fold this back into `upsertUser`. */
  setDisabled: (username: string, body: SetAccessDisabledRequest) =>
    api.put<UserAccessEntry>(`/api/access/users/${enc(username)}/disabled`, body),

  upsertTemplate: (name: string, body: ApprovalTemplate) =>
    api.put<ApprovalTemplate>(`/api/access/approval-templates/${enc(name)}`, body),
  deleteTemplate: (name: string) => api.del<void>(`/api/access/approval-templates/${enc(name)}`),
}
