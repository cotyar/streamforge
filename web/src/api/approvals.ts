import { api } from './client'
import type {
  ApprovalDecisionRequest,
  ApprovalRequest,
  ApprovalState,
  FileApprovalRequest,
} from './types'

const enc = encodeURIComponent

/** Plan 015 wave 6-B. The `/api/approvals` surface.
 *
 *  These routes are **Viewer**-gated, not Admin: filing a request is not a privilege, and the listing
 *  filters server-side to the administrator, the requester and the entitled approver. So the inbox is
 *  for every logged-in user and must not carry a client-side role gate.
 *
 *  Every route answers 503 with a sentence when `Approvals:Enabled=false` (the shipped default) — the
 *  caller shows that sentence rather than an empty inbox. */
export const approvalsApi = {
  file: (body: FileApprovalRequest) => api.post<ApprovalRequest>('/api/approvals', body),

  list: (state?: ApprovalState | null, limit = 100) => {
    const q = new URLSearchParams()
    if (state) q.set('state', state)
    q.set('limit', String(limit))
    return api.get<ApprovalRequest[]>(`/api/approvals?${q.toString()}`)
  },

  get: (id: string) => api.get<ApprovalRequest>(`/api/approvals/${enc(id)}`),

  /** 403 when you filed it yourself or are not an approver, 409 when it is no longer pending — both
   *  carry the server's own sentence naming the reason. */
  approve: (id: string, body?: ApprovalDecisionRequest) =>
    api.post<ApprovalRequest>(`/api/approvals/${enc(id)}/approve`, body ?? {}),

  reject: (id: string, body?: ApprovalDecisionRequest) =>
    api.post<ApprovalRequest>(`/api/approvals/${enc(id)}/reject`, body ?? {}),

  /** Withdrawing your own request. Idempotent: an already-cancelled request answers 200. */
  cancel: (id: string, body?: ApprovalDecisionRequest) =>
    api.post<ApprovalRequest>(`/api/approvals/${enc(id)}/cancel`, body ?? {}),
}
