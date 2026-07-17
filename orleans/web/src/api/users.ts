import { api } from './client'
import type { CreateUserRequest, UpdateUserRequest, UserInfo } from './types'

export const usersApi = {
  list: () => api.get<UserInfo[]>('/api/users'),
  create: (body: CreateUserRequest) => api.post<UserInfo>('/api/users', body),
  update: (username: string, body: UpdateUserRequest) =>
    api.put<UserInfo>(`/api/users/${encodeURIComponent(username)}`, body),
  remove: (username: string) => api.del<void>(`/api/users/${encodeURIComponent(username)}`),
}
