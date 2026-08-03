import { api } from './client'
import type { ChatMessage, ChatResponse } from './types'

export const chatApi = {
  /** POST /api/chat — stateless: send the full text history every turn (Editor policy). */
  send: (messages: ChatMessage[]) => api.post<ChatResponse>('/api/chat', { messages }),
}
