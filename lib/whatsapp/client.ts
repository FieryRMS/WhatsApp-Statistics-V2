/**
 * Browser-side fetchers for the explorer API (app/api/**).
 */
import { anchorToParams } from "./anchor"
import type { ChatAnchor, ChatDetails, ChatSummary, Message, MessagePage } from "./types"

async function json<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

const chatPath = (jid: string) => `/api/chats/${encodeURIComponent(jid)}`

export function fetchChats(signal?: AbortSignal) {
  return fetch("/api/chats", { signal }).then(json<ChatSummary[]>)
}

export function fetchChat(jid: string, signal?: AbortSignal) {
  return fetch(chatPath(jid), { signal }).then(json<ChatDetails>)
}

export type PageRequest = { anchor: ChatAnchor } | { before: number } | { after: number }

export function fetchMessages(jid: string, req: PageRequest, limit?: number, signal?: AbortSignal) {
  const params =
    "anchor" in req ? anchorToParams(req.anchor) : new URLSearchParams("before" in req ? { before: String(req.before) } : { after: String(req.after) })
  if (limit) params.set("limit", String(limit))
  return fetch(`${chatPath(jid)}/messages?${params}`, { signal }).then(json<MessagePage>)
}

export function fetchMessage(id: number, signal?: AbortSignal) {
  return fetch(`/api/messages/${id}`, { signal }).then(json<Message>)
}
