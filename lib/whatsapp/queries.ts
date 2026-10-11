/**
 * Server-side data access. This is the single entry point other parts of the
 * app should use to read the WhatsApp database. Works with both the legacy and
 * modern msgstore.db layouts (see backend/).
 *
 *   listChats()                         – every chat, newest first
 *   getChat(jid)                        – one chat with date range
 *   getMessages(jid, query)             – a page of messages (cursor-based)
 *   locateMessage(jid, at)              – first message at/after a timestamp
 *   getMessageById(id)                  – one message
 *   getThumbnail(id) / getMediaFile(id) – binary payloads
 */
import { backend, type ChatStat, type RawRow } from "./backend"
import { chatName, me, participant } from "./contacts"
import { cached, clearCache } from "./db"
import { messagePreview } from "./format"
import { chatKind } from "./jid"
import { mapMessage, mapQuote, mediaUrl, thumbnailUrl, type MapContext } from "./mapper"
import { resolveMediaPath } from "./media"
import type { ChatAnchor, ChatDetails, ChatSummary, Message, MessagePage, QuotedMessage, Reaction } from "./types"

// Derived data is memoized on globalThis; drop it when this module is (re)loaded so
// code changes during development never serve values computed by the old code.
clearCache()

// ---------------------------------------------------------------------------
// Hydration: reactions + quoted messages for a batch of rows
// ---------------------------------------------------------------------------

function hydrate(rows: RawRow[]): Message[] {
  const b = backend()
  const reactions = new Map<number, Reaction[]>()
  for (const r of b.reactions(rows.map((r) => r.id))) {
    const list = reactions.get(r.messageId) ?? []
    list.push({ emoji: r.emoji, sender: r.senderJid ? participant(r.senderJid) : me(), timestamp: r.timestamp })
    reactions.set(r.messageId, list)
  }
  const quotes = new Map<number, QuotedMessage>()
  for (const [key, q] of b.quotes(rows.flatMap((r) => (r.quoteKey != null ? [r.quoteKey] : [])))) quotes.set(key, mapQuote(q))
  const ctx: MapContext = { reactions, quotes }
  return rows.map((r) => mapMessage(r, ctx))
}

export function getMessagesByIds(ids: number[]): Message[] {
  return hydrate(backend().byIds(ids))
}

export function getMessageById(id: number): Message | null {
  return getMessagesByIds([id])[0] ?? null
}

// ---------------------------------------------------------------------------
// Chats
// ---------------------------------------------------------------------------

/**
 * All chats with at least one real message, newest first. Chats that only hold
 * system notices (security-code changes etc.) are hidden, like in WhatsApp, but
 * still reachable via getChat(). Cached until the data files change.
 */
export function listChats(): ChatSummary[] {
  return cached("chats", () =>
    chatStats()
      .filter((c) => c.realCount > 0)
      .map(summarize)
      .sort((a, b) => (b.lastMessage?.timestamp ?? 0) - (a.lastMessage?.timestamp ?? 0))
  )
}

const chatStats = () => cached("chatStats", () => backend().chats())
const chatStatsByJid = () => cached("chatStatsByJid", () => new Map(chatStats().map((c) => [c.jid, c])))

/** Preview message of every chat (last non-system message), loaded in one batch. */
const previewId = (c: ChatStat) => c.lastRealId ?? c.lastId
const lastMessages = () =>
  cached("lastMessages", () => new Map(getMessagesByIds(chatStats().map(previewId)).map((m) => [m.id, m])))

function summarize(c: ChatStat): ChatSummary {
  const m = lastMessages().get(previewId(c))
  const system = m?.kind === "system"
  const kind = chatKind(c.jid)
  return {
    jid: c.jid,
    name: chatName(c.jid, c.subject),
    kind,
    messageCount: c.realCount,
    archived: c.archived,
    lastMessage: m
      ? {
          id: m.id,
          timestamp: m.timestamp,
          fromMe: m.fromMe && !system,
          senderName: m.sender && !system && kind !== "individual" ? m.sender.name : null,
          // Resolve "@<number>" mentions to names, as the chat list does in WhatsApp
          preview: messagePreview(m)
            .replace(/@(\d{5,})/g, (raw, user: string) => (m.mentions[user] ? `@${m.mentions[user].replace(/^~/, "")}` : raw))
            .slice(0, 200),
        }
      : null,
  }
}

export function getChat(jid: string): ChatDetails | null {
  const stat = chatStatsByJid().get(jid)
  if (!stat) return null
  const range = backend().range(jid)
  return {
    ...summarize(stat),
    firstMessageAt: range.first,
    lastMessageAt: range.last,
    createdAt: stat.createdAt,
  }
}

// ---------------------------------------------------------------------------
// Messages (cursor pagination by row id — rows are inserted in chat order)
// ---------------------------------------------------------------------------

export interface MessageQuery {
  /** Older messages than this id. */
  before?: number
  /** Newer messages than this id. */
  after?: number
  /** Where to open the chat when no cursor is given. */
  anchor?: ChatAnchor
  limit?: number
}

export const DEFAULT_PAGE_SIZE = 60
const MAX_PAGE_SIZE = 200

function olderThan(jid: string, id: number, limit: number) {
  const rows = backend().older(jid, id, limit + 1)
  return { rows: rows.slice(0, limit).reverse(), hasMore: rows.length > limit }
}

function newerThan(jid: string, id: number, limit: number) {
  const rows = backend().newer(jid, id, limit + 1)
  return { rows: rows.slice(0, limit), hasMore: rows.length > limit }
}

/** Row id of the first message in `jid` at or after `at` (ms), or the last message if none. */
export function locateMessage(jid: string, at: number): number | null {
  return backend().locate(jid, at)
}

export function getMessages(jid: string, query: MessageQuery = {}): MessagePage {
  const limit = Math.min(Math.max(query.limit ?? DEFAULT_PAGE_SIZE, 1), MAX_PAGE_SIZE)

  if (query.before != null) {
    const { rows, hasMore } = olderThan(jid, query.before, limit)
    return { messages: hydrate(rows), hasOlder: hasMore, hasNewer: true, targetId: null }
  }
  if (query.after != null) {
    const { rows, hasMore } = newerThan(jid, query.after, limit)
    return { messages: hydrate(rows), hasOlder: true, hasNewer: hasMore, targetId: null }
  }

  const anchor = query.anchor ?? { type: "latest" }
  let targetId: number | null = null
  if (anchor.type === "message") targetId = backend().contains(jid, anchor.id) ? anchor.id : null
  else if (anchor.type === "time") targetId = locateMessage(jid, anchor.at)

  if (targetId == null) {
    const { rows, hasMore } = olderThan(jid, Number.MAX_SAFE_INTEGER, limit)
    return { messages: hydrate(rows), hasOlder: hasMore, hasNewer: false, targetId: null }
  }

  // Centre the page on the target (target included in the "older" half).
  const half = Math.floor(limit / 2)
  const older = olderThan(jid, targetId + 1, half + 1)
  const newer = newerThan(jid, targetId, limit - half)
  return {
    messages: hydrate([...older.rows, ...newer.rows]),
    hasOlder: older.hasMore,
    hasNewer: newer.hasMore,
    targetId,
  }
}

// ---------------------------------------------------------------------------
// Binary payloads
// ---------------------------------------------------------------------------

/** Embedded thumbnail for a message, or for the message it quotes (`quoted`). */
export function getThumbnail(id: number, opts: { quoted?: boolean } = {}): Uint8Array | null {
  return opts.quoted ? backend().quotedThumbnail(id) : backend().thumbnail(id)
}

/** Absolute path + mime of the original media file, if it exists on disk. */
export function getMediaFile(id: number): { path: string; mime: string | null } | null {
  const row = backend().mediaFile(id)
  const abs = resolveMediaPath(row?.path)
  if (!row || !abs) return null
  return { path: abs, mime: row.mime }
}

export { mediaUrl, thumbnailUrl }
