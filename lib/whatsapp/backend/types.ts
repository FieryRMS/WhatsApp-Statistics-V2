import "server-only"

/**
 * Schema-independent contract between a msgstore.db layout and the rest of the app.
 *
 * WhatsApp has shipped two layouts:
 *   legacy – everything in the `messages` table (until ~2022)
 *   modern – the `message` table plus many side tables
 *
 * Each backend turns its rows into `RawRow`s; `mapper.ts` turns those into
 * the `Message` objects the UI renders. JIDs are raw strings ("…@s.whatsapp.net",
 * "…@lid", "…@g.us"); name/LID resolution happens later in contacts.ts.
 */

export interface RawMedia {
  mime: string | null
  size: number | null
  width: number | null
  height: number | null
  duration: number | null
  name: string | null
  path: string | null
  pageCount: number | null
  animated: boolean
}

export interface RawSystem {
  action: number
  actor: string | null
  targets: string[]
  /** New value (subject, description, "true"/"false" for block…). */
  value: string | null
  oldValue: string | null
  numberFrom: string | null
  numberTo: string | null
  video: boolean
  /** Disappearing-messages duration in seconds. */
  duration: number | null
  /** Broadcast recipients, when the payload only gives a count. */
  count: number | null
}

export interface RawRow {
  id: number
  chatJid: string
  fromMe: boolean
  keyId: string
  status: number | null
  timestamp: number
  /** media_wa_type (legacy) / message_type (modern) — the same code space. */
  type: number
  text: string | null
  /** Sender for incoming messages; null when from me. */
  senderJid: string | null
  caption: string | null
  media: RawMedia | null
  /** Embedded thumbnail available at /api/messages/:id/thumbnail. */
  hasThumb: boolean
  location: { latitude: number; longitude: number; name: string | null; address: string | null } | null
  link: { title: string | null; description: string | null; url: string | null } | null
  starred: boolean
  forwarded: boolean
  forwardScore: number | null
  viewOnceState: number | null
  voiceNote: boolean
  missedVideo: boolean | null
  call: { video: boolean; duration: number | null; result: number | null } | null
  poll: { selectable: number | null; options: { name: string; votes: number }[] } | null
  album: { images: number; videos: number } | null
  vcards: string[]
  payment: Uint8Array | null
  system: RawSystem | null
  edited: boolean
  revoked: boolean
  revokedByAdmin: string | null
  /** Key for `Backend.quotes()`; null when not a reply. */
  quoteKey: number | null
  mentions: string[]
  ephemeralDuration: number | null
}

export interface RawQuote {
  /** Row id of the original message, if it's still in the chat. */
  targetId: number | null
  fromMe: boolean
  senderJid: string | null
  chatJid: string
  type: number
  text: string | null
  caption: string | null
  fileName: string | null
  thumbnailUrl: string | null
  /** JIDs mentioned in the quoted text. */
  mentions: string[]
}

export interface RawReaction {
  messageId: number
  emoji: string
  /** null = from me */
  senderJid: string | null
  timestamp: number
}

export interface ChatStat {
  jid: string
  /** All rows, including system notices. */
  count: number
  /** Rows that aren't system notices — chats with 0 are hidden, like in WhatsApp. */
  realCount: number
  lastId: number
  /** Last row that isn't a system notice — what WhatsApp sorts and previews by. */
  lastRealId: number | null
  subject: string | null
  archived: boolean
  createdAt: number | null
}

/** One message, as streamed for the statistics index (system notices excluded). */
export interface ScanRow {
  id: number
  chatJid: string
  fromMe: boolean
  /** Raw sender JID for incoming messages; null when from me. */
  senderJid: string | null
  timestamp: number
  type: number
  /** Text or caption. */
  text: string | null
  voiceNote: boolean
  revoked: boolean
}

export interface Backend {
  readonly name: "legacy" | "modern"
  /** Stream every non-system message in id order. */
  scanMessages(cb: (r: ScanRow) => void): void
  /** Stream every reaction: target message id, reactor (null = me), emoji, time. */
  scanReactions(cb: (r: { messageId: number; senderJid: string | null; emoji: string; timestamp: number }) => void): void
  /** Stream every @mention: message id and the mentioned JID. */
  scanMentions(cb: (r: { messageId: number; jid: string }) => void): void
  /** Stream messages whose text contains `term` (case-insensitive for ASCII). */
  searchText(term: string, cb: (r: { id: number; text: string }) => void): void
  chats(): ChatStat[]
  range(jid: string): { first: number | null; last: number | null }
  byIds(ids: number[]): RawRow[]
  /** Up to `limit` rows before `id`, newest first. */
  older(jid: string, id: number, limit: number): RawRow[]
  /** Up to `limit` rows after `id`, oldest first. */
  newer(jid: string, id: number, limit: number): RawRow[]
  /** First message at/after `at` (ms), or the last message. */
  locate(jid: string, at: number): number | null
  contains(jid: string, id: number): boolean
  reactions(ids: number[]): RawReaction[]
  quotes(keys: number[]): Map<number, RawQuote>
  thumbnail(id: number): Uint8Array | null
  quotedThumbnail(key: number): Uint8Array | null
  mediaFile(id: number): { path: string | null; mime: string | null } | null
}

export const emptyRow = (partial: Partial<RawRow> & Pick<RawRow, "id" | "chatJid" | "keyId" | "timestamp" | "type">): RawRow => ({
  fromMe: false,
  status: null,
  text: null,
  senderJid: null,
  caption: null,
  media: null,
  hasThumb: false,
  location: null,
  link: null,
  starred: false,
  forwarded: false,
  forwardScore: null,
  viewOnceState: null,
  voiceNote: false,
  missedVideo: null,
  call: null,
  poll: null,
  album: null,
  vcards: [],
  payment: null,
  system: null,
  edited: false,
  revoked: false,
  revokedByAdmin: null,
  quoteKey: null,
  mentions: [],
  ephemeralDuration: null,
  ...partial,
})
