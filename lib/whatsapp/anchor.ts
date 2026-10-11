/**
 * Addressing a point in a chat. Client-safe.
 *
 *   chatHref("8801…@g.us")                              → open at the latest message
 *   chatHref("8801…@g.us", { at: new Date("2021-03-05") }) → open at that moment
 *   chatHref("8801…@g.us", { messageId: 123456 })       → open at a specific message
 */
import type { ChatAnchor } from "./types"

export interface ChatLocation {
  /** Date, epoch milliseconds, or any string `Date` can parse. */
  at?: Date | number | string
  messageId?: number
}

export function toAnchor(loc: ChatLocation = {}): ChatAnchor {
  if (loc.messageId != null) return { type: "message", id: loc.messageId }
  if (loc.at != null) {
    const at = parseTime(loc.at)
    if (at != null) return { type: "time", at }
  }
  return { type: "latest" }
}

export function parseTime(v: Date | number | string): number | null {
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v.getTime()
  if (typeof v === "number") return Number.isFinite(v) ? v : null
  if (/^\d+$/.test(v)) return Number(v)
  const t = Date.parse(v)
  return isNaN(t) ? null : t
}

type ParamSource = URLSearchParams | Record<string, string | string[] | undefined>

function read(params: ParamSource, key: string): string | undefined {
  if (params instanceof URLSearchParams) return params.get(key) ?? undefined
  const v = params[key]
  return Array.isArray(v) ? v[0] : v
}

/** Reads `?message=<id>` or `?at=<ms|ISO>` from a URL. */
export function anchorFromParams(params: ParamSource): ChatAnchor {
  const message = read(params, "message")
  const at = read(params, "at")
  return toAnchor({
    messageId: message && /^\d+$/.test(message) ? Number(message) : undefined,
    at,
  })
}

export function anchorToParams(anchor: ChatAnchor): URLSearchParams {
  const p = new URLSearchParams()
  if (anchor.type === "message") p.set("message", String(anchor.id))
  if (anchor.type === "time") p.set("at", String(anchor.at))
  return p
}

export function anchorKey(anchor: ChatAnchor): string {
  return anchor.type === "latest" ? "latest" : anchor.type === "message" ? `m${anchor.id}` : `t${anchor.at}`
}

export function chatHref(jid: string, loc?: ChatLocation): string {
  const qs = anchorToParams(toAnchor(loc)).toString()
  return `/chats/${encodeURIComponent(jid)}${qs ? `?${qs}` : ""}`
}

/** Route params may arrive encoded or decoded depending on the caller. */
export function decodeJidParam(param: string): string {
  try {
    return decodeURIComponent(param)
  } catch {
    return param
  }
}
