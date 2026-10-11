import "server-only"

import { canonicalJid, ownerName, personMembers } from "./contacts"
import { currentViewerJid } from "./db"
import { chatKind } from "./jid"
import type { ChatSummary, Message, Participant, QuotedMessage, SystemEvent } from "./types"

/**
 * Presenting the data from the current viewer's point of view.
 *
 * The raw data only knows the phone owner (every "from me" message). A session
 * can say someone else is viewing — e.g. Faiza — and then her messages are
 * shown as "You" (on the right) and the owner's appear under the owner's name.
 *
 * This runs on query *results*, after every cache, so viewers sharing an
 * account never see each other's perspective.
 */

export interface ViewerInfo {
  /** Canonical JID of the viewer, or null when the viewer is the phone owner. */
  jid: string | null
  /** Every identity of the viewer (numbers merged via identical names); empty for the owner. */
  jids: Set<string>
  /** How the phone owner is labelled when someone else is viewing. */
  ownerLabel: string
}

export const DEFAULT_OWNER_LABEL = "Phone owner"

export function viewer(): ViewerInfo {
  const raw = currentViewerJid()
  const jid = raw ? canonicalJid(raw) : null
  return { jid, jids: new Set(jid ? personMembers(jid) : []), ownerLabel: ownerName() ?? DEFAULT_OWNER_LABEL }
}

export function viewParticipant(p: Participant, v: ViewerInfo = viewer()): Participant {
  if (!v.jid) return p
  if (p.jid === null) return { jid: null, name: v.ownerLabel, phone: null, known: true }
  if (v.jids.has(p.jid)) return { ...p, name: "You", known: true }
  return p
}

const vp = (p: Participant | null, v: ViewerInfo) => (p ? viewParticipant(p, v) : p)

function viewEvent(e: SystemEvent, v: ViewerInfo): SystemEvent {
  const out = { ...e } as Record<string, unknown>
  for (const key of ["actor", "who", "from", "to"]) if (key in out) out[key] = vp(out[key] as Participant | null, v)
  if ("targets" in out) out.targets = (out.targets as Participant[]).map((p) => viewParticipant(p, v))
  return out as unknown as SystemEvent
}

function viewQuote(q: QuotedMessage, v: ViewerInfo): QuotedMessage {
  return { ...q, sender: viewParticipant(q.sender, v) }
}

export function viewMessage(m: Message, v: ViewerInfo = viewer()): Message {
  if (!v.jid) return m
  const mine = m.kind !== "system" && !!m.sender?.jid && v.jids.has(m.sender.jid)
  const out = {
    ...m,
    // Bubble side follows the viewer; ticks only exist for the owner's own messages.
    fromMe: m.kind === "system" ? m.fromMe : mine,
    status: mine ? ("received" as const) : m.status,
    sender: vp(m.sender, v),
    quoted: m.quoted ? viewQuote(m.quoted, v) : null,
    reactions: m.reactions.map((r) => ({ ...r, sender: viewParticipant(r.sender, v) })),
  } as Message
  if (out.kind === "system") out.event = viewEvent(out.event, v)
  return out
}

export function viewChat<T extends ChatSummary>(c: T, v: ViewerInfo = viewer()): T {
  if (!v.jid) return c
  const out = { ...c }
  // A direct chat with the viewer is, from their side, the chat with the phone owner.
  if (chatKind(c.jid) === "individual" && v.jids.has(canonicalJid(c.jid))) out.name = v.ownerLabel
  const last = c.lastMessage
  if (last && !last.system) {
    const mine = last.senderJid != null && v.jids.has(canonicalJid(last.senderJid))
    out.lastMessage = {
      ...last,
      fromMe: mine,
      senderName: mine ? null : chatKind(c.jid) !== "individual" ? (last.senderJid === null ? v.ownerLabel : last.senderName) : null,
    }
  }
  return out
}
