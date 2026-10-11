import "server-only"

import { OWNER_KEY, canonicalJid, ownerName, personMembers, samePerson } from "./contacts"
import { currentViewerJid } from "./db"
import { chatKind } from "./jid"
import type { ChatSummary, Message, Participant, QuotedMessage, SystemEvent } from "./types"

/**
 * Presenting the data from the current viewer's point of view.
 *
 * The raw data only knows the phone owner (every "from me" message). A session
 * can say someone else is viewing — e.g. Faiza — and then her messages are
 * shown as "You" (on the right) and the owner's appear under the owner's name.
 * People merged in the contact names (same name) count as the same person,
 * including numbers merged into the phone owner.
 *
 * This runs on query *results*, after every cache, so viewers sharing an
 * account never see each other's perspective.
 */

export interface ViewerInfo {
  /** Canonical JID chosen as the viewer, or null when the viewer is the phone owner. */
  jid: string | null
  isOwner: boolean
  /** JIDs that are also "you" (numbers merged with the viewer via identical names). */
  jids: Set<string>
  /** How the phone owner is labelled. */
  ownerLabel: string
}

export const DEFAULT_OWNER_LABEL = "Phone owner"

export function viewer(): ViewerInfo {
  const raw = currentViewerJid()
  const ownerLabel = ownerName() ?? DEFAULT_OWNER_LABEL
  const jid = raw ? canonicalJid(raw) : null
  // Choosing a number that's merged into the owner means the owner is viewing.
  const isOwner = !jid || samePerson(jid) === OWNER_KEY
  const members = isOwner ? personMembers(OWNER_KEY) : personMembers(jid!)
  return { jid: isOwner ? null : jid, isOwner, jids: new Set(members.filter((m) => m !== OWNER_KEY)), ownerLabel }
}

const neutral = (v: ViewerInfo) => v.isOwner && v.jids.size === 0

export function viewParticipant(p: Participant, v: ViewerInfo = viewer()): Participant {
  if (p.jid === null) return v.isOwner ? p : { jid: null, name: v.ownerLabel, phone: null, known: true }
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
  if (neutral(v)) return m
  const fromOtherNumber = !!m.sender?.jid && v.jids.has(m.sender.jid)
  const mine = m.kind !== "system" && ((v.isOwner && m.fromMe) || fromOtherNumber)
  const out = {
    ...m,
    // Bubble side follows the viewer; ticks only exist for the owner's own device.
    fromMe: m.kind === "system" ? m.fromMe : mine,
    status: mine && !m.fromMe ? ("received" as const) : m.status,
    sender: vp(m.sender, v),
    quoted: m.quoted ? viewQuote(m.quoted, v) : null,
    reactions: m.reactions.map((r) => ({ ...r, sender: viewParticipant(r.sender, v) })),
  } as Message
  if (out.kind === "system") out.event = viewEvent(out.event, v)
  return out
}

export function viewChat<T extends ChatSummary>(c: T, v: ViewerInfo = viewer()): T {
  if (neutral(v)) return c
  const out = { ...c }
  // A direct chat with the viewer is, from their side, the chat with the phone owner.
  if (!v.isOwner && chatKind(c.jid) === "individual" && v.jids.has(canonicalJid(c.jid))) out.name = v.ownerLabel
  const last = c.lastMessage
  if (last && !last.system) {
    const fromOtherNumber = last.senderJid != null && v.jids.has(canonicalJid(last.senderJid))
    const mine = (v.isOwner && last.senderJid === null) || fromOtherNumber
    out.lastMessage = {
      ...last,
      fromMe: mine,
      senderName: mine ? null : chatKind(c.jid) !== "individual" ? (last.senderJid === null ? v.ownerLabel : last.senderName) : null,
    }
  }
  return out
}
