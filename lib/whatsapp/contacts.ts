import "server-only"

import fs from "node:fs"
import type { DatabaseSync } from "node:sqlite"

import { all, cached, clearCache, hasTable, openReadOnly, sourcePaths } from "./db"
import { parseVCard } from "./decoders"
import { chatKind, jidServer, jidUser, phoneFromJid } from "./jid"
import type { Participant } from "./types"

/**
 * Display-name resolution, built automatically from whatever databases are present.
 *
 * msgstore.db never stores your address book (that lives in wa.db), but it does
 * contain names in several places. Sources, highest priority first:
 *
 *   1. contacts.json (optional)  — manual { jid: name } overrides, if the user creates one
 *   2. wa.db: wa_contacts        — address-book name (display_name)
 *   3. msgstore: chat.subject    — current group names
 *   4. msgstore: newsletter      — channel names
 *   5. msgstore: group history   — last "subject changed"/"group created" event, invites
 *   6. business names            — wa.db wa_vnames, business system messages
 *   7. wa.db: wa_name            — WhatsApp push name, shown as "~Name"
 *   8. msgstore: lid_display_name — the name a LID user set for themselves, shown as "~Name"
 *   9. msgstore: shared vCards   — names other people saved for a number, shown as "~Name"
 *  10. the phone number
 *
 * Modern databases identify some people by LID ("…@lid") instead of phone number.
 * jid_map links each LID to its phone JID; participants are always reported by
 * phone JID when the mapping is known, so one person is one identity everywhere.
 *
 * Every source is optional and guarded, so older/newer schemas just skip what's missing.
 */

clearCache()

/** WhatsApp's own account (tips, privacy notices). */
const WHATSAPP_JID = "0@s.whatsapp.net"

interface NameEntry {
  name: string
  /** false = a hint (push name / someone else's label), rendered with "~". */
  known: boolean
}

type Put = (jid: string | null | undefined, name: string | null | undefined, known?: boolean) => void

function safely(label: string, fn: () => void) {
  try {
    fn()
  } catch (e) {
    console.warn(`[contacts] skipped ${label}:`, e instanceof Error ? e.message : e)
  }
}

// ---------------------------------------------------------------------------
// LID ↔ phone JID
// ---------------------------------------------------------------------------

/** "…@lid" → "…@s.whatsapp.net", from msgstore's jid_map. */
function lidMap(): Map<string, string> {
  return cached("lidMap", () => {
    const map = new Map<string, string>()
    safely("jid_map", () => {
      if (!hasTable("jid_map") || !hasTable("jid")) return
      for (const r of all<{ lid: string; phone: string }>(
        `SELECT l.raw_string AS lid, p.raw_string AS phone FROM jid_map m
         JOIN jid l ON l._id = m.lid_row_id JOIN jid p ON p._id = m.jid_row_id`
      ))
        map.set(r.lid, r.phone)
    })
    return map
  })
}

/** Phone JID for a LID when known, otherwise the JID unchanged. */
export function canonicalJid(jid: string): string {
  return jidServer(jid) === "lid" ? (lidMap().get(jid) ?? jid) : jid
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

function fromOverrides(put: Put) {
  const file = sourcePaths().contacts
  if (!fs.existsSync(file)) return
  const json = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, string>
  for (const [jid, name] of Object.entries(json)) put(jid, name)
}

function withWaDb(fn: (wa: DatabaseSync) => void) {
  const file = sourcePaths().waDb
  if (!fs.existsSync(file)) return
  const wa = openReadOnly(file)
  try {
    fn(wa)
  } finally {
    wa.close()
  }
}

function fromWaContacts(put: Put, column: "display_name" | "wa_name", known: boolean) {
  withWaDb((wa) => {
    const rows = wa
      .prepare(`SELECT jid, ${column} AS name FROM wa_contacts WHERE ${column} IS NOT NULL AND ${column} != ''`)
      .all() as { jid: string; name: string }[]
    for (const r of rows) put(r.jid, r.name, known)
  })
}

function fromWaBusinessNames(put: Put) {
  withWaDb((wa) => {
    const rows = wa
      .prepare("SELECT jid, verified_name AS name FROM wa_vnames WHERE verified_name IS NOT NULL AND verified_name != ''")
      .all() as { jid: string; name: string }[]
    for (const r of rows) put(r.jid, r.name)
  })
}

function fromChatSubjects(put: Put) {
  if (!hasTable("chat") || !hasTable("jid")) return
  for (const r of all<{ jid: string; subject: string }>(
    `SELECT j.raw_string AS jid, c.subject FROM chat c JOIN jid j ON j._id = c.jid_row_id
     WHERE c.subject IS NOT NULL AND c.subject != ''`
  ))
    put(r.jid, r.subject)
}

function fromNewsletters(put: Put) {
  if (!hasTable("newsletter")) return
  for (const r of all<{ jid: string; name: string }>(
    `SELECT j.raw_string AS jid, n.name FROM newsletter n
     JOIN chat c ON c._id = n.chat_row_id JOIN jid j ON j._id = c.jid_row_id
     WHERE n.name IS NOT NULL AND n.name != ''`
  ))
    put(r.jid, r.name)
}

function fromGroupHistory(put: Put) {
  // Action 1 = subject changed, 11 = group created; the newest event wins.
  if (hasTable("messages")) {
    for (const r of all<{ jid: string; subject: string }>(
      `SELECT key_remote_jid AS jid, data AS subject FROM messages
       WHERE status = 6 AND media_size IN (1, 11) AND data IS NOT NULL AND data != ''
       ORDER BY _id DESC`
    ))
      put(r.jid, r.subject)
  }
  if (hasTable("message_system") && hasTable("message")) {
    for (const r of all<{ jid: string; subject: string }>(
      `SELECT j.raw_string AS jid, m.text_data AS subject FROM message_system s
       JOIN message m ON m._id = s.message_row_id
       JOIN chat c ON c._id = m.chat_row_id JOIN jid j ON j._id = c.jid_row_id
       WHERE s.action_type IN (1, 11) AND m.text_data IS NOT NULL AND m.text_data != ''
       ORDER BY m._id DESC`
    ))
      put(r.jid, r.subject)
  }
  // Group invite messages carry the group's name.
  if (hasTable("message_group_invite")) {
    for (const r of all<{ jid: string; name: string }>(
      `SELECT j.raw_string AS jid, i.group_name AS name FROM message_group_invite i
       JOIN jid j ON j._id = i.group_jid_row_id
       WHERE i.group_name IS NOT NULL AND i.group_name != '' ORDER BY i.message_row_id DESC`
    ))
      put(r.jid, r.name)
  }
}

function fromBusinessMessages(put: Put) {
  for (const table of ["message_system_business_state", "message_privacy_state"]) {
    if (!hasTable(table)) continue
    if (hasTable("messages")) {
      for (const r of all<{ jid: string; name: string }>(
        `SELECT m.key_remote_jid AS jid, b.business_name AS name FROM ${table} b
         JOIN messages m ON m._id = b.message_row_id
         WHERE b.business_name IS NOT NULL AND b.business_name != ''`
      ))
        put(r.jid, r.name)
    }
    if (hasTable("message")) {
      for (const r of all<{ jid: string; name: string }>(
        `SELECT j.raw_string AS jid, b.business_name AS name FROM ${table} b
         JOIN message m ON m._id = b.message_row_id
         JOIN chat c ON c._id = m.chat_row_id JOIN jid j ON j._id = c.jid_row_id
         WHERE b.business_name IS NOT NULL AND b.business_name != ''`
      ))
        put(r.jid, r.name)
    }
  }
}

function fromLidDisplayNames(put: Put) {
  if (!hasTable("lid_display_name")) return
  for (const r of all<{ jid: string; name: string }>(
    `SELECT j.raw_string AS jid, d.display_name AS name FROM lid_display_name d
     JOIN jid j ON j._id = d.lid_row_id WHERE d.display_name != ''`
  ))
    put(r.jid, r.name, false)
}

function fromVCards(put: Put) {
  const cards: { id: number; vcard: string; table: "legacy" | "new" }[] = []
  if (hasTable("messages_vcards"))
    for (const c of all<{ id: number; vcard: string }>("SELECT _id AS id, vcard FROM messages_vcards")) cards.push({ ...c, table: "legacy" })
  if (hasTable("message_vcard"))
    for (const c of all<{ id: number; vcard: string }>("SELECT _id AS id, vcard FROM message_vcard")) cards.push({ ...c, table: "new" })

  // Explicit vCard → JID links, when WhatsApp recorded them.
  const links = new Map<string, string[]>()
  const link = (key: string, jid: string) => links.set(key, [...(links.get(key) ?? []), jid])
  if (hasTable("messages_vcards_jids"))
    for (const r of all<{ vid: number; jid: string }>("SELECT vcard_row_id AS vid, vcard_jid AS jid FROM messages_vcards_jids WHERE vcard_jid IS NOT NULL"))
      link(`legacy:${r.vid}`, r.jid)
  if (hasTable("message_vcard_jid"))
    for (const r of all<{ vid: number; jid: string }>(
      "SELECT v.vcard_row_id AS vid, j.raw_string AS jid FROM message_vcard_jid v JOIN jid j ON j._id = v.vcard_jid_row_id"
    ))
      link(`new:${r.vid}`, r.jid)

  for (const c of cards) {
    if (!c.vcard) continue
    const card = parseVCard(c.vcard)
    const jids = new Set(links.get(`${c.table}:${c.id}`) ?? [])
    for (const p of card.phones) if (p.waId) jids.add(`${p.waId}@s.whatsapp.net`)
    for (const jid of jids) put(jid, card.name, false)
  }
}

function loadNames(): Map<string, NameEntry> {
  const names = new Map<string, NameEntry>()
  names.set(WHATSAPP_JID, { name: "WhatsApp", known: true })
  const put: Put = (jid, name, known = true) => {
    const clean = name?.trim()
    if (!jid || !clean) return
    // Store under the phone JID so LID and phone identities share one name.
    const key = canonicalJid(jid)
    if (!names.has(key)) names.set(key, { name: clean, known })
  }

  safely("contacts.json", () => fromOverrides(put))
  safely("wa.db contacts", () => fromWaContacts(put, "display_name", true))
  safely("group subjects", () => fromChatSubjects(put))
  safely("channel names", () => fromNewsletters(put))
  safely("group history", () => fromGroupHistory(put))
  safely("wa.db business names", () => fromWaBusinessNames(put))
  safely("business messages", () => fromBusinessMessages(put))
  safely("wa.db push names", () => fromWaContacts(put, "wa_name", false))
  safely("LID display names", () => fromLidDisplayNames(put))
  safely("vCards", () => fromVCards(put))

  return names
}

const names = () => cached("names", loadNames)

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

const ME: Participant = { jid: null, name: "You", phone: null, known: true }

export function me(): Participant {
  return ME
}

export function participant(jid: string | null | undefined): Participant {
  if (!jid) return ME
  const id = canonicalJid(jid)
  const phone = phoneFromJid(id)
  const entry = names().get(id)
  if (entry) return { jid: id, name: entry.known ? entry.name : `~${entry.name}`, phone, known: entry.known }
  return { jid: id, name: phone ?? jidUser(id), phone, known: false }
}

export function chatName(jid: string, subject?: string | null): string {
  if (subject) return subject
  const kind = chatKind(jid)
  if (kind === "status") return "Status updates"
  const entry = names().get(canonicalJid(jid))
  if (kind === "broadcast") return entry?.name ?? "Broadcast list"
  if (kind === "group") return entry?.name ?? `Group ${jidUser(jid)}`
  if (kind === "channel") return entry?.name ?? "Channel"
  if (kind === "bot") return entry?.name ?? "WhatsApp bot"
  if (kind === "individual") return participant(jid).name
  return entry?.name ?? jid
}
