import "server-only"

import fs from "node:fs"
import type { DatabaseSync } from "node:sqlite"

import { all, cached, clearCache, hasTable, openReadOnly, sourcePaths } from "./db"
import { parseVCard } from "./decoders"
import { chatKind, jidServer, jidUser, phoneFromJid } from "./jid"
import type { Participant } from "./types"

/**
 * Display-name resolution.
 *
 * First comes the viewing account's own name mapping (lib/names/store.ts): a
 * name renames someone, null hides their name. For restricted accounts
 * (`namesOnly`) that file is the *only* source — nothing below is consulted.
 *
 * Otherwise names are built automatically from whatever databases are present.
 * msgstore.db never stores your address book (that lives in wa.db), but it does
 * contain names in several places. Sources, highest priority first:
 *
 *   1. contacts.json (optional)  — global { jid: name } overrides shipped with the data
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
  /** null = name deliberately hidden by the account's mapping (show the number). */
  name: string | null
  /** false = a hint (push name / someone else's label), rendered with "~". */
  known: boolean
  /** Came from the account's own name mapping. */
  mapped?: boolean
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
  if (!file || !fs.existsSync(file)) return
  const json = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, string>
  for (const [jid, name] of Object.entries(json)) put(jid, name)
}

function withWaDb(fn: (wa: DatabaseSync) => void) {
  const file = sourcePaths().waDb
  if (!file || !fs.existsSync(file)) return
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

/** The account's mapping file: strings rename, null hides. Read raw so this module needs no app-data imports. */
function fromAccountMapping(names: Map<string, NameEntry>) {
  const file = sourcePaths().names
  if (!file || !fs.existsSync(file)) return
  const json = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>
  for (const [jid, value] of Object.entries(json)) {
    const key = canonicalJid(jid)
    if (names.has(key)) continue
    if (value === null) names.set(key, { name: null, known: false, mapped: true })
    else if (typeof value === "string" && value.trim()) {
      const name = value.trim()
      // "~Name" in a mapping keeps the hint styling it was seeded with.
      names.set(key, name.startsWith("~") ? { name: name.slice(1), known: false, mapped: true } : { name, known: true, mapped: true })
    }
  }
}

function loadNames(): Map<string, NameEntry> {
  const names = new Map<string, NameEntry>()
  safely("name mapping", () => fromAccountMapping(names))
  // Restricted accounts: the mapping is the only source of names.
  if (sourcePaths().namesOnly) return names

  if (!names.has(WHATSAPP_JID)) names.set(WHATSAPP_JID, { name: "WhatsApp", known: true })
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
  if (entry?.name) return { jid: id, name: entry.known ? entry.name : `~${entry.name}`, phone, known: entry.known }
  return { jid: id, name: phone ?? jidUser(id), phone, known: false }
}

export function chatName(jid: string, subject?: string | null): string {
  const mapped = names().get(canonicalJid(jid))
  if (mapped?.mapped && mapped.name) return mapped.name
  if (subject) return subject
  const kind = chatKind(jid)
  if (kind === "status") return "Status updates"
  const found = names().get(canonicalJid(jid))
  const entry = found?.name ? { name: found.name } : undefined
  if (kind === "broadcast") return entry?.name ?? "Broadcast list"
  if (kind === "group") return entry?.name ?? `Group ${jidUser(jid)}`
  if (kind === "channel") return entry?.name ?? "Channel"
  if (kind === "bot") return entry?.name ?? "WhatsApp bot"
  if (kind === "individual") return participant(jid).name
  return entry?.name ?? jid
}

/** Name from the automatic sources only (ignores the account mapping) — used to seed/suggest names. */
export function automaticName(jid: string): { name: string; hint: boolean } | null {
  const id = canonicalJid(jid)
  const entry = names().get(id)
  if (!entry?.name || entry.mapped) return null
  return { name: entry.name, hint: !entry.known }
}

/**
 * Every person (not group/channel) who appears in this source: individual chats,
 * message senders, system-event participants, mentions and reaction senders.
 * Canonical phone JIDs, sorted. Cached per source.
 */
export function listPeople(): string[] {
  return cached("people", () => {
    const raw = new Set<string>()
    const add = (rows: { jid: string | null }[]) => rows.forEach((r) => r.jid && raw.add(r.jid))
    safely("people", () => {
      if (hasTable("message") && hasTable("jid")) {
        const refs: string[] = ["SELECT jid_row_id AS id FROM chat", "SELECT sender_jid_row_id AS id FROM message"]
        if (hasTable("message_system_chat_participant")) refs.push("SELECT user_jid_row_id FROM message_system_chat_participant")
        if (hasTable("message_mentions")) refs.push("SELECT jid_row_id FROM message_mentions")
        if (hasTable("message_add_on")) refs.push("SELECT sender_jid_row_id FROM message_add_on")
        add(all(`SELECT raw_string AS jid FROM jid WHERE _id IN (${refs.join(" UNION ")})`))
      }
      if (hasTable("messages")) {
        add(all("SELECT DISTINCT key_remote_jid AS jid FROM messages"))
        add(all("SELECT DISTINCT remote_resource AS jid FROM messages WHERE remote_resource IS NOT NULL AND remote_resource != ''"))
      }
    })
    const people = new Set<string>()
    for (const jid of raw) {
      if (chatKind(jid) !== "individual" || !/^\d+@/.test(jid)) continue
      people.add(canonicalJid(jid))
    }
    // Anyone already in the mapping stays listed, even if their messages left the account's access.
    for (const [jid, entry] of names()) if (entry.mapped && jid.includes("@")) people.add(jid)
    return [...people].sort()
  })
}

/** Key in an account's name mapping for the phone owner ("you" in the raw data). */
export const OWNER_KEY = "me"

/** The phone owner's name from the account's mapping, if one was set. */
export function ownerName(): string | null {
  const entry = names().get(OWNER_KEY)
  return entry?.mapped && entry.name ? entry.name : null
}

/**
 * People merged by the account's name mapping: JIDs given exactly the same
 * name there are the same person (e.g. someone's two numbers). Only explicit
 * mapping entries count — automatic names never merge anyone.
 *
 * Returns jid → the group's representative JID (the alphabetically first member).
 */
export function personGroups(): Map<string, string> {
  return cached("personGroups", () => {
    const byName = new Map<string, string[]>()
    for (const [jid, entry] of names()) {
      if (!entry.mapped || !entry.name || !jid.includes("@")) continue
      const key = entry.known ? entry.name : `~${entry.name}`
      byName.set(key, [...(byName.get(key) ?? []), jid])
    }
    const groups = new Map<string, string>()
    for (const members of byName.values()) {
      if (members.length < 2) continue
      const rep = [...members].sort()[0]
      for (const m of members) groups.set(m, rep)
    }
    return groups
  })
}

/** The representative JID for `jid` (itself when not merged). */
export function samePerson(jid: string): string {
  const id = canonicalJid(jid)
  return personGroups().get(id) ?? id
}

/** Every JID merged with `jid` (including itself). */
export function personMembers(jid: string): string[] {
  const rep = samePerson(jid)
  const members = [...personGroups()].filter(([, r]) => r === rep).map(([m]) => m)
  return members.length ? members : [canonicalJid(jid)]
}
