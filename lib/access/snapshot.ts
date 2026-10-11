import "server-only"

import { createHash, randomBytes } from "node:crypto"
import fs from "node:fs"
import { DatabaseSync } from "node:sqlite"
import path from "node:path"

import { appDataDir, getSnapshotRecord, listGrants, putSnapshotRecord, deleteSnapshotRecord, type Grant, type User } from "../auth/store"
import { namesFile, seedNames, type NameMap } from "../names/store"
import { automaticName, canonicalJid } from "../whatsapp/contacts"
import {
  dropSource,
  filesVersion,
  getSource,
  openReadOnly,
  primaryPaths,
  primarySource,
  runWithSource,
  sqliteUri,
  type DataSource,
} from "../whatsapp/db"
import { javaStrings } from "../whatsapp/decoders"
import { chatKind } from "../whatsapp/jid"

/**
 * Per-account filtered copies ("snapshots") of the WhatsApp data.
 *
 * A restricted account never reads the primary database. It reads its own
 * msgstore.db/wa.db/contacts.json containing only what its grants allow, so no
 * query, bug or future feature can reach anything else.
 *
 * Built by allowlist: only the tables the app reads are copied, each filtered
 * to the granted chats/date ranges. Everything else (full-text index, call log,
 * statuses, tables added by future WhatsApp versions) is simply absent.
 * Every build is verified before it is used.
 *
 * Snapshots are rebuilt automatically when the grants or the primary files change.
 * They carry no wa.db/contacts.json: restricted accounts get names only from
 * their own mapping file (lib/names/store.ts), which is seeded from here.
 */

/** Bump when the snapshot contents change, so existing snapshots are rebuilt. */
const SNAPSHOT_FORMAT = 2

const snapshotsRoot = path.join(appDataDir, "snapshots")

type Db = DatabaseSync

// ---------------------------------------------------------------------------
// Copy plans
// ---------------------------------------------------------------------------

/** Modern layout: side tables keyed by message_row_id. */
const MODERN_MESSAGE_TABLES = [
  "message_media",
  "message_thumbnail",
  "message_location",
  "message_text",
  "message_forwarded",
  "message_view_once_media",
  "missed_call_logs",
  "message_call_log",
  "message_system",
  "message_system_chat_participant",
  "message_system_value_change",
  "message_system_number_change",
  "message_system_photo_change",
  "message_system_linked_group_call",
  "message_system_business_state",
  "message_privacy_state",
  "message_ephemeral_setting",
  "message_edit_info",
  "message_revoked",
  "message_quoted",
  "message_quoted_media",
  "message_quoted_text",
  "message_album",
  "message_poll",
  "message_poll_option",
  "message_vcard",
  "message_vcard_jid",
  "message_mentions",
  "message_quoted_mentions",
  "message_group_invite",
]

/** Legacy layout: side tables keyed by message_row_id (= messages._id). */
const LEGACY_MESSAGE_TABLES = [
  "message_media",
  "message_view_once_media",
  "missed_call_logs",
  "message_forwarded",
  "messages_vcards",
  "messages_vcards_jids",
  "message_system_business_state",
  "message_privacy_state",
  "message_group_invite",
]

class Copier {
  constructor(readonly out: Db) {}

  srcHas(table: string) {
    return !!this.out.prepare("SELECT 1 FROM src.sqlite_master WHERE type = 'table' AND name = ?").get(table)
  }

  has(table: string) {
    return !!this.out.prepare("SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?").get(table)
  }

  columns(table: string, schema = "main"): string[] {
    return (this.out.prepare(`SELECT name FROM pragma_table_info(?, ?)`).all(table, schema) as { name: string }[]).map((c) => c.name)
  }

  /** Copy a table's definition and the rows matching `where` (an SQL condition on alias `t`). */
  copy(table: string, where: string) {
    if (!this.srcHas(table)) return false
    const def = this.out.prepare("SELECT sql FROM src.sqlite_master WHERE type = 'table' AND name = ?").get(table) as { sql: string }
    this.out.exec(def.sql)
    this.out.exec(`INSERT INTO main."${table}" SELECT t.* FROM src."${table}" t WHERE ${where}`)
    const indexes = this.out
      .prepare("SELECT sql FROM src.sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL")
      .all(table) as { sql: string }[]
    for (const ix of indexes) this.out.exec(ix.sql)
    return true
  }

  count(sql: string): number {
    return (this.out.prepare(sql).get() as { n: number }).n
  }
}

function layoutOf(c: Copier): "legacy" | "modern" {
  const maxId = (t: string) => (c.srcHas(t) ? ((c.out.prepare(`SELECT MAX(_id) AS id FROM src."${t}"`).get() as { id: number | null }).id ?? 0) : -1)
  const legacy = maxId("messages")
  const modern = maxId("message")
  if (legacy < 0 && modern < 0) throw new Error("Unrecognized msgstore.db: no messages table")
  return legacy >= modern ? "legacy" : "modern"
}

/** Every jid_row_id-style column in the copied tables → the jid rows the snapshot needs. */
function collectJidRows(c: Copier) {
  c.out.exec("CREATE TEMP TABLE ref_jid (id INTEGER PRIMARY KEY)")
  const tables = (c.out.prepare("SELECT name FROM main.sqlite_master WHERE type = 'table'").all() as { name: string }[]).map((t) => t.name)
  for (const t of tables) {
    if (t === "jid") continue
    for (const col of c.columns(t)) {
      if (col.endsWith("jid_row_id")) c.out.exec(`INSERT OR IGNORE INTO temp.ref_jid SELECT "${col}" FROM main."${t}" WHERE "${col}" > 0`)
    }
  }
}

/** Add both ends of LID ↔ phone mappings for the referenced people, then copy jid/jid_map. */
function copyJids(c: Copier) {
  if (c.srcHas("jid_map")) {
    c.out.exec("INSERT OR IGNORE INTO temp.ref_jid SELECT jid_row_id FROM src.jid_map WHERE lid_row_id IN (SELECT id FROM temp.ref_jid)")
    c.out.exec("INSERT OR IGNORE INTO temp.ref_jid SELECT lid_row_id FROM src.jid_map WHERE jid_row_id IN (SELECT id FROM temp.ref_jid)")
  }
  c.copy("jid", "t._id IN (SELECT id FROM temp.ref_jid)")
  c.copy("jid_map", "t.lid_row_id IN (SELECT id FROM temp.ref_jid) AND t.jid_row_id IN (SELECT id FROM temp.ref_jid)")
  c.copy("lid_display_name", "t.lid_row_id IN (SELECT id FROM temp.ref_jid)")
}

const inSel = (col: string) => `t."${col}" IN (SELECT id FROM temp.sel_msg)`

function buildModern(c: Copier) {
  c.out.exec(`
    CREATE TEMP TABLE sel_chat AS
      SELECT c._id AS id, g.from_ts, g.to_ts FROM src.chat c
      JOIN src.jid j ON j._id = c.jid_row_id
      JOIN temp.grants g ON g.jid = j.raw_string;
    CREATE TEMP TABLE sel_msg (id INTEGER PRIMARY KEY);
    INSERT OR IGNORE INTO temp.sel_msg
      SELECT m._id FROM src.message m JOIN temp.sel_chat s ON s.id = m.chat_row_id
      WHERE (s.from_ts IS NULL OR m.timestamp >= s.from_ts) AND (s.to_ts IS NULL OR m.timestamp < s.to_ts);
  `)
  c.copy("message", inSel("_id"))
  c.copy("chat", "t._id IN (SELECT id FROM temp.sel_chat)")
  for (const t of MODERN_MESSAGE_TABLES) c.copy(t, inSel("message_row_id"))
  c.copy("message_add_on", inSel("parent_message_row_id"))
  if (c.has("message_add_on")) c.copy("message_add_on_reaction", "t.message_add_on_row_id IN (SELECT _id FROM main.message_add_on)")
  if (c.has("message_call_log")) c.copy("call_log", "t._id IN (SELECT call_log_row_id FROM main.message_call_log)")
  if (c.has("message_media")) c.copy("media_hash_thumbnail", "t.media_hash IN (SELECT file_hash FROM main.message_media)")
  c.copy("newsletter", "t.chat_row_id IN (SELECT id FROM temp.sel_chat)")
  collectJidRows(c)
  copyJids(c)
}

function buildLegacy(c: Copier) {
  c.out.exec(`
    CREATE TEMP TABLE sel_msg (id INTEGER PRIMARY KEY);
    INSERT OR IGNORE INTO temp.sel_msg
      SELECT m._id FROM src.messages m JOIN temp.grants g ON g.jid = m.key_remote_jid
      WHERE (g.from_ts IS NULL OR m.timestamp >= g.from_ts) AND (g.to_ts IS NULL OR m.timestamp < g.to_ts);
  `)
  c.copy("messages", inSel("_id"))
  if (c.srcHas("chat") && c.srcHas("jid"))
    c.copy("chat", "t.jid_row_id IN (SELECT _id FROM src.jid WHERE raw_string IN (SELECT jid FROM temp.grants))")
  for (const t of LEGACY_MESSAGE_TABLES) c.copy(t, inSel("message_row_id"))
  c.copy(
    "message_thumbnails",
    `EXISTS (SELECT 1 FROM main.messages m
             WHERE m.key_remote_jid = t.key_remote_jid AND m.key_from_me = t.key_from_me AND m.key_id = t.key_id)`
  )
  c.copy("messages_quotes", "t._id IN (SELECT quoted_row_id FROM main.messages WHERE quoted_row_id > 0)")
  c.copy("message_add_on", inSel("parent_message_row_id"))
  if (c.has("message_add_on")) c.copy("message_add_on_reaction", "t.message_add_on_row_id IN (SELECT _id FROM main.message_add_on)")
  c.copy("media_hash_thumbnail", "t.media_hash IN (SELECT media_hash FROM main.messages WHERE media_hash IS NOT NULL)")

  collectJidRows(c)
  // Legacy rows reference people by raw JID string rather than jid row id.
  if (c.srcHas("jid")) {
    c.out.exec(`
      INSERT OR IGNORE INTO temp.ref_jid SELECT _id FROM src.jid WHERE raw_string IN (
        SELECT key_remote_jid FROM main.messages UNION SELECT remote_resource FROM main.messages WHERE remote_resource != ''
      )`)
    copyJids(c)
  }
}

// ---------------------------------------------------------------------------
// Verification — a snapshot is only used if it provably contains nothing else
// ---------------------------------------------------------------------------

function verify(c: Copier, layout: "legacy" | "modern") {
  const problems: string[] = []
  const timeOk = (alias: string, g: string) =>
    `(${g}.from_ts IS NULL OR ${alias}.timestamp >= ${g}.from_ts) AND (${g}.to_ts IS NULL OR ${alias}.timestamp < ${g}.to_ts)`

  const outside =
    layout === "modern"
      ? c.count(`SELECT COUNT(*) AS n FROM main.message m WHERE NOT EXISTS (
           SELECT 1 FROM main.chat ch JOIN main.jid j ON j._id = ch.jid_row_id JOIN temp.grants g ON g.jid = j.raw_string
           WHERE ch._id = m.chat_row_id AND ${timeOk("m", "g")})`)
      : c.count(`SELECT COUNT(*) AS n FROM main.messages m WHERE NOT EXISTS (
           SELECT 1 FROM temp.grants g WHERE g.jid = m.key_remote_jid AND ${timeOk("m", "g")})`)
  if (outside) problems.push(`${outside} messages outside the grants`)

  const msgTable = layout === "modern" ? "message" : "messages"
  const tables = (c.out.prepare("SELECT name FROM main.sqlite_master WHERE type = 'table'").all() as { name: string }[]).map((t) => t.name)
  for (const t of tables) {
    for (const col of ["message_row_id", "parent_message_row_id"]) {
      if (!c.columns(t).includes(col) || t === msgTable) continue
      const n = c.count(`SELECT COUNT(*) AS n FROM main."${t}" WHERE "${col}" NOT IN (SELECT _id FROM main."${msgTable}")`)
      if (n) problems.push(`${t}: ${n} rows reference messages outside the snapshot`)
    }
  }
  const chatsOutside =
    c.has("chat") && c.has("jid")
      ? c.count(`SELECT COUNT(*) AS n FROM main.chat ch LEFT JOIN main.jid j ON j._id = ch.jid_row_id
                 WHERE j.raw_string IS NULL OR j.raw_string NOT IN (SELECT jid FROM temp.grants)`)
      : 0
  if (chatsOutside) problems.push(`${chatsOutside} chats outside the grants`)

  if (problems.length) throw new Error(`Snapshot verification failed: ${problems.join("; ")}`)
}

// ---------------------------------------------------------------------------
// Names: restricted accounts get names only from their mapping file, seeded here
// ---------------------------------------------------------------------------

/** Raw JID strings that appear anywhere in the snapshot. */
function visibleJids(out: Db, layout: "legacy" | "modern"): Set<string> {
  const set = new Set<string>()
  const add = (s: unknown) => typeof s === "string" && s && set.add(s)
  const exists = (t: string) => !!out.prepare("SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?").get(t)
  if (exists("jid")) for (const r of out.prepare("SELECT raw_string FROM main.jid").all() as { raw_string: string }[]) add(r.raw_string)
  if (layout === "legacy") {
    const rows = out
      .prepare(
        `SELECT key_remote_jid, remote_resource, mentioned_jids, media_name, media_caption,
           CASE WHEN status = 6 THEN thumb_image END AS sys FROM main.messages`
      )
      .all() as { key_remote_jid: string; remote_resource: string | null; mentioned_jids: string | null; media_name: string | null; media_caption: string | null; sys: Uint8Array | null }[]
    for (const r of rows) {
      add(r.key_remote_jid)
      add(r.remote_resource)
      r.mentioned_jids?.split(",").forEach((j) => add(j.trim()))
      if (r.sys) {
        javaStrings(r.sys).forEach(add)
        if (r.media_name?.includes("@")) add(r.media_name)
        if (r.media_caption?.includes("@")) add(r.media_caption)
      }
    }
  }
  return set
}

// ---------------------------------------------------------------------------
// Build + lifecycle
// ---------------------------------------------------------------------------

function grantsHash(grants: Grant[]) {
  return createHash("sha256")
    .update(JSON.stringify([SNAPSHOT_FORMAT, grants.map((g) => [g.chatJid, g.fromTs, g.toTs]).sort()]))
    .digest("hex")
}

/**
 * Add everyone visible in the snapshot to the account's name mapping, with the
 * name they'd see automatically (or null = phone number). Existing entries —
 * including names the account or an admin changed or hid — are left alone.
 */
function seedNamesFromSnapshot(userId: number, dir: string): number {
  const db = openReadOnly(path.join(dir, "msgstore.db"))
  let visible: Set<string>
  try {
    const legacy = !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'messages'").get()
    visible = visibleJids(db, legacy ? "legacy" : "modern")
  } finally {
    db.close()
  }
  const seed: NameMap = {}
  runWithSource(primarySource(), () => {
    for (const jid of visible) {
      if (chatKind(jid) !== "individual" || !/^\d+@/.test(jid)) continue
      const id = canonicalJid(jid)
      if (id in seed) continue
      const auto = automaticName(id)
      seed[id] = auto ? (auto.hint ? `~${auto.name}` : auto.name) : null
    }
  })
  return seedNames(userId, seed)
}

function userDir(userId: number) {
  return path.join(snapshotsRoot, `u${userId}`)
}

export interface BuildResult {
  dir: string
  layout: "legacy" | "modern"
  messages: number
  chats: number
  ms: number
}

function build(userId: number, grants: Grant[]): BuildResult {
  const started = Date.now()
  const dir = path.join(userDir(userId), `${started}-${randomBytes(4).toString("hex")}`)
  fs.mkdirSync(dir, { recursive: true })
  try {
    const out = new DatabaseSync(sqliteUri(path.join(dir, "msgstore.db"), "mode=rwc"))
    let layout: "legacy" | "modern"
    let messages = 0
    let chats = 0
    try {
      out.exec("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
      out.prepare("ATTACH DATABASE ? AS src").run(sqliteUri(primaryPaths.db, "immutable=1&mode=ro"))
      out.exec("CREATE TEMP TABLE grants (jid TEXT NOT NULL, from_ts INTEGER, to_ts INTEGER)")
      const ins = out.prepare("INSERT INTO temp.grants VALUES (?, ?, ?)")
      for (const g of grants) ins.run(g.chatJid, g.fromTs, g.toTs)

      const c = new Copier(out)
      layout = layoutOf(c)
      if (layout === "modern") buildModern(c)
      else buildLegacy(c)
      verify(c, layout)

      messages = c.count(`SELECT COUNT(*) AS n FROM main.${layout === "modern" ? "message" : "messages"}`)
      chats = new Set(grants.map((g) => g.chatJid)).size
      out.exec("DETACH DATABASE src")
    } finally {
      out.close()
    }
    return { dir, layout, messages, chats, ms: Date.now() - started }
  } catch (e) {
    fs.rmSync(dir, { recursive: true, force: true })
    throw e
  }
}

function sourceFor(dir: string, user: User): DataSource {
  return getSource({
    db: path.join(dir, "msgstore.db"),
    waDb: null,
    contacts: null,
    // Media files are served only to accounts allowed to see them.
    mediaRoot: user.mediaAccess ? primaryPaths.mediaRoot : null,
    // Names come only from the account's own mapping.
    names: namesFile(user.id),
    namesOnly: true,
  })
}

/** Remove every snapshot directory of a user except `keep`. */
function cleanup(userId: number, keep: string | null) {
  const base = userDir(userId)
  if (!fs.existsSync(base)) return
  for (const name of fs.readdirSync(base)) {
    const dir = path.join(base, name)
    if (dir === keep) continue
    dropSource(path.join(dir, "msgstore.db"))
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

const globalForBuilds = globalThis as unknown as { __waBuilds?: Map<number, Promise<DataSource>> }
const inFlight = (globalForBuilds.__waBuilds ??= new Map())

/**
 * The data source for a restricted account, (re)building its snapshot if the
 * grants or the primary data changed. Concurrent callers share one build.
 */
export async function ensureSnapshot(user: User): Promise<DataSource> {
  const grants = listGrants(user.id)
  const wantHash = grantsHash(grants)
  const wantVersion = filesVersion(primaryPaths)
  const rec = getSnapshotRecord(user.id)
  if (rec && rec.grantsHash === wantHash && rec.sourceVersion === wantVersion && fs.existsSync(path.join(rec.dir, "msgstore.db"))) {
    if (!fs.existsSync(namesFile(user.id))) seedNamesFromSnapshot(user.id, rec.dir)
    return sourceFor(rec.dir, user)
  }

  const pending = inFlight.get(user.id)
  if (pending) return pending
  const job = (async () => {
    try {
      const result = build(user.id, grants)
      const seeded = seedNamesFromSnapshot(user.id, result.dir)
      putSnapshotRecord({ userId: user.id, dir: result.dir, grantsHash: wantHash, sourceVersion: wantVersion, builtAt: Date.now() })
      cleanup(user.id, result.dir)
      console.log(
        `[access] built snapshot for user ${user.id}: ${result.messages} messages in ${result.chats} chats (${result.layout}, ${result.ms} ms); ${seeded} new name entries`
      )
      return sourceFor(result.dir, user)
    } finally {
      inFlight.delete(user.id)
    }
  })()
  inFlight.set(user.id, job)
  return job
}

/** Delete a user's snapshots (on account deletion). */
export function removeSnapshots(userId: number) {
  cleanup(userId, null)
  deleteSnapshotRecord(userId)
  fs.rmSync(userDir(userId), { recursive: true, force: true })
}
