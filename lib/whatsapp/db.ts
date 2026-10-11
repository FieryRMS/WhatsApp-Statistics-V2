import fs from "node:fs"
import { DatabaseSync, type StatementSync } from "node:sqlite"
import path from "node:path"

/**
 * Read-only connection to msgstore.db.
 *
 * Everything defaults to ./whatsapp-data. Override with env vars:
 *   WHATSAPP_DATA_DIR    – the folder itself
 *   WHATSAPP_DB_PATH     – msgstore.db                (default: <data dir>/msgstore.db)
 *   WHATSAPP_WA_DB_PATH  – wa.db, optional            (default: <data dir>/wa.db)
 *   WHATSAPP_CONTACTS    – contacts.json, optional    (default: <data dir>/contacts.json)
 *   WHATSAPP_MEDIA_DIR   – folder containing "Media/" (default: <data dir>)
 */
const dataDir = process.env.WHATSAPP_DATA_DIR ?? path.join(process.cwd(), "whatsapp-data")

export const paths = {
  dataDir,
  db: process.env.WHATSAPP_DB_PATH ?? path.join(dataDir, "msgstore.db"),
  waDb: process.env.WHATSAPP_WA_DB_PATH ?? path.join(dataDir, "wa.db"),
  contacts: process.env.WHATSAPP_CONTACTS ?? path.join(dataDir, "contacts.json"),
  mediaRoot: process.env.WHATSAPP_MEDIA_DIR ?? dataDir,
}

const tableCache = new Map<string, boolean>()

interface DbState {
  db: DatabaseSync
  statements: Map<string, StatementSync>
  version: string
}

const globalForDb = globalThis as unknown as {
  __waDb?: DbState
  __waCache?: Map<string, { version: string; value: unknown }>
  __waVersion?: { checkedAt: number; value: string }
}

function stamp(p: string) {
  try {
    const s = fs.statSync(p)
    return `${s.mtimeMs}:${s.size}`
  } catch {
    return "-"
  }
}

/**
 * Changes whenever msgstore.db, wa.db or contacts.json is replaced/added/removed
 * (checked at most every 2s), so swapping in another person's database needs no restart.
 */
export function dataVersion(): string {
  const now = Date.now()
  const v = globalForDb.__waVersion
  if (v && now - v.checkedAt < 2000) return v.value
  const value = [paths.db, paths.waDb, paths.contacts].map(stamp).join("|")
  globalForDb.__waVersion = { checkedAt: now, value }
  return value
}

/**
 * Open a SQLite file strictly read-only. `immutable=1` also stops SQLite from
 * creating -shm/-wal files next to the user's database.
 */
export function openReadOnly(file: string): DatabaseSync {
  const uri = `file:${encodeURI(path.resolve(file)).replace(/\?/g, "%3F").replace(/#/g, "%23")}?immutable=1`
  return new DatabaseSync(uri, { readOnly: true })
}

function state(): DbState {
  const version = dataVersion()
  const s = globalForDb.__waDb
  if (s && s.version === version) return s
  s?.db.close()
  tableCache.clear()
  globalForDb.__waDb = {
    db: openReadOnly(paths.db),
    statements: new Map(),
    version,
  }
  return globalForDb.__waDb
}

/** Drop memoized values (called when a data module is reloaded in dev). */
export function clearCache() {
  globalForDb.__waCache?.clear()
}

/** Memoize a derived value until the underlying data files change. */
export function cached<T>(key: string, compute: () => T): T {
  const cache = (globalForDb.__waCache ??= new Map())
  const version = dataVersion()
  const hit = cache.get(key)
  if (hit && hit.version === version) return hit.value as T
  const value = compute()
  cache.set(key, { version, value })
  return value
}

export function db() {
  return state().db
}

/** Prepared statement, cached by SQL text. */
export function stmt(sql: string): StatementSync {
  const s = state()
  let st = s.statements.get(sql)
  if (!st) {
    st = s.db.prepare(sql)
    s.statements.set(sql, st)
  }
  return st
}

export type Row = Record<string, unknown>

export function all<T = Row>(sql: string, ...params: (string | number | null)[]): T[] {
  return stmt(sql).all(...params) as T[]
}

export function get<T = Row>(sql: string, ...params: (string | number | null)[]): T | undefined {
  return stmt(sql).get(...params) as T | undefined
}

/** Schemas differ between WhatsApp versions — check before querying optional tables. */
export function hasTable(name: string): boolean {
  let hit = tableCache.get(name)
  if (hit === undefined) {
    hit = !!get("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", name)
    tableCache.set(name, hit)
  }
  return hit
}

/** "?, ?, ?" for IN clauses. */
export function placeholders(n: number) {
  return Array.from({ length: n }, () => "?").join(", ")
}
