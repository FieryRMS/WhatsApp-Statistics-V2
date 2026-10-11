import "server-only"

import { AsyncLocalStorage } from "node:async_hooks"
import fs from "node:fs"
import { DatabaseSync, type StatementSync } from "node:sqlite"
import path from "node:path"

/**
 * Data sources.
 *
 * A DataSource is one set of WhatsApp files (msgstore.db, optional wa.db and
 * contacts.json, optional media folder) with its own connection and caches.
 * Admins read the primary source; each restricted account reads its own
 * filtered copy (see lib/access/snapshot.ts).
 *
 * Every query helper below runs against the source selected for the current
 * request with `runWithSource()`. Outside of that there is no source, and they
 * throw — data access fails closed instead of defaulting to the full database.
 *
 * The primary source defaults to ./whatsapp-data. Override with env vars:
 *   WHATSAPP_DATA_DIR    – the folder itself
 *   WHATSAPP_DB_PATH     – msgstore.db                (default: <data dir>/msgstore.db)
 *   WHATSAPP_WA_DB_PATH  – wa.db, optional            (default: <data dir>/wa.db)
 *   WHATSAPP_CONTACTS    – contacts.json, optional    (default: <data dir>/contacts.json)
 *   WHATSAPP_MEDIA_DIR   – folder containing "Media/" (default: <data dir>)
 */

export interface SourcePaths {
  db: string
  /** wa.db, or null when this source may not use it (restricted accounts). */
  waDb: string | null
  /** Global contacts.json overrides shipped with the data, or null. */
  contacts: string | null
  /** Folder containing "Media/", or null when this source may not serve media files. */
  mediaRoot: string | null
  /** The viewing account's name mapping (lib/names/store.ts), or null for none. */
  names: string | null
  /** true = names come only from `names`; every automatic name source is off (restricted accounts). */
  namesOnly: boolean
}

const dataDir = process.env.WHATSAPP_DATA_DIR ?? path.join(/* turbopackIgnore: true */ process.cwd(), "whatsapp-data")

/** The full WhatsApp data with automatic names and no account mapping. */
export const primaryPaths: SourcePaths = {
  db: process.env.WHATSAPP_DB_PATH ?? path.join(dataDir, "msgstore.db"),
  waDb: process.env.WHATSAPP_WA_DB_PATH ?? path.join(dataDir, "wa.db"),
  contacts: process.env.WHATSAPP_CONTACTS ?? path.join(dataDir, "contacts.json"),
  mediaRoot: process.env.WHATSAPP_MEDIA_DIR ?? dataDir,
  names: null,
  namesOnly: false,
}

function stamp(p: string | null) {
  if (!p) return "-"
  try {
    const s = fs.statSync(p)
    return `${s.mtimeMs}:${s.size}`
  } catch {
    return "-"
  }
}

/** Fingerprint of a set of files; changes when any is replaced, added or removed. */
export function filesVersion(p: Pick<SourcePaths, "db" | "waDb" | "contacts"> & { names?: string | null }): string {
  return [p.db, p.waDb, p.contacts, p.names ?? null].map(stamp).join("|")
}

/**
 * Open a SQLite file strictly read-only. `immutable=1` also stops SQLite from
 * creating -shm/-wal files next to the user's database.
 */
export function openReadOnly(file: string): DatabaseSync {
  return new DatabaseSync(sqliteUri(file, "immutable=1"), { readOnly: true })
}

export function sqliteUri(file: string, query: string) {
  return `file:${encodeURI(path.resolve(file)).replace(/\?/g, "%3F").replace(/#/g, "%23")}?${query}`
}

export class DataSource {
  readonly paths: SourcePaths
  private conn: DatabaseSync | null = null
  private connVersion = ""
  private statements = new Map<string, StatementSync>()
  private tables = new Map<string, boolean>()
  private values = new Map<string, { version: string; value: unknown }>()
  private versionCheck = { at: 0, value: "" }

  constructor(paths: SourcePaths) {
    this.paths = paths
  }

  /** Changes when the source's files change (checked at most every 2s). */
  version(): string {
    const now = Date.now()
    if (now - this.versionCheck.at > 2000 || !this.versionCheck.value) {
      this.versionCheck = { at: now, value: filesVersion(this.paths) }
    }
    return this.versionCheck.value
  }

  db(): DatabaseSync {
    const v = this.version()
    if (!this.conn || this.connVersion !== v) {
      this.close()
      this.conn = openReadOnly(this.paths.db)
      this.connVersion = v
    }
    return this.conn
  }

  stmt(sql: string): StatementSync {
    const db = this.db()
    let st = this.statements.get(sql)
    if (!st) {
      st = db.prepare(sql)
      this.statements.set(sql, st)
    }
    return st
  }

  hasTable(name: string): boolean {
    this.db()
    let hit = this.tables.get(name)
    if (hit === undefined) {
      hit = !!this.stmt("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name)
      this.tables.set(name, hit)
    }
    return hit
  }

  cached<T>(key: string, compute: () => T): T {
    const version = this.version()
    const hit = this.values.get(key)
    if (hit && hit.version === version) return hit.value as T
    const value = compute()
    this.values.set(key, { version, value })
    return value
  }

  clearCache() {
    this.values.clear()
    this.versionCheck = { at: 0, value: "" }
  }

  close() {
    this.conn?.close()
    this.conn = null
    this.statements.clear()
    this.tables.clear()
  }
}

// ---------------------------------------------------------------------------
// Source registry + per-request scope
// ---------------------------------------------------------------------------

const globalForDb = globalThis as unknown as {
  __waSources?: Map<string, DataSource>
  __waScope?: AsyncLocalStorage<DataSource>
}

const sources = (globalForDb.__waSources ??= new Map())
const scope = (globalForDb.__waScope ??= new AsyncLocalStorage<DataSource>())

/** The shared DataSource for a set of paths (one connection/cache per distinct config). */
export function getSource(paths: SourcePaths): DataSource {
  const key = JSON.stringify([paths.db, paths.waDb, paths.contacts, paths.mediaRoot, paths.names, paths.namesOnly])
  let s = sources.get(key)
  if (!s) {
    s = new DataSource(paths)
    sources.set(key, s)
  }
  return s
}

/** Close and forget a source (e.g. a replaced snapshot) so its files can be deleted. */
export function dropSource(dbPath: string) {
  for (const [key, s] of sources) {
    if (s.paths.db === dbPath) {
      s.close()
      sources.delete(key)
    }
  }
}

/** The full data with automatic names only — for admin-side checks and seeding name mappings. */
export function primarySource(): DataSource {
  return getSource(primaryPaths)
}

/** Drop cached values of every source that reads `namesFile`, so an edit shows up immediately. */
export function invalidateNames(namesFile: string) {
  for (const s of sources.values()) if (s.paths.names === namesFile) s.clearCache()
}

/** Run `fn` with `source` as the data source for every query helper it calls. */
export function runWithSource<T>(source: DataSource, fn: () => T): T {
  return scope.run(source, fn)
}

/** The source selected for this request. Throws if none — never falls back to the primary DB. */
export function currentSource(): DataSource {
  const s = scope.getStore()
  if (!s) throw new Error("No data source in scope: wrap data access in runWithSource() (see lib/access/dal.ts)")
  return s
}

/** Drop memoized values in every source (called when a data module is reloaded in dev). */
export function clearCache() {
  for (const s of sources.values()) s.clearCache()
}

// ---------------------------------------------------------------------------
// Query helpers (all scoped to currentSource())
// ---------------------------------------------------------------------------

export type Row = Record<string, unknown>

export function sourcePaths(): SourcePaths {
  return currentSource().paths
}

export function cached<T>(key: string, compute: () => T): T {
  return currentSource().cached(key, compute)
}

export function all<T = Row>(sql: string, ...params: (string | number | null)[]): T[] {
  return currentSource().stmt(sql).all(...params) as T[]
}

export function get<T = Row>(sql: string, ...params: (string | number | null)[]): T | undefined {
  return currentSource().stmt(sql).get(...params) as T | undefined
}

/** Schemas differ between WhatsApp versions — check before querying optional tables. */
export function hasTable(name: string): boolean {
  return currentSource().hasTable(name)
}

/** "?, ?, ?" for IN clauses. */
export function placeholders(n: number) {
  return Array.from({ length: n }, () => "?").join(", ")
}
