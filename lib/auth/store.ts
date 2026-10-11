import "server-only"

import fs from "node:fs"
import { DatabaseSync } from "node:sqlite"
import path from "node:path"

/**
 * Account store: users, sessions, access grants and snapshot bookkeeping.
 * Lives in its own database (default ./app-data/app.db, override with
 * WHATSAPP_APP_DATA_DIR), never mixed with WhatsApp data.
 */

export const appDataDir = process.env.WHATSAPP_APP_DATA_DIR ?? path.join(process.cwd(), "app-data")

export type Role = "admin" | "user"

export interface User {
  id: number
  username: string
  role: Role
  disabled: boolean
  mediaAccess: boolean
  createdAt: number
}

export interface Grant {
  id: number
  userId: number
  chatJid: string
  /** Inclusive start (ms), or null for "from the beginning". */
  fromTs: number | null
  /** Exclusive end (ms), or null for "until the end". */
  toTs: number | null
}

export interface SnapshotRecord {
  userId: number
  dir: string
  grantsHash: string
  sourceVersion: string
  builtAt: number
}

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  id            INTEGER PRIMARY KEY,
  username      TEXT NOT NULL UNIQUE COLLATE NOCASE,
  password_hash TEXT NOT NULL,
  role          TEXT NOT NULL CHECK (role IN ('admin', 'user')),
  disabled      INTEGER NOT NULL DEFAULT 0,
  media_access  INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id           TEXT PRIMARY KEY,           -- sha256(token); the raw token only lives in the cookie
  user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at   INTEGER NOT NULL,
  expires_at   INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL,
  user_agent   TEXT
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS grants (
  id       INTEGER PRIMARY KEY,
  user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  chat_jid TEXT NOT NULL,
  from_ts  INTEGER,
  to_ts    INTEGER
);
CREATE INDEX IF NOT EXISTS grants_user ON grants(user_id);

CREATE TABLE IF NOT EXISTS snapshots (
  user_id        INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  dir            TEXT NOT NULL,
  grants_hash    TEXT NOT NULL,
  source_version TEXT NOT NULL,
  built_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

const globalForStore = globalThis as unknown as { __waAppDb?: DatabaseSync }

export function appDb(): DatabaseSync {
  if (!globalForStore.__waAppDb) {
    fs.mkdirSync(appDataDir, { recursive: true })
    const db = new DatabaseSync(path.join(appDataDir, "app.db"))
    db.exec(SCHEMA)
    globalForStore.__waAppDb = db
  }
  return globalForStore.__waAppDb
}

interface UserRow {
  id: number
  username: string
  password_hash: string
  role: Role
  disabled: number
  media_access: number
  created_at: number
}

const toUser = (r: UserRow): User => ({
  id: r.id,
  username: r.username,
  role: r.role,
  disabled: !!r.disabled,
  mediaAccess: !!r.media_access,
  createdAt: r.created_at,
})

// --- users -----------------------------------------------------------------

export function countAdmins(): number {
  return (appDb().prepare("SELECT COUNT(*) AS n FROM users WHERE role = 'admin'").get() as { n: number }).n
}

export function listUsers(): User[] {
  return (appDb().prepare("SELECT * FROM users ORDER BY role, username").all() as unknown as UserRow[]).map(toUser)
}

export function getUser(id: number): User | null {
  const r = appDb().prepare("SELECT * FROM users WHERE id = ?").get(id) as unknown as UserRow | undefined
  return r ? toUser(r) : null
}

export function getUserWithHash(username: string): (User & { passwordHash: string }) | null {
  const r = appDb().prepare("SELECT * FROM users WHERE username = ?").get(username) as unknown as UserRow | undefined
  return r ? { ...toUser(r), passwordHash: r.password_hash } : null
}

export function createUser(u: { username: string; passwordHash: string; role: Role; mediaAccess: boolean }): number {
  const res = appDb()
    .prepare("INSERT INTO users (username, password_hash, role, media_access, created_at) VALUES (?, ?, ?, ?, ?)")
    .run(u.username, u.passwordHash, u.role, u.mediaAccess ? 1 : 0, Date.now())
  return Number(res.lastInsertRowid)
}

export function updateUser(id: number, patch: Partial<{ role: Role; disabled: boolean; mediaAccess: boolean; passwordHash: string }>) {
  const db = appDb()
  if (patch.role) db.prepare("UPDATE users SET role = ? WHERE id = ?").run(patch.role, id)
  if (patch.disabled !== undefined) db.prepare("UPDATE users SET disabled = ? WHERE id = ?").run(patch.disabled ? 1 : 0, id)
  if (patch.mediaAccess !== undefined) db.prepare("UPDATE users SET media_access = ? WHERE id = ?").run(patch.mediaAccess ? 1 : 0, id)
  if (patch.passwordHash) db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(patch.passwordHash, id)
}

export function deleteUser(id: number) {
  appDb().prepare("DELETE FROM users WHERE id = ?").run(id)
}

// --- sessions --------------------------------------------------------------

export interface SessionRow {
  id: string
  user_id: number
  created_at: number
  expires_at: number
  last_seen_at: number
}

export function insertSession(s: { id: string; userId: number; expiresAt: number; userAgent: string | null }) {
  const now = Date.now()
  appDb()
    .prepare("INSERT INTO sessions (id, user_id, created_at, expires_at, last_seen_at, user_agent) VALUES (?, ?, ?, ?, ?, ?)")
    .run(s.id, s.userId, now, s.expiresAt, now, s.userAgent)
}

export function findSession(id: string): SessionRow | null {
  return (appDb().prepare("SELECT * FROM sessions WHERE id = ?").get(id) as unknown as SessionRow | undefined) ?? null
}

export function touchSession(id: string, expiresAt: number) {
  appDb().prepare("UPDATE sessions SET last_seen_at = ?, expires_at = ? WHERE id = ?").run(Date.now(), expiresAt, id)
}

export function deleteSession(id: string) {
  appDb().prepare("DELETE FROM sessions WHERE id = ?").run(id)
}

export function deleteUserSessions(userId: number) {
  appDb().prepare("DELETE FROM sessions WHERE user_id = ?").run(userId)
}

export function countUserSessions(userId: number): number {
  return (appDb().prepare("SELECT COUNT(*) AS n FROM sessions WHERE user_id = ? AND expires_at > ?").get(userId, Date.now()) as { n: number }).n
}

export function purgeExpiredSessions() {
  appDb().prepare("DELETE FROM sessions WHERE expires_at <= ?").run(Date.now())
}

// --- grants ----------------------------------------------------------------

export function listGrants(userId: number): Grant[] {
  return (
    appDb().prepare("SELECT * FROM grants WHERE user_id = ? ORDER BY chat_jid, from_ts").all(userId) as unknown as {
      id: number
      user_id: number
      chat_jid: string
      from_ts: number | null
      to_ts: number | null
    }[]
  ).map((r) => ({ id: r.id, userId: r.user_id, chatJid: r.chat_jid, fromTs: r.from_ts, toTs: r.to_ts }))
}

/** Replace a user's grants atomically. */
export function replaceGrants(userId: number, grants: { chatJid: string; fromTs: number | null; toTs: number | null }[]) {
  const db = appDb()
  db.exec("BEGIN")
  try {
    db.prepare("DELETE FROM grants WHERE user_id = ?").run(userId)
    const ins = db.prepare("INSERT INTO grants (user_id, chat_jid, from_ts, to_ts) VALUES (?, ?, ?, ?)")
    for (const g of grants) ins.run(userId, g.chatJid, g.fromTs, g.toTs)
    db.exec("COMMIT")
  } catch (e) {
    db.exec("ROLLBACK")
    throw e
  }
}

// --- snapshots -------------------------------------------------------------

export function getSnapshotRecord(userId: number): SnapshotRecord | null {
  const r = appDb().prepare("SELECT * FROM snapshots WHERE user_id = ?").get(userId) as
    | { user_id: number; dir: string; grants_hash: string; source_version: string; built_at: number }
    | undefined
  return r ? { userId: r.user_id, dir: r.dir, grantsHash: r.grants_hash, sourceVersion: r.source_version, builtAt: r.built_at } : null
}

export function putSnapshotRecord(s: SnapshotRecord) {
  appDb()
    .prepare(
      `INSERT INTO snapshots (user_id, dir, grants_hash, source_version, built_at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(user_id) DO UPDATE SET dir = excluded.dir, grants_hash = excluded.grants_hash,
         source_version = excluded.source_version, built_at = excluded.built_at`
    )
    .run(s.userId, s.dir, s.grantsHash, s.sourceVersion, s.builtAt)
}

export function deleteSnapshotRecord(userId: number) {
  appDb().prepare("DELETE FROM snapshots WHERE user_id = ?").run(userId)
}

// --- meta ------------------------------------------------------------------

export function getMeta(key: string): string | null {
  return (appDb().prepare("SELECT value FROM meta WHERE key = ?").get(key) as { value: string } | undefined)?.value ?? null
}

export function setMeta(key: string, value: string) {
  appDb().prepare("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(key, value)
}

export function deleteMeta(key: string) {
  appDb().prepare("DELETE FROM meta WHERE key = ?").run(key)
}
