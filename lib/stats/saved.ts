import "server-only"

import { appDb, getUser } from "../auth/store"
import { parseQuery } from "./schema"
import type { StatQuery } from "./types"

/**
 * Saved statistics. A saved query is only the definition — never results —
 * so sharing it is safe: every account runs it against its own data.
 */

export interface SavedQuery {
  id: number
  ownerId: number
  ownerName: string
  name: string
  description: string
  query: StatQuery
  shared: boolean
  updatedAt: number
}

let ready = false
function db() {
  const d = appDb()
  if (!ready) {
    d.exec(`
      CREATE TABLE IF NOT EXISTS stat_queries (
        id          INTEGER PRIMARY KEY,
        owner_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name        TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        definition  TEXT NOT NULL,
        shared      INTEGER NOT NULL DEFAULT 0,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS stat_queries_owner ON stat_queries(owner_id);
    `)
    ready = true
  }
  return d
}

interface Row {
  id: number
  owner_id: number
  name: string
  description: string
  definition: string
  shared: number
  updated_at: number
}

function toSaved(r: Row): SavedQuery | null {
  const parsed = parseQuery(JSON.parse(r.definition))
  if (!parsed.ok) return null
  return {
    id: r.id,
    ownerId: r.owner_id,
    ownerName: getUser(r.owner_id)?.username ?? "deleted account",
    name: r.name,
    description: r.description,
    query: parsed.query,
    shared: !!r.shared,
    updatedAt: r.updated_at,
  }
}

/** The viewer's own queries plus everything shared by others. */
export function listSaved(viewerId: number): SavedQuery[] {
  const rows = db()
    .prepare("SELECT * FROM stat_queries WHERE owner_id = ? OR shared = 1 ORDER BY updated_at DESC")
    .all(viewerId) as unknown as Row[]
  return rows.map(toSaved).filter((q): q is SavedQuery => !!q)
}

export function getSaved(id: number, viewerId: number): SavedQuery | null {
  const r = db().prepare("SELECT * FROM stat_queries WHERE id = ? AND (owner_id = ? OR shared = 1)").get(id, viewerId) as unknown as Row | undefined
  return r ? toSaved(r) : null
}

export function saveQuery(ownerId: number, q: { id?: number; name: string; description: string; query: StatQuery; shared: boolean }): number {
  const now = Date.now()
  const def = JSON.stringify(q.query)
  if (q.id) {
    const res = db()
      .prepare("UPDATE stat_queries SET name = ?, description = ?, definition = ?, shared = ?, updated_at = ? WHERE id = ? AND owner_id = ?")
      .run(q.name, q.description, def, q.shared ? 1 : 0, now, q.id, ownerId)
    if (res.changes) return q.id
  }
  const res = db()
    .prepare("INSERT INTO stat_queries (owner_id, name, description, definition, shared, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
    .run(ownerId, q.name, q.description, def, q.shared ? 1 : 0, now, now)
  return Number(res.lastInsertRowid)
}

/** Owners delete their own queries; admins may delete any shared one. */
export function deleteSaved(id: number, actor: { id: number; role: string }): boolean {
  const res =
    actor.role === "admin"
      ? db().prepare("DELETE FROM stat_queries WHERE id = ? AND (owner_id = ? OR shared = 1)").run(id, actor.id)
      : db().prepare("DELETE FROM stat_queries WHERE id = ? AND owner_id = ?").run(id, actor.id)
  return res.changes > 0
}
