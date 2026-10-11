import "server-only"

import fs from "node:fs"
import path from "node:path"

import { appDataDir } from "../auth/store"
import { invalidateNames } from "../whatsapp/db"

/**
 * Per-account contact-name mappings: app-data/names/u<id>.json
 *
 *   { "8801712345678@s.whatsapp.net": "Alice", "8801…@s.whatsapp.net": null }
 *
 * A string renames the person; null hides their name (the phone number shows).
 * Keys are phone JIDs (LIDs are mapped to phone JIDs when known).
 *
 * - Restricted accounts see names *only* from their file. It is seeded with the
 *   names they would otherwise have seen (lib/access/snapshot.ts) and new people
 *   are added when their access grows; existing entries are never overwritten.
 * - Admins see automatic names, with their file applied on top as overrides.
 *
 * The account and any admin edit the same file.
 */

export type NameMap = Record<string, string | null>

const namesDir = path.join(appDataDir, "names")

export const MAX_NAME_LENGTH = 100

export function namesFile(userId: number): string {
  return path.join(namesDir, `u${userId}.json`)
}

export function readNames(userId: number): NameMap | null {
  try {
    const raw = JSON.parse(fs.readFileSync(namesFile(userId), "utf8")) as unknown
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {}
    const out: NameMap = {}
    for (const [jid, v] of Object.entries(raw)) if (typeof v === "string" || v === null) out[jid] = v
    return out
  } catch {
    return null
  }
}

/** Write atomically (temp file + rename) so a crash never leaves half a file. */
export function writeNames(userId: number, map: NameMap) {
  fs.mkdirSync(namesDir, { recursive: true })
  const file = namesFile(userId)
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`
  const sorted = Object.fromEntries(Object.entries(map).sort(([a], [b]) => a.localeCompare(b)))
  fs.writeFileSync(tmp, JSON.stringify(sorted, null, 2))
  fs.renameSync(tmp, file)
  invalidateNames(file)
}

/**
 * Add entries for keys not yet in the file; never touches existing entries.
 * Identical names mean "same person" (they're merged in statistics), so a
 * seeded name that's already taken gets the number's last digits appended —
 * seeding must never merge people on its own. Returns how many were added.
 */
export function seedNames(userId: number, seed: NameMap): number {
  const current = readNames(userId) ?? {}
  const usedBy = new Map<string, string>()
  for (const [jid, name] of Object.entries(current)) if (typeof name === "string") usedBy.set(name.trim(), jid)
  let added = 0
  for (const [jid, name] of Object.entries(seed)) {
    if (jid in current) continue
    let value = name
    if (typeof value === "string") {
      const taken = usedBy.get(value.trim())
      if (taken && taken !== jid) value = `${value} (…${jid.split("@")[0].slice(-4)})`
      usedBy.set(value.trim(), jid)
    }
    current[jid] = value
    added++
  }
  if (added || !fs.existsSync(namesFile(userId))) writeNames(userId, current)
  return added
}

export type NameChange = { jid: string; name: string | null } | { jid: string; reset: true }

/** Apply edits: set a name, hide it (null) or reset (remove the entry). */
export function applyNameChanges(userId: number, changes: NameChange[]) {
  const map = readNames(userId) ?? {}
  for (const c of changes) {
    if ("reset" in c) delete map[c.jid]
    else map[c.jid] = c.name
  }
  writeNames(userId, map)
}

export function removeNames(userId: number) {
  fs.rmSync(namesFile(userId), { force: true })
}
