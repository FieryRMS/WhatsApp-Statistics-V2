import "server-only"

import { sourceForUser } from "../access/dal"
import type { User } from "../auth/store"
import { automaticName, listPeople, participant } from "../whatsapp/contacts"
import { primarySource, runWithSource } from "../whatsapp/db"
import { phoneFromJid } from "../whatsapp/jid"
import { readNames } from "./store"

export interface NameRow {
  jid: string
  phone: string | null
  /** Whether the account's mapping has an entry for this person. */
  inMapping: boolean
  /** The entry: a name, or null (hidden / no entry). */
  mapped: string | null
  /** What the account currently sees for this person. */
  shown: string
  /** Automatic name, offered to admins as a suggestion. Never sent to restricted viewers. */
  suggestion: string | null
  /** Other identities with exactly the same mapped name — treated as the same person. */
  mergedWith: string[]
}

/** People `target` can see, as a set — used to validate edits. */
export async function peopleOf(target: User): Promise<Set<string>> {
  return new Set(runWithSource(await sourceForUser(target), () => listPeople()))
}

/**
 * Rows for the name editor of `target`. `withSuggestions` (admin viewers only)
 * adds the automatic name from the full data for each person.
 */
export async function nameRows(target: User, withSuggestions: boolean): Promise<NameRow[]> {
  const source = await sourceForUser(target)
  const mapping = readNames(target.id) ?? {}
  const rows = runWithSource(source, () =>
    listPeople().map((jid) => ({
      jid,
      phone: phoneFromJid(jid),
      inMapping: jid in mapping,
      mapped: mapping[jid] ?? null,
      shown: participant(jid).name,
      suggestion: null as string | null,
      mergedWith: [] as string[],
    }))
  )
  // Same mapped name = same person.
  const byName = new Map<string, NameRow[]>()
  for (const r of rows) if (r.inMapping && r.mapped) byName.set(r.mapped, [...(byName.get(r.mapped) ?? []), r])
  for (const group of byName.values()) {
    if (group.length < 2) continue
    for (const r of group) r.mergedWith = group.filter((o) => o !== r).map((o) => o.phone ?? "a privacy ID")
  }
  if (withSuggestions) {
    runWithSource(primarySource(), () => {
      for (const r of rows) {
        const auto = automaticName(r.jid)
        r.suggestion = auto ? (auto.hint ? `~${auto.name}` : auto.name) : null
      }
    })
  }
  return rows
}
