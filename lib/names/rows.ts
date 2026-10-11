import "server-only"

import { sourceForUser } from "../access/dal"
import type { User } from "../auth/store"
import { OWNER_KEY, automaticName, listPeople, ownerName, participant } from "../whatsapp/contacts"
import { DEFAULT_OWNER_LABEL } from "../whatsapp/viewer"
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
  /** The phone owner's row (key "me"): "you" in the raw data. */
  owner?: boolean
}

/** People `target` can see (plus the phone owner), as a set — used to validate edits. */
export async function peopleOf(target: User): Promise<Set<string>> {
  return new Set([OWNER_KEY, ...runWithSource(await sourceForUser(target), () => listPeople())])
}

/**
 * Rows for the name editor of `target`. `withSuggestions` (admin viewers only)
 * adds the automatic name from the full data for each person.
 */
export async function nameRows(target: User, withSuggestions: boolean): Promise<NameRow[]> {
  const source = await sourceForUser(target)
  const mapping = readNames(target.id) ?? {}
  const rows: NameRow[] = runWithSource(source, (): NameRow[] => [
    // The phone owner comes first: naming them here is what graphs and other viewers see.
    {
      jid: OWNER_KEY,
      phone: null,
      inMapping: OWNER_KEY in mapping,
      mapped: mapping[OWNER_KEY] ?? null,
      shown: ownerName() ?? DEFAULT_OWNER_LABEL,
      suggestion: null as string | null,
      mergedWith: [] as string[],
      owner: true,
    },
    ...listPeople().map((jid) => ({
      jid,
      phone: phoneFromJid(jid),
      inMapping: jid in mapping,
      mapped: mapping[jid] ?? null,
      shown: participant(jid).name,
      suggestion: null as string | null,
      mergedWith: [] as string[],
    })),
  ])
  // Same mapped name = same person.
  const byName = new Map<string, NameRow[]>()
  for (const r of rows) if (r.inMapping && r.mapped) byName.set(r.mapped, [...(byName.get(r.mapped) ?? []), r])
  for (const group of byName.values()) {
    if (group.length < 2) continue
    for (const r of group) r.mergedWith = group.filter((o) => o !== r).map((o) => (o.owner ? "the phone owner" : (o.phone ?? "a privacy ID")))
  }
  if (withSuggestions) {
    runWithSource(primarySource(), () => {
      for (const r of rows) {
        if (r.owner) continue
        const auto = automaticName(r.jid)
        r.suggestion = auto ? (auto.hint ? `~${auto.name}` : auto.name) : null
      }
    })
  }
  return rows
}
