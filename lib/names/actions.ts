"use server"

import { revalidatePath } from "next/cache"

import { requireSession } from "../access/dal"
import { getUser } from "../auth/store"
import { automaticName } from "../whatsapp/contacts"
import { primarySource, runWithSource } from "../whatsapp/db"
import { peopleOf } from "./rows"
import { MAX_NAME_LENGTH, applyNameChanges, type NameChange } from "./store"

export interface NamesFormState {
  error?: string
  ok?: string
}

export interface NameEdit {
  jid: string
  /** New name; "" or null hides the name (shows the number). */
  name?: string | null
  /** Admins only: drop the custom entry and go back to the automatic name. */
  reset?: boolean
}

/**
 * Edit an account's name mapping. Allowed for the account itself and for admins.
 * Only people the account can actually see may be named.
 */
export async function saveNamesAction(targetUserId: number, edits: NameEdit[]): Promise<NamesFormState> {
  const { user: actor } = await requireSession()
  const target = getUser(targetUserId)
  if (!target) return { error: "No such account." }
  const isAdmin = actor.role === "admin"
  if (!isAdmin && actor.id !== target.id) return { error: "You can only edit your own names." }
  if (!Array.isArray(edits) || edits.length > 5000) return { error: "Invalid request." }

  const allowed = await peopleOf(target)
  const changes: NameChange[] = []
  for (const e of edits) {
    if (typeof e?.jid !== "string" || !allowed.has(e.jid)) return { error: "That person isn't in this account's chats." }
    if (e.reset) {
      if (!isAdmin) return { error: "Only admins can reset names." }
      if (target.role === "admin") {
        changes.push({ jid: e.jid, reset: true })
      } else {
        // Restricted accounts only see their mapping, so "reset" re-seeds the automatic name into it.
        const auto = runWithSource(primarySource(), () => automaticName(e.jid))
        changes.push({ jid: e.jid, name: auto ? (auto.hint ? `~${auto.name}` : auto.name) : null })
      }
      continue
    }
    const name = typeof e.name === "string" ? e.name.trim() : null
    if (name && name.length > MAX_NAME_LENGTH) return { error: `Names can be at most ${MAX_NAME_LENGTH} characters.` }
    changes.push({ jid: e.jid, name: name || null })
  }

  applyNameChanges(target.id, changes)
  revalidatePath("/", "layout")
  return { ok: `Saved ${changes.length} change${changes.length === 1 ? "" : "s"}.` }
}
