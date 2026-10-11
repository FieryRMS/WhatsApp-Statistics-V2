"use server"

import { revalidatePath } from "next/cache"
import { redirect } from "next/navigation"

import { requireSession, withData } from "../access/dal"
import { setSessionViewer } from "../auth/store"
import { MAX_NAME_LENGTH, applyNameChanges } from "../names/store"
import { OWNER_KEY, listPeople } from "../whatsapp/contacts"

/** Same-site relative paths only. */
function safeNext(next: string | undefined): string {
  return next && next.startsWith("/") && !next.startsWith("//") && !next.startsWith("/\\") ? next : "/"
}

/**
 * Choose who is viewing in this session (null = the phone owner). Only people
 * in the account's own data can be chosen. Stored on the session only.
 */
export async function setViewerAction(jid: string | null, next?: string): Promise<{ error?: string }> {
  const session = await requireSession()
  if (jid !== null) {
    const people = await withData(() => listPeople())
    if (!people.includes(jid)) return { error: "That person isn't in this account's chats." }
  }
  setSessionViewer(session.id, jid)
  revalidatePath("/", "layout")
  if (next !== undefined) redirect(safeNext(next))
  return {}
}

/** Name the phone owner for this account (stored in the account's name mapping). Empty = clear. */
export async function setOwnerNameAction(name: string): Promise<{ error?: string; ok?: string }> {
  const { user } = await requireSession()
  const clean = name.trim()
  if (clean.length > MAX_NAME_LENGTH) return { error: `Names can be at most ${MAX_NAME_LENGTH} characters.` }
  applyNameChanges(user.id, [clean ? { jid: OWNER_KEY, name: clean } : { jid: OWNER_KEY, reset: true }])
  revalidatePath("/", "layout")
  return { ok: clean ? `The phone owner is now shown as “${clean}”.` : "Name cleared." }
}
