"use server"

import { revalidatePath } from "next/cache"
import { redirect } from "next/navigation"

import { requireSession, withData } from "../access/dal"
import { setSessionViewer } from "../auth/store"
import { listPeople } from "../whatsapp/contacts"

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
