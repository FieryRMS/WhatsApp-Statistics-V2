"use server"

import { revalidatePath } from "next/cache"

import { requireSession, withData } from "../access/dal"
import { sanitizeForViewer } from "./engine"
import { deleteSaved, saveQuery } from "./saved"
import { parseQuery } from "./schema"

export interface SaveResult {
  error?: string
  id?: number
}

export async function saveStatAction(input: {
  id?: number
  name: string
  description?: string
  shared: boolean
  query: unknown
}): Promise<SaveResult> {
  const { user } = await requireSession()
  const name = String(input.name ?? "").trim().slice(0, 120)
  if (!name) return { error: "Give it a name." }
  const parsed = parseQuery(input.query)
  if (!parsed.ok) return { error: `That query isn't valid: ${parsed.error}` }
  // Never store ids the author can't see themselves (e.g. pasted from someone else's link).
  const { query } = await withData(() => sanitizeForViewer(parsed.query))
  const id = saveQuery(user.id, {
    id: typeof input.id === "number" ? input.id : undefined,
    name,
    description: String(input.description ?? "").trim().slice(0, 500),
    query: { ...query, title: name },
    shared: !!input.shared,
  })
  revalidatePath("/stats")
  return { id }
}

export async function deleteStatAction(id: number): Promise<SaveResult> {
  const { user } = await requireSession()
  if (!deleteSaved(id, user)) return { error: "You can only delete your own statistics." }
  revalidatePath("/stats")
  return {}
}
