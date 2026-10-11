"use server"

import { revalidatePath } from "next/cache"
import { redirect } from "next/navigation"

import { requireAdmin } from "../access/dal"
import { ensureSnapshot, removeSnapshots } from "../access/snapshot"
import { hashPassword } from "../auth/password"
import {
  countAdmins,
  createUser,
  deleteUser,
  deleteUserSessions,
  getUser,
  getUserWithHash,
  replaceGrants,
  updateUser,
  type Role,
} from "../auth/store"
import { validatePassword, validateUsername } from "../auth/validate"
import { removeNames } from "../names/store"
import { primarySource, runWithSource } from "../whatsapp/db"
import { getChat } from "../whatsapp/queries"

/**
 * Admin-only mutations. Server Actions are public HTTP endpoints, so every
 * action re-checks the caller with requireAdmin() before doing anything.
 */

export interface AdminFormState {
  error?: string
  ok?: string
}

function targetUser(userId: number) {
  const user = getUser(userId)
  if (!user) throw new Error("No such account")
  return user
}

export async function createUserAction(_prev: AdminFormState, form: FormData): Promise<AdminFormState> {
  await requireAdmin()
  const username = String(form.get("username") ?? "").trim()
  const password = String(form.get("password") ?? "")
  const role: Role = form.get("role") === "admin" ? "admin" : "user"
  const problem = validateUsername(username) ?? validatePassword(password)
  if (problem) return { error: problem }
  if (getUserWithHash(username)) return { error: "That username is taken." }
  const id = createUser({ username, passwordHash: await hashPassword(password), role, mediaAccess: form.get("media") === "on" })
  revalidatePath("/admin")
  redirect(`/admin/users/${id}`)
}

export async function updateUserAction(userId: number, _prev: AdminFormState, form: FormData): Promise<AdminFormState> {
  const { user: me } = await requireAdmin()
  const user = targetUser(userId)
  const role: Role = form.get("role") === "admin" ? "admin" : "user"
  const disabled = form.get("disabled") === "on"
  const mediaAccess = form.get("media") === "on"
  if (user.id === me.id && (role !== "admin" || disabled)) return { error: "You can't demote or disable your own account." }
  if (user.role === "admin" && role !== "admin" && countAdmins() <= 1) return { error: "There must be at least one admin." }

  updateUser(user.id, { role, disabled, mediaAccess })
  if (disabled) deleteUserSessions(user.id)
  revalidatePath("/admin", "layout")
  return { ok: "Saved." }
}

export async function resetPasswordAction(userId: number, _prev: AdminFormState, form: FormData): Promise<AdminFormState> {
  await requireAdmin()
  const user = targetUser(userId)
  const password = String(form.get("password") ?? "")
  const problem = validatePassword(password)
  if (problem) return { error: problem }
  updateUser(user.id, { passwordHash: await hashPassword(password) })
  deleteUserSessions(user.id)
  return { ok: "Password changed. The account was signed out everywhere." }
}

export async function revokeSessionsAction(userId: number): Promise<AdminFormState> {
  await requireAdmin()
  deleteUserSessions(targetUser(userId).id)
  revalidatePath("/admin", "layout")
  return { ok: "Signed out everywhere." }
}

export async function deleteUserAction(userId: number): Promise<AdminFormState> {
  const { user: me } = await requireAdmin()
  const user = targetUser(userId)
  if (user.id === me.id) return { error: "You can't delete your own account." }
  if (user.role === "admin" && countAdmins() <= 1) return { error: "There must be at least one admin." }
  deleteUser(user.id)
  removeSnapshots(user.id)
  removeNames(user.id)
  revalidatePath("/admin")
  redirect("/admin")
}

export interface GrantInput {
  chatJid: string
  /** "YYYY-MM-DD" or empty */
  from: string
  /** "YYYY-MM-DD" or empty (inclusive day) */
  to: string
}

const DAY = 86_400_000

/** Local-midnight timestamp for a YYYY-MM-DD string. */
function dayStart(s: string): number | null {
  if (!s) return null
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s)
  if (!m) throw new Error(`Invalid date: ${s}`)
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])).getTime()
}

export async function saveGrantsAction(userId: number, grants: GrantInput[]): Promise<AdminFormState> {
  await requireAdmin()
  const user = targetUser(userId)
  if (user.role === "admin") return { error: "Admins can already see everything." }

  const clean: { chatJid: string; fromTs: number | null; toTs: number | null }[] = []
  for (const g of grants) {
    const exists = runWithSource(primarySource(), () => getChat(g.chatJid))
    if (!exists) return { error: `Unknown chat: ${g.chatJid}` }
    let fromTs: number | null
    let toTs: number | null
    try {
      fromTs = dayStart(g.from)
      const to = dayStart(g.to)
      toTs = to == null ? null : to + DAY // "to" is inclusive of that whole day
    } catch (e) {
      return { error: (e as Error).message }
    }
    if (fromTs != null && toTs != null && fromTs >= toTs) return { error: `${exists.name}: the start date is after the end date.` }
    clean.push({ chatJid: g.chatJid, fromTs, toTs })
  }

  replaceGrants(user.id, clean)
  // Build now so the account never sees a stale snapshot and the admin sees build errors immediately.
  try {
    await ensureSnapshot(user)
  } catch (e) {
    return { error: `Access saved, but building the filtered copy failed: ${(e as Error).message}` }
  }
  revalidatePath(`/admin/users/${user.id}`)
  return { ok: `Access saved: ${clean.length} chat${clean.length === 1 ? "" : "s"}.` }
}
