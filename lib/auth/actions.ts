"use server"

import { redirect } from "next/navigation"

import { getDummyHash, hashPassword, verifyPassword } from "./password"
import { clearLoginFailures, loginRetryAfter, recordLoginFailure } from "./rate-limit"
import { endSession, startSession } from "./session"
import { checkSetupToken, finishSetup, needsSetup } from "./setup"
import { createUser, getUserWithHash } from "./store"
import { validatePassword, validateUsername } from "./validate"

export interface FormState {
  error?: string
  /** Echo back non-secret fields so the form keeps them after an error. */
  username?: string
}

/** Only same-site relative paths, so `?next=` can't be used to redirect elsewhere. */
function safeNext(next: FormDataEntryValue | null): string {
  const s = typeof next === "string" ? next : ""
  return s.startsWith("/") && !s.startsWith("//") && !s.startsWith("/\\") ? s : "/"
}

export async function setupAction(_prev: FormState, form: FormData): Promise<FormState> {
  const username = String(form.get("username") ?? "").trim()
  const password = String(form.get("password") ?? "")
  if (!needsSetup()) return { error: "Setup has already been completed. Sign in instead." }
  if (!checkSetupToken(String(form.get("token") ?? ""))) {
    return { error: "That setup token isn't valid. Use the one printed in the server console.", username }
  }
  const problem = validateUsername(username) ?? validatePassword(password, String(form.get("confirm") ?? ""))
  if (problem) return { error: problem, username }

  const id = createUser({ username, passwordHash: await hashPassword(password), role: "admin", mediaAccess: true })
  finishSetup()
  await startSession(id)
  redirect("/admin")
}

export async function loginAction(_prev: FormState, form: FormData): Promise<FormState> {
  const username = String(form.get("username") ?? "").trim()
  const password = String(form.get("password") ?? "")
  const wait = loginRetryAfter(username)
  if (wait > 0) return { error: `Too many attempts. Try again in ${Math.ceil(wait / 60000)} minutes.`, username }

  const user = username ? getUserWithHash(username) : null
  // Always run one hash so response time doesn't reveal whether the username exists.
  const ok = await verifyPassword(password, user?.passwordHash ?? (await getDummyHash()))
  if (!user || !ok || user.disabled) {
    recordLoginFailure(username)
    return { error: "Incorrect username or password.", username }
  }

  clearLoginFailures(username)
  await startSession(user.id)
  redirect(safeNext(form.get("next")))
}

export async function logoutAction() {
  await endSession()
  redirect("/login")
}
