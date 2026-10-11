import "server-only"

import { cache } from "react"
import { redirect } from "next/navigation"

import { readSession, type Session } from "../auth/session"
import { needsSetup } from "../auth/setup"
import { primarySource, runWithSource, type DataSource } from "../whatsapp/db"
import { ensureSnapshot } from "./snapshot"

/**
 * Data Access Layer — the only way pages and API routes reach WhatsApp data.
 *
 *   const chats = await withData(() => listChats())
 *
 * resolves the signed-in account, picks its data source (the primary database
 * for admins, the account's filtered snapshot otherwise) and runs the query
 * inside it. Query helpers throw when called outside withData(), so forgetting
 * this step fails loudly instead of leaking.
 */

/** The signed-in session for this request (checked against the database once per request). */
export const getSession = cache(async (): Promise<Session | null> => readSession())

/** Session or redirect to login/setup. For pages and server actions. */
export async function requireSession(): Promise<Session> {
  const session = await getSession()
  if (!session) redirect(needsSetup() ? "/setup" : "/login")
  return session
}

export async function requireAdmin(): Promise<Session> {
  const session = await requireSession()
  if (session.user.role !== "admin") redirect("/")
  return session
}

export async function sourceFor(session: Session): Promise<DataSource> {
  return session.user.role === "admin" ? primarySource() : ensureSnapshot(session.user)
}

const getRequestSource = cache(async (): Promise<DataSource> => sourceFor(await requireSession()))

/** Run a (synchronous) data query against the signed-in account's data. */
export async function withData<T>(fn: () => T): Promise<T> {
  const source = await getRequestSource()
  return runWithSource(source, fn)
}

/**
 * For Route Handlers: run `fn` against the caller's data, or answer 401.
 * `fn` returns a Response (or data to serialise as JSON).
 */
export async function apiWithData(fn: () => Response | object | null): Promise<Response> {
  const session = await getSession()
  if (!session) return Response.json({ error: "Not signed in" }, { status: 401 })
  const source = await sourceFor(session)
  const result = runWithSource(source, fn)
  if (result instanceof Response) return result
  if (result == null) return Response.json({ error: "Not found" }, { status: 404 })
  return Response.json(result)
}
