import "server-only"

import { createHash, randomBytes } from "node:crypto"
import { cookies, headers } from "next/headers"

import { deleteSession, findSession, getUser, insertSession, purgeExpiredSessions, type User } from "./store"

/**
 * Database-backed sessions. The cookie holds a random 256-bit token; the
 * database stores only its SHA-256, so a leaked app.db can't be replayed.
 * Sessions are revocable (logout, disable, delete, "sign out everywhere").
 */

export const SESSION_COOKIE = "wa_session"
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000

export interface Session {
  id: string
  user: User
  expiresAt: number
  /** Who is viewing in this session (chosen after each login); null = the phone owner. */
  viewerJid: string | null
}

const hashToken = (token: string) => createHash("sha256").update(token).digest("hex")

/**
 * Whether the browser reached us over HTTPS (directly or via a reverse proxy).
 * Decides the cookie's Secure flag: required on HTTPS, impossible on plain-HTTP LAN.
 */
async function isHttps(): Promise<boolean> {
  const h = await headers()
  const proto = h.get("x-forwarded-proto")?.split(",")[0]?.trim()
  return proto === "https"
}

/** Create a session for `userId` and set the cookie. Call only from a Server Action / Route Handler. */
export async function startSession(userId: number) {
  purgeExpiredSessions()
  const token = randomBytes(32).toString("base64url")
  const expiresAt = Date.now() + SESSION_TTL_MS
  const h = await headers()
  insertSession({ id: hashToken(token), userId, expiresAt, userAgent: h.get("user-agent")?.slice(0, 300) ?? null })
  ;(await cookies()).set(SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: await isHttps(),
    path: "/",
    expires: new Date(expiresAt),
  })
}

/** The current valid session, or null. Re-checks the database on every call (revocation is immediate). */
export async function readSession(): Promise<Session | null> {
  const token = (await cookies()).get(SESSION_COOKIE)?.value
  if (!token || token.length > 100) return null
  const row = findSession(hashToken(token))
  if (!row) return null
  if (row.expires_at <= Date.now()) {
    deleteSession(row.id)
    return null
  }
  const user = getUser(row.user_id)
  if (!user || user.disabled) return null
  return { id: row.id, user, expiresAt: row.expires_at, viewerJid: row.viewer_jid ?? null }
}

/** Delete the current session and its cookie. Call only from a Server Action / Route Handler. */
export async function endSession() {
  const jar = await cookies()
  const token = jar.get(SESSION_COOKIE)?.value
  if (token) deleteSession(hashToken(token))
  jar.delete(SESSION_COOKIE)
}
