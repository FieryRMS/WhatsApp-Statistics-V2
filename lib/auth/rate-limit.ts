import "server-only"

/**
 * Login throttling, keyed by username (client IPs can be forged behind proxies).
 * After MAX_FAILURES failed attempts within WINDOW_MS the username is locked
 * until the window passes. A global cap slows down spraying many usernames.
 */

const WINDOW_MS = 15 * 60 * 1000
const MAX_FAILURES = 5
const GLOBAL_MAX_FAILURES = 50

const globalForLimits = globalThis as unknown as { __waLoginFailures?: Map<string, number[]> }
const failures = (globalForLimits.__waLoginFailures ??= new Map())

function recent(key: string) {
  const cutoff = Date.now() - WINDOW_MS
  const list = (failures.get(key) ?? []).filter((t: number) => t > cutoff)
  failures.set(key, list)
  return list
}

/** Milliseconds until `username` may try again, or 0 if allowed. */
export function loginRetryAfter(username: string): number {
  const own = recent(`u:${username.toLowerCase()}`)
  const all = recent("*")
  const blocked = own.length >= MAX_FAILURES ? own : all.length >= GLOBAL_MAX_FAILURES ? all : null
  return blocked ? Math.max(0, blocked[0] + WINDOW_MS - Date.now()) : 0
}

export function recordLoginFailure(username: string) {
  recent(`u:${username.toLowerCase()}`).push(Date.now())
  recent("*").push(Date.now())
}

export function clearLoginFailures(username: string) {
  failures.delete(`u:${username.toLowerCase()}`)
}
