import "server-only"

import { createHash, randomBytes, timingSafeEqual } from "node:crypto"

import { countAdmins, deleteMeta, getMeta, setMeta } from "./store"

/**
 * First-boot setup. Until an admin exists, creating one requires a one-time
 * token printed to the server console — otherwise whoever opened the page
 * first would become admin.
 */

const META_KEY = "setup_token_hash"
const hash = (t: string) => createHash("sha256").update(t).digest()

export function needsSetup(): boolean {
  return countAdmins() === 0
}

/** Generate a fresh setup token (if setup is pending) and print it. Called at server start. */
export function announceSetupToken() {
  if (!needsSetup()) {
    deleteMeta(META_KEY)
    return
  }
  const token = randomBytes(18).toString("base64url")
  setMeta(META_KEY, hash(token).toString("hex"))
  const line = "─".repeat(64)
  console.log(
    `\n${line}\n  WhatsApp Stats — first-time setup\n\n  Open /setup and enter this one-time setup token:\n\n      ${token}\n\n  (A new token is printed every time the server starts until setup is done.)\n${line}\n`
  )
}

export function checkSetupToken(token: string): boolean {
  const stored = getMeta(META_KEY)
  if (!stored || !token) return false
  const a = Buffer.from(stored, "hex")
  const b = hash(token.trim())
  return a.length === b.length && timingSafeEqual(a, b)
}

export function finishSetup() {
  deleteMeta(META_KEY)
}
