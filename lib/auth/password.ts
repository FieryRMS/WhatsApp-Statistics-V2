import "server-only"

import { randomBytes, scrypt, timingSafeEqual } from "node:crypto"
import { promisify } from "node:util"

const scryptAsync = promisify(scrypt) as (pw: string, salt: Buffer, len: number, opts: { N: number; r: number; p: number; maxmem: number }) => Promise<Buffer>

// OWASP-recommended scrypt parameters (N=2^17, r=8, p=1) ≈ 128 MiB, ~100–300 ms.
const N = 2 ** 17
const R = 8
const P = 1
const KEYLEN = 32
const MAXMEM = 256 * 1024 * 1024

export const MIN_PASSWORD_LENGTH = 8

/** "scrypt$N$r$p$salt$hash" (base64url). */
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16)
  const hash = await scryptAsync(password, salt, KEYLEN, { N, r: R, p: P, maxmem: MAXMEM })
  return ["scrypt", N, R, P, salt.toString("base64url"), hash.toString("base64url")].join("$")
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [alg, n, r, p, saltB64, hashB64] = stored.split("$")
  if (alg !== "scrypt" || !saltB64 || !hashB64) return false
  const expected = Buffer.from(hashB64, "base64url")
  const actual = await scryptAsync(password, Buffer.from(saltB64, "base64url"), expected.length, {
    N: Number(n),
    r: Number(r),
    p: Number(p),
    maxmem: MAXMEM,
  })
  return actual.length === expected.length && timingSafeEqual(actual, expected)
}

/** A hash to verify against when the username doesn't exist, so timing doesn't reveal valid usernames. */
let dummyHash: Promise<string> | null = null
export function getDummyHash() {
  return (dummyHash ??= hashPassword(randomBytes(16).toString("hex")))
}
