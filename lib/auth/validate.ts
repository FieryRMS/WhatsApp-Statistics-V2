import { MIN_PASSWORD_LENGTH } from "./password"

const USERNAME_RE = /^[a-zA-Z0-9._-]{3,32}$/

export function validateUsername(username: string): string | null {
  return USERNAME_RE.test(username) ? null : "Usernames are 3–32 characters: letters, numbers, dots, dashes and underscores."
}

export function validatePassword(password: string, confirm?: string): string | null {
  if (password.length < MIN_PASSWORD_LENGTH) return `Passwords need at least ${MIN_PASSWORD_LENGTH} characters.`
  if (password.length > 256) return "That password is too long."
  if (confirm !== undefined && password !== confirm) return "The passwords don't match."
  return null
}
