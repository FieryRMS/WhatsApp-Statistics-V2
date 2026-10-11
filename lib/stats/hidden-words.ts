import "server-only"

import fs from "node:fs"
import path from "node:path"

import { appDataDir } from "../auth/store"
import { DEFAULT_HIDDEN_WORDS, normalizeWord } from "./stopwords"

/**
 * Per-account list of words left out of word statistics and clouds:
 * app-data/hidden-words/u<id>.json. Starts as DEFAULT_HIDDEN_WORDS; the account
 * edits it from the statistics builder.
 */

const dir = path.join(appDataDir, "hidden-words")
const file = (userId: number) => path.join(dir, `u${userId}.json`)

export const MAX_HIDDEN_WORDS = 2000

export function readHiddenWords(userId: number): string[] {
  try {
    const raw = JSON.parse(fs.readFileSync(file(userId), "utf8")) as unknown
    if (Array.isArray(raw)) return raw.filter((w): w is string => typeof w === "string")
  } catch {
    // not created yet
  }
  return DEFAULT_HIDDEN_WORDS
}

export function writeHiddenWords(userId: number, words: string[]) {
  fs.mkdirSync(dir, { recursive: true })
  const clean = [...new Set(words.map(normalizeWord).filter((w) => w && w.length <= 60))]
    .sort((a, b) => a.localeCompare(b))
    .slice(0, MAX_HIDDEN_WORDS)
  const tmp = `${file(userId)}.${process.pid}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(clean, null, 2))
  fs.renameSync(tmp, file(userId))
  return clean
}
