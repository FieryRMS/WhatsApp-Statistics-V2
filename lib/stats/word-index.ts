import "server-only"

import { backend } from "../whatsapp/backend"
import { cached } from "../whatsapp/db"
import { statsIndex, type StatsIndex } from "./stats-index"

/**
 * Words used in each message, aligned with the StatsIndex rows, for the "word"
 * dimension (top words, word clouds, words over time). Built on first use per
 * data source and cached until the data changes — a restricted account's word
 * index only ever covers its own filtered data.
 *
 * Words are lowercased runs of letters/marks/digits (so Bangla script works),
 * with links, @mentions and pure numbers left out.
 */

export interface WordIndex {
  /** Words of row i are list[off[i] .. off[i+1]). */
  off: Int32Array
  list: Int32Array
  vocab: string[]
  builtMs: number
}

const URL_RE = /\b(?:https?:\/\/|www\.)\S+/gi
const MENTION_RE = /@\d+/g
const TOKEN_RE = /[\p{L}\p{M}\p{N}']+/gu
const HAS_LETTER = /\p{L}/u

export function tokenize(text: string): string[] {
  const out: string[] = []
  const clean = text.replace(URL_RE, " ").replace(MENTION_RE, " ").toLowerCase()
  for (const m of clean.matchAll(TOKEN_RE)) {
    const w = m[0].replace(/^'+|'+$/g, "")
    if (w.length < 2 || !HAS_LETTER.test(w)) continue
    out.push(w)
  }
  return out
}

function build(ix: StatsIndex): WordIndex {
  const started = Date.now()
  const vocab: string[] = []
  const ids = new Map<string, number>()
  const off = new Int32Array(ix.n + 1)
  let list = new Int32Array(1 << 20)
  let len = 0
  let row = 0
  backend().scanMessages((r) => {
    // Both scans run in id order; skip rows the stats index left out (bad timestamps).
    while (row < ix.n && ix.ids[row] < r.id) off[++row] = len
    if (row >= ix.n || ix.ids[row] !== r.id) return
    if (r.text && !r.revoked) {
      for (const w of tokenize(r.text)) {
        let id = ids.get(w)
        if (id === undefined) {
          id = vocab.length
          vocab.push(w)
          ids.set(w, id)
        }
        if (len === list.length) {
          const next = new Int32Array(list.length * 2)
          next.set(list)
          list = next
        }
        list[len++] = id
      }
    }
    off[++row] = len
  })
  while (row < ix.n) off[++row] = len
  return { off, list: list.slice(0, len), vocab, builtMs: Date.now() - started }
}

export function wordIndex(): { words: WordIndex; fresh: boolean } {
  let fresh = false
  const { index } = statsIndex()
  const words = cached("wordIndex", () => {
    fresh = true
    return build(index)
  })
  return { words, fresh }
}
