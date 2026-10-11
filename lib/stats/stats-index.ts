import "server-only"

import { backend } from "../whatsapp/backend"
import { ModernType } from "../whatsapp/backend/modern"
import { WaType } from "../whatsapp/constants"
import { canonicalJid } from "../whatsapp/contacts"
import { cached } from "../whatsapp/db"
import { extractEmojis } from "./emoji"
import { KINDS, type Kind } from "./types"

/**
 * A compact, columnar index of every real message in the current data source,
 * built once per source (and rebuilt when its files change). Queries then run
 * in milliseconds over typed arrays instead of re-reading the database.
 *
 * Because it is built inside the request's data source, a restricted account's
 * index only ever contains its own filtered data.
 */

export const KIND_LIST = Object.keys(KINDS) as Kind[]
const KIND_CODE = new Map(KIND_LIST.map((k, i) => [k, i]))

export interface StatsIndex {
  n: number
  ids: Int32Array
  ts: Float64Array
  /** Local calendar day number (days since 1970-01-01, server timezone). */
  day: Int32Array
  hour: Uint8Array
  /** 0 = Monday … 6 = Sunday */
  weekday: Uint8Array
  chat: Int32Array
  /** Index into `people`; 0 is always "me". */
  person: Int32Array
  kind: Uint8Array
  words: Int32Array
  chars: Int32Array
  /** Emojis of row i are emojiList[emojiOff[i] .. emojiOff[i+1]). */
  emojiOff: Int32Array
  emojiList: Int32Array
  chats: string[]
  people: string[]
  emojis: string[]
  reactions: { n: number; row: Int32Array; person: Int32Array; ts: Float64Array; day: Int32Array; emoji: Int32Array }
  mentions: { n: number; row: Int32Array; person: Int32Array }
  /** Messages left out because their timestamp is impossible. */
  skipped: number
  builtMs: number
}

export const ME = "me"

function kindOf(type: number, voice: boolean, revoked: boolean): Kind {
  if (revoked) return "deleted"
  switch (type) {
    case WaType.TEXT:
      return "text"
    case WaType.IMAGE:
      return "image"
    case WaType.VIDEO:
      return "video"
    case WaType.GIF:
      return "gif"
    case WaType.AUDIO:
      return voice ? "voice" : "audio"
    case WaType.DOCUMENT:
      return "document"
    case WaType.STICKER:
      return "sticker"
    case WaType.CONTACT:
    case WaType.CONTACT_LIST:
      return "contact"
    case WaType.LOCATION:
    case ModernType.LIVE_LOCATION:
      return "location"
    case ModernType.POLL:
      return "poll"
    case ModernType.CALL:
      return "call"
    case WaType.MISSED_CALL:
      return "missed_call"
    case WaType.VIEW_ONCE_IMAGE:
    case WaType.VIEW_ONCE_VIDEO:
      return "view_once"
    default:
      return "other"
  }
}

const DAY_MS = 86_400_000

/** Local day number for a timestamp (server timezone). */
export function localDay(ts: number): number {
  const d = new Date(ts)
  return Math.floor((Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())) / DAY_MS)
}

/** Start of a local day number, in ms. */
export function dayStart(day: number): number {
  const d = new Date(day * DAY_MS)
  return new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()).getTime()
}

class Interner {
  list: string[] = []
  map = new Map<string, number>()
  constructor(initial: string[] = []) {
    for (const s of initial) this.id(s)
  }
  id(s: string): number {
    let i = this.map.get(s)
    if (i === undefined) {
      i = this.list.length
      this.list.push(s)
      this.map.set(s, i)
    }
    return i
  }
}

class Grow {
  arr: Int32Array
  n = 0
  constructor(size = 1024) {
    this.arr = new Int32Array(size)
  }
  push(v: number) {
    if (this.n === this.arr.length) {
      const next = new Int32Array(this.arr.length * 2)
      next.set(this.arr)
      this.arr = next
    }
    this.arr[this.n++] = v
  }
  done() {
    return this.arr.slice(0, this.n)
  }
}

function build(): StatsIndex {
  const started = Date.now()
  const b = backend()
  const chats = new Interner()
  const people = new Interner([ME])
  const emojis = new Interner()

  const ids = new Grow(1 << 16)
  const ts: number[] = []
  const chat = new Grow(1 << 16)
  const person = new Grow(1 << 16)
  const kind: number[] = []
  const words = new Grow(1 << 16)
  const chars = new Grow(1 << 16)
  const emojiOff = new Grow(1 << 16)
  const emojiList = new Grow(1 << 12)
  const canon = new Map<string, number>()
  const personId = (jid: string | null) => {
    if (!jid) return 0
    let p = canon.get(jid)
    if (p === undefined) {
      p = people.id(canonicalJid(jid))
      canon.set(jid, p)
    }
    return p
  }

  // Corrupt rows occasionally carry absurd timestamps (e.g. the year 2116); they'd
  // stretch every timeline across a century, so they're left out of statistics.
  const minTs = Date.UTC(2009, 0, 1)
  const maxTs = Date.now() + DAY_MS
  let skipped = 0

  b.scanMessages((r) => {
    if (!(r.timestamp >= minTs && r.timestamp <= maxTs)) {
      skipped++
      return
    }
    ids.push(r.id)
    ts.push(r.timestamp)
    chat.push(chats.id(r.chatJid))
    person.push(r.fromMe ? 0 : r.senderJid ? personId(r.senderJid) : -1)
    kind.push(KIND_CODE.get(kindOf(r.type, r.voiceNote, r.revoked))!)
    const text = r.revoked ? null : r.text
    if (text) {
      const t = text.trim()
      words.push(t ? t.split(/\s+/).length : 0)
      chars.push(t.length)
      emojiOff.push(emojiList.n)
      for (const e of extractEmojis(t)) emojiList.push(emojis.id(e))
    } else {
      words.push(0)
      chars.push(0)
      emojiOff.push(emojiList.n)
    }
  })
  emojiOff.push(emojiList.n)

  const n = ids.n
  const idArr = ids.done()
  const tsArr = Float64Array.from(ts)
  const day = new Int32Array(n)
  const hour = new Uint8Array(n)
  const weekday = new Uint8Array(n)
  for (let i = 0; i < n; i++) {
    const d = new Date(tsArr[i])
    day[i] = Math.floor(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()) / DAY_MS)
    hour[i] = d.getHours()
    weekday[i] = (d.getDay() + 6) % 7
  }

  const rowOf = (id: number) => {
    // ids are ascending — binary search
    let lo = 0
    let hi = n - 1
    while (lo <= hi) {
      const mid = (lo + hi) >> 1
      const v = idArr[mid]
      if (v === id) return mid
      if (v < id) lo = mid + 1
      else hi = mid - 1
    }
    return -1
  }

  const rRow = new Grow()
  const rPerson = new Grow()
  const rTs: number[] = []
  const rEmoji = new Grow()
  b.scanReactions((r) => {
    const row = rowOf(r.messageId)
    if (row < 0) return
    rRow.push(row)
    rPerson.push(personId(r.senderJid))
    rTs.push(r.timestamp || tsArr[row])
    rEmoji.push(emojis.id(r.emoji.replace(/️/g, "")))
  })
  const rTsArr = Float64Array.from(rTs)
  const rDay = new Int32Array(rTsArr.length)
  for (let i = 0; i < rTsArr.length; i++) rDay[i] = localDay(rTsArr[i])

  const mRow = new Grow()
  const mPerson = new Grow()
  b.scanMentions((m) => {
    const row = rowOf(m.messageId)
    if (row < 0) return
    mRow.push(row)
    mPerson.push(personId(m.jid))
  })

  return {
    n,
    ids: idArr,
    ts: tsArr,
    day,
    hour,
    weekday,
    chat: chat.done(),
    person: person.done(),
    kind: Uint8Array.from(kind),
    words: words.done(),
    chars: chars.done(),
    emojiOff: emojiOff.done(),
    emojiList: emojiList.done(),
    chats: chats.list,
    people: people.list,
    emojis: emojis.list,
    reactions: { n: rRow.n, row: rRow.done(), person: rPerson.done(), ts: rTsArr, day: rDay, emoji: rEmoji.done() },
    mentions: { n: mRow.n, row: mRow.done(), person: mPerson.done() },
    skipped,
    builtMs: Date.now() - started,
  }
}

/** The index for the current data source (built on first use, cached until the data changes). */
export function statsIndex(): { index: StatsIndex; fresh: boolean } {
  let fresh = false
  const index = cached("statsIndex", () => {
    fresh = true
    return build()
  })
  return { index, fresh }
}

/**
 * Rows containing `term` and how many times, for the "word or phrase" metric/filter.
 * Cached per source and term.
 */
export function termMatches(index: StatsIndex, term: string, wholeWord: boolean): Map<number, number> {
  const key = `term:${wholeWord ? "w" : "s"}:${term.toLowerCase()}`
  return cached(key, () => {
    const escaped = term.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    const re = new RegExp(wholeWord ? `(?<![\\p{L}\\p{N}_])${escaped}(?![\\p{L}\\p{N}_])` : escaped, "gu")
    const rowById = new Map<number, number>()
    const out = new Map<number, number>()
    backend().searchText(term, (r) => {
      const hits = r.text.toLowerCase().match(re)?.length ?? 0
      if (!hits) return
      // binary search the row
      let lo = 0
      let hi = index.n - 1
      let row = rowById.get(r.id) ?? -1
      while (row < 0 && lo <= hi) {
        const mid = (lo + hi) >> 1
        const v = index.ids[mid]
        if (v === r.id) row = mid
        else if (v < r.id) lo = mid + 1
        else hi = mid - 1
      }
      if (row >= 0) out.set(row, hits)
    })
    return out
  })
}
