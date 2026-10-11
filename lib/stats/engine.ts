import "server-only"

import { OWNER_KEY, canonicalJid, chatName, participant, samePerson } from "../whatsapp/contacts"
import { listChats } from "../whatsapp/queries"
import { viewer, type ViewerInfo } from "../whatsapp/viewer"
import { KIND_LIST, ME, dayStart, statsIndex, termMatches, type StatsIndex } from "./stats-index"
import { normalizeEmoji } from "./emoji"
import {
  KINDS,
  MEDIA_KINDS,
  METRICS,
  isTimeDimension,
  type Dimension,
  type Filters,
  type PointChat,
  type ResultCategory,
  type ResultSeries,
  type SeriesDef,
  type Split,
  type StatQuery,
  type StatResult,
  type StatsCatalog,
} from "./types"

/**
 * Runs a StatQuery against the current data source's StatsIndex.
 *
 * Every metric is reduced to "units" (a message, an emoji, a reaction, a mention),
 * each with a row (the message, for chat/time/link), a person, a time and an
 * optional emoji. Units are bucketed by the x-axis dimension and the series
 * split, and each bucket remembers which chats contributed (for click-through).
 */

const WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
const DAY_MS = 86_400_000
const MAX_SERIES = 8
const POINT_CHATS = 3
/** Averages over fewer messages than this are hidden (one long message isn't a habit). */
const MIN_AVG_SAMPLE = 10

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

interface CompiledFilter {
  chatOk: Uint8Array | null
  personOk: Uint8Array | null
  kindOk: Uint8Array | null
  from: number | null
  to: number | null
  termRows: Map<number, number> | null
  emojiSet: Set<number> | null
}

function compile(ix: StatsIndex, f: Filters | undefined, warnings: string[], grp: Int32Array): CompiledFilter {
  const flags = (len: number, idxs: number[]) => {
    const a = new Uint8Array(len)
    for (const i of idxs) if (i >= 0) a[i] = 1
    return a
  }
  const chatIdx = new Map(ix.chats.map((c, i) => [c, i]))
  const personIdx = new Map(ix.people.map((p, i) => [p, i]))
  const emojiIdx = new Map(ix.emojis.map((e, i) => [e, i]))
  return {
    chatOk: f?.chats?.length ? flags(ix.chats.length, f.chats.map((c) => chatIdx.get(c) ?? -1)) : null,
    personOk: f?.people?.length ? flags(ix.people.length, f.people.map((p) => personIndex(ix, personIdx, p, grp))) : null,
    kindOk: f?.kinds?.length ? flags(KIND_LIST.length, f.kinds.map((k) => KIND_LIST.indexOf(k))) : null,
    from: f?.from ?? null,
    to: f?.to ?? null,
    termRows: f?.text ? termMatches(ix, f.text, f.wholeWord !== false) : null,
    emojiSet: f?.emojis?.length ? new Set(f.emojis.map((e) => emojiIdx.get(normalizeEmoji(e)) ?? -1)) : null,
  }
  void warnings
}

/**
 * Person tokens: "me" = the phone owner (whoever's phone this is); "viewer" =
 * whoever is viewing in this session (the owner unless they chose otherwise).
 */
export const VIEWER = "viewer"

function personIndex(ix: StatsIndex, personIdx: Map<string, number>, p: string, grp: Int32Array): number {
  let idx: number
  if (p === ME) idx = 0
  else if (p === VIEWER) {
    const v = viewer()
    idx = v.isOwner ? 0 : (personIdx.get(v.jid!) ?? -1)
  } else idx = personIdx.get(canonicalJid(p)) ?? -1
  return idx >= 0 ? grp[idx] : -1
}

/**
 * Person index → index of its merged group's representative. People given
 * exactly the same name in the account's mapping count as one person.
 */
function personGroupsFor(ix: StatsIndex): Int32Array {
  const grp = new Int32Array(ix.people.length)
  const repIdx = new Map<string, number>()
  for (let i = 0; i < ix.people.length; i++) {
    const jid = ix.people[i]
    if (i === 0 || !jid.includes("@")) {
      grp[i] = i
      continue
    }
    const rep = samePerson(jid)
    // Numbers given the phone owner's name are the owner.
    if (rep === OWNER_KEY) {
      grp[i] = 0
      continue
    }
    const existing = repIdx.get(rep)
    if (existing === undefined) {
      repIdx.set(rep, i)
      grp[i] = i
    } else grp[i] = existing
  }
  return grp
}

/** How many identities each group representative stands for. */
function groupSizes(grp: Int32Array): Int32Array {
  const sizes = new Int32Array(grp.length)
  for (let i = 0; i < grp.length; i++) sizes[grp[i]]++
  return sizes
}

/**
 * Display name of a person index. Graphs always use real names — never "You" —
 * so a chart reads the same whoever is viewing; the phone owner is shown under
 * the name set for them in the contact names.
 */
function personLabel(ix: StatsIndex, key: number, v: ViewerInfo): string {
  if (key < 0) return "Unknown"
  if (key === 0) return v.ownerLabel
  return participant(ix.people[key]).name
}

function rowPasses(ix: StatsIndex, f: CompiledFilter, row: number, emojiContainment: boolean): boolean {
  if (f.chatOk && !f.chatOk[ix.chat[row]]) return false
  if (f.kindOk && !f.kindOk[ix.kind[row]]) return false
  if (f.termRows && !f.termRows.has(row)) return false
  if (emojiContainment && f.emojiSet) {
    let found = false
    for (let k = ix.emojiOff[row]; k < ix.emojiOff[row + 1]; k++) if (f.emojiSet.has(ix.emojiList[k])) found = true
    if (!found) return false
  }
  return true
}

const timePasses = (f: CompiledFilter, ts: number) => (f.from == null || ts >= f.from) && (f.to == null || ts < f.to)
const personPasses = (f: CompiledFilter, p: number) => !f.personOk || (p >= 0 && !!f.personOk[p])

// ---------------------------------------------------------------------------
// Units
// ---------------------------------------------------------------------------

interface Unit {
  row: number
  person: number
  ts: number
  day: number
  emoji: number
  weight: number
  /** message id to open for this unit */
  link: number
}

type UnitFn = (u: Unit) => void

function forEachUnit(ix: StatsIndex, s: SeriesDef, filters: CompiledFilter[], warnings: string[], grp: Int32Array, cb: UnitFn) {
  const G = (p: number) => (p >= 0 ? grp[p] : p)
  const u: Unit = { row: 0, person: 0, ts: 0, day: 0, emoji: -1, weight: 1, link: 0 }
  const metric = s.metric
  const emojiMetric = metric === "emoji" || metric === "reactions_given" || metric === "reactions_received"
  const rowsOk = (row: number) => filters.every((f) => rowPasses(ix, f, row, !emojiMetric))
  const unitOk = (person: number, ts: number) => filters.every((f) => personPasses(f, person) && timePasses(f, ts))
  const emojiOk = (e: number) => filters.every((f) => !f.emojiSet || f.emojiSet.has(e))

  if (metric === "reactions_given" || metric === "reactions_received") {
    const R = ix.reactions
    for (let r = 0; r < R.n; r++) {
      const row = R.row[r]
      const person = G(metric === "reactions_given" ? R.person[r] : ix.person[row])
      if (!rowsOk(row) || !unitOk(person, R.ts[r]) || !emojiOk(R.emoji[r])) continue
      u.row = row
      u.person = person
      u.ts = R.ts[r]
      u.day = R.day[r]
      u.emoji = R.emoji[r]
      u.weight = 1
      u.link = ix.ids[row]
      cb(u)
    }
    return
  }

  if (metric === "mentions_received" || metric === "mentions_made") {
    const M = ix.mentions
    for (let m = 0; m < M.n; m++) {
      const row = M.row[m]
      const person = G(metric === "mentions_received" ? M.person[m] : ix.person[row])
      if (!rowsOk(row) || !unitOk(person, ix.ts[row])) continue
      u.row = row
      u.person = person
      u.ts = ix.ts[row]
      u.day = ix.day[row]
      u.emoji = -1
      u.weight = 1
      u.link = ix.ids[row]
      cb(u)
    }
    return
  }

  let termRows: Map<number, number> | null = null
  if (metric === "term") {
    const text = s.filters?.text
    if (!text) {
      warnings.push(`"${s.label ?? METRICS.term.label}": set a word or phrase to count.`)
      return
    }
    termRows = termMatches(ix, text, s.filters?.wholeWord !== false)
  }
  const mediaKinds = new Set(MEDIA_KINDS.map((k) => KIND_LIST.indexOf(k)))

  const visit = (row: number) => {
    const person = G(ix.person[row])
    if (!unitOk(person, ix.ts[row]) || !rowsOk(row)) return
    u.row = row
    u.person = person
    u.ts = ix.ts[row]
    u.day = ix.day[row]
    u.link = ix.ids[row]
    u.emoji = -1
    switch (metric) {
      case "words":
      case "avg_words":
        u.weight = ix.words[row]
        break
      case "characters":
        u.weight = ix.chars[row]
        break
      case "media":
        if (!mediaKinds.has(ix.kind[row])) return
        u.weight = 1
        break
      case "term":
        u.weight = termRows!.get(row) ?? 0
        break
      case "emoji":
        for (let k = ix.emojiOff[row]; k < ix.emojiOff[row + 1]; k++) {
          const e = ix.emojiList[k]
          if (!emojiOk(e)) continue
          u.emoji = e
          u.weight = 1
          cb(u)
        }
        return
      default:
        u.weight = 1
    }
    cb(u)
  }

  if (termRows) for (const row of [...termRows.keys()].sort((a, b) => a - b)) visit(row)
  else for (let row = 0; row < ix.n; row++) visit(row)
}

// ---------------------------------------------------------------------------
// Bucketing
// ---------------------------------------------------------------------------

const monthCache = new Map<number, number>()
function monthOf(day: number) {
  let m = monthCache.get(day)
  if (m === undefined) {
    const d = new Date(day * DAY_MS)
    m = d.getUTCFullYear() * 12 + d.getUTCMonth()
    monthCache.set(day, m)
  }
  return m
}

/** 1970-01-01 was a Thursday: shift so weeks start on Monday. */
const weekOf = (day: number) => Math.floor((day + 3) / 7)

function keyFor(ix: StatsIndex, dim: Dimension | Split, u: Unit): number {
  switch (dim) {
    case "day":
      return u.day
    case "week":
      return weekOf(u.day)
    case "month":
      return monthOf(u.day)
    case "year":
      return Math.floor(monthOf(u.day) / 12)
    case "hour":
      return u.ts === ix.ts[u.row] ? ix.hour[u.row] : new Date(u.ts).getHours()
    case "weekday":
      return u.ts === ix.ts[u.row] ? ix.weekday[u.row] : (new Date(u.ts).getDay() + 6) % 7
    case "person":
      return u.person
    case "chat":
      return ix.chat[u.row]
    case "kind":
      return ix.kind[u.row]
    case "emoji":
      return u.emoji
    case "none":
      return 0
  }
}

interface Acc {
  v: number
  c: number
  people: Set<number> | null
  chats: Map<number, { n: number; first: number; last: number }>
}

const newAcc = (distinct: boolean): Acc => ({ v: 0, c: 0, people: distinct ? new Set() : null, chats: new Map() })

function addTo(acc: Acc, u: Unit, chat: number) {
  acc.v += u.weight
  acc.c += 1
  acc.people?.add(u.person)
  const c = acc.chats.get(chat)
  if (c) {
    c.n += u.weight
    if (u.link < c.first) c.first = u.link
    if (u.link > c.last) c.last = u.link
  } else acc.chats.set(chat, { n: u.weight, first: u.link, last: u.link })
}

function merge(into: Acc, from: Acc) {
  into.v += from.v
  into.c += from.c
  if (into.people && from.people) for (const p of from.people) into.people.add(p)
  for (const [chat, c] of from.chats) {
    const t = into.chats.get(chat)
    if (t) {
      t.n += c.n
      t.first = Math.min(t.first, c.first)
      t.last = Math.max(t.last, c.last)
    } else into.chats.set(chat, { ...c })
  }
}

function value(metric: SeriesDef["metric"], acc: Acc | undefined): number {
  if (!acc) return 0
  if (metric === "avg_words") return acc.c >= MIN_AVG_SAMPLE ? Math.round((acc.v / acc.c) * 10) / 10 : 0
  if (metric === "active_people") return acc.people?.size ?? 0
  return acc.v
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

function chatLabels(): Map<string, string> {
  return new Map(listChats().map((c) => [c.jid, c.name]))
}

function labelFor(ix: StatsIndex, dim: Dimension | Split, key: number, chats: Map<string, string>, v: ViewerInfo = viewer()): string {
  switch (dim) {
    case "day": {
      const d = new Date(key * DAY_MS)
      return d.toISOString().slice(0, 10)
    }
    case "week": {
      const d = new Date((key * 7 - 3) * DAY_MS)
      return `Week of ${d.toISOString().slice(0, 10)}`
    }
    case "month":
      return `${Math.floor(key / 12)}-${String((key % 12) + 1).padStart(2, "0")}`
    case "year":
      return String(key)
    case "hour":
      return `${String(key).padStart(2, "0")}:00`
    case "weekday":
      return WEEKDAYS[key] ?? "?"
    case "person":
      return personLabel(ix, key, v)
    case "chat":
      return chats.get(ix.chats[key]) ?? chatName(ix.chats[key])
    case "kind":
      return KINDS[KIND_LIST[key]] ?? "Other"
    case "emoji":
      return ix.emojis[key] ?? "?"
    case "none":
      return "Total"
  }
}

function timeRange(dim: Dimension, key: number): { from: number; to: number } | undefined {
  switch (dim) {
    case "day":
      return { from: dayStart(key), to: dayStart(key + 1) }
    case "week":
      return { from: dayStart(key * 7 - 3), to: dayStart(key * 7 + 4) }
    case "month": {
      const y = Math.floor(key / 12)
      const m = key % 12
      return { from: new Date(y, m, 1).getTime(), to: new Date(y, m + 1, 1).getTime() }
    }
    case "year":
      return { from: new Date(key, 0, 1).getTime(), to: new Date(key + 1, 0, 1).getTime() }
    default:
      return undefined
  }
}

function seriesBaseLabel(s: SeriesDef): string {
  if (s.label) return s.label
  const base = METRICS[s.metric].label
  const bits: string[] = []
  if (s.filters?.emojis?.length) bits.push(s.filters.emojis.join(" "))
  if (s.filters?.text) bits.push(`“${s.filters.text}”`)
  if (s.filters?.kinds?.length) bits.push(s.filters.kinds.map((k) => KINDS[k]).join(", "))
  if (s.metric === "term" && s.filters?.text) return `“${s.filters.text}”`
  return bits.length ? `${base}: ${bits.join(" · ")}` : base
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

export function runQuery(query: StatQuery): StatResult {
  const started = Date.now()
  const { index: ix, fresh } = statsIndex()
  const warnings: string[] = []
  const chats = chatLabels()
  const dim = query.groupBy
  const isTime = isTimeDimension(dim)
  const grp = personGroupsFor(ix)
  const global = compile(ix, query.filters, warnings, grp)

  interface Built {
    def: SeriesDef
    splitKey: number | null
    splitLabel: string | null
    buckets: Map<number, Acc>
    total: number
  }
  const built: Built[] = []

  for (const def of query.series) {
    const split = def.split && def.split !== dim ? def.split : null
    if (split === "emoji" && !["emoji", "reactions_given", "reactions_received"].includes(def.metric)) {
      warnings.push(`"${seriesBaseLabel(def)}" can't be split by emoji — only emoji and reaction counts can.`)
    }
    if (dim === "emoji" && !["emoji", "reactions_given", "reactions_received"].includes(def.metric)) {
      warnings.push(`"${seriesBaseLabel(def)}" has no emojis to group by — pick an emoji or reaction metric.`)
    }
    const local = compile(ix, def.filters, warnings, grp)
    const filters = def.metric === "term" ? [global, { ...local, termRows: null }] : [global, local]
    const distinct = def.metric === "active_people"
    const bySplit = new Map<number, Map<number, Acc>>()
    forEachUnit(ix, def, filters, warnings, grp, (u) => {
      const sk = split ? keyFor(ix, split, u) : 0
      const bk = keyFor(ix, dim, u)
      if (bk < 0 && (dim === "emoji" || dim === "person")) return
      let buckets = bySplit.get(sk)
      if (!buckets) bySplit.set(sk, (buckets = new Map()))
      let acc = buckets.get(bk)
      if (!acc) buckets.set(bk, (acc = newAcc(distinct)))
      addTo(acc, u, ix.chat[u.row])
    })

    if (!split) {
      const buckets = bySplit.get(0) ?? new Map()
      built.push({ def, splitKey: null, splitLabel: null, buckets, total: sumBuckets(def, buckets) })
      continue
    }
    // Keep the biggest split values; fold the rest into "Other".
    const ranked = [...bySplit].map(([k, b]) => ({ k, b, total: sumBuckets(def, b) })).sort((a, b) => b.total - a.total)
    const keep = Math.min(def.splitLimit ?? 5, 7)
    // Ordered splits (weekday, hour, year) read naturally in their own order once chosen.
    const kept = ranked.slice(0, keep)
    if (split === "weekday" || split === "hour" || split === "year") kept.sort((a, b) => a.k - b.k)
    for (const r of kept) {
      built.push({ def, splitKey: r.k, splitLabel: labelFor(ix, split, r.k, chats), buckets: r.b, total: r.total })
    }
    if (ranked.length > keep) {
      const other = new Map<number, Acc>()
      for (const r of ranked.slice(keep))
        for (const [bk, acc] of r.b) {
          let t = other.get(bk)
          if (!t) other.set(bk, (t = newAcc(distinct)))
          merge(t, acc)
        }
      built.push({ def, splitKey: -1, splitLabel: `Other (${ranked.length - keep})`, buckets: other, total: sumBuckets(def, other) })
    }
  }

  if (built.length > MAX_SERIES + 1) {
    warnings.push(`Showing the first ${MAX_SERIES} series. Split fewer ways or remove a series to see the rest.`)
    built.splice(MAX_SERIES)
  }

  // ---- categories (x-axis) ----
  const allKeys = new Set<number>()
  for (const b of built) for (const k of b.buckets.keys()) allKeys.add(k)
  let keys: number[]
  if (dim === "none") keys = [0]
  else if (dim === "hour") keys = Array.from({ length: 24 }, (_, i) => i)
  else if (dim === "weekday") keys = Array.from({ length: 7 }, (_, i) => i)
  else if (isTime && query.sort === "key") {
    const sorted = [...allKeys].sort((a, b) => a - b)
    keys = []
    if (sorted.length) {
      const lo = sorted[0]
      const hi = sorted[sorted.length - 1]
      if (hi - lo > 5000) {
        warnings.push("That's a lot of points — showing the most recent 5,000. Try weeks or months.")
        for (let k = hi - 5000; k <= hi; k++) keys.push(k)
      } else for (let k = lo; k <= hi; k++) keys.push(k)
    }
  } else {
    // Rank by the first series' value, biggest first.
    const first = built[0]
    const score = (k: number) => built.filter((b) => b.def === first?.def).reduce((n, b) => n + value(b.def.metric, b.buckets.get(k)), 0)
    keys = [...allKeys].sort((a, b) => score(b) - score(a)).slice(0, query.limit)
  }

  const categories: ResultCategory[] = keys.map((k) => ({ key: String(k), label: labelFor(ix, dim, k, chats), ...timeRange(dim, k) }))

  // ---- series ----
  let slot = 0
  const series: ResultSeries[] = built.map((b) => {
    const base = seriesBaseLabel(b.def)
    const label = b.splitLabel == null ? base : query.series.length > 1 ? `${base} · ${b.splitLabel}` : b.splitLabel
    const raw = keys.map((k) => (b.buckets.has(k) || isTime || dim === "hour" || dim === "weekday" ? value(b.def.metric, b.buckets.get(k)) : 0))
    const points = keys.map((k) => {
      const acc = b.buckets.get(k)
      if (!acc || !acc.chats.size) return null
      return [...acc.chats]
        .sort((x, y) => y[1].n - x[1].n)
        .slice(0, POINT_CHATS)
        .map<PointChat>(([chat, c]) => ({
          jid: ix.chats[chat],
          name: chats.get(ix.chats[chat]) ?? chatName(ix.chats[chat]),
          count: c.n,
          messageId: isTime ? c.first : c.last,
        }))
    })
    return {
      key: `${b.def.id}:${b.splitKey ?? "all"}`,
      label,
      slot: b.splitKey === -1 ? 0 : ((slot++ % 8) + 1),
      values: raw.slice(),
      raw,
      points,
    }
  })

  applyTransform(query.transform, series, isTime)

  const unit = query.transform === "percent" ? "%" : query.transform === "index" ? "index (peak = 100)" : unitOf(query)
  return {
    query,
    categories,
    series,
    unit,
    chart: resolveChart(query, isTime, series.length),
    isTime,
    stats: { messages: ix.n, ms: Date.now() - started, indexMs: fresh ? ix.builtMs : null },
    warnings: [
      ...new Set(warnings),
      ...(query.series.some((s) => s.metric === "avg_words") ? [`Averages only count people/periods with at least ${MIN_AVG_SAMPLE} messages.`] : []),
    ],
  }
}

function sumBuckets(def: SeriesDef, buckets: Map<number, Acc>): number {
  if (def.metric === "active_people") {
    const all = new Set<number>()
    for (const a of buckets.values()) for (const p of a.people ?? []) all.add(p)
    return all.size
  }
  let n = 0
  for (const a of buckets.values()) n += def.metric === "avg_words" ? a.c : a.v
  return n
}

function unitOf(q: StatQuery): string {
  const units = [...new Set(q.series.map((s) => METRICS[s.metric].unit))]
  return units.length === 1 ? units[0] : "count"
}

function resolveChart(q: StatQuery, isTime: boolean, n: number): StatResult["chart"] {
  if (q.chart === "heatmap" && n > 1) return "heatmap"
  if (q.chart !== "auto" && q.chart !== "heatmap") return q.chart
  if (isTime && q.sort === "key") return n === 1 ? "area" : "line"
  return n > 1 && q.series.some((s) => s.split) ? "stacked" : "bar"
}

function applyTransform(t: StatQuery["transform"], series: ResultSeries[], isTime: boolean) {
  if (t === "none") return
  const len = series[0]?.values.length ?? 0
  if (t === "cumulative") {
    for (const s of series) {
      let run = 0
      s.values = s.values.map((v) => (run += v ?? 0))
    }
  } else if (t === "rolling") {
    const w = 7
    for (const s of series) {
      const src = s.raw.map((v) => v ?? 0)
      s.values = src.map((_, i) => {
        const lo = Math.max(0, i - Math.floor(w / 2))
        const hi = Math.min(src.length, lo + w)
        const slice = src.slice(lo, hi)
        return Math.round((slice.reduce((a, b) => a + b, 0) / slice.length) * 10) / 10
      })
    }
  } else if (t === "percent") {
    for (let i = 0; i < len; i++) {
      const total = series.reduce((n, s) => n + (s.raw[i] ?? 0), 0)
      for (const s of series) s.values[i] = total ? Math.round(((s.raw[i] ?? 0) / total) * 1000) / 10 : 0
    }
  } else if (t === "index") {
    for (const s of series) {
      const peak = Math.max(0, ...s.raw.map((v) => v ?? 0))
      s.values = s.raw.map((v) => (peak ? Math.round(((v ?? 0) / peak) * 1000) / 10 : 0))
    }
  }
  void isTime
}

// ---------------------------------------------------------------------------
// Catalog (picker options) — computed from the viewer's own data
// ---------------------------------------------------------------------------

export function statsCatalog(): StatsCatalog {
  const { index: ix } = statsIndex()
  const v = viewer()
  const chats = chatLabels()
  const chatCount = new Int32Array(ix.chats.length)
  const grp = personGroupsFor(ix)
  const personCount = new Int32Array(ix.people.length)
  const emojiCount = new Int32Array(ix.emojis.length)
  for (let i = 0; i < ix.n; i++) {
    chatCount[ix.chat[i]]++
    if (ix.person[i] >= 0) personCount[grp[ix.person[i]]]++
  }
  for (let k = 0; k < ix.emojiList.length; k++) emojiCount[ix.emojiList[k]]++
  for (let r = 0; r < ix.reactions.n; r++) emojiCount[ix.reactions.emoji[r]]++
  return {
    chats: ix.chats
      .map((jid, i) => ({ jid, name: chats.get(jid) ?? chatName(jid), kind: jid.endsWith("@g.us") ? "group" : "individual", messages: chatCount[i] }))
      .filter((c) => c.messages > 0)
      .sort((a, b) => b.messages - a.messages),
    people: ix.people
      .map((jid, i) => ({ jid, name: personLabel(ix, i, v), messages: personCount[i], rep: grp[i] === i }))
      .filter((p) => p.messages > 0 && p.rep)
      .map(({ rep, ...p }) => (void rep, p))
      .sort((a, b) => b.messages - a.messages),
    emojis: ix.emojis
      .map((emoji, i) => ({ emoji, count: emojiCount[i] }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 300),
    range: { first: ix.n ? ix.ts[0] : null, last: ix.n ? ix.ts[ix.n - 1] : null },
  }
}

/**
 * Drop chat/person ids from a (possibly shared) query that aren't in the
 * viewer's data, so a shared query never reveals chats or numbers the viewer
 * can't see. Returns how many were removed.
 */
export function sanitizeForViewer(query: StatQuery): { query: StatQuery; removed: number } {
  const { index: ix } = statsIndex()
  const chats = new Set(ix.chats)
  const people = new Set(ix.people)
  let removed = 0
  const clean = (f: Filters | undefined): Filters | undefined => {
    if (!f) return f
    const out = { ...f }
    if (f.chats) {
      out.chats = f.chats.filter((c) => chats.has(c))
      removed += f.chats.length - out.chats.length
    }
    if (f.people) {
      out.people = f.people.filter((p) => p === ME || p === VIEWER || people.has(canonicalJid(p)))
      removed += f.people.length - out.people.length
    }
    return out
  }
  return {
    query: { ...query, filters: clean(query.filters) ?? {}, series: query.series.map((s) => ({ ...s, filters: clean(s.filters) })) },
    removed,
  }
}

/** Everyone who sent messages, with real names (not viewer-relative), busiest first — for "Who's viewing?". */
export function peopleWithCounts(): { jid: string; name: string; messages: number; numbers: number }[] {
  const { index: ix } = statsIndex()
  const grp = personGroupsFor(ix)
  const sizes = groupSizes(grp)
  const counts = new Int32Array(ix.people.length)
  for (let i = 0; i < ix.n; i++) if (ix.person[i] > 0) counts[grp[ix.person[i]]]++
  return ix.people
    .map((jid, i) => ({ jid, name: participant(jid).name, messages: counts[i], numbers: sizes[i], rep: grp[i] === i }))
    .filter((p, i) => i > 0 && p.rep && p.messages > 0 && p.jid.includes("@"))
    .map(({ rep, ...p }) => (void rep, p))
    .sort((a, b) => b.messages - a.messages)
}
