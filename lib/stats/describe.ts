import { DIMENSIONS, KINDS, METRICS, TRANSFORMS, type Filters, type SeriesDef, type StatQuery, type StatsCatalog } from "./types"

/** Plain-language summary of a query, shown above the chart so it's always clear what's plotted. */

function names(ids: string[] | undefined, lookup: Map<string, string>, max = 2) {
  if (!ids?.length) return null
  const list = ids.map((id) => (id === "viewer" ? "you" : (lookup.get(id) ?? "an unavailable chat or person")))
  return list.length > max ? `${list.slice(0, max).join(", ")} and ${list.length - max} more` : list.join(" and ")
}

function day(ts: number) {
  return new Date(ts).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" })
}

export function describeFilters(f: Filters | undefined, catalog: StatsCatalog): string[] {
  if (!f) return []
  const chats = new Map(catalog.chats.map((c) => [c.jid, c.name]))
  const people = new Map(catalog.people.map((p) => [p.jid, p.name]))
  const out: string[] = []
  const c = names(f.chats, chats)
  if (c) out.push(`in ${c}`)
  const p = names(f.people, people)
  if (p) out.push(`by ${p}`)
  if (f.kinds?.length) out.push(`only ${f.kinds.map((k) => KINDS[k].toLowerCase()).join(", ")}`)
  if (f.text) out.push(`containing “${f.text}”`)
  if (f.emojis?.length) out.push(`with ${f.emojis.join(" ")}`)
  if (f.from && f.to) out.push(`from ${day(f.from)} to ${day(f.to - 1)}`)
  else if (f.from) out.push(`since ${day(f.from)}`)
  else if (f.to) out.push(`until ${day(f.to - 1)}`)
  return out
}

export function describeSeries(s: SeriesDef, catalog: StatsCatalog): string {
  const base = s.label || (s.metric === "term" && s.filters?.text ? `uses of “${s.filters.text}”` : METRICS[s.metric].label.toLowerCase())
  const bits = describeFilters(s.metric === "term" ? { ...s.filters, text: undefined } : s.filters, catalog)
  const split = s.split ? `, split by ${DIMENSIONS[s.split].label.toLowerCase()}` : ""
  return `${base}${bits.length ? ` ${bits.join(", ")}` : ""}${split}`
}

export function describeQuery(q: StatQuery, catalog: StatsCatalog): string {
  const what = q.series.map((s) => describeSeries(s, catalog)).join(" vs ")
  // "Words by word" reads oddly; say what the ranking is.
  if (q.groupBy === "word" && q.series.length === 1 && q.series[0].metric === "words" && !q.series[0].label) {
    const bits = describeFilters(q.series[0].filters, catalog).concat(describeFilters(q.filters, catalog))
    return `The ${q.limit} most used words${bits.length ? `, ${bits.join(", ")}` : ""} (hidden words left out).`
  }
  const by =
    q.groupBy === "none"
      ? ""
      : ["day", "week", "month", "year"].includes(q.groupBy)
        ? q.sort === "value"
          ? `, top ${q.limit} ${DIMENSIONS[q.groupBy].label.toLowerCase()}s`
          : ` per ${DIMENSIONS[q.groupBy].label.toLowerCase()}`
        : ` by ${DIMENSIONS[q.groupBy].label.toLowerCase()}`
  const filters = describeFilters(q.filters, catalog)
  const transform = q.transform !== "none" ? ` (${TRANSFORMS[q.transform].label.toLowerCase()})` : ""
  const text = `${what}${by}${filters.length ? `, ${filters.join(", ")}` : ""}${transform}.`
  return text.charAt(0).toUpperCase() + text.slice(1)
}
