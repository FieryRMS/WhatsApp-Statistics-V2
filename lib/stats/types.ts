/**
 * The statistics query language. Client-safe.
 *
 * A query is plain JSON — never SQL — so it can be validated, saved, shared and
 * run against whichever data the viewer is allowed to see.
 *
 *   series   – one or more things to count (each may be split into several lines)
 *   groupBy  – the x-axis (time, hour, weekday, person, chat, type, emoji)
 *   filters  – narrow everything (chats, people, types, dates, words, emojis)
 */

export const METRICS = {
  messages: { label: "Messages", unit: "messages", description: "How many messages were sent." },
  words: { label: "Words", unit: "words", description: "Total words written in texts and captions." },
  characters: { label: "Characters", unit: "characters", description: "Total characters typed." },
  avg_words: { label: "Average words per message", unit: "words / message", description: "How long messages are on average." },
  media: { label: "Media", unit: "items", description: "Photos, videos, GIFs, stickers, voice notes, audio and documents." },
  emoji: { label: "Emojis", unit: "emojis", description: "Every emoji used in a message. Filter to specific emojis to track them." },
  term: { label: "Word or phrase", unit: "times", description: "How often a word or phrase appears in messages." },
  reactions_given: { label: "Reactions given", unit: "reactions", description: "Reactions people left on messages." },
  reactions_received: { label: "Reactions received", unit: "reactions", description: "Reactions on each person's messages." },
  mentions_received: { label: "Times tagged", unit: "mentions", description: "How often each person was @mentioned." },
  mentions_made: { label: "Tags made", unit: "mentions", description: "How often each person @mentioned someone." },
  active_people: { label: "Active people", unit: "people", description: "How many different people sent messages." },
} as const
export type Metric = keyof typeof METRICS

export const TIME_DIMENSIONS = ["day", "week", "month", "year"] as const
export const DIMENSIONS = {
  day: { label: "Day", group: "Time" },
  week: { label: "Week", group: "Time" },
  month: { label: "Month", group: "Time" },
  year: { label: "Year", group: "Time" },
  hour: { label: "Hour of day", group: "Pattern" },
  weekday: { label: "Day of week", group: "Pattern" },
  person: { label: "Person", group: "Category" },
  chat: { label: "Chat", group: "Category" },
  kind: { label: "Message type", group: "Category" },
  emoji: { label: "Emoji", group: "Category" },
  none: { label: "Total", group: "Category" },
} as const
export type Dimension = keyof typeof DIMENSIONS
export type TimeDimension = (typeof TIME_DIMENSIONS)[number]

/** Dimensions a series can be split by (one line/segment per value). */
export const SPLITS = ["person", "chat", "kind", "emoji", "weekday", "hour", "year"] as const
export type Split = (typeof SPLITS)[number]

export const KINDS = {
  text: "Text",
  image: "Photo",
  video: "Video",
  gif: "GIF",
  voice: "Voice note",
  audio: "Audio",
  document: "Document",
  sticker: "Sticker",
  contact: "Contact",
  location: "Location",
  poll: "Poll",
  call: "Call",
  missed_call: "Missed call",
  view_once: "View once",
  deleted: "Deleted",
  other: "Other",
} as const
export type Kind = keyof typeof KINDS
export const MEDIA_KINDS: Kind[] = ["image", "video", "gif", "voice", "audio", "document", "sticker", "view_once"]

export interface Filters {
  /** Chat JIDs. */
  chats?: string[]
  /** Person JIDs, or "me". */
  people?: string[]
  kinds?: Kind[]
  /** Inclusive start, ms. */
  from?: number | null
  /** Exclusive end, ms. */
  to?: number | null
  /** Only messages containing this word/phrase (for the "term" metric: the thing to count). */
  text?: string
  /** Match whole words only (default true). */
  wholeWord?: boolean
  /** Emojis: for emoji/reaction metrics, which ones to count; otherwise "messages containing". */
  emojis?: string[]
}

export interface SeriesDef {
  id: string
  label?: string
  metric: Metric
  filters?: Filters
  split?: Split | null
  /** Max split values before the rest fold into "Other" (1–7). */
  splitLimit?: number
}

export type ChartType = "auto" | "line" | "area" | "bar" | "stacked" | "heatmap"
export type Transform = "none" | "cumulative" | "percent" | "rolling" | "index"

export const TRANSFORMS: Record<Transform, { label: string; description: string }> = {
  none: { label: "Raw values", description: "The numbers as counted." },
  cumulative: { label: "Running total", description: "Each point adds up everything before it." },
  percent: { label: "Share of total (%)", description: "Each series as a percentage of all series at that point." },
  rolling: { label: "Smoothed (7-point average)", description: "Averages each point with its neighbours to show the trend." },
  index: {
    label: "Indexed to peak (= 100)",
    description: "Scales every series so its highest point is 100. Use it to compare series with very different sizes on one chart.",
  },
}

export interface StatQuery {
  version: 1
  title?: string
  description?: string
  series: SeriesDef[]
  groupBy: Dimension
  filters: Filters
  chart: ChartType
  transform: Transform
  /** For categorical axes: how many categories to show. For time axes with sort=value: top N periods. */
  limit: number
  /** "key" = natural order (time, hour…); "value" = biggest first. */
  sort: "key" | "value"
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

export interface PointChat {
  jid: string
  name: string
  count: number
  /** Message to open: the first match in a time bucket, the latest match otherwise. */
  messageId: number
}

export interface ResultCategory {
  key: string
  label: string
  /** For time buckets: [from, to) in ms — used for "open this day". */
  from?: number
  to?: number
}

export interface ResultSeries {
  key: string
  label: string
  /** Fixed palette slot 1–8, or 0 for "Other" (gray). */
  slot: number
  values: (number | null)[]
  /** Raw counts before the transform (for the table and tooltip). */
  raw: (number | null)[]
  /** Where each point's data came from (top chats), for click-through. */
  points: (PointChat[] | null)[]
}

export interface StatResult {
  query: StatQuery
  categories: ResultCategory[]
  series: ResultSeries[]
  unit: string
  /** Resolved chart type (auto picked). */
  chart: Exclude<ChartType, "auto">
  isTime: boolean
  stats: { messages: number; ms: number; indexMs: number | null }
  warnings: string[]
}

/** Options for the builder's pickers, computed from the viewer's own data. */
export interface StatsCatalog {
  chats: { jid: string; name: string; kind: string; messages: number }[]
  people: { jid: string; name: string; messages: number }[]
  emojis: { emoji: string; count: number }[]
  range: { first: number | null; last: number | null }
}

export const DEFAULT_QUERY: StatQuery = {
  version: 1,
  series: [{ id: "s1", metric: "messages" }],
  groupBy: "month",
  filters: {},
  chart: "auto",
  transform: "none",
  limit: 20,
  sort: "key",
}

export const isTimeDimension = (d: Dimension): d is TimeDimension => (TIME_DIMENSIONS as readonly string[]).includes(d)

/** Metrics whose "person" is someone other than the message author. */
export const METRIC_PERSON: Record<Metric, string> = {
  messages: "sender",
  words: "sender",
  characters: "sender",
  avg_words: "sender",
  media: "sender",
  emoji: "sender",
  term: "sender",
  reactions_given: "person reacting",
  reactions_received: "author of the message",
  mentions_received: "person tagged",
  mentions_made: "person tagging",
  active_people: "sender",
}

/** Is `dim` meaningful for `metric`? (The emoji axis needs emoji or reaction units.) */
export function dimensionAllowed(metric: Metric, dim: Dimension | Split): boolean {
  if (dim === "emoji") return metric === "emoji" || metric === "reactions_given" || metric === "reactions_received"
  if (metric === "active_people" && dim === "person") return false
  return true
}
