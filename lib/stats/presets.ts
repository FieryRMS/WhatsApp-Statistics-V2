import type { StatQuery } from "./types"

/**
 * Ready-made statistics. They contain no chat- or person-specific filters, so
 * they work for every account on whatever data it can see.
 */

export interface Preset {
  id: string
  group: "Activity" | "People" | "Emojis & reactions" | "Content" | "Patterns" | "Comparisons"
  title: string
  description: string
  query: StatQuery
}

const q = (p: Partial<StatQuery> & Pick<StatQuery, "series" | "groupBy">): StatQuery => ({
  version: 1,
  filters: {},
  chart: "auto",
  transform: "none",
  limit: 20,
  sort: "key",
  ...p,
})

export const PRESETS: Preset[] = [
  // ---- Activity
  {
    id: "messages-over-time",
    group: "Activity",
    title: "Messages over time",
    description: "How busy your chats have been, month by month.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "month" }),
  },
  {
    id: "busiest-days",
    group: "Activity",
    title: "Busiest days ever",
    description: "The 20 days with the most messages. Click a bar to read that day.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "day", sort: "value", limit: 20 }),
  },
  {
    id: "top-chats",
    group: "Activity",
    title: "Most active chats",
    description: "Which chats have the most messages.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "chat", sort: "value" }),
  },
  {
    id: "chats-over-time",
    group: "Activity",
    title: "Top chats over time",
    description: "Your five busiest chats, month by month, stacked.",
    query: q({ series: [{ id: "s1", metric: "messages", split: "chat", splitLimit: 5 }], groupBy: "month", chart: "stacked" }),
  },
  {
    id: "active-people",
    group: "Activity",
    title: "Active people per month",
    description: "How many different people messaged each month.",
    query: q({ series: [{ id: "s1", metric: "active_people" }], groupBy: "month" }),
  },
  // ---- People
  {
    id: "top-talkers",
    group: "People",
    title: "Who texts the most",
    description: "Everyone ranked by number of messages.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "person", sort: "value" }),
  },
  {
    id: "wordiest",
    group: "People",
    title: "Who writes the longest messages",
    description: "Average words per text message, by person.",
    query: q({ series: [{ id: "s1", metric: "avg_words", filters: { kinds: ["text"] } }], groupBy: "person", sort: "value" }),
  },
  {
    id: "media-senders",
    group: "People",
    title: "Who sends the most media",
    description: "Photos, videos, stickers, voice notes and more, by person and type.",
    query: q({ series: [{ id: "s1", metric: "media", split: "kind", splitLimit: 6 }], groupBy: "person", sort: "value", chart: "stacked" }),
  },
  {
    id: "most-tagged",
    group: "People",
    title: "Who gets tagged the most",
    description: "How often each person was @mentioned.",
    query: q({ series: [{ id: "s1", metric: "mentions_received" }], groupBy: "person", sort: "value" }),
  },
  {
    id: "deleters",
    group: "People",
    title: "Who deletes the most messages",
    description: "Messages deleted for everyone, by person.",
    query: q({ series: [{ id: "s1", metric: "messages", filters: { kinds: ["deleted"] } }], groupBy: "person", sort: "value" }),
  },
  {
    id: "people-over-time",
    group: "People",
    title: "Top people over time",
    description: "Who carried the conversation each month (share of messages).",
    query: q({ series: [{ id: "s1", metric: "messages", split: "person", splitLimit: 5 }], groupBy: "month", chart: "area", transform: "percent" }),
  },
  // ---- Emojis & reactions
  {
    id: "top-emojis",
    group: "Emojis & reactions",
    title: "Most used emojis",
    description: "Every emoji typed in messages, ranked.",
    query: q({ series: [{ id: "s1", metric: "emoji" }], groupBy: "emoji", sort: "value" }),
  },
  {
    id: "emojis-over-time",
    group: "Emojis & reactions",
    title: "Emoji trends",
    description: "Your five favourite emojis over time.",
    query: q({ series: [{ id: "s1", metric: "emoji", split: "emoji", splitLimit: 5 }], groupBy: "month" }),
  },
  {
    id: "laugh-champion",
    group: "Emojis & reactions",
    title: "Who laughs the most 😂",
    description: "Who uses 😂 the most. Change the emoji to track any other.",
    query: q({ series: [{ id: "s1", metric: "emoji", filters: { emojis: ["😂"] } }], groupBy: "person", sort: "value" }),
  },
  {
    id: "emoji-by-person",
    group: "Emojis & reactions",
    title: "Everyone's favourite emoji",
    description: "Top emojis split by who used them.",
    query: q({ series: [{ id: "s1", metric: "emoji", split: "emoji", splitLimit: 5 }], groupBy: "person", sort: "value", limit: 12, chart: "stacked" }),
  },
  {
    id: "reactions-received",
    group: "Emojis & reactions",
    title: "Whose messages get the most reactions",
    description: "Reactions received, by the author of the message.",
    query: q({ series: [{ id: "s1", metric: "reactions_received", split: "emoji", splitLimit: 5 }], groupBy: "person", sort: "value", chart: "stacked" }),
  },
  {
    id: "top-reactions",
    group: "Emojis & reactions",
    title: "Most used reactions",
    description: "Which reaction emojis people pick.",
    query: q({ series: [{ id: "s1", metric: "reactions_given" }], groupBy: "emoji", sort: "value" }),
  },
  // ---- Content
  {
    id: "types-over-time",
    group: "Content",
    title: "Your media mix over time",
    description: "Each month's share of photos, stickers, voice notes, videos and more.",
    query: q({ series: [{ id: "s1", metric: "media", split: "kind", splitLimit: 6 }], groupBy: "month", chart: "area", transform: "percent" }),
  },
  {
    id: "message-types",
    group: "Content",
    title: "Message types",
    description: "How many of each kind of message.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "kind", sort: "value" }),
  },
  {
    id: "voice-notes",
    group: "Content",
    title: "Voice notes over time",
    description: "Voice notes per month, by the people who send them most.",
    query: q({ series: [{ id: "s1", metric: "messages", filters: { kinds: ["voice"] }, split: "person", splitLimit: 4 }], groupBy: "month", chart: "stacked" }),
  },
  {
    id: "words-over-time",
    group: "Content",
    title: "Words written per month",
    description: "How much text you write, smoothed to show the trend.",
    query: q({ series: [{ id: "s1", metric: "words" }], groupBy: "month", transform: "rolling" }),
  },
  // ---- Patterns
  {
    id: "by-hour",
    group: "Patterns",
    title: "When during the day",
    description: "Messages by hour of the day.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "hour" }),
  },
  {
    id: "by-weekday",
    group: "Patterns",
    title: "Busiest day of the week",
    description: "Messages by day of the week.",
    query: q({ series: [{ id: "s1", metric: "messages" }], groupBy: "weekday" }),
  },
  {
    id: "week-heatmap",
    group: "Patterns",
    title: "Weekly rhythm heatmap",
    description: "Hour of day against day of the week — darker means busier.",
    query: q({ series: [{ id: "s1", metric: "messages", split: "weekday", splitLimit: 7 }], groupBy: "hour", chart: "heatmap" }),
  },
  {
    id: "night-owls",
    group: "Patterns",
    title: "Night owls",
    description: "Messages by hour for the five most active people. Look at who's still going after midnight.",
    query: q({ series: [{ id: "s1", metric: "messages", split: "person", splitLimit: 5 }], groupBy: "hour", chart: "stacked" }),
  },
  {
    id: "years-by-weekday",
    group: "Patterns",
    title: "How your week changed over the years",
    description: "Messages by weekday, one line per year.",
    query: q({ series: [{ id: "s1", metric: "messages", split: "year", splitLimit: 6 }], groupBy: "weekday", chart: "line" }),
  },
  // ---- Comparisons (several statistics on one chart)
  {
    id: "laughs-vs-messages",
    group: "Comparisons",
    title: "Do we laugh more when we talk more?",
    description: "Messages vs 😂 per month, each indexed to its peak so both fit one axis.",
    query: q({
      series: [
        { id: "s1", metric: "messages" },
        { id: "s2", metric: "emoji", filters: { emojis: ["😂"] } },
      ],
      groupBy: "month",
      chart: "line",
      transform: "index",
    }),
  },
  {
    id: "media-vs-text",
    group: "Comparisons",
    title: "Texts vs media",
    description: "Text messages against media items, per month.",
    query: q({
      series: [
        { id: "s1", metric: "messages", label: "Texts", filters: { kinds: ["text"] } },
        { id: "s2", metric: "media", label: "Media" },
      ],
      groupBy: "month",
      chart: "line",
    }),
  },
  {
    id: "you-vs-everyone",
    group: "Comparisons",
    title: "You vs everyone else",
    description: "Your messages against everyone else's, over time.",
    query: q({
      series: [
        { id: "s1", metric: "messages", label: "You", filters: { people: ["me"] } },
        { id: "s2", metric: "messages", label: "Everyone (incl. you)" },
      ],
      groupBy: "month",
      chart: "line",
    }),
  },
  {
    id: "reactions-vs-messages",
    group: "Comparisons",
    title: "Reactions vs messages",
    description: "Are reactions replacing replies? Both indexed to their peak.",
    query: q({
      series: [
        { id: "s1", metric: "messages" },
        { id: "s2", metric: "reactions_given" },
      ],
      groupBy: "month",
      chart: "line",
      transform: "index",
    }),
  },
]
