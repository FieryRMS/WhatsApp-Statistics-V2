import { z } from "zod"

import { DIMENSIONS, KINDS, METRICS, SPLITS, TRANSFORMS, type StatQuery } from "./types"

/**
 * Validation for queries arriving from the browser, saved queries and share
 * links. Everything is bounded so a query can't ask the server for unbounded work.
 */

const keys = <T extends Record<string, unknown>>(o: T) => Object.keys(o) as [keyof T & string, ...(keyof T & string)[]]

const filters = z
  .object({
    chats: z.array(z.string().max(120)).max(500).optional(),
    people: z.array(z.string().max(120)).max(500).optional(),
    kinds: z.array(z.enum(keys(KINDS))).max(20).optional(),
    from: z.number().finite().nullable().optional(),
    to: z.number().finite().nullable().optional(),
    text: z.string().trim().max(100).optional(),
    wholeWord: z.boolean().optional(),
    emojis: z.array(z.string().max(40)).max(50).optional(),
  })
  .strip()

const series = z
  .object({
    id: z.string().max(40),
    label: z.string().trim().max(80).optional(),
    metric: z.enum(keys(METRICS)),
    filters: filters.optional(),
    split: z.enum(SPLITS).nullable().optional(),
    splitLimit: z.number().int().min(1).max(7).optional(),
  })
  .strip()

export const statQuerySchema = z
  .object({
    version: z.literal(1),
    title: z.string().trim().max(120).optional(),
    description: z.string().trim().max(500).optional(),
    series: z.array(series).min(1).max(8),
    groupBy: z.enum(keys(DIMENSIONS)),
    filters: filters.default({}),
    chart: z.enum(["auto", "line", "area", "bar", "stacked", "heatmap", "cloud"]).default("auto"),
    transform: z.enum(keys(TRANSFORMS)).default("none"),
    limit: z.number().int().min(1).max(100).default(20),
    sort: z.enum(["key", "value"]).default("key"),
  })
  .strip()

export function parseQuery(input: unknown): { ok: true; query: StatQuery } | { ok: false; error: string } {
  const r = statQuerySchema.safeParse(input)
  if (!r.success) return { ok: false, error: r.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ") }
  return { ok: true, query: r.data as StatQuery }
}

/** Share links carry the query as base64url JSON in `?q=`. */
export function encodeQuery(q: StatQuery): string {
  const json = JSON.stringify(q)
  const bytes = new TextEncoder().encode(json)
  let bin = ""
  for (const b of bytes) bin += String.fromCharCode(b)
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")
}

export function decodeQuery(s: string): unknown {
  try {
    const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/"))
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0))
    return JSON.parse(new TextDecoder().decode(bytes))
  } catch {
    return null
  }
}
