import { apiWithData } from "@/lib/access/dal"
import { runQuery } from "@/lib/stats/engine"
import { parseQuery } from "@/lib/stats/schema"

/**
 * POST /api/stats  { query: StatQuery }
 * Runs a statistics query against the caller's own data (admins: everything;
 * restricted accounts: their filtered copy).
 */
export async function POST(req: Request) {
  let body: unknown
  try {
    body = await req.json()
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 })
  }
  const parsed = parseQuery((body as { query?: unknown })?.query)
  if (!parsed.ok) return Response.json({ error: parsed.error }, { status: 400 })
  return apiWithData(() => runQuery(parsed.query))
}
