import { apiWithData, getSession } from "@/lib/access/dal"
import { runQuery } from "@/lib/stats/engine"
import { readHiddenWords } from "@/lib/stats/hidden-words"
import { parseQuery } from "@/lib/stats/schema"

/**
 * POST /api/stats  { query: StatQuery }
 * Runs a statistics query against the caller's own data (admins: everything;
 * restricted accounts: their filtered copy). Word statistics leave out the
 * caller's hidden words.
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
  const session = await getSession()
  const hiddenWords = session ? readHiddenWords(session.user.id) : []
  return apiWithData(() => runQuery(parsed.query, { hiddenWords }))
}
