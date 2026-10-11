import { StatsWorkbench, type SavedSummary } from "@/components/stats/stats-workbench"
import { requireSession, withData } from "@/lib/access/dal"
import { sanitizeForViewer, statsCatalog } from "@/lib/stats/engine"
import { PRESETS } from "@/lib/stats/presets"
import { getSaved, listSaved } from "@/lib/stats/saved"
import { decodeQuery, parseQuery } from "@/lib/stats/schema"
import type { StatQuery } from "@/lib/stats/types"

export const metadata = { title: "Statistics · WhatsApp Stats" }

/**
 * /stats                → default statistic
 * /stats?preset=<id>    → a ready-made idea
 * /stats?id=<saved id>  → a saved (own or shared) statistic
 * /stats?q=<base64url>  → a shared link (also how the page restores the exact chart on Back)
 * &view=table           → open on the table view
 *
 * Whatever the source, the query is sanitised against the viewer's own data
 * before it reaches the browser.
 */
export default async function StatsPage(props: PageProps<"/stats">) {
  const { user } = await requireSession()
  const sp = await props.searchParams
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v)

  let query: StatQuery = { ...PRESETS[0].query, title: PRESETS[0].title }
  let notice: string | null = null
  let initialSavedId: number | null = null
  const id = Number(one(sp.id))
  const q = one(sp.q)
  const preset = PRESETS.find((p) => p.id === one(sp.preset))
  if (Number.isSafeInteger(id) && id > 0) {
    const s = getSaved(id, user.id)
    if (s) {
      query = s.query
      initialSavedId = s.id
    } else notice = "That saved statistic doesn't exist or isn't shared with you."
    // Edits made since opening it are carried in ?q (restored when navigating back).
    const edited = q ? parseQuery(decodeQuery(q)) : null
    if (edited?.ok) query = edited.query
  } else if (q) {
    const parsed = parseQuery(decodeQuery(q))
    if (parsed.ok) query = parsed.query
    else notice = "That link doesn't contain a valid statistic."
  } else if (preset) query = { ...preset.query, title: preset.title }

  const { catalog, initial, removed, saved } = await withData(() => {
    const clean = sanitizeForViewer(query)
    return {
      catalog: statsCatalog(),
      initial: clean.query,
      removed: clean.removed,
      saved: listSaved(user.id).map<SavedSummary>((s) => ({
        id: s.id,
        name: s.name,
        description: s.description,
        ownerName: s.ownerName,
        mine: s.ownerId === user.id,
        shared: s.shared,
        query: sanitizeForViewer(s.query).query,
      })),
    }
  })
  if (removed) {
    notice = `This statistic mentions ${removed} chat${removed === 1 ? "" : "s"} or ${removed === 1 ? "person" : "people"} you don't have access to; ${removed === 1 ? "it was" : "they were"} left out.`
  }

  return (
    <StatsWorkbench
      key={JSON.stringify(sp)}
      initialQuery={initial}
      initialSaved={saved.find((s) => s.id === initialSavedId) ?? null}
      initialView={one(sp.view) === "table" ? "table" : "chart"}
      catalog={catalog}
      presets={PRESETS}
      saved={saved}
      notice={notice}
      viewerIsAdmin={user.role === "admin"}
    />
  )
}
