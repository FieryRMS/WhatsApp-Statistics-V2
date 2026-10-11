"use client"

import * as React from "react"
import { useRouter } from "next/navigation"
import { BarChart3, Bookmark, Link2, Loader2, RotateCcw, Sparkles, Table2, Trash2, Users } from "lucide-react"
import { toast } from "sonner"

import { QueryBuilder } from "./query-builder"
import { ChartLegend, DataTable, StatChart, categoryLabel, formatValue, seriesColor } from "./stat-chart"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { chatHref } from "@/lib/whatsapp/anchor"
import { deleteStatAction, saveStatAction } from "@/lib/stats/actions"
import { describeQuery } from "@/lib/stats/describe"
import type { Preset } from "@/lib/stats/presets"
import { encodeQuery } from "@/lib/stats/schema"
import { DEFAULT_QUERY, type PointChat, type StatQuery, type StatResult, type StatsCatalog } from "@/lib/stats/types"
import { cn } from "@/lib/utils"

export interface SavedSummary {
  id: number
  name: string
  description: string
  ownerName: string
  mine: boolean
  shared: boolean
  query: StatQuery
}

interface Selection {
  category: number
  series?: number
}

export function StatsWorkbench({
  initialQuery,
  initialSaved,
  catalog,
  presets,
  saved,
  notice,
  viewerIsAdmin,
}: {
  initialQuery: StatQuery
  initialSaved: SavedSummary | null
  catalog: StatsCatalog
  presets: Preset[]
  saved: SavedSummary[]
  notice: string | null
  viewerIsAdmin: boolean
}) {
  const router = useRouter()
  const [query, setQuery] = React.useState<StatQuery>(initialQuery)
  const [current, setCurrent] = React.useState<SavedSummary | null>(initialSaved)
  const [result, setResult] = React.useState<StatResult | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [loading, setLoading] = React.useState(true)
  const [view, setView] = React.useState<"chart" | "table">("chart")
  const [selection, setSelection] = React.useState<Selection | null>(null)
  const [dialog, setDialog] = React.useState<"presets" | "saved" | "save" | null>(null)

  // Run the query whenever it changes (debounced); keep the previous chart visible meanwhile.
  React.useEffect(() => {
    const ctrl = new AbortController()
    const t = setTimeout(async () => {
      setLoading(true)
      try {
        const res = await fetch("/api/stats", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query }),
          signal: ctrl.signal,
        })
        const body = await res.json()
        if (!res.ok) throw new Error(body.error ?? res.statusText)
        setResult(body as StatResult)
        setError(null)
      } catch (e) {
        if ((e as Error).name !== "AbortError") setError((e as Error).message)
      } finally {
        if (!ctrl.signal.aborted) setLoading(false)
      }
    }, 300)
    return () => {
      clearTimeout(t)
      ctrl.abort()
    }
  }, [query])

  const load = (q: StatQuery, s: SavedSummary | null = null) => {
    setQuery(q)
    setCurrent(s)
    setDialog(null)
    setSelection(null)
  }

  const open = (p: PointChat) => router.push(chatHref(p.jid, { messageId: p.messageId }))

  const select = (category: number, series?: number) => {
    if (!result) return
    const points = (series != null ? [result.series[series]] : result.series).flatMap((s) => s.points[category] ?? [])
    if (points.length === 0) return
    const chats = new Set(points.map((p) => p.jid))
    if (chats.size === 1 && (series != null || result.series.length === 1)) open(points[0])
    else setSelection({ category, series })
  }

  const copyLink = async () => {
    const url = `${location.origin}/stats?q=${encodeQuery(query)}`
    await navigator.clipboard.writeText(url)
    toast.success("Link copied", { description: "Anyone with an account can open it; they'll see the numbers for the chats they have access to." })
  }

  const summary = describeQuery(query, catalog)
  const title = current?.name ?? query.title ?? "New statistic"

  return (
    <div className="flex h-svh flex-col">
      <header className="flex flex-wrap items-center gap-2 border-b bg-sidebar px-3 py-2.5 md:px-5">
        <SidebarTrigger className="md:hidden" />
        <BarChart3 className="size-5 text-wa-accent" />
        <div className="min-w-0 flex-1">
          <h1 className="truncate text-base font-semibold">{title}</h1>
          {current && !current.mine && <p className="text-xs text-muted-foreground">Shared by {current.ownerName}</p>}
        </div>
        <Button variant="outline" size="sm" onClick={() => setDialog("presets")}>
          <Sparkles /> Ideas
        </Button>
        <Button variant="outline" size="sm" onClick={() => setDialog("saved")}>
          <Bookmark /> Saved{saved.length ? ` (${saved.length})` : ""}
        </Button>
        <Button variant="outline" size="sm" onClick={copyLink}>
          <Link2 /> Copy link
        </Button>
        <Button size="sm" onClick={() => setDialog("save")}>
          Save
        </Button>
      </header>

      <div className="flex min-h-0 flex-1 flex-col lg:flex-row">
        <aside className="shrink-0 overflow-y-auto border-b p-4 lg:w-[400px] lg:border-r lg:border-b-0">
          <div className="mb-4 flex items-center justify-between">
            <span className="text-xs text-muted-foreground">Build your own statistic, or start from an idea.</span>
            <Button variant="ghost" size="sm" onClick={() => load(DEFAULT_QUERY)} title="Start over">
              <RotateCcw /> Reset
            </Button>
          </div>
          <QueryBuilder query={query} onChange={(q) => setQuery(q)} catalog={catalog} />
        </aside>

        <main className="flex min-w-0 flex-1 flex-col gap-4 overflow-y-auto p-4 md:p-6">
          {notice && (
            <Alert>
              <AlertDescription>{notice}</AlertDescription>
            </Alert>
          )}
          <div className="flex flex-wrap items-start justify-between gap-3">
            <p className="max-w-3xl text-sm text-foreground">{summary}</p>
            <div className="flex rounded-md border p-0.5">
              <Button variant={view === "chart" ? "secondary" : "ghost"} size="sm" onClick={() => setView("chart")}>
                <BarChart3 /> Chart
              </Button>
              <Button variant={view === "table" ? "secondary" : "ghost"} size="sm" onClick={() => setView("table")}>
                <Table2 /> Table
              </Button>
            </div>
          </div>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          {result?.warnings.map((w) => (
            <p key={w} className="text-xs text-muted-foreground">
              ⓘ {w}
            </p>
          ))}

          <div className={cn("relative rounded-xl border bg-card p-4 transition-opacity", loading && result && "opacity-60")}>
            {!result && loading && (
              <div className="flex h-[380px] flex-col items-center justify-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="size-6 animate-spin text-wa-accent" />
                Counting your messages… the first run indexes everything and can take a few seconds.
              </div>
            )}
            {result && result.categories.length === 0 && (
              <div className="flex h-[380px] items-center justify-center text-sm text-muted-foreground">Nothing matches these settings.</div>
            )}
            {result && result.categories.length > 0 && (
              <div className="flex flex-col gap-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  {result.chart !== "heatmap" ? <ChartLegend result={result} shape={result.chart === "line" ? "line" : "rect"} /> : <span />}
                  <span className="text-xs text-muted-foreground">
                    {result.unit} · {result.stats.messages.toLocaleString()} messages searched in {result.stats.ms} ms
                    {result.stats.indexMs ? ` (indexed in ${(result.stats.indexMs / 1000).toFixed(1)} s)` : ""}
                  </span>
                </div>
                {view === "chart" ? <StatChart result={result} onSelect={select} /> : <DataTable result={result} onSelect={select} />}
                <p className="text-xs text-muted-foreground">Tip: click any point, bar or cell to jump to that moment in the chat.</p>
              </div>
            )}
          </div>
        </main>
      </div>

      <PointDialog result={result} selection={selection} onClose={() => setSelection(null)} onOpen={open} />
      <PresetsDialog open={dialog === "presets"} onClose={() => setDialog(null)} presets={presets} onPick={(p) => load({ ...p.query, title: p.title })} />
      <SavedDialog
        open={dialog === "saved"}
        onClose={() => setDialog(null)}
        saved={saved}
        viewerIsAdmin={viewerIsAdmin}
        onPick={(s) => load(s.query, s)}
        onDeleted={() => router.refresh()}
      />
      <SaveDialog
        key={dialog === "save" ? `open-${current?.id ?? "new"}` : "closed"}
        open={dialog === "save"}
        onClose={() => setDialog(null)}
        query={query}
        current={current?.mine ? current : null}
        defaultName={current?.name ?? ""}
        onSaved={(s) => {
          setCurrent(s)
          router.refresh()
        }}
      />
    </div>
  )
}

function PointDialog({
  result,
  selection,
  onClose,
  onOpen,
}: {
  result: StatResult | null
  selection: Selection | null
  onClose: () => void
  onOpen: (p: PointChat) => void
}) {
  if (!result || !selection) return null
  const series = selection.series != null ? [result.series[selection.series]] : result.series
  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{categoryLabel(result, selection.category)}</DialogTitle>
          <DialogDescription>
            {result.isTime ? "Jump to the first matching message in each chat." : "Jump to the latest matching message in each chat."}
          </DialogDescription>
        </DialogHeader>
        <div className="flex max-h-[60vh] flex-col gap-4 overflow-y-auto">
          {series.map((s) => {
            const points = s.points[selection.category] ?? []
            if (!points.length) return null
            return (
              <div key={s.key}>
                <div className="mb-1.5 flex items-center gap-2 text-sm">
                  <span className="h-0.5 w-3 rounded-full" style={{ background: seriesColor(s.slot) }} />
                  <span className="font-semibold tabular-nums">{formatValue(s.values[selection.category], result.unit)}</span>
                  <span className="text-muted-foreground">{s.label}</span>
                </div>
                <ul className="flex flex-col gap-1">
                  {points.map((p) => (
                    <li key={p.jid}>
                      <button type="button" onClick={() => onOpen(p)} className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-muted">
                        <Users className="size-4 text-muted-foreground" />
                        <span className="flex-1 truncate">{p.name}</span>
                        <span className="text-xs text-muted-foreground tabular-nums">{p.count.toLocaleString()}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )
          })}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function PresetsDialog({ open, onClose, presets, onPick }: { open: boolean; onClose: () => void; presets: Preset[]; onPick: (p: Preset) => void }) {
  const groups = [...new Set(presets.map((p) => p.group))]
  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>Ideas to start from</DialogTitle>
          <DialogDescription>Pick one, then change anything you like in the builder.</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-5">
          {groups.map((g) => (
            <section key={g}>
              <h3 className="mb-2 text-sm font-semibold">{g}</h3>
              <div className="grid gap-2 sm:grid-cols-2">
                {presets
                  .filter((p) => p.group === g)
                  .map((p) => (
                    <button key={p.id} type="button" onClick={() => onPick(p)} className="rounded-lg border p-3 text-left hover:border-wa-accent hover:bg-wa-accent/5">
                      <div className="text-sm font-medium">{p.title}</div>
                      <div className="mt-0.5 text-xs text-muted-foreground">{p.description}</div>
                    </button>
                  ))}
              </div>
            </section>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function SavedDialog({
  open,
  onClose,
  saved,
  viewerIsAdmin,
  onPick,
  onDeleted,
}: {
  open: boolean
  onClose: () => void
  saved: SavedSummary[]
  viewerIsAdmin: boolean
  onPick: (s: SavedSummary) => void
  onDeleted: () => void
}) {
  const mine = saved.filter((s) => s.mine)
  const others = saved.filter((s) => !s.mine)
  const del = async (s: SavedSummary) => {
    if (!confirm(`Delete “${s.name}”?`)) return
    const res = await deleteStatAction(s.id)
    if (res.error) toast.error(res.error)
    else onDeleted()
  }
  const list = (items: SavedSummary[], empty: string) =>
    items.length === 0 ? (
      <p className="text-sm text-muted-foreground">{empty}</p>
    ) : (
      <ul className="flex flex-col gap-1.5">
        {items.map((s) => (
          <li key={s.id} className="flex items-center gap-2 rounded-lg border p-2.5">
            <button type="button" className="min-w-0 flex-1 text-left" onClick={() => onPick(s)}>
              <div className="truncate text-sm font-medium">
                {s.name}
                {s.mine && s.shared && <span className="ml-2 rounded bg-muted px-1.5 py-0.5 text-[11px] font-normal">Shared</span>}
              </div>
              <div className="truncate text-xs text-muted-foreground">{s.description || (s.mine ? "Only you" : `by ${s.ownerName}`)}</div>
            </button>
            {(s.mine || viewerIsAdmin) && (
              <Button variant="ghost" size="icon" className="size-8" onClick={() => del(s)} aria-label="Delete">
                <Trash2 />
              </Button>
            )}
          </li>
        ))}
      </ul>
    )
  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Saved statistics</DialogTitle>
          <DialogDescription>Shared statistics run on each person&apos;s own chats, so everyone sees their own numbers.</DialogDescription>
        </DialogHeader>
        <h3 className="text-sm font-semibold">Yours</h3>
        {list(mine, "Nothing saved yet. Use Save to keep a statistic.")}
        <h3 className="mt-2 text-sm font-semibold">Shared by others</h3>
        {list(others, "Nobody has shared anything yet.")}
      </DialogContent>
    </Dialog>
  )
}

function SaveDialog({
  open,
  onClose,
  query,
  current,
  defaultName,
  onSaved,
}: {
  open: boolean
  onClose: () => void
  query: StatQuery
  current: SavedSummary | null
  defaultName: string
  onSaved: (s: SavedSummary) => void
}) {
  const [name, setName] = React.useState(defaultName)
  const [description, setDescription] = React.useState(current?.description ?? "")
  const [shared, setShared] = React.useState(current?.shared ?? false)
  const [asNew, setAsNew] = React.useState(!current)
  // Fresh state each time it opens: the parent remounts this dialog via `key`.
  const [pending, start] = React.useTransition()
  const [error, setError] = React.useState<string | null>(null)

  const save = () =>
    start(async () => {
      const res = await saveStatAction({ id: asNew ? undefined : current?.id, name, description, shared, query })
      if (res.error || !res.id) return setError(res.error ?? "Couldn't save.")
      onSaved({ id: res.id, name, description, shared, query, mine: true, ownerName: "you" })
      toast.success(shared ? "Saved and shared" : "Saved")
      onClose()
    })

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Save statistic</DialogTitle>
          <DialogDescription>Only the settings are saved, never the results.</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-3">
          <div className="grid gap-1.5">
            <Label htmlFor="stat-name">Name</Label>
            <Input id="stat-name" value={name} onChange={(e) => setName(e.target.value)} maxLength={120} autoFocus />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="stat-desc">Description (optional)</Label>
            <Textarea id="stat-desc" value={description} onChange={(e) => setDescription(e.target.value)} maxLength={500} rows={2} />
          </div>
          <label className="flex items-start gap-2 text-sm">
            <Switch checked={shared} onCheckedChange={setShared} className="mt-0.5" />
            <span>
              Share with everyone
              <span className="block text-xs text-muted-foreground">
                Others see it under Saved and get their own numbers. Chats or people they can&apos;t access are removed from it for them.
              </span>
            </span>
          </label>
          {current && (
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={asNew} onCheckedChange={setAsNew} />
              Save as a new copy
            </label>
          )}
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={save} disabled={pending || !name.trim()}>
            {pending ? "Saving…" : "Save"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
