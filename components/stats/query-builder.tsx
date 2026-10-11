"use client"

import * as React from "react"
import { ChevronDown, Filter, Plus, Trash2 } from "lucide-react"

import { FiltersEditor, clean, countFilters } from "./filters-editor"
import { seriesColor } from "./stat-chart"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectGroup, SelectItem, SelectLabel, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  DIMENSIONS,
  METRICS,
  METRIC_PERSON,
  SPLITS,
  TRANSFORMS,
  dimensionAllowed,
  isTimeDimension,
  type ChartType,
  type Dimension,
  type Metric,
  type SeriesDef,
  type StatQuery,
  type StatsCatalog,
  type Transform,
} from "@/lib/stats/types"
import { cn } from "@/lib/utils"

function Section({ step, title, hint, children }: { step: number; title: string; hint: string; children: React.ReactNode }) {
  return (
    <section className="flex flex-col gap-3">
      <div>
        <h3 className="flex items-center gap-2 text-sm font-semibold">
          <span className="flex size-5 items-center justify-center rounded-full bg-wa-accent/15 text-[11px] text-wa-accent">{step}</span>
          {title}
        </h3>
        <p className="mt-0.5 pl-7 text-xs text-muted-foreground">{hint}</p>
      </div>
      <div className="pl-7">{children}</div>
    </section>
  )
}

const METRIC_GROUPS: { label: string; metrics: Metric[] }[] = [
  { label: "Messages", metrics: ["messages", "media", "active_people"] },
  { label: "Text", metrics: ["words", "characters", "avg_words", "term"] },
  { label: "Emojis & reactions", metrics: ["emoji", "reactions_given", "reactions_received"] },
  { label: "Tags", metrics: ["mentions_received", "mentions_made"] },
]

function SeriesCard({
  series,
  index,
  slot,
  groupBy,
  catalog,
  canRemove,
  onChange,
  onRemove,
}: {
  series: SeriesDef
  index: number
  slot: number
  groupBy: Dimension
  catalog: StatsCatalog
  canRemove: boolean
  onChange: (s: SeriesDef) => void
  onRemove: () => void
}) {
  const [showFilters, setShowFilters] = React.useState(countFilters(series.filters) > 0 || series.metric === "term")
  // For "word or phrase" the word is what's counted, not a filter.
  const nFilters = countFilters(series.metric === "term" ? { ...series.filters, text: undefined } : series.filters)
  const isTerm = series.metric === "term"
  const emojiCapable = ["emoji", "reactions_given", "reactions_received"].includes(series.metric)

  return (
    <div className="rounded-lg border bg-card p-3">
      <div className="flex items-center gap-2">
        <span className="h-0.5 w-4 shrink-0 rounded-full" style={{ background: seriesColor(slot) }} aria-hidden />
        <Select value={series.metric} onValueChange={(m) => onChange({ ...series, metric: m as Metric, split: series.split && dimensionAllowed(m as Metric, series.split) ? series.split : null })}>
          <SelectTrigger className="h-9 flex-1">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {METRIC_GROUPS.map((g) => (
              <SelectGroup key={g.label}>
                <SelectLabel>{g.label}</SelectLabel>
                {g.metrics.map((m) => (
                  <SelectItem key={m} value={m}>
                    {METRICS[m].label}
                  </SelectItem>
                ))}
              </SelectGroup>
            ))}
          </SelectContent>
        </Select>
        {canRemove && (
          <Button variant="ghost" size="icon" className="size-8" onClick={onRemove} aria-label={`Remove series ${index + 1}`}>
            <Trash2 />
          </Button>
        )}
      </div>
      <p className="mt-1.5 text-xs text-muted-foreground">
        {METRICS[series.metric].description}
        {groupBy === "person" && METRIC_PERSON[series.metric] !== "sender" && <> “Person” here means the {METRIC_PERSON[series.metric]}.</>}
      </p>

      {isTerm && (
        <div className="mt-3 grid gap-1.5">
          <Label className="text-xs text-muted-foreground">Word or phrase to count</Label>
          <Input
            value={series.filters?.text ?? ""}
            placeholder="e.g. lol"
            onChange={(e) => onChange({ ...series, filters: clean({ ...series.filters, text: e.target.value }) })}
          />
        </div>
      )}

      <div className="mt-3 grid grid-cols-2 gap-2">
        <div className="grid gap-1">
          <Label className="text-xs text-muted-foreground">Split into lines by</Label>
          <Select value={series.split ?? "none"} onValueChange={(v) => onChange({ ...series, split: v === "none" ? null : (v as SeriesDef["split"]) })}>
            <SelectTrigger className="h-8">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="none">Don&apos;t split</SelectItem>
              {SPLITS.filter((sp) => sp !== groupBy).map((sp) => (
                <SelectItem key={sp} value={sp} disabled={!dimensionAllowed(series.metric, sp)}>
                  {DIMENSIONS[sp].label}
                  {sp === "emoji" && !emojiCapable && " (emoji/reaction counts only)"}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {series.split ? (
          <div className="grid gap-1">
            <Label className="text-xs text-muted-foreground">Show top</Label>
            <Select value={String(series.splitLimit ?? 5)} onValueChange={(v) => onChange({ ...series, splitLimit: Number(v) })}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {[2, 3, 4, 5, 6, 7].map((n) => (
                  <SelectItem key={n} value={String(n)}>
                    {n} + Other
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        ) : (
          <div className="grid gap-1">
            <Label className="text-xs text-muted-foreground">Name (optional)</Label>
            <Input className="h-8" value={series.label ?? ""} placeholder={METRICS[series.metric].label} onChange={(e) => onChange({ ...series, label: e.target.value || undefined })} />
          </div>
        )}
      </div>

      <button
        type="button"
        onClick={() => setShowFilters((v) => !v)}
        className="mt-3 flex items-center gap-1.5 text-xs font-medium text-wa-accent"
      >
        <Filter className="size-3.5" />
        Only count some {series.metric.startsWith("reaction") ? "reactions" : series.metric === "emoji" ? "emojis" : "messages"}
        {nFilters > 0 && <span className="rounded-full bg-wa-accent/15 px-1.5">{nFilters}</span>}
        <ChevronDown className={cn("size-3.5 transition-transform", showFilters && "rotate-180")} />
      </button>
      {showFilters && (
        <div className="mt-3 border-t pt-3">
          <FiltersEditor
            value={series.filters ?? {}}
            onChange={(f) => onChange({ ...series, filters: f })}
            catalog={catalog}
            show={{ dates: false, text: !isTerm }}
          />
        </div>
      )}
    </div>
  )
}

const DIMENSION_GROUPS: { label: string; dims: Dimension[] }[] = [
  { label: "Over time", dims: ["day", "week", "month", "year"] },
  { label: "Patterns", dims: ["hour", "weekday"] },
  { label: "Compare", dims: ["person", "chat", "kind", "emoji", "none"] },
]

const CHARTS: { value: ChartType; label: string }[] = [
  { value: "auto", label: "Automatic" },
  { value: "line", label: "Lines" },
  { value: "area", label: "Area" },
  { value: "bar", label: "Bars" },
  { value: "stacked", label: "Stacked bars" },
  { value: "heatmap", label: "Heatmap (needs a split)" },
]

export function QueryBuilder({ query, onChange, catalog }: { query: StatQuery; onChange: (q: StatQuery) => void; catalog: StatsCatalog }) {
  const set = (patch: Partial<StatQuery>) => onChange({ ...query, ...patch })
  const isTime = isTimeDimension(query.groupBy)
  const ranked = !isTime || query.sort === "value"
  // Slots follow the order series appear (matches the chart's colour assignment for unsplit series).
  const slots = query.series.reduce<number[]>((acc, s, i) => {
    const prev = i === 0 ? 0 : acc[i - 1] + (query.series[i - 1].split ? (query.series[i - 1].splitLimit ?? 5) : 1) - 1
    acc.push((prev % 8) + 1)
    return acc
  }, [])

  return (
    <div className="flex flex-col gap-6">
      <Section step={1} title="What to count" hint="Add more than one to compare them on the same chart.">
        <div className="flex flex-col gap-3">
          {query.series.map((s, i) => (
            <SeriesCard
              key={s.id}
              series={s}
              index={i}
              slot={slots[i]}
              groupBy={query.groupBy}
              catalog={catalog}
              canRemove={query.series.length > 1}
              onChange={(next) => set({ series: query.series.map((x) => (x.id === s.id ? next : x)) })}
              onRemove={() => set({ series: query.series.filter((x) => x.id !== s.id) })}
            />
          ))}
          {query.series.length < 8 && (
            <Button
              variant="outline"
              size="sm"
              className="self-start"
              onClick={() => set({ series: [...query.series, { id: `s${Date.now().toString(36)}`, metric: "messages" }] })}
            >
              <Plus /> Add something to compare
            </Button>
          )}
        </div>
      </Section>

      <Section step={2} title="Group by" hint="This becomes the chart's horizontal axis.">
        <div className="flex flex-col gap-2">
          {DIMENSION_GROUPS.map((g) => (
            <div key={g.label} className="flex flex-wrap items-center gap-1">
              <span className="w-20 text-xs text-muted-foreground">{g.label}</span>
              {g.dims.map((d) => {
                const allowed = query.series.some((s) => dimensionAllowed(s.metric, d))
                return (
                  <button
                    key={d}
                    type="button"
                    disabled={!allowed}
                    title={allowed ? undefined : "Needs an emoji or reaction count"}
                    onClick={() => set({ groupBy: d, sort: isTimeDimension(d) ? "key" : "value" })}
                    className={cn(
                      "rounded-md border px-2.5 py-1 text-xs disabled:cursor-not-allowed disabled:opacity-40",
                      query.groupBy === d ? "border-wa-accent bg-wa-accent/10 font-medium text-wa-accent" : "hover:bg-muted"
                    )}
                  >
                    {DIMENSIONS[d].label}
                  </button>
                )
              })}
            </div>
          ))}
          {isTime && (
            <label className="mt-1 flex items-center gap-2 text-xs">
              <input type="checkbox" checked={query.sort === "value"} onChange={(e) => set({ sort: e.target.checked ? "value" : "key" })} className="accent-[var(--wa-accent)]" />
              Rank the busiest {DIMENSIONS[query.groupBy].label.toLowerCase()}s instead of a timeline
            </label>
          )}
          {ranked && query.groupBy !== "hour" && query.groupBy !== "weekday" && query.groupBy !== "none" && (
            <div className="mt-1 flex items-center gap-2 text-xs">
              Show the top
              <Select value={String(query.limit)} onValueChange={(v) => set({ limit: Number(v) })}>
                <SelectTrigger size="sm" className="h-7 w-20">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {[5, 10, 15, 20, 30, 50, 100].map((n) => (
                    <SelectItem key={n} value={String(n)}>
                      {n}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}
        </div>
      </Section>

      <Section step={3} title="Filters" hint="Apply to everything on the chart.">
        <FiltersEditor value={query.filters} onChange={(filters) => set({ filters })} catalog={catalog} />
      </Section>

      <Section step={4} title="Display" hint="How the numbers are drawn.">
        <div className="grid gap-3">
          <div className="grid gap-1">
            <Label className="text-xs text-muted-foreground">Chart</Label>
            <Select value={query.chart} onValueChange={(v) => set({ chart: v as ChartType })}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {CHARTS.map((c) => (
                  <SelectItem key={c.value} value={c.value}>
                    {c.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-1">
            <Label className="text-xs text-muted-foreground">Values</Label>
            <Select value={query.transform} onValueChange={(v) => set({ transform: v as Transform })}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(Object.keys(TRANSFORMS) as Transform[]).map((t) => (
                  <SelectItem key={t} value={t}>
                    {TRANSFORMS[t].label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">{TRANSFORMS[query.transform].description}</p>
          </div>
        </div>
      </Section>
    </div>
  )
}
