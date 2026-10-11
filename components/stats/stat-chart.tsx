"use client"

import * as React from "react"
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  Brush,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  type TooltipContentProps,
} from "recharts"

import type { StatResult } from "@/lib/stats/types"
import { cn } from "@/lib/utils"
import { EmojiText } from "@/components/emoji/emoji-text"
import { emojiSrc } from "@/lib/emoji/sequence"
import { useIsMobile } from "@/hooks/use-mobile"
import { CloudChart } from "./cloud-chart"

export const seriesColor = (slot: number) =>
  slot === 0 ? "var(--series-other)" : `var(--series-${slot})`

const compact = new Intl.NumberFormat(undefined, {
  notation: "compact",
  maximumFractionDigits: 1,
})
const full = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 })

/** Human label for a category (time buckets are formatted in the viewer's locale). */
export function categoryLabel(
  result: StatResult,
  i: number,
  short = false
): string {
  const c = result.categories[i]
  if (!c) return ""
  if (!result.isTime || c.from == null) return c.label
  const d = new Date(c.from)
  switch (result.query.groupBy) {
    case "day":
      return d.toLocaleDateString(
        undefined,
        short
          ? { day: "numeric", month: "short" }
          : {
              weekday: "short",
              day: "numeric",
              month: "short",
              year: "numeric",
            }
      )
    case "week":
      return short
        ? d.toLocaleDateString(undefined, { day: "numeric", month: "short" })
        : `Week of ${d.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" })}`
    case "month":
      return d.toLocaleDateString(
        undefined,
        short
          ? { month: "short", year: "2-digit" }
          : { month: "long", year: "numeric" }
      )
    default:
      return String(d.getFullYear())
  }
}

export function formatValue(v: number | null | undefined, unit: string) {
  if (v == null) return "—"
  return unit === "%" ? `${full.format(v)}%` : full.format(v)
}

/** One tooltip lists every series at the hovered position; the value leads, the name follows. */
function ChartTooltip({
  active,
  payload,
  label,
  result,
}: TooltipContentProps<number, string> & { result: StatResult }) {
  if (!active || !payload?.length) return null
  const i = Number(label)
  const rows = result.series
    .map((s, si) => ({ s, si, v: s.values[i] }))
    .filter((r) => r.v != null)
    .sort((a, b) => (b.v ?? 0) - (a.v ?? 0))
  return (
    <div className="min-w-44 rounded-lg border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="mb-1.5 font-medium text-muted-foreground">
        <EmojiText text={categoryLabel(result, i)} />
      </div>
      <ul className="space-y-1">
        {rows.map(({ s, v }) => (
          <li key={s.key} className="flex items-center gap-2">
            <span
              className="h-0.5 w-3 shrink-0 rounded-full"
              style={{ background: seriesColor(s.slot) }}
            />
            <span className="font-semibold text-foreground tabular-nums">
              {formatValue(v, result.unit)}
            </span>
            <span className="truncate text-muted-foreground">
              <EmojiText text={s.label} />
            </span>
          </li>
        ))}
      </ul>
      {result.series.some((s) => s.points[i]?.length) && (
        <div className="mt-1.5 text-[11px] text-muted-foreground">
          Click to open in the chat
        </div>
      )}
    </div>
  )
}

export function ChartLegend({
  result,
  shape,
}: {
  result: StatResult
  shape: "line" | "rect"
}) {
  if (result.series.length < 2) return null
  return (
    <ul className="flex flex-wrap gap-x-4 gap-y-1.5 text-xs text-muted-foreground">
      {result.series.map((s) => (
        <li key={s.key} className="flex items-center gap-1.5">
          <span
            className={cn(
              "shrink-0",
              shape === "line"
                ? "h-0.5 w-3.5 rounded-full"
                : "size-2.5 rounded-[3px]"
            )}
            style={{ background: seriesColor(s.slot) }}
          />
          <span className="max-w-56 truncate">
            <EmojiText text={s.label} />
          </span>
        </li>
      ))}
    </ul>
  )
}

const CATEGORICAL = new Set(["person", "chat", "emoji", "word", "kind", "none"])

export type Zoom = [start: number, end: number] | null

type ChartMouse =
  | {
      activeLabel?: string | number
      activeTooltipIndex?: number | string | null
    }
  | null
  | undefined

/** Category index under the pointer — the x value (not the position), so it stays right while zoomed. */
const pointerIndex = (s: ChartMouse): number | null => {
  const v = s?.activeLabel ?? s?.activeTooltipIndex
  return v == null || v === "" ? null : Number(v)
}

export function StatChart({
  result,
  onSelect,
  zoom = null,
  onZoom,
  height = 380,
}: {
  result: StatResult
  /** Clicked a category (x position); seriesIndex is set for heatmap cells. */
  onSelect: (categoryIndex: number, seriesIndex?: number) => void
  /** Visible category range on timelines (null = everything). */
  zoom?: Zoom
  onZoom?: (z: Zoom) => void
  height?: number
}) {
  const [drag, setDrag] = React.useState<{ a: number; b: number } | null>(null)
  const [panning, setPanning] = React.useState(false)
  // Narrow screens: shorter category labels so horizontal bars keep room to draw.
  const narrow = useIsMobile()
  const wrapRef = React.useRef<HTMLDivElement>(null)

  const horizontal =
    (result.chart === "bar" || result.chart === "stacked") &&
    CATEGORICAL.has(result.query.groupBy)
  const n = result.categories.length
  // Timelines can be zoomed: drag across the plot, use the range slider, ctrl+scroll / pinch,
  // and shift+drag (or a sideways trackpad swipe) to pan.
  const zoomable =
    !!onZoom &&
    result.chart !== "heatmap" &&
    result.isTime &&
    result.query.sort === "key" &&
    !horizontal &&
    n > 12
  const range = zoom && zoom[1] < n && zoom[0] <= zoom[1] ? zoom : null
  const start = range?.[0] ?? 0
  const end = range?.[1] ?? n - 1

  /** Where the pointer is across the plot area, 0 (left) … 1 (right). */
  const plotFraction = (clientX: number) => {
    const grid = wrapRef.current
      ?.querySelector(".recharts-cartesian-grid")
      ?.getBoundingClientRect()
    if (!grid || grid.width <= 0) return 0.5
    return Math.min(1, Math.max(0, (clientX - grid.left) / grid.width))
  }
  const setWindow = React.useCallback(
    (a: number, b: number) => {
      const width = b - a
      if (width >= n - 1) return onZoom?.(null)
      const lo = Math.max(0, Math.min(n - 1 - width, Math.round(a)))
      onZoom?.([lo, lo + width])
    },
    [n, onZoom]
  )

  // Ctrl+wheel (and trackpad pinch, which browsers report as ctrl+wheel) zooms around the
  // pointer; a sideways swipe pans. Native listener so preventDefault can stop page zoom.
  React.useEffect(() => {
    const el = wrapRef.current
    if (!el || !zoomable) return
    const onWheel = (e: WheelEvent) => {
      const width = end - start
      if (e.ctrlKey || e.metaKey) {
        e.preventDefault()
        const f = plotFraction(e.clientX)
        const anchor = start + f * width
        const factor = Math.exp(Math.max(-1, Math.min(1, e.deltaY * 0.01)))
        const next = Math.max(4, Math.min(n - 1, Math.round(width * factor)))
        if (next !== width)
          setWindow(anchor - f * next, anchor - f * next + next)
      } else if (range && Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        e.preventDefault()
        const step =
          Math.round((e.deltaX / el.clientWidth) * width) || Math.sign(e.deltaX)
        setWindow(start + step, end + step)
      }
    }
    el.addEventListener("wheel", onWheel, { passive: false })
    return () => el.removeEventListener("wheel", onWheel)
  }, [zoomable, start, end, n, range, setWindow])

  /** Shift+drag pans the zoomed window (captured before the chart sees it, so it doesn't zoom). */
  const onPanStart = (e: React.MouseEvent) => {
    if (!zoomable || !range || !e.shiftKey) return
    e.preventDefault()
    e.stopPropagation()
    const grid = wrapRef.current
      ?.querySelector(".recharts-cartesian-grid")
      ?.getBoundingClientRect()
    const perPx = (end - start) / Math.max(1, grid?.width ?? 1)
    const x0 = e.clientX
    const [a0, b0] = [start, end]
    setPanning(true)
    const move = (ev: MouseEvent) => {
      const shift = Math.round((x0 - ev.clientX) * perPx)
      setWindow(a0 + shift, b0 + shift)
    }
    const up = () => {
      setPanning(false)
      window.removeEventListener("mousemove", move)
      window.removeEventListener("mouseup", up)
    }
    window.addEventListener("mousemove", move)
    window.addEventListener("mouseup", up)
  }
  const data = React.useMemo(
    () =>
      result.categories.map((_, i) => {
        const row: Record<string, number | null | string> = { i }
        result.series.forEach((s, si) => (row[`s${si}`] = s.values[i]))
        return row
      }),
    [result]
  )

  if (result.chart === "heatmap")
    return <Heatmap result={result} onSelect={onSelect} />
  if (result.chart === "cloud")
    return <CloudChart result={result} onSelect={onSelect} height={height} />

  const tick = { fontSize: 11, fill: "var(--muted-foreground)" }
  const grid = (
    <CartesianGrid
      stroke="var(--chart-grid)"
      strokeDasharray=""
      vertical={horizontal}
      horizontal={!horizontal}
    />
  )
  const valueAxisProps = {
    tick,
    tickLine: false,
    axisLine: false,
    tickFormatter: (v: number) =>
      result.unit === "%" ? `${v}%` : compact.format(v),
    width: 48,
  }
  const catAxisProps = {
    dataKey: "i",
    tick,
    tickLine: false,
    axisLine: { stroke: "var(--chart-grid)" },
    tickFormatter: (i: number) => categoryLabel(result, i, true),
    minTickGap: 16,
  }
  const tooltip = (
    <Tooltip
      content={(p) => (
        <ChartTooltip
          {...(p as TooltipContentProps<number, string>)}
          result={result}
        />
      )}
      cursor={{
        stroke: "var(--muted-foreground)",
        strokeWidth: 1,
        fill: "var(--muted)",
        fillOpacity: 0.4,
      }}
      isAnimationActive={false}
    />
  )
  const mouse = zoomable
    ? {
        onMouseDown: (s: ChartMouse) => {
          const i = pointerIndex(s)
          if (i != null) setDrag({ a: i, b: i })
        },
        onMouseMove: (s: ChartMouse) => {
          const i = pointerIndex(s)
          if (drag && i != null && i !== drag.b) setDrag({ ...drag, b: i })
        },
        onMouseUp: () => {
          if (!drag) return
          setDrag(null)
          // A drag across at least one point zooms; anything else is a click.
          if (drag.a !== drag.b)
            onZoom!([Math.min(drag.a, drag.b), Math.max(drag.a, drag.b)])
          else onSelect(drag.a)
        },
        onMouseLeave: () => setDrag(null),
      }
    : {
        onClick: (s: ChartMouse) => {
          const i = pointerIndex(s)
          if (i != null) onSelect(i)
        },
      }
  const common = {
    data,
    ...mouse,
    margin: { top: 8, right: 16, bottom: 4, left: 0 },
    style: {
      cursor: panning ? "grabbing" : drag ? "col-resize" : "pointer",
      userSelect: "none" as const,
    },
  }
  const zoomLayers = zoomable && (
    <>
      {drag && drag.a !== drag.b && (
        <ReferenceArea
          x1={drag.a}
          x2={drag.b}
          fill="var(--muted-foreground)"
          fillOpacity={0.15}
          strokeOpacity={0}
          ifOverflow="extendDomain"
        />
      )}
      <Brush
        // Recharts' Brush keeps its scale from the previous data; when the number of
        // points changes (e.g. month → week) it renders one frame with NaN positions.
        // A fresh Brush per timeline avoids that.
        key={`${result.query.groupBy}:${n}:${result.categories[0]?.key ?? ""}`}
        dataKey="i"
        height={26}
        travellerWidth={8}
        stroke="var(--muted-foreground)"
        fill="var(--card)"
        startIndex={range?.[0] ?? 0}
        endIndex={range?.[1] ?? n - 1}
        tickFormatter={(i: number) => categoryLabel(result, i, true)}
        onChange={(r: { startIndex?: number; endIndex?: number }) => {
          const a = r.startIndex ?? 0
          const b = r.endIndex ?? n - 1
          onZoom!(a === 0 && b === n - 1 ? null : [a, b])
        }}
      />
    </>
  )

  if (result.chart === "line" || result.chart === "area") {
    const Chart = result.chart === "area" ? AreaChart : LineChart
    const stacked = result.chart === "area" && result.series.length > 1
    return (
      <div ref={wrapRef} style={{ height }} onMouseDownCapture={onPanStart}>
        <ResponsiveContainer width="100%" height="100%">
          <Chart {...common}>
            {grid}
            <XAxis {...catAxisProps} />
            <YAxis
              {...valueAxisProps}
              domain={result.unit === "%" && stacked ? [0, 100] : undefined}
              allowDataOverflow={result.unit === "%" && stacked}
            />
            {tooltip}
            {zoomLayers}
            {result.series.map((s, si) =>
              result.chart === "area" ? (
                <Area
                  key={s.key}
                  dataKey={`s${si}`}
                  type="monotone"
                  stackId={stacked ? "a" : undefined}
                  stroke={seriesColor(s.slot)}
                  strokeWidth={2}
                  fill={seriesColor(s.slot)}
                  fillOpacity={0.1}
                  dot={false}
                  activeDot={{
                    r: 4,
                    strokeWidth: 2,
                    stroke: "var(--background)",
                  }}
                  isAnimationActive={false}
                />
              ) : (
                <Line
                  key={s.key}
                  dataKey={`s${si}`}
                  type="monotone"
                  stroke={seriesColor(s.slot)}
                  strokeWidth={2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  dot={
                    result.categories.length <= 31
                      ? {
                          r: 3,
                          strokeWidth: 2,
                          stroke: "var(--background)",
                          fill: seriesColor(s.slot),
                        }
                      : false
                  }
                  activeDot={{
                    r: 4,
                    strokeWidth: 2,
                    stroke: "var(--background)",
                  }}
                  isAnimationActive={false}
                />
              )
            )}
          </Chart>
        </ResponsiveContainer>
      </div>
    )
  }

  // Bars: thin (≤ 24px), rounded at the data end, square at the baseline; stacked segments
  // are separated by a 2px surface-coloured gap.
  const stacked = result.chart === "stacked" && result.series.length > 1
  const last = result.series.length - 1
  const barHeight = horizontal
    ? Math.max(height, result.categories.length * 30 + 40)
    : height
  return (
    <div
      ref={wrapRef}
      style={{ height: barHeight }}
      onMouseDownCapture={onPanStart}
    >
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          {...common}
          layout={horizontal ? "vertical" : "horizontal"}
          barCategoryGap="22%"
          barGap={2}
        >
          {grid}
          {horizontal ? (
            <>
              <XAxis type="number" {...valueAxisProps} />
              <YAxis
                type="category"
                {...catAxisProps}
                width={narrow ? 112 : 150}
                interval={0}
                tickFormatter={(i: number) =>
                  truncate(categoryLabel(result, i, true), narrow ? 10 : 22)
                }
                // SVG text can't use the WhatsApp emoji artwork, so the emoji axis draws images.
                tick={
                  result.query.groupBy === "emoji"
                    ? (props) => (
                        <EmojiAxisTick
                          x={Number(props.x)}
                          y={Number(props.y)}
                          label={categoryLabel(
                            result,
                            Number(props.payload?.value),
                            true
                          )}
                        />
                      )
                    : undefined
                }
              />
            </>
          ) : (
            <>
              <XAxis {...catAxisProps} />
              <YAxis {...valueAxisProps} />
            </>
          )}
          {tooltip}
          {result.series.map((s, si) => (
            <Bar
              key={s.key}
              dataKey={`s${si}`}
              stackId={stacked ? "a" : undefined}
              fill={seriesColor(s.slot)}
              maxBarSize={24}
              stroke={stacked ? "var(--background)" : undefined}
              strokeWidth={stacked ? 2 : 0}
              radius={
                !stacked || si === last
                  ? horizontal
                    ? [0, 4, 4, 0]
                    : [4, 4, 0, 0]
                  : 0
              }
              isAnimationActive={false}
            />
          ))}
          {zoomLayers}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function EmojiAxisTick({
  x,
  y,
  label,
}: {
  x: number
  y: number
  label: string
}) {
  return (
    <g transform={`translate(${x},${y})`}>
      <title>{label}</title>
      <image href={emojiSrc(label)} x={-26} y={-10} width={20} height={20} />
    </g>
  )
}

const graphemes =
  typeof Intl !== "undefined" && "Segmenter" in Intl
    ? new Intl.Segmenter(undefined, { granularity: "grapheme" })
    : null

/**
 * Shorten to roughly `n` letters' width by whole characters, so an emoji (or flag, or
 * ZWJ sequence) is never cut in half. Emoji and other wide characters count double.
 */
function truncate(s: string, n: number) {
  const chars = graphemes
    ? Array.from(graphemes.segment(s), (g) => g.segment)
    : Array.from(s)
  const width = (c: string) => (c.codePointAt(0)! > 0x2000 ? 2 : 1)
  if (chars.reduce((w, c) => w + width(c), 0) <= n) return s
  let used = 1 // room for the ellipsis
  let out = ""
  for (const c of chars) {
    if (used + width(c) > n) break
    used += width(c)
    out += c
  }
  return `${out}…`
}

/** Rows = series (e.g. weekdays), columns = categories (e.g. hours). One hue, light → dark. */
function Heatmap({
  result,
  onSelect,
}: {
  result: StatResult
  onSelect: (c: number, s?: number) => void
}) {
  const max = Math.max(
    1,
    ...result.series.flatMap((s) => s.values.map((v) => v ?? 0))
  )
  const step = (v: number | null) =>
    v ? Math.min(7, 1 + Math.floor((v / max) * 7)) : 0
  const [hover, setHover] = React.useState<{ s: number; c: number } | null>(
    null
  )
  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto">
        <table className="w-full border-separate border-spacing-[2px] text-[11px]">
          <thead>
            <tr>
              <th />
              {result.categories.map((_, c) => (
                <th
                  key={c}
                  className="px-0.5 pb-1 text-center font-normal text-muted-foreground"
                >
                  {result.categories.length > 12 && c % 2
                    ? ""
                    : categoryLabel(result, c, true).replace(":00", "")}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.series.map((s, si) => (
              <tr key={s.key}>
                <th className="pr-2 text-right font-normal whitespace-nowrap text-muted-foreground">
                  <EmojiText text={s.label} />
                </th>
                {s.values.map((v, c) => (
                  <td key={c} className="p-0">
                    <button
                      type="button"
                      onClick={() => onSelect(c, si)}
                      onMouseEnter={() => setHover({ s: si, c })}
                      onMouseLeave={() => setHover(null)}
                      onFocus={() => setHover({ s: si, c })}
                      className="block h-7 w-full min-w-3 rounded-[3px] outline-offset-1 hover:outline-2 hover:outline-foreground/40"
                      style={{ background: `var(--seq-${step(v)})` }}
                      aria-label={`${s.label}, ${categoryLabel(result, c)}: ${formatValue(v, result.unit)}`}
                    />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span className="min-h-4">
          {hover ? (
            <>
              <span className="font-semibold text-foreground tabular-nums">
                {formatValue(
                  result.series[hover.s].values[hover.c],
                  result.unit
                )}
              </span>{" "}
              {result.series[hover.s].label}, {categoryLabel(result, hover.c)}
            </>
          ) : (
            "Hover a cell for its value · click to open it in the chat"
          )}
        </span>
        <span className="flex items-center gap-1.5">
          Fewer
          {Array.from({ length: 8 }, (_, i) => (
            <span
              key={i}
              className="size-3 rounded-[2px]"
              style={{ background: `var(--seq-${i})` }}
            />
          ))}
          More
        </span>
      </div>
    </div>
  )
}

export function DataTable({
  result,
  onSelect,
}: {
  result: StatResult
  onSelect: (c: number, s?: number) => void
}) {
  return (
    <div className="max-h-[420px] overflow-auto rounded-md border">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-muted/80 backdrop-blur">
          <tr>
            <th className="px-3 py-2 text-left font-medium">
              {result.isTime ? "Period" : "Category"}
            </th>
            {result.series.map((s) => (
              <th key={s.key} className="px-3 py-2 text-right font-medium">
                <span className="inline-flex items-center gap-1.5">
                  <span
                    className="size-2 rounded-[2px]"
                    style={{ background: seriesColor(s.slot) }}
                  />
                  <EmojiText text={s.label} />
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y">
          {result.categories.map((_, c) => (
            <tr
              key={c}
              tabIndex={0}
              className="cursor-pointer outline-none hover:bg-muted/50 focus-visible:bg-muted"
              onClick={() => onSelect(c)}
              onKeyDown={(e) =>
                (e.key === "Enter" || e.key === " ") &&
                (e.preventDefault(), onSelect(c))
              }
              title="Open in the chat"
            >
              <td className="px-3 py-1.5">
                <EmojiText text={categoryLabel(result, c)} />
              </td>
              {result.series.map((s) => (
                <td key={s.key} className="px-3 py-1.5 text-right tabular-nums">
                  {formatValue(s.values[c], result.unit)}
                  {result.query.transform !== "none" &&
                    s.raw[c] != null &&
                    s.raw[c] !== s.values[c] && (
                      <span className="ml-1.5 text-xs text-muted-foreground">
                        ({full.format(s.raw[c]!)})
                      </span>
                    )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
