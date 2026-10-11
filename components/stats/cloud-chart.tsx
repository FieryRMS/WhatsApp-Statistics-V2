"use client"

import * as React from "react"
import cloud, { type Word } from "d3-cloud"

import type { StatResult } from "@/lib/stats/types"
import { emojiSrc } from "@/lib/emoji/sequence"
import { formatValue } from "./stat-chart"

/**
 * Word (or emoji) cloud for a single ranked series: size and shade both follow
 * the count, so the biggest, darkest words are the most used. Words stay
 * horizontal so they are easy to read. Click a word to see where it was used.
 */

interface Placed {
  i: number
  text: string
  size: number
  x: number
  y: number
}

/** Small seeded PRNG so the same data always lays out the same way. */
function seeded(seed: number) {
  let s = seed >>> 0 || 1
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0
    return s / 2 ** 32
  }
}

export function CloudChart({
  result,
  onSelect,
  height = 380,
}: {
  result: StatResult
  onSelect: (categoryIndex: number) => void
  height?: number
}) {
  const wrapRef = React.useRef<HTMLDivElement>(null)
  const [width, setWidth] = React.useState(0)
  const [placed, setPlaced] = React.useState<Placed[]>([])
  const [hover, setHover] = React.useState<number | null>(null)
  const isEmoji = result.query.groupBy === "emoji"
  const values = React.useMemo(() => result.series[0]?.values ?? [], [result])

  React.useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(([e]) =>
      setWidth(Math.round(e.contentRect.width))
    )
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const max = Math.max(1, ...values.map((v) => v ?? 0))
  const min = Math.min(max, ...values.map((v) => v ?? max))
  /** 0 … 1 on a square-root scale, so area (not height) tracks the count. */
  const t = React.useCallback(
    (v: number) =>
      max === min
        ? 1
        : (Math.sqrt(v) - Math.sqrt(min)) / (Math.sqrt(max) - Math.sqrt(min)),
    [max, min]
  )

  React.useEffect(() => {
    if (width <= 0) return
    const font = getComputedStyle(wrapRef.current!).fontFamily
    const biggest = Math.max(28, Math.min(84, width / 7))
    const words = result.categories
      .map((c, i) => ({ i, label: c.label, v: values[i] ?? 0 }))
      .filter((w) => w.v > 0)
      .map<Word & { i: number; label: string; size: number }>((w) => ({
        i: w.i,
        // Emoji are drawn as images; a square letter stands in for them while laying out.
        text: isEmoji ? "W" : w.label,
        label: w.label,
        size: Math.round(12 + t(w.v) * (biggest - 12)),
      }))
    const layout = cloud<(typeof words)[number]>()
      .size([width, height])
      .words(words)
      .padding(isEmoji ? 4 : 2)
      .rotate(0)
      .font(font)
      .fontWeight(600)
      .fontSize((d) => d.size)
      .random(seeded(words.length * 7919 + width))
      .on("end", (out) =>
        setPlaced(
          out.map((d) => ({
            i: d.i,
            text: d.label,
            size: d.size!,
            x: d.x!,
            y: d.y!,
          }))
        )
      )
    layout.start()
    return () => {
      layout.stop()
    }
  }, [result, values, width, height, isEmoji, t])

  const step = (v: number) => 4 + Math.round(t(v) * 3)
  const dropped = values.filter((v) => (v ?? 0) > 0).length - placed.length

  return (
    <div className="flex flex-col gap-3">
      <div ref={wrapRef} style={{ height }} className="w-full font-sans">
        {width > 0 && (
          <svg
            width={width}
            height={height}
            role="img"
            aria-label={`${isEmoji ? "Emoji" : "Word"} cloud of the ${placed.length} most used`}
          >
            <g transform={`translate(${width / 2},${height / 2})`}>
              {placed.map((p) => {
                const v = values[p.i] ?? 0
                const common = {
                  role: "button",
                  tabIndex: 0,
                  "aria-label": `${p.text}: ${formatValue(v, result.unit)}`,
                  onClick: () => onSelect(p.i),
                  onKeyDown: (e: React.KeyboardEvent) =>
                    (e.key === "Enter" || e.key === " ") && onSelect(p.i),
                  onMouseEnter: () => setHover(p.i),
                  onMouseLeave: () => setHover(null),
                  onFocus: () => setHover(p.i),
                  onBlur: () => setHover(null),
                  className: "cursor-pointer outline-none",
                  opacity: hover == null || hover === p.i ? 1 : 0.45,
                }
                return isEmoji ? (
                  <image
                    key={p.i}
                    {...common}
                    href={emojiSrc(p.text)}
                    x={p.x - p.size / 2}
                    y={p.y - p.size * 0.85}
                    width={p.size}
                    height={p.size}
                  />
                ) : (
                  <text
                    key={p.i}
                    {...common}
                    x={p.x}
                    y={p.y}
                    textAnchor="middle"
                    fontSize={p.size}
                    fontWeight={600}
                    fill={`var(--seq-${step(v)})`}
                  >
                    {p.text}
                  </text>
                )
              })}
            </g>
          </svg>
        )}
      </div>
      <p className="min-h-4 text-xs text-muted-foreground">
        {hover != null ? (
          <>
            <span className="font-semibold text-foreground tabular-nums">
              {formatValue(values[hover], result.unit)}
            </span>{" "}
            {result.categories[hover]?.label}
          </>
        ) : (
          <>
            Bigger and darker means used more · click{" "}
            {isEmoji ? "an emoji" : "a word"} to see where
            {dropped > 0 &&
              ` · ${dropped} didn't fit; widen the window or see the table`}
          </>
        )}
      </p>
    </div>
  )
}
