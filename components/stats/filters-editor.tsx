"use client"

import * as React from "react"
import { Check, ChevronsUpDown, X } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Switch } from "@/components/ui/switch"
import { KINDS, type Filters, type Kind, type StatsCatalog } from "@/lib/stats/types"
import { cn } from "@/lib/utils"

function MultiSelect({
  label,
  placeholder,
  options,
  value,
  onChange,
}: {
  label: string
  placeholder: string
  options: { value: string; label: string; hint?: string }[]
  value: string[]
  onChange: (v: string[]) => void
}) {
  const [open, setOpen] = React.useState(false)
  const byValue = new Map(options.map((o) => [o.value, o]))
  const toggle = (v: string) => onChange(value.includes(v) ? value.filter((x) => x !== v) : [...value, v])
  return (
    <div className="grid gap-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button variant="outline" className="h-auto min-h-9 justify-between px-2.5 py-1.5 font-normal">
            <span className="flex flex-wrap gap-1">
              {value.length === 0 && <span className="text-muted-foreground">{placeholder}</span>}
              {value.slice(0, 3).map((v) => (
                <span key={v} className="inline-flex max-w-40 items-center gap-1 rounded bg-muted px-1.5 py-0.5 text-xs">
                  <span className="truncate">{byValue.get(v)?.label ?? "Unavailable"}</span>
                  <X
                    className="size-3 shrink-0 cursor-pointer opacity-60 hover:opacity-100"
                    onClick={(e) => {
                      e.stopPropagation()
                      toggle(v)
                    }}
                  />
                </span>
              ))}
              {value.length > 3 && <span className="text-xs text-muted-foreground">+{value.length - 3} more</span>}
            </span>
            <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start">
          <Command>
            <CommandInput placeholder="Search…" />
            <CommandList>
              <CommandEmpty>Nothing found.</CommandEmpty>
              <CommandGroup>
                {options.map((o) => (
                  <CommandItem key={o.value} value={`${o.label} ${o.value}`} onSelect={() => toggle(o.value)}>
                    <Check className={cn("size-4", value.includes(o.value) ? "opacity-100" : "opacity-0")} />
                    <span className="flex-1 truncate">{o.label}</span>
                    {o.hint && <span className="text-xs text-muted-foreground">{o.hint}</span>}
                  </CommandItem>
                ))}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  )
}

const toDateInput = (ts: number | null | undefined) => {
  if (ts == null) return ""
  const d = new Date(ts)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
}
const fromDateInput = (s: string, endOfDay: boolean) => {
  if (!s) return null
  const [y, m, d] = s.split("-").map(Number)
  return new Date(y, m - 1, d + (endOfDay ? 1 : 0)).getTime()
}

const DATE_PRESETS: { label: string; days: number | null }[] = [
  { label: "Last 30 days", days: 30 },
  { label: "Last 90 days", days: 90 },
  { label: "Last year", days: 365 },
  { label: "All time", days: null },
]

export interface FiltersEditorProps {
  value: Filters
  onChange: (f: Filters) => void
  catalog: StatsCatalog
  /** Which controls to show. */
  show?: { chats?: boolean; people?: boolean; kinds?: boolean; dates?: boolean; text?: boolean; emojis?: boolean }
  textLabel?: string
}

export function FiltersEditor({ value, onChange, catalog, show = {}, textLabel = "Containing word or phrase" }: FiltersEditorProps) {
  const s = { chats: true, people: true, kinds: true, dates: true, text: true, emojis: true, ...show }
  const set = (patch: Partial<Filters>) => onChange(clean({ ...value, ...patch }))
  const [customEmoji, setCustomEmoji] = React.useState("")

  return (
    <div className="flex flex-col gap-3">
      {s.dates && (
        <div className="grid gap-1.5">
          <Label className="text-xs text-muted-foreground">Dates</Label>
          <div className="flex flex-wrap gap-1">
            {DATE_PRESETS.map((p) => {
              // Relative to the newest message, so presets make sense for old backups too.
              const end = catalog.range.last ?? 0
              const from = p.days == null ? null : end - p.days * 86_400_000
              const active = p.days == null ? !value.from && !value.to : value.from != null && !value.to && Math.abs(value.from - (from ?? 0)) < 86_400_000
              return (
                <button
                  key={p.label}
                  type="button"
                  onClick={() => set({ from, to: null })}
                  className={cn("rounded-full px-2.5 py-0.5 text-xs", active ? "bg-wa-accent/15 font-medium text-wa-accent" : "bg-muted text-muted-foreground hover:text-foreground")}
                >
                  {p.label}
                </button>
              )
            })}
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Input type="date" value={toDateInput(value.from)} onChange={(e) => set({ from: fromDateInput(e.target.value, false) })} aria-label="From" />
            <Input
              type="date"
              value={value.to ? toDateInput(value.to - 1) : ""}
              onChange={(e) => set({ to: fromDateInput(e.target.value, true) })}
              aria-label="To (inclusive)"
            />
          </div>
        </div>
      )}
      {s.chats && (
        <MultiSelect
          label="Chats"
          placeholder="All chats"
          options={catalog.chats.map((c) => ({ value: c.jid, label: c.name, hint: c.messages.toLocaleString() }))}
          value={value.chats ?? []}
          onChange={(chats) => set({ chats })}
        />
      )}
      {s.people && (
        <MultiSelect
          label="People"
          placeholder="Everyone"
          options={[
            { value: "viewer", label: "You (whoever is viewing)" },
            ...catalog.people.map((p) => ({ value: p.jid, label: p.jid === "me" ? `${p.name} (phone owner)` : p.name, hint: p.messages.toLocaleString() })),
          ]}
          value={value.people ?? []}
          onChange={(people) => set({ people })}
        />
      )}
      {s.kinds && (
        <div className="grid gap-1.5">
          <Label className="text-xs text-muted-foreground">Message types</Label>
          <div className="flex flex-wrap gap-1">
            {(Object.keys(KINDS) as Kind[]).map((k) => {
              const on = value.kinds?.includes(k)
              return (
                <button
                  key={k}
                  type="button"
                  onClick={() => set({ kinds: on ? value.kinds!.filter((x) => x !== k) : [...(value.kinds ?? []), k] })}
                  className={cn("rounded-full px-2.5 py-0.5 text-xs", on ? "bg-wa-accent/15 font-medium text-wa-accent" : "bg-muted text-muted-foreground hover:text-foreground")}
                >
                  {KINDS[k]}
                </button>
              )
            })}
          </div>
        </div>
      )}
      {s.text && (
        <div className="grid gap-1.5">
          <Label className="text-xs text-muted-foreground">{textLabel}</Label>
          <Input value={value.text ?? ""} placeholder="e.g. lol, good night" onChange={(e) => set({ text: e.target.value })} maxLength={100} />
          {value.text && (
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Switch checked={value.wholeWord !== false} onCheckedChange={(v) => set({ wholeWord: v })} />
              Whole words only
            </label>
          )}
        </div>
      )}
      {s.emojis && (
        <div className="grid gap-1.5">
          <Label className="text-xs text-muted-foreground">Emojis</Label>
          <div className="flex max-h-24 flex-wrap gap-0.5 overflow-y-auto rounded-md border p-1">
            {catalog.emojis.slice(0, 80).map(({ emoji, count }) => {
              const on = value.emojis?.includes(emoji)
              return (
                <button
                  key={emoji}
                  type="button"
                  title={`${count.toLocaleString()} uses`}
                  onClick={() => set({ emojis: on ? value.emojis!.filter((x) => x !== emoji) : [...(value.emojis ?? []), emoji] })}
                  className={cn("rounded px-1 text-lg leading-7", on ? "bg-wa-accent/20 ring-1 ring-wa-accent" : "hover:bg-muted")}
                >
                  {emoji}
                </button>
              )
            })}
          </div>
          <div className="flex gap-2">
            <Input value={customEmoji} onChange={(e) => setCustomEmoji(e.target.value)} placeholder="Other emoji…" className="h-8" />
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!customEmoji.trim()}
              onClick={() => {
                set({ emojis: [...new Set([...(value.emojis ?? []), customEmoji.trim()])] })
                setCustomEmoji("")
              }}
            >
              Add
            </Button>
          </div>
          {!!value.emojis?.length && (
            <div className="flex flex-wrap items-center gap-1 text-sm">
              Selected:
              {value.emojis.map((e) => (
                <button key={e} type="button" className="rounded bg-muted px-1.5" onClick={() => set({ emojis: value.emojis!.filter((x) => x !== e) })}>
                  {e} ×
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/** Drop empty fields so saved/shared queries stay small and readable. */
export function clean(f: Filters): Filters {
  const out: Filters = {}
  if (f.chats?.length) out.chats = f.chats
  if (f.people?.length) out.people = f.people
  if (f.kinds?.length) out.kinds = f.kinds
  if (f.from != null) out.from = f.from
  if (f.to != null) out.to = f.to
  if (f.text) out.text = f.text
  if (f.text && f.wholeWord === false) out.wholeWord = false
  if (f.emojis?.length) out.emojis = f.emojis
  return out
}

export function countFilters(f: Filters | undefined): number {
  if (!f) return 0
  return (
    (f.chats?.length ? 1 : 0) +
    (f.people?.length ? 1 : 0) +
    (f.kinds?.length ? 1 : 0) +
    (f.from != null || f.to != null ? 1 : 0) +
    (f.text ? 1 : 0) +
    (f.emojis?.length ? 1 : 0)
  )
}
