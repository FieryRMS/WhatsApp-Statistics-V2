"use client"

import * as React from "react"
import { Eye, EyeOff, Link2, RotateCcw, Search } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  saveNamesAction,
  type NameEdit,
  type NamesFormState,
} from "@/lib/names/actions"
import type { NameRow } from "@/lib/names/rows"
import { cn } from "@/lib/utils"

type Draft =
  { kind: "name"; value: string } | { kind: "hide" } | { kind: "reset" }
type Filter = "all" | "named" | "hidden" | "custom" | "merged"

const PAGE = 150

/**
 * Edit an account's contact-name mapping.
 *
 * `namesOnly` accounts (restricted) see exactly what's in the mapping; admins'
 * mappings are overrides on top of automatic names. `canReset` is for admins.
 */
export function NamesEditor({
  userId,
  rows,
  namesOnly,
  canReset,
}: {
  userId: number
  rows: NameRow[]
  namesOnly: boolean
  canReset: boolean
}) {
  const [drafts, setDrafts] = React.useState<Map<string, Draft>>(new Map())
  const [query, setQuery] = React.useState("")
  const [filter, setFilter] = React.useState<Filter>("all")
  const [limit, setLimit] = React.useState(PAGE)
  const [state, setState] = React.useState<NamesFormState>({})
  const [pending, start] = React.useTransition()
  const deferredQuery = React.useDeferredValue(query)

  const setDraft = (jid: string, d: Draft | null) =>
    setDrafts((m) => {
      const next = new Map(m)
      if (d) next.set(jid, d)
      else next.delete(jid)
      return next
    })

  const q = deferredQuery.trim().toLowerCase()
  const filtered = rows.filter((r) => {
    if (filter === "named" && !(r.inMapping ? r.mapped : r.shown !== r.phone))
      return false
    if (filter === "hidden" && !(r.inMapping && r.mapped === null)) return false
    if (filter === "custom" && !r.inMapping) return false
    if (filter === "merged" && r.mergedWith.length === 0) return false
    if (!q) return true
    return (
      r.jid.includes(q) ||
      r.shown.toLowerCase().includes(q) ||
      (r.mapped ?? "").toLowerCase().includes(q) ||
      (r.suggestion ?? "").toLowerCase().includes(q)
    )
  })

  const save = () =>
    start(async () => {
      const edits: NameEdit[] = [...drafts].map(([jid, d]) =>
        d.kind === "reset"
          ? { jid, reset: true }
          : { jid, name: d.kind === "hide" ? null : d.value }
      )
      const res = await saveNamesAction(userId, edits)
      setState(res)
      if (!res.error) setDrafts(new Map())
    })

  return (
    <div className="flex flex-col gap-3">
      <p className="text-sm text-muted-foreground">
        {namesOnly
          ? "This account sees only the names listed here. Clear a name (or hide it) to show the phone number instead."
          : "Names here override the automatic names (address book, push names…). Hide a name to show the phone number; reset to go back to the automatic name."}
      </p>
      <p className="rounded-md bg-muted/60 px-3 py-2 text-sm text-muted-foreground">
        <strong className="font-medium text-foreground">
          Same person, several numbers?
        </strong>{" "}
        Give them exactly the same name here. They&apos;ll count as one person
        in statistics, and choosing either as “Who&apos;s viewing” makes both
        “You”. This works for the phone owner too: give their own number the
        owner&apos;s name.
      </p>

      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-56 flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by name or number"
            className="pl-8"
          />
        </div>
        {(["all", "named", "hidden", "custom", "merged"] as Filter[]).map(
          (f) => (
            <button
              key={f}
              type="button"
              onClick={() => setFilter(f)}
              className={cn(
                "rounded-full px-3 py-1 text-[13px]",
                filter === f
                  ? "bg-wa-accent/15 font-medium text-wa-accent"
                  : "bg-muted text-muted-foreground hover:text-foreground"
              )}
            >
              {f === "all"
                ? `All (${rows.length})`
                : f === "named"
                  ? "Named"
                  : f === "hidden"
                    ? "Hidden"
                    : f === "merged"
                      ? `Same person (${rows.filter((r) => r.mergedWith.length).length})`
                      : namesOnly
                        ? "In file"
                        : "Custom"}
            </button>
          )
        )}
      </div>

      <div className="overflow-hidden rounded-md border">
        <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_auto] gap-3 border-b bg-muted/50 px-3 py-2 text-xs font-medium text-muted-foreground">
          <span>Person</span>
          <span>Name shown</span>
          <span className="w-16" />
        </div>
        <ul className="max-h-[32rem] divide-y overflow-y-auto">
          {filtered.slice(0, limit).map((r) => {
            const d = drafts.get(r.jid)
            const hidden = d
              ? d.kind === "hide"
              : r.inMapping && r.mapped === null
            const value =
              d?.kind === "name"
                ? d.value
                : d
                  ? ""
                  : r.inMapping
                    ? (r.mapped ?? "")
                    : ""
            const placeholder = hidden
              ? `Hidden: shows ${r.phone ?? r.jid}`
              : namesOnly
                ? (r.phone ?? r.jid)
                : r.suggestion
                  ? `Automatic: ${r.suggestion}`
                  : (r.phone ?? r.jid)
            return (
              <li
                key={r.jid}
                className={cn(
                  "grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_auto] items-center gap-3 px-3 py-2",
                  d && "bg-wa-accent/5",
                  r.owner && "bg-muted/40"
                )}
              >
                <div className="min-w-0">
                  <div className="truncate text-sm">
                    {r.owner ? (
                      <strong className="font-medium">Phone owner</strong>
                    ) : (
                      (r.phone ?? r.jid)
                    )}
                  </div>
                  {r.owner && (
                    <div className="truncate text-xs text-muted-foreground">
                      “You” in the raw data: whoever&apos;s phone this is
                    </div>
                  )}
                  <div className="truncate text-xs text-muted-foreground">
                    Now: {r.shown}
                    {canReset && r.suggestion && r.suggestion !== r.shown && (
                      <> · automatic: {r.suggestion}</>
                    )}
                  </div>
                  {r.mergedWith.length > 0 && (
                    <div className="mt-0.5 flex items-center gap-1 truncate text-xs text-wa-accent">
                      <Link2 className="size-3 shrink-0" /> Same person as{" "}
                      {r.mergedWith.join(", ")}
                    </div>
                  )}
                </div>
                <Input
                  value={value}
                  placeholder={
                    r.owner ? "Their name, e.g. Raiyan" : placeholder
                  }
                  maxLength={100}
                  onChange={(e) =>
                    setDraft(r.jid, { kind: "name", value: e.target.value })
                  }
                  className={cn(hidden && "placeholder:italic")}
                />
                <div className="flex w-16 justify-end gap-0.5">
                  {!r.owner && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="size-8"
                      title={
                        hidden ? "Show a name" : "Hide name (show the number)"
                      }
                      onClick={() =>
                        setDraft(
                          r.jid,
                          hidden
                            ? {
                                kind: "name",
                                value: r.suggestion?.replace(/^~/, "") ?? "",
                              }
                            : { kind: "hide" }
                        )
                      }
                    >
                      {hidden ? <Eye /> : <EyeOff />}
                    </Button>
                  )}
                  {canReset && (r.inMapping || d) && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="size-8"
                      title={d ? "Undo" : "Reset to automatic name"}
                      onClick={() =>
                        setDraft(r.jid, d ? null : { kind: "reset" })
                      }
                    >
                      <RotateCcw />
                    </Button>
                  )}
                </div>
              </li>
            )
          })}
          {filtered.length === 0 && (
            <li className="px-3 py-6 text-center text-sm text-muted-foreground">
              Nobody matches
            </li>
          )}
        </ul>
        {filtered.length > limit && (
          <button
            type="button"
            onClick={() => setLimit((l) => l + PAGE)}
            className="w-full border-t py-2 text-sm text-wa-accent hover:bg-muted/50"
          >
            Show more ({filtered.length - limit} left)
          </button>
        )}
      </div>

      {state.error && (
        <Alert variant="destructive">
          <AlertDescription>{state.error}</AlertDescription>
        </Alert>
      )}
      {state.ok && !drafts.size && (
        <Alert>
          <AlertDescription>{state.ok}</AlertDescription>
        </Alert>
      )}
      <div className="flex items-center gap-2">
        <Button onClick={save} disabled={pending || drafts.size === 0}>
          {pending
            ? "Saving…"
            : drafts.size
              ? `Save ${drafts.size} change${drafts.size === 1 ? "" : "s"}`
              : "Save"}
        </Button>
        {drafts.size > 0 && (
          <Button
            variant="ghost"
            onClick={() => setDrafts(new Map())}
            disabled={pending}
          >
            Discard
          </Button>
        )}
      </div>
    </div>
  )
}
