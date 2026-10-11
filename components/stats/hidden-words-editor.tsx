"use client"

import * as React from "react"
import { ChevronDown, EyeOff, Plus, RotateCcw, X } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"

/**
 * The account's hidden words: left out of word counts, top-word charts and
 * clouds. Starts with common filler words ("in", "to", "the", "ki", …).
 */
export function HiddenWordsEditor({
  words,
  onChange,
  saving,
}: {
  words: string[]
  /** null restores the default list. */
  onChange: (words: string[] | null) => void
  saving: boolean
}) {
  const [open, setOpen] = React.useState(false)
  const [draft, setDraft] = React.useState("")
  const [filter, setFilter] = React.useState("")

  const add = () => {
    const next = draft
      .split(/[\s,]+/)
      .map((w) => w.trim().toLowerCase())
      .filter(Boolean)
    if (!next.length) return
    onChange([...words, ...next])
    setDraft("")
  }
  const shown = filter
    ? words.filter((w) => w.includes(filter.trim().toLowerCase()))
    : words

  return (
    <div className="rounded-lg border bg-card p-3">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-1.5 text-left text-xs font-medium"
      >
        <EyeOff className="size-3.5 text-muted-foreground" />
        Hidden words
        <span className="rounded-full bg-muted px-1.5 text-muted-foreground tabular-nums">
          {words.length}
        </span>
        <span className="flex-1" />
        <ChevronDown
          className={cn(
            "size-3.5 text-muted-foreground transition-transform",
            open && "rotate-180"
          )}
        />
      </button>
      <p className="mt-1 text-xs text-muted-foreground">
        Left out of word counts and clouds. Saved to your account.
      </p>
      {open && (
        <div className="mt-3 flex flex-col gap-2">
          <form
            className="flex gap-2"
            onSubmit={(e) => {
              e.preventDefault()
              add()
            }}
          >
            <Input
              className="h-8"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="Add words (space or comma separated)"
              aria-label="Words to hide"
            />
            <Button type="submit" size="sm" variant="outline" disabled={saving}>
              <Plus /> Hide
            </Button>
          </form>
          {words.length > 30 && (
            <Input
              className="h-8"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Find a hidden word"
              aria-label="Find a hidden word"
            />
          )}
          <div className="flex max-h-48 flex-wrap gap-1 overflow-y-auto">
            {shown.map((w) => (
              <span
                key={w}
                className="inline-flex items-center gap-0.5 rounded-full border bg-muted/50 py-0.5 pr-0.5 pl-2 text-xs"
              >
                {w}
                <button
                  type="button"
                  onClick={() => onChange(words.filter((x) => x !== w))}
                  disabled={saving}
                  className="rounded-full p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                  aria-label={`Show "${w}" again`}
                >
                  <X className="size-3" />
                </button>
              </span>
            ))}
            {!shown.length && (
              <span className="text-xs text-muted-foreground">
                {words.length ? "No match." : "Nothing hidden."}
              </span>
            )}
          </div>
          <div className="flex gap-2">
            <Button
              size="sm"
              variant="ghost"
              disabled={saving}
              onClick={() => onChange(null)}
            >
              <RotateCcw /> Restore defaults
            </Button>
            {words.length > 0 && (
              <Button
                size="sm"
                variant="ghost"
                disabled={saving}
                onClick={() => onChange([])}
              >
                Show all words
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
