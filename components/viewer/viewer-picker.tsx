"use client"

import * as React from "react"
import { Check, Search, Smartphone } from "lucide-react"

import { Input } from "@/components/ui/input"
import { setViewerAction } from "@/lib/viewer/actions"
import { cn } from "@/lib/utils"

export interface PickerPerson {
  jid: string
  name: string
  messages: number
  /** Identities merged into this person (same name in the contact names). */
  numbers?: number
}

/**
 * "Who's viewing?" — pick yourself from the people in these chats.
 * `next` set: navigate there after choosing (used right after login).
 */
export function ViewerPicker({
  people,
  current,
  ownerLabel,
  next,
}: {
  people: PickerPerson[]
  current: string | null
  ownerLabel: string
  next?: string
}) {
  const [query, setQuery] = React.useState("")
  const [pending, start] = React.useTransition()
  const [chosen, setChosen] = React.useState<string | null | undefined>(undefined)
  const [error, setError] = React.useState<string | null>(null)
  const q = query.trim().toLowerCase()
  const visible = people.filter((p) => !q || p.name.toLowerCase().includes(q) || p.jid.includes(q)).slice(0, 60)
  const active = chosen === undefined ? current : chosen

  const choose = (jid: string | null) => {
    setChosen(jid)
    start(async () => {
      const res = await setViewerAction(jid, next)
      if (res?.error) setError(res.error)
    })
  }

  const row = (jid: string | null, label: string, hint: string, icon?: React.ReactNode) => (
    <li key={jid ?? "owner"}>
      <button
        type="button"
        disabled={pending}
        onClick={() => choose(jid)}
        className={cn(
          "flex w-full items-center gap-3 rounded-md px-3 py-2 text-left hover:bg-muted disabled:opacity-60",
          active === jid && "bg-wa-accent/10"
        )}
      >
        {icon}
        <span className="min-w-0 flex-1">
          <span className="block truncate text-sm font-medium">{label}</span>
          <span className="block truncate text-xs text-muted-foreground">{hint}</span>
        </span>
        {active === jid && <Check className="size-4 text-wa-accent" />}
      </button>
    </li>
  )

  return (
    <div className="flex flex-col gap-3">
      <div className="relative">
        <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Find yourself by name or number" className="pl-8" autoFocus />
      </div>
      <ul className="flex max-h-[50vh] flex-col gap-0.5 overflow-y-auto rounded-md border p-1">
        {!q && row(null, ownerLabel, "The person whose phone this data came from", <Smartphone className="size-4 text-muted-foreground" />)}
        {visible.map((p) => {
          // Same name can belong to several identities (e.g. an unlinked privacy ID), so show the number too.
          const id =
            (p.numbers ?? 1) > 1 ? `${p.numbers} numbers combined` : p.jid.endsWith("@s.whatsapp.net") ? `+${p.jid.split("@")[0]}` : "privacy ID"
          return row(p.jid, p.name, `${id} · ${p.messages.toLocaleString()} messages`)
        })}
        {visible.length === 0 && <li className="px-3 py-6 text-center text-sm text-muted-foreground">Nobody matches</li>}
      </ul>
      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  )
}
