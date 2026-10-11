"use client"

import * as React from "react"
import { Search, X } from "lucide-react"

import { FormStatus } from "./form-status"
import { ChatAvatar } from "@/components/chat/chat-avatar"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import {
  saveGrantsAction,
  type AdminFormState,
  type GrantInput,
} from "@/lib/admin/actions"
import type { ChatKind } from "@/lib/whatsapp/types"
import { EmojiText } from "@/components/emoji/emoji-text"

interface ChatOption {
  jid: string
  name: string
  kind: ChatKind
  messageCount: number
}

export function GrantsEditor({
  userId,
  chats,
  initial,
}: {
  userId: number
  chats: ChatOption[]
  initial: GrantInput[]
}) {
  const [grants, setGrants] = React.useState<GrantInput[]>(initial)
  const [query, setQuery] = React.useState("")
  const [state, setState] = React.useState<AdminFormState>({})
  const [pending, start] = React.useTransition()

  const byJid = React.useMemo(
    () => new Map(chats.map((c) => [c.jid, c])),
    [chats]
  )
  const granted = new Set(grants.map((g) => g.chatJid))
  const q = query.trim().toLowerCase()
  const visible = chats
    .filter((c) => !q || c.name.toLowerCase().includes(q) || c.jid.includes(q))
    .slice(0, 300)
  const key = (gs: GrantInput[]) =>
    JSON.stringify([...gs].sort((x, y) => x.chatJid.localeCompare(y.chatJid)))
  const dirty = key(grants) !== key(initial)

  const toggle = (jid: string, on: boolean) =>
    setGrants((gs) =>
      on
        ? [...gs, { chatJid: jid, from: "", to: "" }]
        : gs.filter((g) => g.chatJid !== jid)
    )
  const patch = (jid: string, p: Partial<GrantInput>) =>
    setGrants((gs) => gs.map((g) => (g.chatJid === jid ? { ...g, ...p } : g)))

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <div className="flex min-h-0 flex-col gap-2">
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search chats"
            className="pl-8"
          />
        </div>
        <ul className="max-h-[28rem] overflow-y-auto rounded-md border">
          {visible.map((c) => (
            <li key={c.jid}>
              <label className="flex cursor-pointer items-center gap-3 px-3 py-2 hover:bg-muted/60">
                <Checkbox
                  checked={granted.has(c.jid)}
                  onCheckedChange={(v) => toggle(c.jid, v === true)}
                />
                <ChatAvatar
                  jid={c.jid}
                  name={c.name}
                  kind={c.kind}
                  className="size-8"
                />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm">
                    <EmojiText text={c.name} />
                  </span>
                  <span className="block text-xs text-muted-foreground">
                    {c.messageCount.toLocaleString()} messages
                  </span>
                </span>
              </label>
            </li>
          ))}
          {visible.length === 0 && (
            <li className="px-3 py-6 text-center text-sm text-muted-foreground">
              No chats match
            </li>
          )}
        </ul>
      </div>

      <div className="flex flex-col gap-3">
        <div className="text-sm font-medium">
          Granted ({grants.length})
          <span className="ml-2 font-normal text-muted-foreground">
            Leave dates empty for the whole chat.
          </span>
        </div>
        <ul className="flex max-h-[28rem] flex-col gap-2 overflow-y-auto">
          {grants.map((g) => {
            const chat = byJid.get(g.chatJid)
            return (
              <li key={g.chatJid} className="rounded-md border p-3">
                <div className="flex items-center gap-2">
                  <span className="min-w-0 flex-1 truncate text-sm font-medium">
                    <EmojiText text={chat?.name ?? g.chatJid} />
                  </span>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    onClick={() => toggle(g.chatJid, false)}
                    aria-label="Remove"
                  >
                    <X />
                  </Button>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-2">
                  <label className="grid gap-1 text-xs text-muted-foreground">
                    From
                    <Input
                      type="date"
                      value={g.from}
                      max={g.to || undefined}
                      onChange={(e) =>
                        patch(g.chatJid, { from: e.target.value })
                      }
                    />
                  </label>
                  <label className="grid gap-1 text-xs text-muted-foreground">
                    To (inclusive)
                    <Input
                      type="date"
                      value={g.to}
                      min={g.from || undefined}
                      onChange={(e) => patch(g.chatJid, { to: e.target.value })}
                    />
                  </label>
                </div>
              </li>
            )
          })}
          {grants.length === 0 && (
            <li className="rounded-md border border-dashed px-3 py-6 text-center text-sm text-muted-foreground">
              No access yet
            </li>
          )}
        </ul>
        <FormStatus state={state} />
        <div>
          <Button
            disabled={pending || !dirty}
            onClick={() =>
              start(async () =>
                setState(await saveGrantsAction(userId, grants))
              )
            }
          >
            {pending ? "Saving and building…" : "Save access"}
          </Button>
        </div>
      </div>
    </div>
  )
}
