"use client"

import * as React from "react"
import { ArrowDownToLine, ArrowUpToLine, CalendarDays } from "lucide-react"

import { ChatAvatar } from "./chat-avatar"
import { Button } from "@/components/ui/button"
import { Calendar } from "@/components/ui/calendar"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import type { ChatLocation } from "@/lib/whatsapp/anchor"
import type { ChatDetails } from "@/lib/whatsapp/types"

const KIND_LABEL: Record<ChatDetails["kind"], string> = {
  individual: "Chat",
  group: "Group",
  broadcast: "Broadcast list",
  status: "Status",
  channel: "Channel",
  bot: "Bot",
  other: "Chat",
}

function monthYear(ts: number) {
  return new Date(ts).toLocaleDateString(undefined, { month: "short", year: "numeric" })
}

function JumpToDate({ chat, onJump }: { chat: ChatDetails; onJump: (loc: ChatLocation) => void }) {
  const [open, setOpen] = React.useState(false)
  const first = chat.firstMessageAt ? new Date(chat.firstMessageAt) : undefined
  const last = chat.lastMessageAt ? new Date(chat.lastMessageAt) : undefined
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <Tooltip>
        <TooltipTrigger asChild>
          <PopoverTrigger asChild>
            <Button variant="ghost" size="icon" aria-label="Jump to date">
              <CalendarDays />
            </Button>
          </PopoverTrigger>
        </TooltipTrigger>
        <TooltipContent>Jump to date</TooltipContent>
      </Tooltip>
      <PopoverContent align="end" className="w-auto p-0">
        <Calendar
          mode="single"
          captionLayout="dropdown"
          defaultMonth={last}
          startMonth={first}
          endMonth={last}
          disabled={first && last ? [{ before: first }, { after: last }] : undefined}
          onSelect={(date) => {
            if (!date) return
            const d = new Date(date)
            d.setHours(0, 0, 0, 0)
            onJump({ at: d })
            setOpen(false)
          }}
        />
      </PopoverContent>
    </Popover>
  )
}

export function ChatHeader({ chat, onJump }: { chat: ChatDetails; onJump: (loc: ChatLocation) => void }) {
  const subtitle = [
    KIND_LABEL[chat.kind],
    `${chat.messageCount.toLocaleString()} messages`,
    chat.firstMessageAt && chat.lastMessageAt
      ? `${monthYear(chat.firstMessageAt)} – ${monthYear(chat.lastMessageAt)}`
      : null,
  ]
    .filter(Boolean)
    .join(" · ")

  return (
    <header className="flex h-16 shrink-0 items-center gap-3 border-b bg-sidebar px-3 md:px-4">
      <SidebarTrigger className="md:hidden" />
      <ChatAvatar jid={chat.jid} name={chat.name} kind={chat.kind} />
      <div className="min-w-0 flex-1">
        <h1 className="truncate text-base font-medium">{chat.name}</h1>
        <p className="truncate text-xs text-muted-foreground" suppressHydrationWarning>
          {subtitle}
        </p>
      </div>
      <div className="flex items-center gap-0.5">
        <JumpToDate chat={chat} onJump={onJump} />
        {chat.firstMessageAt != null && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="ghost" size="icon" aria-label="Jump to first message" onClick={() => onJump({ at: 0 })}>
                <ArrowUpToLine />
              </Button>
            </TooltipTrigger>
            <TooltipContent>First message</TooltipContent>
          </Tooltip>
        )}
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" aria-label="Jump to latest message" onClick={() => onJump({})}>
              <ArrowDownToLine />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Latest message</TooltipContent>
        </Tooltip>
      </div>
    </header>
  )
}
