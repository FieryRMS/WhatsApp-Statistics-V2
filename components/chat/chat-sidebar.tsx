"use client"

import * as React from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { Archive, MessageSquareText, Search } from "lucide-react"

import { ChatAvatar } from "./chat-avatar"
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarInput,
  SidebarMenu,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import { chatHref, decodeJidParam } from "@/lib/whatsapp/anchor"
import { formatListTimestamp } from "@/lib/whatsapp/format"
import type { ChatSummary } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

type Filter = "all" | "groups" | "direct" | "archived"

const FILTERS: { id: Filter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "direct", label: "Direct" },
  { id: "groups", label: "Groups" },
  { id: "archived", label: "Archived" },
]

function matches(chat: ChatSummary, filter: Filter, query: string) {
  if (filter === "groups" && chat.kind !== "group") return false
  if (filter === "direct" && chat.kind !== "individual") return false
  if (filter === "archived" ? !chat.archived : chat.archived && filter !== "all") return false
  if (!query) return true
  const q = query.toLowerCase()
  return chat.name.toLowerCase().includes(q) || chat.jid.includes(q)
}

const ChatRow = React.memo(function ChatRow({ chat, active }: { chat: ChatSummary; active: boolean }) {
  const last = chat.lastMessage
  return (
    <SidebarMenuItem>
      <Link
        href={chatHref(chat.jid)}
        className={cn(
          "flex items-center gap-3 rounded-md px-2 py-2 outline-none hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-sidebar-ring",
          active && "bg-sidebar-accent"
        )}
      >
        <ChatAvatar jid={chat.jid} name={chat.name} kind={chat.kind} className="size-12" />
        <div className="min-w-0 flex-1 border-b border-sidebar-border/60 pb-2">
          <div className="flex items-baseline gap-2">
            <span className="min-w-0 flex-1 truncate text-[15px] font-medium">{chat.name}</span>
            {last && (
              <time className="shrink-0 text-xs text-muted-foreground" suppressHydrationWarning>
                {formatListTimestamp(last.timestamp)}
              </time>
            )}
          </div>
          <div className="flex items-center gap-1 text-[13px] text-muted-foreground">
            {chat.archived && <Archive className="size-3.5 shrink-0" />}
            <span className="truncate">
              {last ? (
                <>
                  {last.fromMe ? "You: " : last.senderName ? `${last.senderName.replace(/^~/, "")}: ` : ""}
                  {last.preview}
                </>
              ) : (
                "No messages"
              )}
            </span>
          </div>
        </div>
      </Link>
    </SidebarMenuItem>
  )
})

export function ChatSidebar({ chats }: { chats: ChatSummary[] }) {
  const pathname = usePathname()
  const [query, setQuery] = React.useState("")
  const [filter, setFilter] = React.useState<Filter>("all")
  const deferredQuery = React.useDeferredValue(query)

  const activeJid = pathname.startsWith("/chats/") ? decodeJidParam(pathname.slice("/chats/".length)) : null
  const visible = React.useMemo(
    () => chats.filter((c) => matches(c, filter, deferredQuery.trim())),
    [chats, filter, deferredQuery]
  )

  return (
    <Sidebar>
      <SidebarHeader className="gap-3 px-3 pt-4">
        <div className="flex items-center gap-2 px-1">
          <MessageSquareText className="size-5 text-wa-accent" />
          <span className="text-lg font-semibold">Chats</span>
          <span className="ml-auto text-xs text-muted-foreground">{chats.length}</span>
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
          <SidebarInput
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search chats"
            className="h-9 pl-8"
          />
        </div>
        <div className="flex gap-1.5">
          {FILTERS.map((f) => (
            <button
              key={f.id}
              type="button"
              onClick={() => setFilter(f.id)}
              className={cn(
                "rounded-full px-3 py-1 text-[13px] transition-colors",
                filter === f.id
                  ? "bg-wa-accent/15 font-medium text-wa-accent"
                  : "bg-sidebar-accent text-muted-foreground hover:text-foreground"
              )}
            >
              {f.label}
            </button>
          ))}
        </div>
      </SidebarHeader>
      <SidebarContent className="px-1.5">
        <SidebarMenu className="gap-0">
          {visible.map((chat) => (
            <ChatRow key={chat.jid} chat={chat} active={chat.jid === activeJid} />
          ))}
          {visible.length === 0 && <p className="px-3 py-6 text-center text-sm text-muted-foreground">No chats found</p>}
        </SidebarMenu>
      </SidebarContent>
    </Sidebar>
  )
}
