"use client"

import * as React from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import {
  Archive,
  BarChart3,
  BookUser,
  LogOut,
  MessageSquareText,
  Search,
  Settings,
  Shield,
} from "lucide-react"

import { ChatAvatar } from "./chat-avatar"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarInput,
  SidebarMenu,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import { Button } from "@/components/ui/button"
import { logoutAction } from "@/lib/auth/actions"
import { chatHref, decodeJidParam } from "@/lib/whatsapp/anchor"
import { formatListTimestamp } from "@/lib/whatsapp/format"
import type { ChatSummary } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"
import { EmojiText } from "@/components/emoji/emoji-text"

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
  if (
    filter === "archived" ? !chat.archived : chat.archived && filter !== "all"
  )
    return false
  if (!query) return true
  const q = query.toLowerCase()
  return chat.name.toLowerCase().includes(q) || chat.jid.includes(q)
}

const ChatRow = React.memo(function ChatRow({
  chat,
  active,
}: {
  chat: ChatSummary
  active: boolean
}) {
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
        <ChatAvatar
          jid={chat.jid}
          name={chat.name}
          kind={chat.kind}
          className="size-12"
        />
        <div className="min-w-0 flex-1 border-b border-sidebar-border/60 pb-2">
          <div className="flex items-baseline gap-2">
            <span className="min-w-0 flex-1 truncate text-[15px] font-medium">
              <EmojiText text={chat.name} />
            </span>
            {last && (
              <time
                className="shrink-0 text-xs text-muted-foreground"
                suppressHydrationWarning
              >
                {formatListTimestamp(last.timestamp)}
              </time>
            )}
          </div>
          <div className="flex items-center gap-1 text-[13px] text-muted-foreground">
            {chat.archived && <Archive className="size-3.5 shrink-0" />}
            <span className="truncate">
              {last ? (
                <>
                  <EmojiText
                    text={
                      last.fromMe
                        ? "You: "
                        : last.senderName
                          ? `${last.senderName.replace(/^~/, "")}: `
                          : ""
                    }
                  />
                  <EmojiText text={last.preview} />
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

type FooterUser = {
  username: string
  role: "admin" | "user"
  viewingAs: string
}

function UserFooter({ user }: { user: FooterUser }) {
  return (
    <SidebarFooter className="flex-row items-center gap-1 border-t px-3 py-2.5">
      <Link
        href="/settings"
        className="min-w-0 flex-1 rounded-md px-1 py-0.5 hover:bg-sidebar-accent"
        title="Change who's viewing"
      >
        <div className="truncate text-sm font-medium">{user.viewingAs}</div>
        <div className="truncate text-xs text-muted-foreground">
          {user.username} · {user.role === "admin" ? "admin" : "restricted"}
        </div>
      </Link>
      <Button
        asChild
        variant="ghost"
        size="icon"
        title="Settings"
        aria-label="Settings"
      >
        <Link href="/settings">
          <Settings />
        </Link>
      </Button>
      <Button
        asChild
        variant="ghost"
        size="icon"
        title="Statistics"
        aria-label="Statistics"
      >
        <Link href="/stats">
          <BarChart3 />
        </Link>
      </Button>
      <Button
        asChild
        variant="ghost"
        size="icon"
        title="Contact names"
        aria-label="Contact names"
      >
        <Link href="/names">
          <BookUser />
        </Link>
      </Button>
      {user.role === "admin" && (
        <Button asChild variant="ghost" size="sm">
          <Link href="/admin">
            <Shield /> Admin
          </Link>
        </Button>
      )}
      <form action={logoutAction}>
        <Button
          type="submit"
          variant="ghost"
          size="icon"
          aria-label="Sign out"
          title="Sign out"
        >
          <LogOut />
        </Button>
      </form>
    </SidebarFooter>
  )
}

export function ChatSidebar({
  chats,
  user,
}: {
  chats: ChatSummary[]
  user: FooterUser
}) {
  const pathname = usePathname()
  const [query, setQuery] = React.useState("")
  const [filter, setFilter] = React.useState<Filter>("all")
  const deferredQuery = React.useDeferredValue(query)

  const activeJid = pathname.startsWith("/chats/")
    ? decodeJidParam(pathname.slice("/chats/".length))
    : null
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
          <span className="ml-auto text-xs text-muted-foreground">
            {chats.length}
          </span>
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
            <ChatRow
              key={chat.jid}
              chat={chat}
              active={chat.jid === activeJid}
            />
          ))}
          {visible.length === 0 && (
            <p className="px-3 py-6 text-center text-sm text-muted-foreground">
              {chats.length === 0
                ? "No chats have been shared with this account yet."
                : "No chats found"}
            </p>
          )}
        </SidebarMenu>
      </SidebarContent>
      <UserFooter user={user} />
    </Sidebar>
  )
}
