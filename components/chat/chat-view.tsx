"use client"

import * as React from "react"
import { useRouter } from "next/navigation"

import { ChatHeader } from "./chat-header"
import { MessageList } from "./message-list"
import {
  useMessageStream,
  type MessageStreamOptions,
} from "@/hooks/use-message-stream"
import { chatHref, toAnchor, type ChatLocation } from "@/lib/whatsapp/anchor"
import { fetchChat } from "@/lib/whatsapp/client"
import { chatKind } from "@/lib/whatsapp/jid"
import type { ChatAnchor, ChatDetails } from "@/lib/whatsapp/types"

export interface ChatViewProps {
  jid: string
  /** Pre-fetched chat details (the page passes these from the server). Fetched if omitted. */
  chat?: ChatDetails
  /** Where to open. Changing it jumps there. */
  anchor?: ChatAnchor
  /** Keep the URL (?at= / ?message=) in sync with jumps. Turn off when embedding. */
  syncUrl?: boolean
  hideHeader?: boolean
  streamOptions?: MessageStreamOptions
  className?: string
}

const LATEST: ChatAnchor = { type: "latest" }

/**
 * A full chat: header + streamed message list. Self-contained, so it can be
 * embedded anywhere, e.g. `<ChatView jid={jid} anchor={{ type: "time", at }} syncUrl={false} />`.
 */
export function ChatView({
  jid,
  chat: initialChat,
  anchor = LATEST,
  syncUrl = true,
  hideHeader,
  streamOptions,
  className,
}: ChatViewProps) {
  const router = useRouter()
  const [fetchedChat, setFetchedChat] = React.useState<ChatDetails | null>(null)
  const chat = initialChat ?? fetchedChat
  const stream = useMessageStream(jid, anchor, streamOptions)

  React.useEffect(() => {
    if (initialChat) return
    const ctrl = new AbortController()
    fetchChat(jid, ctrl.signal).then(setFetchedChat, () => {})
    return () => ctrl.abort()
  }, [jid, initialChat])

  const { jumpTo } = stream
  const go = React.useCallback(
    (loc: ChatLocation) => {
      jumpTo(toAnchor(loc))
      if (syncUrl) router.replace(chatHref(jid, loc), { scroll: false })
    },
    [jumpTo, syncUrl, router, jid]
  )

  return (
    <div className={className ?? "flex h-svh min-h-0 flex-col"}>
      {!hideHeader && chat && <ChatHeader chat={chat} onJump={go} />}
      <MessageList
        stream={stream}
        chatKind={chat?.kind ?? chatKind(jid)}
        onQuoteClick={(id) => go({ messageId: id })}
        onJumpToLatest={() => go({})}
      />
    </div>
  )
}
