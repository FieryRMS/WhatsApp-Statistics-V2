"use client"

import * as React from "react"
import { Forward } from "lucide-react"

import { QuotedPreview } from "./quoted-preview"
import { Reactions } from "./reactions"
import { getRenderer } from "./registry"
import { senderColor } from "@/lib/whatsapp/colors"
import type { ChatKind, Message } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

export interface MessageViewProps {
  message: Message
  /** Used to decide whether sender names are shown (groups/broadcasts only). */
  chatKind?: ChatKind
  /** Force the sender name on/off. Defaults to: group chat, incoming, first in a run. */
  showSender?: boolean
  /** First message in a run from the same sender — gets the bubble tail and extra top spacing. */
  isFirstInGroup?: boolean
  highlighted?: boolean
  onQuoteClick?: (messageId: number) => void
  className?: string
}

function Tail({ fromMe }: { fromMe: boolean }) {
  return fromMe ? (
    <svg viewBox="0 0 8 13" width="8" height="13" className="absolute top-0 -right-2 text-bubble-out" aria-hidden>
      <path fill="currentColor" d="M5.188 1H0v11.193l6.467-8.625C7.526 2.156 6.958 1 5.188 1z" />
    </svg>
  ) : (
    <svg viewBox="0 0 8 13" width="8" height="13" className="absolute top-0 -left-2 text-bubble-in" aria-hidden>
      <path fill="currentColor" d="M1.533 3.568 8 12.193V1H2.812C1.042 1 .474 2.156 1.533 3.568z" />
    </svg>
  )
}

function SenderName({ message }: { message: Message }) {
  const s = message.sender
  if (!s) return null
  return (
    <div className="flex min-w-0 items-baseline gap-2 px-1.5 pt-0.5 text-[12.8px] leading-5">
      <span className={cn("truncate font-medium", senderColor(s.jid))}>{s.name}</span>
      {!s.known && s.phone && s.name !== s.phone && <span className="truncate text-xs text-bubble-meta">{s.phone}</span>}
    </div>
  )
}

function ForwardedLabel({ many }: { many: boolean }) {
  return (
    <div className="flex items-center gap-1 px-1.5 pt-0.5 text-xs text-bubble-meta italic">
      <Forward className="size-3.5" />
      {many ? "Forwarded many times" : "Forwarded"}
    </div>
  )
}

/**
 * Renders any message the way WhatsApp would. Standalone — give it a `Message`
 * and it works anywhere (chat view, search results, stats pages…).
 */
export const MessageView = React.memo(function MessageView({
  message,
  chatKind = "individual",
  showSender,
  isFirstInGroup = true,
  highlighted,
  onQuoteClick,
  className,
}: MessageViewProps) {
  const renderer = getRenderer(message.kind)
  const Content = renderer.Content as React.ComponentType<{ message: Message }>

  if (renderer.layout === "center") {
    return (
      <div className={cn("flex justify-center px-[4%] py-1.5", highlighted && "message-highlight", className)}>
        <Content message={message} />
      </div>
    )
  }

  const fromMe = message.fromMe
  const withSender = showSender ?? (chatKind !== "individual" && !fromMe && isFirstInGroup)
  const hasReactions = message.reactions.length > 0

  if (renderer.layout === "bare") {
    return (
      <div
        className={cn(
          "flex flex-col px-[4%] md:px-[6%]",
          fromMe ? "items-end" : "items-start",
          isFirstInGroup ? "pt-2" : "pt-0.5",
          hasReactions && "pb-4",
          highlighted && "message-highlight",
          className
        )}
      >
        {withSender && (
          <div className="mb-1 rounded-md bg-bubble-in px-1 shadow-sm">
            <SenderName message={message} />
          </div>
        )}
        <div className="relative">
          <Content message={message} />
          {hasReactions && <Reactions reactions={message.reactions} className={cn("absolute -bottom-4", fromMe ? "right-2" : "left-2")} />}
        </div>
      </div>
    )
  }

  return (
    <div
      className={cn(
        "flex px-[4%] md:px-[6%]",
        fromMe ? "justify-end" : "justify-start",
        isFirstInGroup ? "pt-2" : "pt-0.5",
        hasReactions && "pb-4",
        highlighted && "message-highlight",
        className
      )}
    >
      <div
        className={cn(
          "relative max-w-[min(88%,560px)] min-w-[90px] rounded-lg p-0.5 text-foreground shadow-[0_1px_0.5px_rgba(11,20,26,0.13)]",
          fromMe ? "bg-bubble-out" : "bg-bubble-in",
          isFirstInGroup && (fromMe ? "rounded-tr-none" : "rounded-tl-none")
        )}
      >
        {isFirstInGroup && <Tail fromMe={fromMe} />}
        {withSender && <SenderName message={message} />}
        {message.forwarded && <ForwardedLabel many={message.frequentlyForwarded} />}
        {message.quoted && (
          <div className="px-0.5 pt-0.5 pb-0.5">
            <QuotedPreview quoted={message.quoted} onClick={onQuoteClick} />
          </div>
        )}
        <Content message={message} />
        {hasReactions && <Reactions reactions={message.reactions} className={cn("absolute -bottom-4", fromMe ? "right-2" : "left-2")} />}
      </div>
    </div>
  )
})
