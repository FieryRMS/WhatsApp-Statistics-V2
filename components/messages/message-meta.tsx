import { Check, CheckCheck, Clock3, Star } from "lucide-react"

import { formatTime } from "@/lib/whatsapp/format"
import type { Message, MessageStatus } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

export function StatusTicks({
  status,
  className,
}: {
  status: MessageStatus
  className?: string
}) {
  switch (status) {
    case "pending":
      return (
        <Clock3 className={cn("size-3.5", className)} aria-label="Pending" />
      )
    case "sent":
      return <Check className={cn("size-4", className)} aria-label="Sent" />
    case "delivered":
      return (
        <CheckCheck
          className={cn("size-4", className)}
          aria-label="Delivered"
        />
      )
    case "read":
    case "played":
      return (
        <CheckCheck
          className={cn("size-4 text-tick-read", className)}
          aria-label="Read"
        />
      )
    default:
      return null
  }
}

/**
 * Time + star + ticks shown in the bottom-right corner of a bubble.
 * `overlay` is for media without a caption (white on a dark gradient).
 */
export function MessageMeta({
  message,
  variant = "inline",
  className,
}: {
  message: Message
  variant?: "inline" | "overlay" | "chip"
  className?: string
}) {
  return (
    <span
      className={cn(
        "pointer-events-none inline-flex items-center gap-1 text-[11px] leading-none whitespace-nowrap select-none",
        variant === "inline" && "text-bubble-meta",
        variant === "overlay" &&
          "rounded-full bg-black/35 px-1.5 py-1 text-white",
        variant === "chip" &&
          "rounded-full bg-bubble-in/90 px-1.5 py-1 text-bubble-meta shadow-sm",
        className
      )}
    >
      {message.edited && <span>Edited</span>}
      {message.starred && (
        <Star className="size-3 fill-current" aria-label="Starred" />
      )}
      <time
        dateTime={new Date(message.timestamp).toISOString()}
        title={new Date(message.timestamp).toLocaleString()}
      >
        {formatTime(message.timestamp)}
      </time>
      {message.fromMe && (
        <StatusTicks
          status={message.status}
          className={variant === "overlay" ? "text-white" : undefined}
        />
      )}
    </span>
  )
}

/** Invisible inline spacer so text never runs under the absolutely-positioned meta. */
export function MetaSpacer({ message }: { message: Message }) {
  return (
    <span
      aria-hidden
      className={cn(
        "inline-block h-3 align-baseline",
        message.fromMe ? "w-[74px]" : "w-[52px]",
        message.starred && "ml-4",
        message.edited && "ml-10"
      )}
    />
  )
}
