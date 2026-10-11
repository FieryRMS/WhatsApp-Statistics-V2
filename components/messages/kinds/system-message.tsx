import { Lock, Timer } from "lucide-react"

import { formatSystemEvent } from "@/lib/whatsapp/format"
import type { DisappearingNoticeMessage, SystemMessage } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

export function SystemChip({ children, className, title }: { children: React.ReactNode; className?: string; title?: string }) {
  return (
    <div
      title={title}
      className={cn(
        "mx-auto max-w-[min(90%,560px)] rounded-lg bg-system-chip px-3 py-1.5 text-center text-[12.5px] leading-snug text-bubble-meta shadow-sm",
        className
      )}
    >
      {children}
    </div>
  )
}

export function SystemMessageContent({ message }: { message: SystemMessage }) {
  const e2e = message.event.type === "e2e_encrypted"
  return (
    <SystemChip
      title={new Date(message.timestamp).toLocaleString()}
      className={cn(e2e && "bg-[#ffeecd] text-[#54656f] dark:bg-[#182229] dark:text-[#ffd279]")}
    >
      {e2e && <Lock className="mr-1 inline size-3 -translate-y-px" />}
      {formatSystemEvent(message.event)}
    </SystemChip>
  )
}

export function DisappearingNoticeContent({ message }: { message: DisappearingNoticeMessage }) {
  return (
    <SystemChip title={new Date(message.timestamp).toLocaleString()}>
      <Timer className="mr-1 inline size-3.5 -translate-y-px" />
      {message.fromMe ? "You changed" : `${message.sender?.name ?? "Someone"} changed`} the disappearing messages setting
    </SystemChip>
  )
}
