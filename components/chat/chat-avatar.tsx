import { Bot, CircleDashed, Megaphone, Radio, UserRound, Users } from "lucide-react"

import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { avatarColor, initials } from "@/lib/whatsapp/colors"
import type { ChatKind } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

const KIND_ICONS: Partial<Record<ChatKind, typeof Users>> = {
  group: Users,
  broadcast: Megaphone,
  status: CircleDashed,
  channel: Radio,
  bot: Bot,
}

/** Initials avatar (the DB has no profile pictures). Unnamed numbers get a generic person icon. */
export function ChatAvatar({
  jid,
  name,
  kind,
  className,
}: {
  jid: string
  name: string
  kind: ChatKind
  className?: string
}) {
  const label = initials(name)
  const Icon = KIND_ICONS[kind] ?? (label === "#" ? UserRound : null)
  return (
    <Avatar className={cn("size-10", className)}>
      <AvatarFallback
        className={cn("text-sm font-medium text-white", Icon ? "bg-[#8696a0] dark:bg-[#6a7175]" : avatarColor(jid))}
      >
        {Icon ? <Icon className="size-5" /> : label}
      </AvatarFallback>
    </Avatar>
  )
}
