import { Lock, MessageSquareText } from "lucide-react"

import { SidebarTrigger } from "@/components/ui/sidebar"

export default function Home() {
  return (
    <div className="relative flex h-svh flex-col items-center justify-center gap-4 border-b-[6px] border-wa-accent bg-chat-bg px-6 text-center">
      <SidebarTrigger className="absolute top-3 left-3 md:hidden" />
      <MessageSquareText className="size-16 text-wa-accent" strokeWidth={1.25} />
      <h1 className="text-3xl font-light">WhatsApp Stats</h1>
      <p className="max-w-md text-sm text-muted-foreground">
        Pick a chat from the sidebar to browse its history. Messages load as you scroll, and you can jump to any date
        from the chat header.
      </p>
      <p className="absolute bottom-8 flex items-center gap-1.5 text-xs text-muted-foreground">
        <Lock className="size-3" /> Read-only. Your database never leaves this machine.
      </p>
    </div>
  )
}
