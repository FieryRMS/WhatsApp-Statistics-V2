import { ChatSidebar } from "@/components/chat/chat-sidebar"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { requireSession, withData } from "@/lib/access/dal"
import { listChats } from "@/lib/whatsapp/queries"

/** Everything behind sign-in. Each account sees only the chats in its data source. */
export default async function ExplorerLayout({ children }: { children: React.ReactNode }) {
  const session = await requireSession()
  const chats = await withData(() => listChats())
  return (
    <SidebarProvider style={{ "--sidebar-width": "26rem" } as React.CSSProperties}>
      <ChatSidebar chats={chats} user={{ username: session.user.username, role: session.user.role }} />
      <SidebarInset className="min-w-0">{children}</SidebarInset>
    </SidebarProvider>
  )
}
