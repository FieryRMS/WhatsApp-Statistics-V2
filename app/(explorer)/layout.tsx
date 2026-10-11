import { ChatSidebar } from "@/components/chat/chat-sidebar"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { requireSession, withData } from "@/lib/access/dal"
import { ownerName, participant } from "@/lib/whatsapp/contacts"
import { listChats } from "@/lib/whatsapp/queries"
import { DEFAULT_OWNER_LABEL } from "@/lib/whatsapp/viewer"

/** Everything behind sign-in. Each account sees only the chats in its data source. */
export default async function ExplorerLayout({ children }: { children: React.ReactNode }) {
  const session = await requireSession()
  const { chats, viewingAs } = await withData(() => ({
    chats: listChats(),
    viewingAs: session.viewerJid ? participant(session.viewerJid).name : (ownerName() ?? DEFAULT_OWNER_LABEL),
  }))
  return (
    <SidebarProvider style={{ "--sidebar-width": "26rem" } as React.CSSProperties}>
      <ChatSidebar chats={chats} user={{ username: session.user.username, role: session.user.role, viewingAs }} />
      <SidebarInset className="min-w-0">{children}</SidebarInset>
    </SidebarProvider>
  )
}
