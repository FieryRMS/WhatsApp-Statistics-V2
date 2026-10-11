import { notFound } from "next/navigation"

import { ChatView } from "@/components/chat/chat-view"
import { anchorFromParams, decodeJidParam } from "@/lib/whatsapp/anchor"
import { getChat } from "@/lib/whatsapp/queries"

/**
 * /chats/<jid>                 → latest messages
 * /chats/<jid>?at=<ms|ISO>     → messages around that moment
 * /chats/<jid>?message=<id>    → messages around that message
 */
export default async function ChatPage(props: PageProps<"/chats/[jid]">) {
  const jid = decodeJidParam((await props.params).jid)
  const chat = getChat(jid)
  if (!chat) notFound()
  const anchor = anchorFromParams(await props.searchParams)
  return <ChatView key={jid} jid={jid} chat={chat} anchor={anchor} />
}

export async function generateMetadata(props: PageProps<"/chats/[jid]">) {
  const chat = getChat(decodeJidParam((await props.params).jid))
  return { title: chat ? `${chat.name} · WhatsApp Stats` : "Chat not found" }
}
