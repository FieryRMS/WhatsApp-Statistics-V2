import type { NextRequest } from "next/server"

import { decodeJidParam } from "@/lib/whatsapp/anchor"
import { getChat } from "@/lib/whatsapp/queries"

export async function GET(_req: NextRequest, ctx: RouteContext<"/api/chats/[jid]">) {
  const chat = getChat(decodeJidParam((await ctx.params).jid))
  if (!chat) return Response.json({ error: "Chat not found" }, { status: 404 })
  return Response.json(chat)
}
