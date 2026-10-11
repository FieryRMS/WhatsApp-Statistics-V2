import type { NextRequest } from "next/server"

import { apiWithData } from "@/lib/access/dal"
import { decodeJidParam } from "@/lib/whatsapp/anchor"
import { getChat } from "@/lib/whatsapp/queries"

export async function GET(_req: NextRequest, ctx: RouteContext<"/api/chats/[jid]">) {
  const jid = decodeJidParam((await ctx.params).jid)
  return apiWithData(() => getChat(jid))
}
