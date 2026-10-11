import type { NextRequest } from "next/server"

import { getMessageById } from "@/lib/whatsapp/queries"

export async function GET(_req: NextRequest, ctx: RouteContext<"/api/messages/[id]">) {
  const message = getMessageById(Number((await ctx.params).id))
  if (!message) return Response.json({ error: "Message not found" }, { status: 404 })
  return Response.json(message)
}
