import type { NextRequest } from "next/server"

import { apiWithData } from "@/lib/access/dal"
import { getMessageById } from "@/lib/whatsapp/queries"

export async function GET(_req: NextRequest, ctx: RouteContext<"/api/messages/[id]">) {
  const id = Number((await ctx.params).id)
  return apiWithData(() => (Number.isSafeInteger(id) ? getMessageById(id) : null))
}
