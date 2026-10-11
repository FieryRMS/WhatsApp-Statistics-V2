import type { NextRequest } from "next/server"

import { anchorFromParams, decodeJidParam } from "@/lib/whatsapp/anchor"
import { getMessages } from "@/lib/whatsapp/queries"

const int = (v: string | null) => (v && /^\d+$/.test(v) ? Number(v) : undefined)

/**
 * GET /api/chats/:jid/messages
 *   ?before=<id>            older page
 *   ?after=<id>             newer page
 *   ?message=<id> | ?at=<ms|ISO>   page centred on a message / moment
 *   ?limit=<n>              page size (default 60, max 200)
 */
export async function GET(req: NextRequest, ctx: RouteContext<"/api/chats/[jid]/messages">) {
  const jid = decodeJidParam((await ctx.params).jid)
  const sp = req.nextUrl.searchParams
  const page = getMessages(jid, {
    before: int(sp.get("before")),
    after: int(sp.get("after")),
    anchor: anchorFromParams(sp),
    limit: int(sp.get("limit")),
  })
  return Response.json(page)
}
