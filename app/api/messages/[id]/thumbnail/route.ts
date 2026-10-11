import type { NextRequest } from "next/server"

import { sniffImageType } from "@/lib/whatsapp/decoders"
import { getThumbnail } from "@/lib/whatsapp/queries"

/** GET /api/messages/:id/thumbnail[?quoted=1] — the message's thumbnail, or that of the message it quotes. */
export async function GET(req: NextRequest, ctx: RouteContext<"/api/messages/[id]/thumbnail">) {
  const thumb = getThumbnail(Number((await ctx.params).id), { quoted: req.nextUrl.searchParams.has("quoted") })
  if (!thumb) return new Response(null, { status: 404 })
  return new Response(Buffer.from(thumb), {
    headers: {
      "Content-Type": sniffImageType(thumb),
      "Cache-Control": "public, max-age=31536000, immutable",
    },
  })
}
