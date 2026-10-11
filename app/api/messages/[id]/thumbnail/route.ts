import type { NextRequest } from "next/server"

import { apiWithData } from "@/lib/access/dal"
import { sniffImageType } from "@/lib/whatsapp/decoders"
import { getThumbnail } from "@/lib/whatsapp/queries"

/** GET /api/messages/:id/thumbnail[?quoted=1] — the message's thumbnail, or that of the message it quotes. */
export async function GET(req: NextRequest, ctx: RouteContext<"/api/messages/[id]/thumbnail">) {
  const id = Number((await ctx.params).id)
  const quoted = req.nextUrl.searchParams.has("quoted")
  return apiWithData(() => {
    const thumb = Number.isSafeInteger(id) ? getThumbnail(id, { quoted }) : null
    if (!thumb) return new Response(null, { status: 404 })
    return new Response(Buffer.from(thumb), {
      headers: {
        "Content-Type": sniffImageType(thumb),
        // Per-account data: browsers may cache it, shared caches must not.
        "Cache-Control": "private, max-age=86400",
      },
    })
  })
}
