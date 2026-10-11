import fs from "node:fs"
import { Readable } from "node:stream"
import type { NextRequest } from "next/server"

import { mimeFromPath } from "@/lib/whatsapp/media"
import { getMediaFile } from "@/lib/whatsapp/queries"

/** Streams the original media file, with Range support for audio/video seeking. */
export async function GET(req: NextRequest, ctx: RouteContext<"/api/messages/[id]/media">) {
  const file = getMediaFile(Number((await ctx.params).id))
  if (!file || !fs.existsSync(file.path)) return new Response(null, { status: 404 })

  const { size } = fs.statSync(file.path)
  const headers = new Headers({
    "Content-Type": mimeFromPath(file.path, file.mime),
    "Accept-Ranges": "bytes",
    "Cache-Control": "public, max-age=31536000, immutable",
  })
  if (req.nextUrl.searchParams.has("download")) {
    headers.set("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(file.path.split("/").pop()!)}`)
  }

  const range = req.headers.get("range")?.match(/bytes=(\d*)-(\d*)/)
  if (range) {
    const start = range[1] ? Number(range[1]) : 0
    const end = range[2] ? Math.min(Number(range[2]), size - 1) : size - 1
    if (start >= size || start > end) {
      return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${size}` } })
    }
    headers.set("Content-Range", `bytes ${start}-${end}/${size}`)
    headers.set("Content-Length", String(end - start + 1))
    const stream = Readable.toWeb(fs.createReadStream(file.path, { start, end })) as ReadableStream
    return new Response(stream, { status: 206, headers })
  }

  headers.set("Content-Length", String(size))
  return new Response(Readable.toWeb(fs.createReadStream(file.path)) as ReadableStream, { headers })
}
