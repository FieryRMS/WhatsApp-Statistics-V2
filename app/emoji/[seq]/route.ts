import fs from "node:fs"
import path from "node:path"

/**
 * GET /emoji/<codepoints>.png — WhatsApp emoji artwork from the
 * vendor/whatsapp-emoji-linux submodule (png/160, Noto naming:
 * emoji_u1f602.png, emoji_u1f468_200d_1f469_200d_1f467.png).
 *
 * Public and immutable: these are the same static images for everyone and
 * contain no user data.
 */

const DIR = path.join(
  /* turbopackIgnore: true */ process.cwd(),
  "vendor",
  "whatsapp-emoji-linux",
  "png",
  "160"
)
const SEQ_RE = /^([0-9a-f]{2,6})(-[0-9a-f]{2,6}){0,15}\.png$/

const globalForEmoji = globalThis as unknown as { __waEmojiFiles?: Set<string> }
function files(): Set<string> {
  if (!globalForEmoji.__waEmojiFiles) {
    try {
      globalForEmoji.__waEmojiFiles = new Set(
        fs.readdirSync(/* turbopackIgnore: true */ DIR)
      )
    } catch {
      globalForEmoji.__waEmojiFiles = new Set()
    }
  }
  return globalForEmoji.__waEmojiFiles
}

/** Exact sequence first, then without variation selectors (Noto drops FE0F in most names). */
function resolve(seq: string): string | null {
  const cps = seq.split("-")
  const candidates = [cps, cps.filter((c) => c !== "fe0f")]
  for (const c of candidates) {
    const name = `emoji_u${c.map((x) => x.padStart(4, "0")).join("_")}.png`
    if (files().has(name)) return name
  }
  return null
}

export async function GET(_req: Request, ctx: RouteContext<"/emoji/[seq]">) {
  const { seq } = await ctx.params
  if (!SEQ_RE.test(seq)) return new Response(null, { status: 404 })
  const name = resolve(seq.slice(0, -4))
  if (!name)
    return new Response(null, {
      status: 404,
      headers: { "Cache-Control": "public, max-age=86400" },
    })
  return new Response(
    fs.readFileSync(path.join(/* turbopackIgnore: true */ DIR, name)),
    {
      headers: {
        "Content-Type": "image/png",
        "Cache-Control": "public, max-age=31536000, immutable",
      },
    }
  )
}
