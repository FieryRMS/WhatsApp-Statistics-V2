import fs from "node:fs"
import path from "node:path"

import { paths } from "./db"

/**
 * Media files aren't in the database — only their path on the phone
 * (e.g. "Media/WhatsApp Images/IMG-20200707-WA0000.jpg"). Copy the phone's
 * WhatsApp folder (the one containing "Media/") to WHATSAPP_MEDIA_DIR
 * (default ./whatsapp-data) and the originals will be served.
 */
export function resolveMediaPath(filePath: string | null | undefined): string | null {
  if (!filePath) return null
  const root = path.resolve(paths.mediaRoot)
  // Paths are relative to the WhatsApp folder; strip any absolute prefix
  // like "/storage/emulated/0/WhatsApp/".
  const rel = filePath.replace(/^.*?\/WhatsApp\//, "").replace(/^\/+/, "")
  const abs = path.resolve(root, rel)
  if (!abs.startsWith(root + path.sep)) return null
  return abs
}

const existsCache = new Map<string, boolean>()

export function mediaExists(filePath: string | null | undefined): boolean {
  const abs = resolveMediaPath(filePath)
  if (!abs) return false
  let hit = existsCache.get(abs)
  if (hit === undefined) {
    hit = fs.existsSync(/* turbopackIgnore: true */ abs)
    existsCache.set(abs, hit)
  }
  return hit
}

const EXT_MIME: Record<string, string> = {
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".mp4": "video/mp4",
  ".3gp": "video/3gpp",
  ".opus": "audio/ogg",
  ".ogg": "audio/ogg",
  ".m4a": "audio/mp4",
  ".aac": "audio/aac",
  ".mp3": "audio/mpeg",
  ".amr": "audio/amr",
  ".pdf": "application/pdf",
}

export function mimeFromPath(p: string, fallback?: string | null) {
  return fallback?.split(";")[0] || EXT_MIME[path.extname(p).toLowerCase()] || "application/octet-stream"
}
