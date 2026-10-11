/**
 * Emoji detection and image URLs. Client-safe.
 *
 * Emoji are drawn with WhatsApp's own artwork (vendor/whatsapp-emoji-linux,
 * served by app/emoji/[seq]) instead of the operating system's emoji font,
 * so they look the same everywhere.
 */

// Flags, keycaps, skin tones and ZWJ sequences (👨‍👩‍👧) as single emoji.
const EMOJI_SOURCE =
  "\\p{Regional_Indicator}{2}|[#*0-9]\\uFE0F?\\u20E3|\\p{Extended_Pictographic}(?:\\uFE0F|\\p{Emoji_Modifier})*(?:\\u200D\\p{Extended_Pictographic}(?:\\uFE0F|\\p{Emoji_Modifier})*)*"

const QUICK_RE = /[©®‼-㊙\uD83C-\uDBFF⃣]/

export type EmojiPart = { text: string } | { emoji: string }

/** Split text into plain runs and emoji. Fast path when there are none. */
export function splitEmoji(text: string): EmojiPart[] {
  if (!text || !QUICK_RE.test(text)) return [{ text }]
  const out: EmojiPart[] = []
  let last = 0
  for (const m of text.matchAll(new RegExp(EMOJI_SOURCE, "gu"))) {
    // Plain symbols like ©, ™, ↔ match \p{Extended_Pictographic} but read as text without FE0F.
    if (m[0].length === 1 && m[0].charCodeAt(0) < 0x2190 && !m[0].endsWith("️")) continue
    if (m.index > last) out.push({ text: text.slice(last, m.index) })
    out.push({ emoji: m[0] })
    last = m.index + m[0].length
  }
  if (last < text.length) out.push({ text: text.slice(last) })
  return out
}

/** "😂" → "/emoji/1f602.png"; "❤️" → "/emoji/2764-fe0f.png" (the route tolerates FE0F). */
export function emojiSrc(emoji: string): string {
  const cps = Array.from(emoji, (c) => c.codePointAt(0)!.toString(16))
  return `/emoji/${cps.join("-")}.png`
}

/** True when `text` is only emoji (and whitespace). */
export function isOnlyEmoji(text: string): boolean {
  const parts = splitEmoji(text.trim())
  return parts.length > 0 && parts.every((p) => "emoji" in p || !p.text.trim())
}
