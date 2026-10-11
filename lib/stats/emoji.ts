/**
 * Emoji extraction. Handles flags (regional-indicator pairs), keycaps,
 * skin tones and ZWJ sequences (👨‍👩‍👧). Variation selectors are dropped so
 * "❤️" and "❤" count as the same emoji.
 */
const EMOJI_RE =
  /\p{Regional_Indicator}{2}|[#*0-9]️?⃣|\p{Extended_Pictographic}(?:️|\p{Emoji_Modifier})*(?:‍\p{Extended_Pictographic}(?:️|\p{Emoji_Modifier})*)*/gu

const QUICK_RE = /[©®‼-㊙\uD83C-\uDBFF]/

export function extractEmojis(text: string): string[] {
  if (!QUICK_RE.test(text) && !/[#*0-9]️?⃣/.test(text)) return []
  const out: string[] = []
  for (const m of text.matchAll(EMOJI_RE)) out.push(m[0].replace(/️/g, ""))
  return out
}

export function normalizeEmoji(e: string): string {
  return e.replace(/️/g, "")
}
