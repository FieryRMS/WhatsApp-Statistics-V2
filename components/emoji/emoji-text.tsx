"use client"

import * as React from "react"

import { emojiSrc, splitEmoji } from "@/lib/emoji/sequence"
import { cn } from "@/lib/utils"

/** One WhatsApp-style emoji image, sized to the surrounding text. Falls back to the system emoji. */
export function Emoji({ emoji, className }: { emoji: string; className?: string }) {
  const [failed, setFailed] = React.useState(false)
  if (failed) return <>{emoji}</>
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={emojiSrc(emoji)}
      alt={emoji}
      draggable={false}
      loading="lazy"
      decoding="async"
      onError={() => setFailed(true)}
      className={cn("inline-block size-[1.25em] align-[-0.25em] select-text", className)}
    />
  )
}

/** Render a plain string with its emoji drawn as WhatsApp emoji. */
export function emojify(text: string, keyPrefix = "e"): React.ReactNode[] {
  return splitEmoji(text).map((p, i) => ("emoji" in p ? <Emoji key={`${keyPrefix}${i}`} emoji={p.emoji} /> : p.text))
}

export function EmojiText({ text, className }: { text: string | null | undefined; className?: string }) {
  if (!text) return null
  const nodes = emojify(text)
  return className ? <span className={className}>{nodes}</span> : <>{nodes}</>
}
