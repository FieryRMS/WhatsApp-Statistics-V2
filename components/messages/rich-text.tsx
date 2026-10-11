import * as React from "react"

import { cn } from "@/lib/utils"

/**
 * Renders WhatsApp-formatted text: *bold*, _italic_, ~strike~, ```mono```,
 * links and @mentions.
 */

const URL_RE = /\b(?:https?:\/\/|www\.)[^\s<>"]+[^\s<>".,;:!?'")\]]/gi
const MENTION_RE = /@(\d{5,})/g

// Marker must hug non-space text and not sit inside a word (so snake_case stays intact).
const FORMAT_RE =
  /```([\s\S]+?)```|(?<![\w*])\*(?=\S)([^*\n]+?)(?<=\S)\*(?![\w*])|(?<![\w_])_(?=\S)([^_\n]+?)(?<=\S)_(?![\w_])|(?<![\w~])~(?=\S)([^~\n]+?)(?<=\S)~(?![\w~])/g

function formatted(text: string, keyPrefix: string): React.ReactNode[] {
  const out: React.ReactNode[] = []
  let last = 0
  let i = 0
  for (const m of text.matchAll(FORMAT_RE)) {
    if (m.index > last) out.push(text.slice(last, m.index))
    const key = `${keyPrefix}f${i++}`
    if (m[1] !== undefined)
      out.push(
        <code key={key} className="font-mono text-[0.92em]">
          {m[1]}
        </code>
      )
    else if (m[2] !== undefined) out.push(<strong key={key}>{formatted(m[2], key)}</strong>)
    else if (m[3] !== undefined) out.push(<em key={key}>{formatted(m[3], key)}</em>)
    else if (m[4] !== undefined) out.push(<s key={key}>{formatted(m[4], key)}</s>)
    last = m.index + m[0].length
  }
  if (last < text.length) out.push(text.slice(last))
  return out
}

function withMentions(text: string, mentions: Record<string, string>, keyPrefix: string): React.ReactNode[] {
  const out: React.ReactNode[] = []
  let last = 0
  let i = 0
  for (const m of text.matchAll(MENTION_RE)) {
    const name = mentions[m[1]]
    if (!name) continue
    if (m.index > last) out.push(...formatted(text.slice(last, m.index), `${keyPrefix}t${i}`))
    out.push(
      <span key={`${keyPrefix}m${i++}`} className="font-medium text-wa-accent">
        @{name.replace(/^~/, "")}
      </span>
    )
    last = m.index + m[0].length
  }
  if (last < text.length) out.push(...formatted(text.slice(last), `${keyPrefix}t${i}`))
  return out
}

export function RichText({
  text,
  mentions = {},
  linkify = true,
  className,
}: {
  text: string
  mentions?: Record<string, string>
  /** Render URLs as links. Off inside other interactive elements (e.g. quote buttons). */
  linkify?: boolean
  className?: string
}) {
  if (!linkify) {
    return <span className={cn("break-words whitespace-pre-wrap", className)}>{withMentions(text, mentions, "s")}</span>
  }
  const nodes: React.ReactNode[] = []
  let last = 0
  let i = 0
  for (const m of text.matchAll(URL_RE)) {
    if (m.index > last) nodes.push(...withMentions(text.slice(last, m.index), mentions, `s${i}`))
    const href = m[0].startsWith("http") ? m[0] : `https://${m[0]}`
    nodes.push(
      <a
        key={`u${i++}`}
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="break-all text-[#027eb5] hover:underline dark:text-[#53bdeb]"
      >
        {m[0]}
      </a>
    )
    last = m.index + m[0].length
  }
  if (last < text.length) nodes.push(...withMentions(text.slice(last), mentions, `s${i}`))
  return <span className={cn("break-words whitespace-pre-wrap", className)}>{nodes}</span>
}

const EMOJI_ONLY_RE = /^(?:\p{Extended_Pictographic}(?:️|‍\p{Extended_Pictographic}|\p{Emoji_Modifier})*\s*){1,3}$/u

/** 1–3 emoji with no other text — WhatsApp shows these large. */
export function isEmojiOnly(text: string) {
  return text.length <= 40 && EMOJI_ONLY_RE.test(text.trim())
}
