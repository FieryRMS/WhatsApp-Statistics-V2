"use client"

import * as React from "react"

import { MessageMeta, MetaSpacer } from "../message-meta"
import { RichText, isEmojiOnly } from "../rich-text"
import type { LinkPreview, TextMessage } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

const READ_MORE_AT = 1200

function LinkPreviewCard({ preview }: { preview: LinkPreview }) {
  let host: string | null = null
  try {
    host = preview.url
      ? new URL(preview.url).hostname.replace(/^www\./, "")
      : null
  } catch {}
  const body = (
    <div className="flex overflow-hidden rounded-md bg-black/5 dark:bg-black/20">
      {preview.thumbnailUrl && (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={preview.thumbnailUrl}
          alt=""
          loading="lazy"
          className="size-20 shrink-0 object-cover"
        />
      )}
      <div className="min-w-0 flex-1 px-2.5 py-2">
        {preview.title && (
          <div className="line-clamp-2 text-[13px] font-medium">
            {preview.title}
          </div>
        )}
        {preview.description && (
          <div className="line-clamp-2 text-xs text-muted-foreground">
            {preview.description}
          </div>
        )}
        {host && (
          <div className="mt-0.5 truncate text-xs text-bubble-meta">{host}</div>
        )}
      </div>
    </div>
  )
  return preview.url ? (
    <a
      href={preview.url}
      target="_blank"
      rel="noopener noreferrer"
      className="mb-1 block"
    >
      {body}
    </a>
  ) : (
    <div className="mb-1">{body}</div>
  )
}

export function TextMessageContent({ message }: { message: TextMessage }) {
  const [expanded, setExpanded] = React.useState(false)
  const long = message.text.length > READ_MORE_AT
  const text =
    long && !expanded ? `${message.text.slice(0, READ_MORE_AT)}…` : message.text
  const big = isEmojiOnly(message.text)

  return (
    <div className="relative px-1.5 pt-0.5 pb-1">
      {message.linkPreview && <LinkPreviewCard preview={message.linkPreview} />}
      <RichText
        text={text}
        mentions={message.mentions}
        className={cn(
          "text-[14.2px] leading-[19px]",
          big && "text-4xl leading-tight"
        )}
      />
      {long && !expanded && (
        <button
          type="button"
          onClick={() => setExpanded(true)}
          className="ml-1 text-[13px] font-medium text-[#027eb5] dark:text-[#53bdeb]"
        >
          Read more
        </button>
      )}
      <MetaSpacer message={message} />
      <MessageMeta message={message} className="absolute right-1.5 bottom-1" />
    </div>
  )
}
