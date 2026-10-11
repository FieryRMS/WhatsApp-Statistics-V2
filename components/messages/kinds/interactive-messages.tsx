import {
  Images,
  Phone,
  PhoneIncoming,
  PhoneMissed,
  PhoneOutgoing,
  Video,
} from "lucide-react"

import { MessageMeta } from "../message-meta"
import { SystemChip } from "./system-message"
import { formatDuration } from "@/lib/whatsapp/format"
import type {
  AlbumMessage,
  CallMessage,
  PollMessage,
} from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"
import { EmojiText } from "@/components/emoji/emoji-text"

export function PollMessageContent({ message }: { message: PollMessage }) {
  const total = message.options.reduce((n, o) => n + o.votes, 0)
  const max = Math.max(1, ...message.options.map((o) => o.votes))
  return (
    <div className="w-[320px] max-w-full px-2 pt-1 pb-1">
      <div className="text-[15px] font-medium break-words">
        <EmojiText text={message.question} />
      </div>
      <div className="mb-2 text-xs text-bubble-meta">
        {message.multipleAnswers ? "Select one or more" : "Select one"}
      </div>
      <ul className="space-y-2.5">
        {message.options.map((o, i) => (
          <li key={i}>
            <div className="flex items-baseline justify-between gap-3 text-[14px]">
              <span className="break-words">
                <EmojiText text={o.name} />
              </span>
              <span className="shrink-0 text-xs text-bubble-meta">
                {o.votes}
              </span>
            </div>
            <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-black/10 dark:bg-white/10">
              <div
                className={cn(
                  "h-full rounded-full bg-wa-accent",
                  o.votes === max && o.votes > 0 ? "opacity-100" : "opacity-60"
                )}
                style={{ width: `${total ? (o.votes / total) * 100 : 0}%` }}
              />
            </div>
          </li>
        ))}
      </ul>
      <div className="mt-2 flex items-center justify-between border-t border-black/10 pt-1.5 dark:border-white/10">
        <span className="text-xs text-bubble-meta">
          {total} vote{total === 1 ? "" : "s"}
        </span>
        <MessageMeta message={message} />
      </div>
    </div>
  )
}

export function CallMessageContent({ message }: { message: CallMessage }) {
  const kind = message.video ? "Video call" : "Voice call"
  const Icon =
    message.outcome === "missed"
      ? PhoneMissed
      : message.fromMe
        ? PhoneOutgoing
        : PhoneIncoming
  const detail =
    message.outcome === "answered"
      ? message.duration
        ? formatDuration(message.duration)
        : "Answered"
      : message.outcome === "missed"
        ? "Missed"
        : "No answer"
  return (
    <div className="flex min-w-[230px] items-center gap-3 px-2 py-1.5">
      <span className="flex size-9 items-center justify-center rounded-full bg-black/5 dark:bg-white/10">
        {message.video ? (
          <Video
            className={cn(
              "size-4.5",
              message.outcome === "missed" && "text-destructive"
            )}
          />
        ) : (
          <Phone
            className={cn(
              "size-4.5",
              message.outcome === "missed" && "text-destructive"
            )}
          />
        )}
      </span>
      <div className="flex-1">
        <div className="text-[14px] font-medium">{kind}</div>
        <div className="flex items-center gap-1 text-xs text-bubble-meta">
          <Icon
            className={cn(
              "size-3.5",
              message.outcome === "missed" && "text-destructive"
            )}
          />
          {detail}
        </div>
      </div>
      <MessageMeta message={message} className="self-end" />
    </div>
  )
}

/** Album header. The photos/videos themselves follow as separate messages. */
export function AlbumMessageContent({ message }: { message: AlbumMessage }) {
  const parts = [
    message.images &&
      `${message.images} photo${message.images === 1 ? "" : "s"}`,
    message.videos &&
      `${message.videos} video${message.videos === 1 ? "" : "s"}`,
  ].filter(Boolean)
  return (
    <SystemChip title={new Date(message.timestamp).toLocaleString()}>
      <Images className="mr-1 inline size-3.5 -translate-y-px" />
      {message.fromMe ? "You" : (message.sender?.name ?? "Someone")} sent an
      album{parts.length ? `: ${parts.join(", ")}` : ""}
    </SystemChip>
  )
}
