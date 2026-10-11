"use client"

import * as React from "react"
import { ChevronDown, Loader2, Lock } from "lucide-react"

import { MessageView, getRenderer } from "@/components/messages"
import { SystemChip } from "@/components/messages/kinds/system-message"
import { Button } from "@/components/ui/button"
import type { MessageStream } from "@/hooks/use-message-stream"
import { formatDayLabel, isSameDay } from "@/lib/whatsapp/format"
import type { ChatKind, Message } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

/** Start fetching when within this many px of either end. */
const PREFETCH_PX = 1500

function sameSender(a: Message, b: Message) {
  if (
    getRenderer(a.kind).layout === "center" ||
    getRenderer(b.kind).layout === "center"
  )
    return false
  if (a.fromMe !== b.fromMe) return false
  return a.fromMe || a.sender?.jid === b.sender?.jid
}

interface Row {
  message: Message
  dayLabel: string | null
  isFirstInGroup: boolean
}

function buildRows(messages: Message[]): Row[] {
  return messages.map((m, i) => {
    const prev = messages[i - 1]
    const newDay = !prev || !isSameDay(prev.timestamp, m.timestamp)
    return {
      message: m,
      dayLabel: newDay ? formatDayLabel(m.timestamp) : null,
      isFirstInGroup: newDay || !sameSender(prev, m),
    }
  })
}

function flash(el: Element) {
  el.classList.remove("message-highlight")
  void (el as HTMLElement).offsetWidth // restart the animation
  el.classList.add("message-highlight")
  el.addEventListener(
    "animationend",
    () => el.classList.remove("message-highlight"),
    { once: true }
  )
}

export function MessageList({
  stream,
  chatKind,
  onQuoteClick,
  onJumpToLatest,
  className,
}: {
  stream: MessageStream
  chatKind: ChatKind
  onQuoteClick?: (messageId: number) => void
  /** Called when the user asks for the newest messages but they aren't loaded. */
  onJumpToLatest?: () => void
  className?: string
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null)
  // First visible message and its offset from the top of the viewport. Restored after
  // every render so prepends, appends and trims never make the content jump.
  const anchor = React.useRef<{ id: number; offset: number } | null>(null)
  const handledJump = React.useRef(-1)
  const contentRef = React.useRef<HTMLDivElement>(null)
  // Pinned to the newest message: stay at the bottom when content resizes.
  const stickToBottom = React.useRef(false)
  const [nearBottom, setNearBottom] = React.useState(true)

  const rows = React.useMemo(
    () => buildRows(stream.messages),
    [stream.messages]
  )

  const captureAnchor = React.useCallback(() => {
    const el = scrollRef.current
    if (!el) return
    const items = el.querySelectorAll<HTMLElement>("[data-mid]")
    // Binary search for the first item whose bottom is below the viewport top.
    let lo = 0
    let hi = items.length - 1
    let found = -1
    while (lo <= hi) {
      const mid = (lo + hi) >> 1
      const it = items[mid]
      if (it.offsetTop + it.offsetHeight > el.scrollTop) {
        found = mid
        hi = mid - 1
      } else lo = mid + 1
    }
    const it = items[found]
    anchor.current = it
      ? { id: Number(it.dataset.mid), offset: it.offsetTop - el.scrollTop }
      : null
  }, [])

  const { status, loadOlder, loadNewer, hasNewer } = stream

  const restore = React.useCallback(() => {
    const el = scrollRef.current
    if (!el) return
    if (stickToBottom.current) {
      el.scrollTop = el.scrollHeight
      return
    }
    if (!anchor.current) return
    const target = el.querySelector<HTMLElement>(
      `[data-mid="${anchor.current.id}"]`
    )
    if (target) el.scrollTop = target.offsetTop - anchor.current.offset
  }, [])
  const maybeLoad = React.useCallback(() => {
    const el = scrollRef.current
    if (!el || status !== "ready") return
    if (el.scrollTop < PREFETCH_PX) loadOlder()
    if (el.scrollHeight - el.scrollTop - el.clientHeight < PREFETCH_PX)
      loadNewer()
  }, [status, loadOlder, loadNewer])

  React.useLayoutEffect(() => {
    const el = scrollRef.current
    if (!el || stream.status !== "ready") return
    if (stream.jumpVersion !== handledJump.current) {
      handledJump.current = stream.jumpVersion
      const target =
        stream.targetId != null
          ? el.querySelector<HTMLElement>(`[data-mid="${stream.targetId}"]`)
          : null
      if (target) {
        stickToBottom.current = false
        el.scrollTop =
          target.offsetTop - el.clientHeight / 2 + target.offsetHeight / 2
        flash(target.firstElementChild ?? target)
      } else {
        stickToBottom.current = !stream.hasNewer
        el.scrollTop = el.scrollHeight
      }
    } else {
      restore()
    }
    captureAnchor()
    maybeLoad()
  }, [
    rows,
    stream.status,
    stream.jumpVersion,
    stream.targetId,
    stream.hasNewer,
    captureAnchor,
    maybeLoad,
    restore,
  ])

  // Late layout changes (web fonts, emoji, images) would otherwise shift what's on screen.
  React.useEffect(() => {
    const content = contentRef.current
    if (!content) return
    const ro = new ResizeObserver(() => restore())
    ro.observe(content)
    return () => ro.disconnect()
  }, [restore])

  const onScroll = () => {
    const el = scrollRef.current
    if (!el) return
    captureAnchor()
    const distance = el.scrollHeight - el.scrollTop - el.clientHeight
    stickToBottom.current = distance < 4 && !hasNewer
    setNearBottom(distance < 200)
    maybeLoad()
  }

  const scrollToBottom = () => {
    if (stream.hasNewer) onJumpToLatest?.()
    else
      scrollRef.current?.scrollTo({
        top: scrollRef.current.scrollHeight,
        behavior: "smooth",
      })
  }

  return (
    <div className={cn("relative min-h-0 flex-1", className)}>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        className="relative h-full overflow-x-hidden overflow-y-auto bg-chat-bg [overflow-anchor:none]"
      >
        <div
          ref={contentRef}
          className="flex min-h-full flex-col justify-end pt-2 pb-4"
        >
          {stream.status === "ready" &&
            (stream.hasOlder ? (
              <div className="flex justify-center py-3 text-bubble-meta">
                <Loader2
                  className={cn(
                    "size-5",
                    stream.loadingOlder ? "animate-spin" : "opacity-0"
                  )}
                />
              </div>
            ) : (
              <div className="px-[4%] pt-2 pb-1">
                <SystemChip className="bg-[#ffeecd] text-[#54656f] dark:bg-[#182229] dark:text-[#ffd279]">
                  <Lock className="mr-1 inline size-3 -translate-y-px" />
                  Start of this chat
                </SystemChip>
              </div>
            ))}

          {rows.map(({ message, dayLabel, isFirstInGroup }) => (
            <React.Fragment key={message.id}>
              {dayLabel && (
                <div className="pointer-events-none sticky top-2 z-10 flex justify-center py-2">
                  <span className="rounded-lg bg-system-chip px-3 py-1 text-[12.5px] text-bubble-meta uppercase shadow-sm">
                    {dayLabel}
                  </span>
                </div>
              )}
              <div data-mid={message.id}>
                <MessageView
                  message={message}
                  chatKind={chatKind}
                  isFirstInGroup={isFirstInGroup}
                  onQuoteClick={onQuoteClick}
                />
              </div>
            </React.Fragment>
          ))}

          {stream.status === "ready" && stream.hasNewer && (
            <div className="flex justify-center py-3 text-bubble-meta">
              <Loader2
                className={cn(
                  "size-5",
                  stream.loadingNewer ? "animate-spin" : "opacity-0"
                )}
              />
            </div>
          )}
        </div>

        {stream.status === "loading" && (
          <div className="absolute inset-0 flex items-center justify-center bg-chat-bg/60">
            <Loader2 className="size-8 animate-spin text-wa-accent" />
          </div>
        )}
        {stream.status === "error" && (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 bg-chat-bg">
            <p className="text-sm text-muted-foreground">
              Couldn&apos;t load messages: {stream.error}
            </p>
            <Button variant="outline" size="sm" onClick={stream.reload}>
              Retry
            </Button>
          </div>
        )}
      </div>

      {stream.status === "ready" && (!nearBottom || stream.hasNewer) && (
        <Button
          size="icon"
          variant="secondary"
          onClick={scrollToBottom}
          className="absolute right-5 bottom-5 z-20 size-10 rounded-full bg-system-chip text-bubble-meta shadow-md hover:bg-system-chip"
          aria-label="Scroll to latest"
        >
          <ChevronDown className="size-5" />
        </Button>
      )}
    </div>
  )
}
