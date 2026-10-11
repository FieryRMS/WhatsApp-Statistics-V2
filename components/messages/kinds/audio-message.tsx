"use client"

import * as React from "react"
import { Headphones, Mic, Pause, Play } from "lucide-react"

import { MessageMeta } from "../message-meta"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { avatarColor, initials } from "@/lib/whatsapp/colors"
import { formatDuration } from "@/lib/whatsapp/format"
import type { AudioMessage } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

/** Deterministic fake waveform so each voice note looks distinct. */
function bars(seed: number, n = 36) {
  let x = seed || 1
  return Array.from({ length: n }, () => {
    x = (x * 16807) % 2147483647
    return 0.25 + ((x % 1000) / 1000) * 0.75
  })
}

export function AudioMessageContent({ message }: { message: AudioMessage }) {
  const { media } = message
  const audioRef = React.useRef<HTMLAudioElement>(null)
  const [playing, setPlaying] = React.useState(false)
  const [progress, setProgress] = React.useState(0)
  const [current, setCurrent] = React.useState(0)
  const duration = media.duration ?? 0
  const available = !!media.url
  const wave = React.useMemo(() => bars(message.id), [message.id])

  const toggle = () => {
    const a = audioRef.current
    if (!a) return
    if (a.paused) void a.play()
    else a.pause()
  }

  const seek = (e: React.MouseEvent<HTMLDivElement>) => {
    const a = audioRef.current
    if (!a || !a.duration) return
    const rect = e.currentTarget.getBoundingClientRect()
    a.currentTime = ((e.clientX - rect.left) / rect.width) * a.duration
  }

  const sender = message.sender
  return (
    <div className="relative flex w-[300px] max-w-full items-center gap-2.5 px-1.5 pt-1.5 pb-1.5">
      {message.voiceNote ? (
        <div className="relative shrink-0">
          <Avatar className="size-11">
            <AvatarFallback className={cn("text-sm text-white", avatarColor(sender?.jid ?? null))}>
              {initials(sender?.name ?? "?")}
            </AvatarFallback>
          </Avatar>
          <Mic className={cn("absolute -right-1 -bottom-0.5 size-4", message.status === "played" || !message.fromMe ? "text-tick-read" : "text-bubble-meta")} />
        </div>
      ) : (
        <div className="flex size-11 shrink-0 items-center justify-center rounded-full bg-[#ff9f2e] text-white">
          <Headphones className="size-5" />
        </div>
      )}

      <button
        type="button"
        onClick={toggle}
        disabled={!available}
        className="shrink-0 text-bubble-meta disabled:opacity-40"
        aria-label={playing ? "Pause" : "Play"}
        title={available ? undefined : "Audio file not available"}
      >
        {playing ? <Pause className="size-7 fill-current" /> : <Play className="size-7 fill-current" />}
      </button>

      <div className="min-w-0 flex-1">
        <div className="flex h-7 cursor-pointer items-center gap-[2px]" onClick={seek}>
          {wave.map((h, i) => (
            <span
              key={i}
              className={cn(
                "w-[3px] flex-1 rounded-full",
                i / wave.length < progress ? "bg-wa-accent" : "bg-bubble-meta/45"
              )}
              style={{ height: `${h * 100}%` }}
            />
          ))}
        </div>
        <div className="mt-0.5 text-[11px] leading-none text-bubble-meta">
          {formatDuration(playing || current ? current : duration)}
        </div>
      </div>

      <MessageMeta message={message} className="absolute right-1.5 bottom-1" />

      {available && (
        <audio
          ref={audioRef}
          src={media.url!}
          preload="none"
          onPlay={() => setPlaying(true)}
          onPause={() => setPlaying(false)}
          onEnded={() => {
            setPlaying(false)
            setProgress(0)
            setCurrent(0)
          }}
          onTimeUpdate={(e) => {
            const a = e.currentTarget
            setCurrent(a.currentTime)
            setProgress(a.duration ? a.currentTime / a.duration : 0)
          }}
        />
      )}
    </div>
  )
}
