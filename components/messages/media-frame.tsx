"use client"

import * as React from "react"
import { ArrowDownToLine, ImageOff } from "lucide-react"

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import type { MediaInfo } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

const MAX_W = 330
const MAX_H = 360
const MIN_H = 120

/** Box size for a media item, keeping aspect ratio within WhatsApp-like bounds. */
export function mediaBox(media: Pick<MediaInfo, "width" | "height">, maxW = MAX_W) {
  const w = media.width ?? 0
  const h = media.height ?? 0
  if (!w || !h) return { width: maxW, height: Math.round(maxW * 0.75) }
  const ratio = h / w
  return { width: maxW, height: Math.round(Math.min(Math.max(maxW * ratio, MIN_H), MAX_H)) }
}

/**
 * Fixed-size frame for image/video/GIF content. The size is known up front from
 * the DB, so the list never jumps when images load (important for scroll anchoring).
 */
export function MediaFrame({
  media,
  children,
  overlay,
  className,
  onClick,
}: {
  media: Pick<MediaInfo, "width" | "height">
  children?: React.ReactNode
  overlay?: React.ReactNode
  className?: string
  onClick?: () => void
}) {
  const box = mediaBox(media)
  return (
    <div
      className={cn(
        "relative max-w-full overflow-hidden rounded-md bg-black/10 dark:bg-black/30",
        onClick && "cursor-pointer",
        className
      )}
      style={{ width: box.width, aspectRatio: `${box.width} / ${box.height}` }}
      onClick={onClick}
    >
      {children}
      {overlay}
    </div>
  )
}

/** The low-res thumbnail from the DB, or the original image if it's on disk. */
export function MediaImage({
  src,
  alt = "",
  className,
}: {
  src: string | null
  alt?: string
  className?: string
}) {
  const [failed, setFailed] = React.useState(false)
  if (!src || failed)
    return (
      <div className="absolute inset-0 flex items-center justify-center text-muted-foreground">
        <ImageOff className="size-8 opacity-50" />
      </div>
    )
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={src}
      alt={alt}
      loading="lazy"
      decoding="async"
      onError={() => setFailed(true)}
      className={cn("absolute inset-0 size-full object-cover", className)}
    />
  )
}

/** Shown on media whose original file isn't in the media folder (like WhatsApp's "not downloaded"). */
export function NotDownloadedBadge({ label = "Media file not available" }: { label?: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="absolute top-1/2 left-1/2 flex size-12 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full bg-black/45 text-white">
          <ArrowDownToLine className="size-6" />
        </span>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  )
}
