"use client"

import * as React from "react"
import { Play, Video } from "lucide-react"

import { Caption } from "../caption"
import { MediaFrame, MediaImage, NotDownloadedBadge } from "../media-frame"
import { MessageMeta } from "../message-meta"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { formatDuration } from "@/lib/whatsapp/format"
import type {
  GifMessage,
  ImageMessage,
  VideoMessage,
} from "@/lib/whatsapp/types"

function OverlayMeta({
  message,
}: {
  message: ImageMessage | VideoMessage | GifMessage
}) {
  return (
    <MessageMeta
      message={message}
      variant="overlay"
      className="absolute right-1.5 bottom-1.5"
    />
  )
}

/** Full-screen viewer. Falls back to the thumbnail if the original isn't available. */
function Lightbox({
  open,
  onOpenChange,
  children,
  title,
}: {
  open: boolean
  onOpenChange: (o: boolean) => void
  children: React.ReactNode
  title: string
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[92svh] w-auto max-w-[92vw] items-center justify-center border-none bg-transparent p-0 shadow-none sm:max-w-[92vw]">
        <DialogTitle className="sr-only">{title}</DialogTitle>
        {children}
      </DialogContent>
    </Dialog>
  )
}

export function ImageMessageContent({ message }: { message: ImageMessage }) {
  const [open, setOpen] = React.useState(false)
  const { media } = message
  const src = media.url ?? media.thumbnailUrl
  return (
    <div className="p-0.5">
      <MediaFrame
        media={media}
        onClick={() => src && setOpen(true)}
        overlay={
          <>
            {!media.url && <NotDownloadedBadge />}
            {!message.caption && <OverlayMeta message={message} />}
          </>
        }
      >
        <MediaImage src={src} alt={message.caption ?? "Photo"} />
      </MediaFrame>
      {message.caption && (
        <Caption message={message} caption={message.caption} />
      )}
      <Lightbox
        open={open}
        onOpenChange={setOpen}
        title={message.caption ?? "Photo"}
      >
        {src && (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={src}
            alt={message.caption ?? ""}
            className="max-h-[92svh] max-w-[92vw] object-contain"
          />
        )}
      </Lightbox>
    </div>
  )
}

export function VideoMessageContent({ message }: { message: VideoMessage }) {
  const [playing, setPlaying] = React.useState(false)
  const { media } = message
  return (
    <div className="p-0.5">
      <MediaFrame
        media={media}
        onClick={media.url && !playing ? () => setPlaying(true) : undefined}
        overlay={
          !playing && (
            <>
              {media.url ? (
                <span className="absolute top-1/2 left-1/2 flex size-12 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full bg-black/45 text-white">
                  <Play className="ml-0.5 size-6 fill-current" />
                </span>
              ) : (
                <NotDownloadedBadge label="Video file not available" />
              )}
              <span className="absolute bottom-1.5 left-1.5 inline-flex items-center gap-1 rounded-full bg-black/35 px-1.5 py-0.5 text-[11px] text-white">
                <Video className="size-3.5" />
                {media.duration ? formatDuration(media.duration) : "Video"}
              </span>
              {!message.caption && <OverlayMeta message={message} />}
            </>
          )
        }
      >
        {playing && media.url ? (
          <video
            src={media.url}
            controls
            autoPlay
            className="absolute inset-0 size-full bg-black object-contain"
          />
        ) : (
          <MediaImage src={media.thumbnailUrl} alt="Video" />
        )}
      </MediaFrame>
      {message.caption && (
        <Caption message={message} caption={message.caption} />
      )}
    </div>
  )
}

export function GifMessageContent({ message }: { message: GifMessage }) {
  const { media } = message
  return (
    <div className="p-0.5">
      <MediaFrame
        media={media}
        overlay={
          <>
            <span className="absolute top-1.5 left-1.5 rounded bg-black/45 px-1.5 py-0.5 text-[11px] font-semibold text-white">
              GIF
            </span>
            {!message.caption && <OverlayMeta message={message} />}
          </>
        }
      >
        {media.url ? (
          <video
            src={media.url}
            autoPlay
            loop
            muted
            playsInline
            poster={media.thumbnailUrl ?? undefined}
            className="absolute inset-0 size-full object-cover"
          />
        ) : (
          <MediaImage src={media.thumbnailUrl} alt="GIF" />
        )}
      </MediaFrame>
      {message.caption && (
        <Caption message={message} caption={message.caption} />
      )}
    </div>
  )
}
