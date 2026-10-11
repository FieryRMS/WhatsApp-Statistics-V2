import {
  Ban,
  Eye,
  Image as ImageIcon,
  MapPin,
  PhoneMissed,
  Video,
  VideoOff,
  Wallet,
} from "lucide-react"

import { MediaFrame, MediaImage } from "../media-frame"
import { MessageMeta, MetaSpacer } from "../message-meta"
import { RichText } from "../rich-text"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { avatarColor, initials } from "@/lib/whatsapp/colors"
import { formatMoney } from "@/lib/whatsapp/format"
import type {
  ContactMessage,
  DeletedMessage,
  LocationMessage,
  MissedCallMessage,
  PaymentMessage,
  StickerMessage,
  UnsupportedMessage,
  ViewOnceMessage,
} from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"
import { EmojiText } from "@/components/emoji/emoji-text"

/** Single-line content with the meta pinned bottom-right. */
function InlineRow({
  children,
  message,
}: {
  children: React.ReactNode
  message: Parameters<typeof MessageMeta>[0]["message"]
}) {
  return (
    <div className="relative px-1.5 pt-0.5 pb-1">
      {children}
      <MetaSpacer message={message} />
      <MessageMeta message={message} className="absolute right-1.5 bottom-1" />
    </div>
  )
}

export function DeletedMessageContent({
  message,
}: {
  message: DeletedMessage
}) {
  return (
    <InlineRow message={message}>
      <span className="inline-flex items-center gap-1.5 text-[14px] text-bubble-meta italic">
        <Ban className="size-4" />
        {message.byAdmin
          ? `This message was deleted by admin ${message.byAdmin.name}`
          : message.fromMe
            ? "You deleted this message"
            : "This message was deleted"}
      </span>
    </InlineRow>
  )
}

export function MissedCallMessageContent({
  message,
}: {
  message: MissedCallMessage
}) {
  const Icon = message.video ? VideoOff : PhoneMissed
  return (
    <div className="flex min-w-[220px] items-center gap-3 px-2 py-1.5">
      <span className="flex size-9 items-center justify-center rounded-full bg-black/5 dark:bg-white/10">
        <Icon className="size-4.5 text-destructive" />
      </span>
      <div className="flex-1">
        <div className="text-[14px] font-medium">
          {message.video ? "Missed video call" : "Missed voice call"}
        </div>
        <MessageMeta message={message} />
      </div>
    </div>
  )
}

export function UnsupportedMessageContent({
  message,
}: {
  message: UnsupportedMessage
}) {
  return (
    <InlineRow message={message}>
      <span className="text-[14px] text-bubble-meta italic">
        {message.text}
      </span>
    </InlineRow>
  )
}

export function ViewOnceMessageContent({
  message,
}: {
  message: ViewOnceMessage
}) {
  const Icon = message.mediaType === "video" ? Video : ImageIcon
  return (
    <InlineRow message={message}>
      <span className="inline-flex items-center gap-2 text-[14px]">
        <span className="flex size-6 items-center justify-center rounded-full border-2 border-dashed border-wa-accent text-[10px] font-bold text-wa-accent">
          1
        </span>
        <Icon className="size-4 text-bubble-meta" />
        <span className={cn(message.opened && "text-bubble-meta")}>
          {message.opened
            ? "Opened"
            : message.mediaType === "video"
              ? "Video"
              : "Photo"}
        </span>
        <Eye className="size-3.5 text-bubble-meta" aria-label="View once" />
      </span>
      {message.caption && (
        <div className="mt-1">
          <RichText
            text={message.caption}
            mentions={message.mentions}
            className="text-[14.2px]"
          />
        </div>
      )}
    </InlineRow>
  )
}

export function PaymentMessageContent({
  message,
}: {
  message: PaymentMessage
}) {
  return (
    <div className="w-[260px] max-w-full p-0.5">
      <div className="rounded-md bg-black/5 p-3 dark:bg-black/20">
        <div className="flex items-center gap-1.5 text-xs text-bubble-meta">
          <Wallet className="size-3.5" />
          Payment request
        </div>
        <div className="mt-1 text-2xl font-semibold">
          {message.amount != null
            ? formatMoney(message.amount, message.currency)
            : "—"}
        </div>
        {message.note && (
          <div className="mt-1 text-sm break-words">{message.note}</div>
        )}
      </div>
      <div className="flex justify-end px-1.5 pt-1 pb-0.5">
        <MessageMeta message={message} />
      </div>
    </div>
  )
}

export function ContactMessageContent({
  message,
}: {
  message: ContactMessage
}) {
  const [first, ...rest] = message.contacts
  if (!first)
    return (
      <UnsupportedMessageContent
        message={{ ...message, kind: "unsupported", text: "Contact" }}
      />
    )
  return (
    <div className="w-[280px] max-w-full p-0.5">
      <div className="flex items-center gap-3 px-2 py-2">
        <Avatar className="size-11">
          <AvatarFallback className={cn("text-white", avatarColor(first.name))}>
            {initials(first.name)}
          </AvatarFallback>
        </Avatar>
        <div className="min-w-0 flex-1">
          <div className="truncate text-[15px] font-medium">
            <EmojiText text={first.name} />
            {rest.length > 0 && (
              <span className="font-normal text-bubble-meta">
                {" "}
                and {rest.length} other contact{rest.length > 1 ? "s" : ""}
              </span>
            )}
          </div>
          {rest.length === 0 &&
            first.phones.map((p, i) => (
              <div key={i} className="truncate text-xs text-bubble-meta">
                {p.label ? `${p.label}: ` : ""}
                {p.number}
              </div>
            ))}
        </div>
      </div>
      {rest.length > 0 && (
        <ul className="mx-2 mb-1 space-y-0.5 border-t border-black/10 pt-1.5 text-xs text-bubble-meta dark:border-white/10">
          {message.contacts.map((c, i) => (
            <li key={i} className="truncate">
              <EmojiText text={c.name} />{" "}
              {c.phones[0] && `· ${c.phones[0].number}`}
            </li>
          ))}
        </ul>
      )}
      <div className="flex justify-end px-1.5 pb-0.5">
        <MessageMeta message={message} />
      </div>
    </div>
  )
}

export function LocationMessageContent({
  message,
}: {
  message: LocationMessage
}) {
  const href = `https://www.google.com/maps/search/?api=1&query=${message.latitude},${message.longitude}`
  return (
    <div className="w-[300px] max-w-full p-0.5">
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="block"
      >
        <MediaFrame
          media={{ width: 300, height: 150 }}
          className="bg-[#e8f0e5] dark:bg-[#1d2b22]"
          overlay={
            <span className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-full text-destructive">
              <MapPin className="size-9 fill-current stroke-white" />
            </span>
          }
        >
          {message.thumbnailUrl && (
            <MediaImage src={message.thumbnailUrl} alt="Map" />
          )}
        </MediaFrame>
      </a>
      <div className="relative px-1.5 pt-1.5 pb-1">
        {message.live && (
          <div className="text-[14px] font-medium">Live location</div>
        )}
        {message.name && (
          <div className="text-[14px] font-medium text-[#027eb5] dark:text-[#53bdeb]">
            {message.name}
          </div>
        )}
        {message.address && (
          <div className="text-xs text-bubble-meta">{message.address}</div>
        )}
        {!message.name && !message.address && !message.live && (
          <div className="text-xs text-bubble-meta">
            {message.latitude.toFixed(5)}, {message.longitude.toFixed(5)}
          </div>
        )}
        <MetaSpacer message={message} />
        <MessageMeta
          message={message}
          className="absolute right-1.5 bottom-1"
        />
      </div>
    </div>
  )
}

/** Stickers render without a bubble. */
export function StickerMessageContent({
  message,
}: {
  message: StickerMessage
}) {
  const src = message.media.url ?? message.media.thumbnailUrl
  return (
    <div className="relative size-40">
      {src ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={src}
          alt="Sticker"
          loading="lazy"
          className="size-full object-contain"
        />
      ) : (
        <div className="flex size-full items-center justify-center rounded-lg bg-black/5 text-xs text-bubble-meta">
          Sticker
        </div>
      )}
      <MessageMeta
        message={message}
        variant="chip"
        className="absolute right-0 bottom-0"
      />
    </div>
  )
}
