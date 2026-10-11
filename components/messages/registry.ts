import type { ComponentType } from "react"

import { AudioMessageContent } from "./kinds/audio-message"
import { DocumentMessageContent } from "./kinds/document-message"
import {
  ContactMessageContent,
  DeletedMessageContent,
  LocationMessageContent,
  MissedCallMessageContent,
  PaymentMessageContent,
  StickerMessageContent,
  UnsupportedMessageContent,
  ViewOnceMessageContent,
} from "./kinds/misc-messages"
import {
  AlbumMessageContent,
  CallMessageContent,
  PollMessageContent,
} from "./kinds/interactive-messages"
import {
  DisappearingNoticeContent,
  SystemMessageContent,
} from "./kinds/system-message"
import { TextMessageContent } from "./kinds/text-message"
import {
  GifMessageContent,
  ImageMessageContent,
  VideoMessageContent,
} from "./kinds/visual-media"
import type { MessageKind, MessageOfKind } from "@/lib/whatsapp/types"

/**
 * How a message kind is laid out:
 *   bubble – inside a chat bubble (with sender name, forwarded label, quote, reactions)
 *   bare   – aligned left/right but without a bubble (stickers)
 *   center – centred chip (system events)
 */
export type MessageLayout = "bubble" | "bare" | "center"

export interface MessageRenderer<K extends MessageKind> {
  layout: MessageLayout
  Content: ComponentType<{ message: MessageOfKind<K> }>
}

/** One entry per message kind. To add a kind: add it to `Message` in types.ts, then here. */
export const messageRenderers: { [K in MessageKind]: MessageRenderer<K> } = {
  text: { layout: "bubble", Content: TextMessageContent },
  image: { layout: "bubble", Content: ImageMessageContent },
  video: { layout: "bubble", Content: VideoMessageContent },
  gif: { layout: "bubble", Content: GifMessageContent },
  audio: { layout: "bubble", Content: AudioMessageContent },
  document: { layout: "bubble", Content: DocumentMessageContent },
  sticker: { layout: "bare", Content: StickerMessageContent },
  contact: { layout: "bubble", Content: ContactMessageContent },
  location: { layout: "bubble", Content: LocationMessageContent },
  view_once: { layout: "bubble", Content: ViewOnceMessageContent },
  deleted: { layout: "bubble", Content: DeletedMessageContent },
  missed_call: { layout: "bubble", Content: MissedCallMessageContent },
  payment: { layout: "bubble", Content: PaymentMessageContent },
  poll: { layout: "bubble", Content: PollMessageContent },
  call: { layout: "bubble", Content: CallMessageContent },
  album: { layout: "center", Content: AlbumMessageContent },
  system: { layout: "center", Content: SystemMessageContent },
  disappearing_notice: { layout: "center", Content: DisappearingNoticeContent },
  unsupported: { layout: "bubble", Content: UnsupportedMessageContent },
}

export function getRenderer<K extends MessageKind>(
  kind: K
): MessageRenderer<K> {
  return messageRenderers[kind]
}
