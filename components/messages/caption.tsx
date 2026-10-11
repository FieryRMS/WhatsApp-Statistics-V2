import { MessageMeta, MetaSpacer } from "./message-meta"
import { RichText } from "./rich-text"
import type { Message } from "@/lib/whatsapp/types"

/** Caption under media, with the meta in the corner. Shared by image/video/gif/document. */
export function Caption({ message, caption }: { message: Message; caption: string }) {
  return (
    <div className="relative px-1.5 pt-1 pb-1">
      <RichText text={caption} mentions={message.mentions} className="text-[14.2px] leading-[19px]" />
      <MetaSpacer message={message} />
      <MessageMeta message={message} className="absolute right-1.5 bottom-1" />
    </div>
  )
}
