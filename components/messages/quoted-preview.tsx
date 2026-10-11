import { RichText } from "./rich-text"
import { senderColor } from "@/lib/whatsapp/colors"
import type { QuotedMessage } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

export function QuotedPreview({
  quoted,
  onClick,
  className,
}: {
  quoted: QuotedMessage
  onClick?: (targetId: number) => void
  className?: string
}) {
  const color = senderColor(quoted.sender.jid)
  const clickable = !!(onClick && quoted.targetId)
  return (
    <button
      type="button"
      disabled={!clickable}
      onClick={() => clickable && onClick!(quoted.targetId!)}
      className={cn(
        "flex w-full min-w-0 overflow-hidden rounded-md bg-black/5 text-left dark:bg-black/20",
        clickable && "cursor-pointer hover:bg-black/10 dark:hover:bg-black/30",
        className
      )}
    >
      <span className={cn("w-1 shrink-0 bg-current", color)} />
      <span className="min-w-0 flex-1 px-2 py-1.5">
        <span className={cn("block truncate text-xs font-medium", color)}>{quoted.sender.name}</span>
        <span className="line-clamp-3 text-[13px] break-words whitespace-pre-wrap text-muted-foreground">
          <RichText text={quoted.preview} linkify={false} />
        </span>
      </span>
      {quoted.thumbnailUrl && (
        // eslint-disable-next-line @next/next/no-img-element
        <img src={quoted.thumbnailUrl} alt="" className="size-14 shrink-0 object-cover" loading="lazy" />
      )}
    </button>
  )
}
