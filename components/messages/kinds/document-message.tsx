import { ArrowDownToLine, FileText } from "lucide-react"

import { Caption } from "../caption"
import { MessageMeta } from "../message-meta"
import { formatBytes } from "@/lib/whatsapp/format"
import type { DocumentMessage } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

const EXT_COLORS: Record<string, string> = {
  pdf: "bg-[#e5252a]",
  doc: "bg-[#2b579a]",
  docx: "bg-[#2b579a]",
  xls: "bg-[#217346]",
  xlsx: "bg-[#217346]",
  ppt: "bg-[#d24726]",
  pptx: "bg-[#d24726]",
  zip: "bg-[#7a6c5d]",
  rar: "bg-[#7a6c5d]",
  apk: "bg-[#3ddc84]",
}

export function DocumentMessageContent({ message }: { message: DocumentMessage }) {
  const { media } = message
  const name = media.fileName ?? "Document"
  const ext = name.includes(".") ? name.split(".").pop()!.toLowerCase() : (media.mimeType?.split("/")[1] ?? "file")
  const details = [
    media.pageCount ? `${media.pageCount} page${media.pageCount === 1 ? "" : "s"}` : null,
    ext.toUpperCase(),
    formatBytes(media.size),
  ].filter(Boolean)

  return (
    <div className="w-[320px] max-w-full p-0.5">
      <div className="flex items-center gap-3 rounded-md bg-black/5 p-2.5 dark:bg-black/20">
        <div
          className={cn(
            "flex h-10 w-8 shrink-0 items-end justify-center rounded-sm pb-1 text-[9px] font-bold text-white uppercase",
            EXT_COLORS[ext] ?? "bg-[#8696a0]"
          )}
        >
          {ext.length <= 4 ? ext : <FileText className="size-4" />}
        </div>
        <div className="min-w-0 flex-1">
          <div className="line-clamp-2 text-sm break-all">{name}</div>
          <div className="text-xs text-bubble-meta">{details.join(" • ")}</div>
        </div>
        {media.url ? (
          <a
            href={`${media.url}?download`}
            className="flex size-9 shrink-0 items-center justify-center rounded-full border border-bubble-meta/40 text-bubble-meta hover:bg-black/5"
            aria-label="Download"
          >
            <ArrowDownToLine className="size-4" />
          </a>
        ) : (
          <span
            title="Document file not available"
            className="flex size-9 shrink-0 items-center justify-center rounded-full border border-bubble-meta/20 text-bubble-meta/40"
          >
            <ArrowDownToLine className="size-4" />
          </span>
        )}
      </div>
      {message.caption ? (
        <Caption message={message} caption={message.caption} />
      ) : (
        <div className="flex justify-end px-1.5 pt-1 pb-0.5">
          <MessageMeta message={message} />
        </div>
      )}
    </div>
  )
}
