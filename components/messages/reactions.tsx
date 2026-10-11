import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import type { Reaction } from "@/lib/whatsapp/types"
import { cn } from "@/lib/utils"

export function Reactions({ reactions, className }: { reactions: Reaction[]; className?: string }) {
  if (!reactions.length) return null
  const grouped = new Map<string, Reaction[]>()
  for (const r of reactions) grouped.set(r.emoji, [...(grouped.get(r.emoji) ?? []), r])
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          className={cn(
            "inline-flex items-center gap-0.5 rounded-full border border-background bg-bubble-in px-1.5 py-0.5 text-sm shadow-sm",
            className
          )}
        >
          {[...grouped.keys()].slice(0, 3).map((e) => (
            <span key={e}>{e}</span>
          ))}
          {reactions.length > 1 && <span className="ml-0.5 text-xs text-bubble-meta">{reactions.length}</span>}
        </div>
      </TooltipTrigger>
      <TooltipContent>
        {[...grouped.entries()].map(([emoji, rs]) => (
          <div key={emoji}>
            {emoji} {rs.map((r) => r.sender.name).join(", ")}
          </div>
        ))}
      </TooltipContent>
    </Tooltip>
  )
}
