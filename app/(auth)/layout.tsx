import { MessageSquareText } from "lucide-react"

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 bg-chat-bg px-4 py-10">
      <div className="flex items-center gap-2 text-lg font-semibold">
        <MessageSquareText className="size-6 text-wa-accent" />
        WhatsApp Stats
      </div>
      <div className="w-full max-w-sm">{children}</div>
    </main>
  )
}
