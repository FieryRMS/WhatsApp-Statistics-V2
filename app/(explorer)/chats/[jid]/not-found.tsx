import Link from "next/link"

export default function ChatNotFound() {
  return (
    <div className="flex h-svh flex-col items-center justify-center gap-3 bg-chat-bg">
      <p className="text-muted-foreground">This chat doesn&apos;t exist in the database.</p>
      <Link href="/" className="text-sm text-wa-accent hover:underline">
        Back to chats
      </Link>
    </div>
  )
}
