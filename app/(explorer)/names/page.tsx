import { NamesEditor } from "@/components/names/names-editor"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { requireSession } from "@/lib/access/dal"
import { nameRows } from "@/lib/names/rows"

export const metadata = { title: "Contact names · WhatsApp Stats" }

/** Every account edits its own name mapping here; admins can also edit it from /admin. */
export default async function NamesPage() {
  const { user } = await requireSession()
  const isAdmin = user.role === "admin"
  // Restricted accounts never get automatic-name suggestions: they'd reveal names outside their mapping.
  const rows = await nameRows(user, isAdmin)
  return (
    <div className="h-svh overflow-y-auto">
      <div className="mx-auto flex max-w-4xl flex-col gap-4 p-4 md:p-8">
        <header className="flex items-center gap-2">
          <SidebarTrigger className="md:hidden" />
          <h1 className="text-2xl font-semibold">Contact names</h1>
        </header>
        <NamesEditor userId={user.id} rows={rows} namesOnly={!isAdmin} canReset={isAdmin} />
      </div>
    </div>
  )
}
