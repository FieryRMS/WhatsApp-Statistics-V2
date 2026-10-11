import Link from "next/link"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { ViewerPicker } from "@/components/viewer/viewer-picker"
import { requireSession, withData } from "@/lib/access/dal"
import { peopleWithCounts } from "@/lib/stats/engine"
import { ownerName } from "@/lib/whatsapp/contacts"
import { DEFAULT_OWNER_LABEL } from "@/lib/whatsapp/viewer"

export const metadata = { title: "Settings · WhatsApp Stats" }

export default async function SettingsPage() {
  const session = await requireSession()
  const { people, owner } = await withData(() => ({ people: peopleWithCounts(), owner: ownerName() }))
  return (
    <div className="h-svh overflow-y-auto">
      <div className="mx-auto flex max-w-2xl flex-col gap-6 p-4 md:p-8">
        <header className="flex items-center gap-2">
          <SidebarTrigger className="md:hidden" />
          <h1 className="text-2xl font-semibold">Settings</h1>
        </header>

        <Card>
          <CardHeader>
            <CardTitle>Who&apos;s viewing</CardTitle>
            <CardDescription>
              Your messages show as “You”, on the right. This applies to this sign-in only. Everyone sharing this account
              picks for themselves.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ViewerPicker people={people} current={session.viewerJid} ownerLabel={owner ? `${owner} (phone owner)` : DEFAULT_OWNER_LABEL} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Phone owner&apos;s name</CardTitle>
            <CardDescription>
              The data comes from one person&apos;s phone. Name them in{" "}
              <Link href="/names" className="text-wa-accent hover:underline">
                Contact names
              </Link>{" "}
              (the pinned “Phone owner” row). That name is used in graphs and for anyone else viewing. Giving their other numbers the
              same name merges them into the owner.
            </CardDescription>
          </CardHeader>
        </Card>
      </div>
    </div>
  )
}
