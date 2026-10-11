import Link from "next/link"
import { notFound } from "next/navigation"
import { ArrowLeft } from "lucide-react"

import { AccountDangerZone } from "@/components/admin/account-danger-zone"
import { GrantsEditor } from "@/components/admin/grants-editor"
import { PasswordForm } from "@/components/admin/password-form"
import { UserSettingsForm } from "@/components/admin/user-settings-form"
import { NamesEditor } from "@/components/names/names-editor"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { requireAdmin, withData } from "@/lib/access/dal"
import { countUserSessions, getSnapshotRecord, getUser, listGrants } from "@/lib/auth/store"
import { nameRows } from "@/lib/names/rows"
import { listChats } from "@/lib/whatsapp/queries"

const DAY = 86_400_000

function isoDay(ts: number | null) {
  if (ts == null) return ""
  const d = new Date(ts)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
}

export default async function UserPage(props: PageProps<"/admin/users/[id]">) {
  const { user: me } = await requireAdmin()
  const user = getUser(Number((await props.params).id))
  if (!user) notFound()

  // Admin's data source = the full database, so this lists every chat.
  const chats = (await withData(() => listChats())).map((c) => ({
    jid: c.jid,
    name: c.name,
    kind: c.kind,
    messageCount: c.messageCount,
  }))
  const grants = listGrants(user.id).map((g) => ({
    chatJid: g.chatJid,
    from: isoDay(g.fromTs),
    to: g.toTs == null ? "" : isoDay(g.toTs - DAY),
  }))
  const snapshot = getSnapshotRecord(user.id)
  const names = await nameRows(user, true)

  return (
    <div className="mx-auto flex max-w-5xl flex-col gap-6 p-4 md:p-8">
      <Link href="/admin" className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-4" /> Accounts
      </Link>
      <h1 className="text-2xl font-semibold">{user.username}</h1>

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Account</CardTitle>
          </CardHeader>
          <CardContent>
            <UserSettingsForm user={user} isSelf={user.id === me.id} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Password</CardTitle>
            <CardDescription>Setting a new password signs the account out everywhere.</CardDescription>
          </CardHeader>
          <CardContent>
            <PasswordForm userId={user.id} />
          </CardContent>
        </Card>
      </div>

      {user.role === "admin" ? (
        <Card>
          <CardHeader>
            <CardTitle>Chat access</CardTitle>
            <CardDescription>Admins can see every chat. Change the role to User to restrict this account.</CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Chat access</CardTitle>
            <CardDescription>
              Pick the chats this account can see, optionally limited to a date range. Saving builds the account&apos;s
              filtered copy of the data; nothing outside it is ever readable by this account.
              {snapshot && <> Last built {new Date(snapshot.builtAt).toLocaleString()}.</>}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <GrantsEditor userId={user.id} chats={chats} initial={grants} />
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Contact names</CardTitle>
          <CardDescription>
            The names this account sees, stored in its own mapping file. The account can edit the same list from its
            Contact names page.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <NamesEditor userId={user.id} rows={names} namesOnly={user.role !== "admin"} canReset />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Sessions and deletion</CardTitle>
        </CardHeader>
        <CardContent>
          <AccountDangerZone userId={user.id} isSelf={user.id === me.id} sessions={countUserSessions(user.id)} />
        </CardContent>
      </Card>
    </div>
  )
}
