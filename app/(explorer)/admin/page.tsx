import Link from "next/link"

import { CreateUserForm } from "@/components/admin/create-user-form"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { requireAdmin } from "@/lib/access/dal"
import { countUserSessions, getSnapshotRecord, listGrants, listUsers } from "@/lib/auth/store"

export const metadata = { title: "Accounts · WhatsApp Stats" }

export default async function AdminPage() {
  await requireAdmin()
  const users = listUsers().map((u) => ({
    ...u,
    grants: new Set(listGrants(u.id).map((g) => g.chatJid)).size,
    sessions: countUserSessions(u.id),
    snapshot: getSnapshotRecord(u.id),
  }))

  return (
    <div className="mx-auto flex max-w-5xl flex-col gap-6 p-4 md:p-8">
      <header className="flex items-center gap-2">
        <SidebarTrigger className="md:hidden" />
        <div>
          <h1 className="text-2xl font-semibold">Accounts</h1>
          <p className="text-sm text-muted-foreground">
            Admins see every chat. Other accounts see only the chats you grant them, served from their own filtered copy
            of the data.
          </p>
        </div>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>All accounts</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Username</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Access</TableHead>
                <TableHead>Media</TableHead>
                <TableHead>Signed in</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {users.map((u) => (
                <TableRow key={u.id}>
                  <TableCell className="font-medium">
                    {u.username}
                    {u.disabled && (
                      <Badge variant="secondary" className="ml-2">
                        Disabled
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell>
                    <Badge variant={u.role === "admin" ? "default" : "outline"}>{u.role === "admin" ? "Admin" : "User"}</Badge>
                  </TableCell>
                  <TableCell>{u.role === "admin" ? "All chats" : `${u.grants} chat${u.grants === 1 ? "" : "s"}`}</TableCell>
                  <TableCell>{u.role === "admin" || u.mediaAccess ? "Yes" : "No"}</TableCell>
                  <TableCell>{u.sessions ? `${u.sessions} session${u.sessions === 1 ? "" : "s"}` : "—"}</TableCell>
                  <TableCell className="text-right">
                    <Link href={`/admin/users/${u.id}`} className="text-sm text-wa-accent hover:underline">
                      Manage
                    </Link>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>New account</CardTitle>
          <CardDescription>After creating it, choose which chats it can see.</CardDescription>
        </CardHeader>
        <CardContent>
          <CreateUserForm />
        </CardContent>
      </Card>
    </div>
  )
}
