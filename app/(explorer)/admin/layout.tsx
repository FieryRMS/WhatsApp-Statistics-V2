import { requireAdmin } from "@/lib/access/dal"

export default async function AdminLayout({ children }: { children: React.ReactNode }) {
  await requireAdmin()
  return <div className="h-svh overflow-y-auto bg-background">{children}</div>
}
