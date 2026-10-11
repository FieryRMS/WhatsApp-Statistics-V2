import { redirect } from "next/navigation"
import { connection } from "next/server"

import { LoginForm } from "@/components/auth/login-form"
import { getSession } from "@/lib/access/dal"
import { needsSetup } from "@/lib/auth/setup"

export const metadata = { title: "Sign in · WhatsApp Stats" }

export default async function LoginPage(props: PageProps<"/login">) {
  await connection() // account state is read per request, never at build time
  if (needsSetup()) redirect("/setup")
  if (await getSession()) redirect("/")
  const next = (await props.searchParams).next
  return <LoginForm next={typeof next === "string" ? next : "/"} />
}
