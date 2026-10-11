import { redirect } from "next/navigation"
import { connection } from "next/server"

import { SetupForm } from "@/components/auth/setup-form"
import { needsSetup } from "@/lib/auth/setup"

export const metadata = { title: "Set up · WhatsApp Stats" }

export default async function SetupPage() {
  await connection() // account state is read per request, never at build time
  if (!needsSetup()) redirect("/login")
  return <SetupForm />
}
