import { connection } from "next/server"

import { listChats } from "@/lib/whatsapp/queries"

export async function GET() {
  await connection()
  return Response.json(listChats())
}
