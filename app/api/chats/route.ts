import { apiWithData } from "@/lib/access/dal"
import { listChats } from "@/lib/whatsapp/queries"

export async function GET() {
  return apiWithData(() => listChats())
}
