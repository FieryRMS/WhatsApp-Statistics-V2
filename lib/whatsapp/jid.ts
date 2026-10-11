import type { ChatKind } from "./types"

export function jidUser(jid: string): string {
  const at = jid.indexOf("@")
  return at === -1 ? jid : jid.slice(0, at)
}

export function jidServer(jid: string): string {
  const at = jid.indexOf("@")
  return at === -1 ? "" : jid.slice(at + 1)
}

export function chatKind(jid: string): ChatKind {
  if (jid === "status@broadcast") return "status"
  switch (jidServer(jid)) {
    case "s.whatsapp.net":
    case "lid":
      return "individual"
    case "g.us":
    case "temp":
      return "group"
    case "broadcast":
      return "broadcast"
    case "newsletter":
      return "channel"
    case "bot":
      return "bot"
    default:
      return "other"
  }
}

/** Phone number for a user JID ("8801712345678@s.whatsapp.net" → "+8801712345678"). */
export function phoneFromJid(jid: string): string | null {
  if (jidServer(jid) !== "s.whatsapp.net") return null
  const user = jidUser(jid)
  return /^\d+$/.test(user) ? `+${user}` : null
}
