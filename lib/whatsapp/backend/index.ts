import "server-only"

import { cached, get, hasTable } from "../db"
import { legacyBackend } from "./legacy"
import { modernBackend } from "./modern"
import type { Backend } from "./types"

export type { Backend, RawRow, RawQuote, RawReaction, RawSystem, ChatStat } from "./types"

const maxId = (table: string) => get<{ id: number | null }>(`SELECT MAX(_id) AS id FROM ${table}`)?.id ?? 0

/**
 * Pick the layout this msgstore.db uses. Databases from the migration period
 * contain both tables; whichever actually holds the messages wins.
 */
export function backend(): Backend {
  return cached("backend", () => {
    const legacy = hasTable("messages")
    const modern = hasTable("message")
    if (legacy && (!modern || maxId("messages") >= maxId("message"))) return legacyBackend
    if (modern) return modernBackend
    throw new Error("Unrecognized msgstore.db: neither a `messages` nor a `message` table was found.")
  })
}
