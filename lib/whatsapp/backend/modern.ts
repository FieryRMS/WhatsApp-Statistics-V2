/**
 * Modern layout (~2022+): `message` + side tables (message_media, message_system,
 * message_quoted, message_location, message_poll, …). Senders may be LIDs (…@lid);
 * contacts.ts maps those to phone JIDs via jid_map.
 *
 * Side tables vary between WhatsApp versions, so every one is optional: a
 * missing table just yields NULLs.
 */
import { all, cached, get, hasTable, placeholders } from "../db"
import { WaType } from "../constants"
import { chatKind } from "../jid"
import { emptyRow, type Backend, type ChatStat, type RawQuote, type RawReaction, type RawRow } from "./types"

/** message_type values introduced with the modern schema. */
export const ModernType = {
  SYSTEM: 7,
  LIVE_LOCATION: 16,
  ADMIN_REVOKED: 64,
  POLL: 66,
  CALL: 90,
  ALBUM: 99,
} as const

interface Row {
  id: number
  chat_jid: string
  from_me: number
  key_id: string
  status: number | null
  timestamp: number
  type: number | null
  text: string | null
  starred: number | null
  origin: number | null
  sender_jid: string | null
  has_media: number | null
  mime: string | null
  size: number | null
  width: number | null
  height: number | null
  duration: number | null
  media_name: string | null
  file_path: string | null
  page_count: number | null
  animated: number | null
  media_caption: string | null
  has_thumb: number | null
  latitude: number | null
  longitude: number | null
  place_name: string | null
  place_address: string | null
  has_link: number | null
  link_title: string | null
  link_desc: string | null
  link_url: string | null
  forward_score: number | null
  is_forwarded: number | null
  vo_state: number | null
  missed_video: number | null
  call_video: number | null
  call_duration: number | null
  call_result: number | null
  sys_action: number | null
  sys_old: string | null
  num_from: string | null
  num_to: string | null
  lgc_video: number | null
  eph_duration: number | null
  edited: number | null
  revoked: number | null
  revoke_admin: string | null
  has_quote: number | null
  album_images: number | null
  album_videos: number | null
  poll_selectable: number | null
  is_poll: number | null
}

interface Part {
  /** Tables that must all exist for this part to be included. */
  tables: string[]
  join?: string
  cols: [expr: string, alias: keyof Row][]
}

const PARTS: Part[] = [
  {
    tables: ["message_media"],
    join: "LEFT JOIN message_media mm ON mm.message_row_id = m._id",
    cols: [
      ["mm.message_row_id IS NOT NULL", "has_media"],
      ["mm.mime_type", "mime"],
      ["COALESCE(NULLIF(mm.file_size, 0), NULLIF(mm.file_length, 0))", "size"],
      ["mm.width", "width"],
      ["mm.height", "height"],
      ["mm.media_duration", "duration"],
      ["mm.media_name", "media_name"],
      ["mm.file_path", "file_path"],
      ["mm.page_count", "page_count"],
      ["mm.is_animated_sticker", "animated"],
      ["mm.media_caption", "media_caption"],
    ],
  },
  {
    tables: ["message_thumbnail"],
    cols: [["EXISTS (SELECT 1 FROM message_thumbnail th WHERE th.message_row_id = m._id)", "has_thumb"]],
  },
  {
    tables: ["message_location"],
    join: "LEFT JOIN message_location loc ON loc.message_row_id = m._id",
    cols: [
      ["loc.latitude", "latitude"],
      ["loc.longitude", "longitude"],
      ["loc.place_name", "place_name"],
      ["loc.place_address", "place_address"],
    ],
  },
  {
    tables: ["message_text"],
    join: "LEFT JOIN message_text mt ON mt.message_row_id = m._id",
    cols: [
      ["mt.message_row_id IS NOT NULL AND (mt.page_title IS NOT NULL OR mt.description IS NOT NULL OR mt.url IS NOT NULL)", "has_link"],
      ["mt.page_title", "link_title"],
      ["mt.description", "link_desc"],
      ["mt.url", "link_url"],
    ],
  },
  {
    tables: ["message_forwarded"],
    cols: [
      ["(SELECT forward_score FROM message_forwarded f WHERE f.message_row_id = m._id LIMIT 1)", "forward_score"],
      ["EXISTS (SELECT 1 FROM message_forwarded f WHERE f.message_row_id = m._id)", "is_forwarded"],
    ],
  },
  {
    tables: ["message_view_once_media"],
    cols: [["(SELECT state FROM message_view_once_media v WHERE v.message_row_id = m._id LIMIT 1)", "vo_state"]],
  },
  {
    tables: ["missed_call_logs"],
    cols: [["(SELECT video_call FROM missed_call_logs mc WHERE mc.message_row_id = m._id LIMIT 1)", "missed_video"]],
  },
  {
    tables: ["message_call_log", "call_log"],
    join: `LEFT JOIN call_log cl ON m.message_type = ${ModernType.CALL}
             AND cl._id = (SELECT call_log_row_id FROM message_call_log x WHERE x.message_row_id = m._id LIMIT 1)`,
    cols: [
      ["cl.video_call", "call_video"],
      ["cl.duration", "call_duration"],
      ["cl.call_result", "call_result"],
    ],
  },
  {
    tables: ["message_system"],
    join: "LEFT JOIN message_system s ON s.message_row_id = m._id",
    cols: [["s.action_type", "sys_action"]],
  },
  {
    tables: ["message_system_value_change"],
    cols: [["(SELECT old_data FROM message_system_value_change v WHERE v.message_row_id = m._id LIMIT 1)", "sys_old"]],
  },
  {
    tables: ["message_system_number_change"],
    join: `LEFT JOIN message_system_number_change nc ON nc.message_row_id = m._id
           LEFT JOIN jid nf ON nf._id = nc.old_jid_row_id
           LEFT JOIN jid nt ON nt._id = nc.new_jid_row_id`,
    cols: [
      ["nf.raw_string", "num_from"],
      ["nt.raw_string", "num_to"],
    ],
  },
  {
    tables: ["message_system_linked_group_call"],
    cols: [["(SELECT is_video_call FROM message_system_linked_group_call g WHERE g.message_row_id = m._id LIMIT 1)", "lgc_video"]],
  },
  {
    tables: ["message_ephemeral_setting"],
    cols: [["(SELECT setting_duration FROM message_ephemeral_setting e WHERE e.message_row_id = m._id LIMIT 1)", "eph_duration"]],
  },
  {
    tables: ["message_edit_info"],
    cols: [["EXISTS (SELECT 1 FROM message_edit_info ei WHERE ei.message_row_id = m._id)", "edited"]],
  },
  {
    tables: ["message_revoked"],
    join: `LEFT JOIN message_revoked rv ON rv.message_row_id = m._id
           LEFT JOIN jid rj ON rj._id = rv.admin_jid_row_id`,
    cols: [
      ["rv.message_row_id IS NOT NULL", "revoked"],
      ["rj.raw_string", "revoke_admin"],
    ],
  },
  {
    tables: ["message_quoted"],
    cols: [["EXISTS (SELECT 1 FROM message_quoted q WHERE q.message_row_id = m._id)", "has_quote"]],
  },
  {
    tables: ["message_album"],
    join: "LEFT JOIN message_album al ON al.message_row_id = m._id",
    cols: [
      ["al.image_count", "album_images"],
      ["al.video_count", "album_videos"],
    ],
  },
  {
    tables: ["message_poll"],
    join: "LEFT JOIN message_poll p ON p.message_row_id = m._id",
    cols: [
      ["p.selectable_options_count", "poll_selectable"],
      ["p.message_row_id IS NOT NULL", "is_poll"],
    ],
  },
]

function selectSql(): string {
  return cached("modern:select", () => {
    const cols: string[] = []
    const joins: string[] = []
    for (const part of PARTS) {
      const ok = part.tables.every(hasTable)
      if (ok && part.join) joins.push(part.join)
      for (const [expr, alias] of part.cols) cols.push(`${ok ? expr : "NULL"} AS ${alias}`)
    }
    return `
SELECT
  m._id AS id, cj.raw_string AS chat_jid, m.from_me, m.key_id, m.status, m.timestamp,
  m.message_type AS type, m.text_data AS text, m.starred, m.origin, sj.raw_string AS sender_jid,
  ${cols.join(",\n  ")}
FROM message m
JOIN chat c ON c._id = m.chat_row_id
JOIN jid cj ON cj._id = c.jid_row_id
LEFT JOIN jid sj ON sj._id = m.sender_jid_row_id
${joins.join("\n")}`
  })
}

/** jid → chat._id */
function chatIds(): Map<string, number> {
  return cached(
    "modern:chatIds",
    () => new Map(all<{ id: number; jid: string }>(`SELECT c._id AS id, j.raw_string AS jid FROM chat c JOIN jid j ON j._id = c.jid_row_id`).map((r) => [r.jid, r.id]))
  )
}

const chatId = (jid: string) => chatIds().get(jid) ?? -1

function groupBy<T extends { id: number }>(rows: T[]): Map<number, T[]> {
  const out = new Map<number, T[]>()
  for (const r of rows) out.set(r.id, [...(out.get(r.id) ?? []), r])
  return out
}

/** Batch-load the one-to-many side data for a page of rows. */
function sideData(rows: Row[]) {
  const ids = (pred: (r: Row) => boolean) => rows.filter(pred).map((r) => r.id)
  const systemIds = ids((r) => r.sys_action != null)
  const pollIds = ids((r) => !!r.is_poll || r.type === ModernType.POLL)
  const vcardIds = ids((r) => r.type === WaType.CONTACT || r.type === WaType.CONTACT_LIST)
  const allIds = rows.map((r) => r.id)

  const participants =
    systemIds.length && hasTable("message_system_chat_participant")
      ? groupBy(
          all<{ id: number; jid: string }>(
            `SELECT p.message_row_id AS id, j.raw_string AS jid FROM message_system_chat_participant p
             JOIN jid j ON j._id = p.user_jid_row_id WHERE p.message_row_id IN (${placeholders(systemIds.length)})`,
            ...systemIds
          )
        )
      : new Map<number, { id: number; jid: string }[]>()

  const mentions =
    allIds.length && hasTable("message_mentions")
      ? groupBy(
          all<{ id: number; jid: string }>(
            `SELECT mn.message_row_id AS id, j.raw_string AS jid FROM message_mentions mn
             JOIN jid j ON j._id = mn.jid_row_id WHERE mn.message_row_id IN (${placeholders(allIds.length)})`,
            ...allIds
          )
        )
      : new Map<number, { id: number; jid: string }[]>()

  const pollOptions =
    pollIds.length && hasTable("message_poll_option")
      ? groupBy(
          all<{ id: number; name: string; votes: number | null }>(
            `SELECT message_row_id AS id, option_name AS name, vote_total AS votes FROM message_poll_option
             WHERE message_row_id IN (${placeholders(pollIds.length)}) ORDER BY _id`,
            ...pollIds
          )
        )
      : new Map<number, { id: number; name: string; votes: number | null }[]>()

  const vcards =
    vcardIds.length && hasTable("message_vcard")
      ? groupBy(
          all<{ id: number; vcard: string }>(
            `SELECT message_row_id AS id, vcard FROM message_vcard WHERE message_row_id IN (${placeholders(vcardIds.length)}) ORDER BY _id`,
            ...vcardIds
          )
        )
      : new Map<number, { id: number; vcard: string }[]>()

  return { participants, mentions, pollOptions, vcards }
}

function toRaw(r: Row, side: ReturnType<typeof sideData>): RawRow {
  const type = r.type ?? -1
  const fromMe = !!r.from_me
  const isSystem = r.sys_action != null
  const individual = chatKind(r.chat_jid) === "individual"
  const hasMedia = !!r.has_media
  const text = r.text || null

  return emptyRow({
    id: r.id,
    chatJid: r.chat_jid,
    fromMe,
    keyId: r.key_id,
    status: r.status,
    timestamp: r.timestamp,
    type,
    text: hasMedia ? null : text,
    senderJid: fromMe || isSystem ? null : (r.sender_jid ?? (individual ? r.chat_jid : null)),
    // For media, text_data is the caption. Documents usually repeat the filename there.
    caption: hasMedia
      ? type === WaType.DOCUMENT
        ? text && text !== r.media_name
          ? text
          : null
        : text || r.media_caption || null
      : null,
    media: hasMedia
      ? {
          mime: r.mime,
          size: r.size || null,
          width: r.width || null,
          height: r.height || null,
          duration: r.duration || null,
          name: r.media_name,
          path: r.file_path,
          pageCount: r.page_count || null,
          animated: !!r.animated,
        }
      : null,
    hasThumb: !!r.has_thumb,
    location:
      r.latitude != null || r.longitude != null
        ? { latitude: r.latitude ?? 0, longitude: r.longitude ?? 0, name: r.place_name, address: r.place_address }
        : null,
    link: r.has_link ? { title: r.link_title, description: r.link_desc, url: r.link_url } : null,
    starred: !!r.starred,
    forwarded: !!r.is_forwarded,
    forwardScore: r.forward_score,
    viewOnceState: r.vo_state,
    voiceNote: type === WaType.AUDIO && (r.origin === 1 || (r.mime ?? "").includes("opus")),
    missedVideo: type === WaType.MISSED_CALL ? !!r.missed_video : null,
    call:
      type === ModernType.CALL
        ? { video: !!r.call_video, duration: r.call_duration, result: r.call_result }
        : null,
    poll: r.is_poll || type === ModernType.POLL
      ? {
          selectable: r.poll_selectable,
          options: (side.pollOptions.get(r.id) ?? []).map((o) => ({ name: o.name, votes: o.votes ?? 0 })),
        }
      : null,
    album: type === ModernType.ALBUM ? { images: r.album_images ?? 0, videos: r.album_videos ?? 0 } : null,
    vcards: (side.vcards.get(r.id) ?? []).map((v) => v.vcard),
    system: isSystem
      ? {
          action: r.sys_action!,
          actor: r.sender_jid,
          targets: (side.participants.get(r.id) ?? []).map((p) => p.jid),
          value: text,
          oldValue: r.sys_old,
          numberFrom: r.num_from,
          numberTo: r.num_to,
          video: !!r.lgc_video,
          duration: r.eph_duration,
          count: null,
        }
      : null,
    edited: !!r.edited,
    revoked: !!r.revoked || type === WaType.REVOKED || type === ModernType.ADMIN_REVOKED,
    revokedByAdmin: r.revoke_admin,
    quoteKey: r.has_quote ? r.id : null,
    mentions: (side.mentions.get(r.id) ?? []).map((m) => m.jid),
    ephemeralDuration: r.eph_duration,
  })
}

function load(sql: string, ...params: (string | number)[]): RawRow[] {
  const rows = all<Row>(sql, ...params)
  const side = sideData(rows)
  return rows.map((r) => toRaw(r, side))
}

export const modernBackend: Backend = {
  name: "modern",

  chats(): ChatStat[] {
    return all<{
      jid: string
      n: number
      real_n: number
      last_id: number
      last_real_id: number | null
      subject: string | null
      archived: number | null
      created: number | null
    }>(
      `SELECT j.raw_string AS jid, x.n, x.real_n, x.last_id, x.last_real_id, c.subject, c.archived, c.created_timestamp AS created
       FROM (
         SELECT chat_row_id, COUNT(*) AS n, MAX(_id) AS last_id,
           SUM(message_type IS NOT ${ModernType.SYSTEM}) AS real_n,
           MAX(CASE WHEN message_type IS NOT ${ModernType.SYSTEM} THEN _id END) AS last_real_id
         FROM message GROUP BY chat_row_id
       ) x
       JOIN chat c ON c._id = x.chat_row_id
       JOIN jid j ON j._id = c.jid_row_id`
    ).map((r) => ({
      jid: r.jid,
      count: r.n,
      realCount: r.real_n,
      lastId: r.last_id,
      lastRealId: r.last_real_id,
      subject: r.subject || null,
      archived: !!r.archived,
      createdAt: r.created || null,
    }))
  },

  range(jid) {
    return (
      get<{ first: number | null; last: number | null }>(
        `SELECT
           (SELECT timestamp FROM message WHERE chat_row_id = ?1 ORDER BY _id ASC LIMIT 1) AS first,
           (SELECT timestamp FROM message WHERE chat_row_id = ?1 ORDER BY _id DESC LIMIT 1) AS last`,
        chatId(jid)
      ) ?? { first: null, last: null }
    )
  },

  byIds(ids) {
    if (!ids.length) return []
    return load(`${selectSql()} WHERE m._id IN (${placeholders(ids.length)})`, ...ids)
  },

  older(jid, id, limit) {
    return load(`${selectSql()} WHERE m.chat_row_id = ? AND m._id < ? ORDER BY m._id DESC LIMIT ?`, chatId(jid), id, limit)
  },

  newer(jid, id, limit) {
    return load(`${selectSql()} WHERE m.chat_row_id = ? AND m._id > ? ORDER BY m._id ASC LIMIT ?`, chatId(jid), id, limit)
  },

  locate(jid, at) {
    const cid = chatId(jid)
    const hit = get<{ id: number | null }>(`SELECT MIN(_id) AS id FROM message WHERE chat_row_id = ? AND timestamp >= ?`, cid, at)
    if (hit?.id != null) return hit.id
    return get<{ id: number | null }>(`SELECT MAX(_id) AS id FROM message WHERE chat_row_id = ?`, cid)?.id ?? null
  },

  contains(jid, id) {
    return !!get(`SELECT 1 FROM message WHERE _id = ? AND chat_row_id = ?`, id, chatId(jid))
  },

  reactions(ids): RawReaction[] {
    if (!ids.length || !hasTable("message_add_on") || !hasTable("message_add_on_reaction")) return []
    return all<{ pid: number; from_me: number; sender: string | null; reaction: string; ts: number }>(
      `SELECT a.parent_message_row_id AS pid, a.from_me, j.raw_string AS sender, r.reaction, a.timestamp AS ts
       FROM message_add_on a
       JOIN message_add_on_reaction r ON r.message_add_on_row_id = a._id
       LEFT JOIN jid j ON j._id = a.sender_jid_row_id
       WHERE a.parent_message_row_id IN (${placeholders(ids.length)}) AND r.reaction IS NOT NULL AND r.reaction != ''
       ORDER BY a.timestamp`,
      ...ids
    ).map((r) => ({ messageId: r.pid, emoji: r.reaction, senderJid: r.from_me ? null : r.sender, timestamp: r.ts }))
  },

  quotes(keys) {
    const out = new Map<number, RawQuote>()
    if (!keys.length || !hasTable("message_quoted")) return out
    const qm = hasTable("message_quoted_media")
    const qt = hasTable("message_quoted_text")
    const rows = all<{
      id: number
      from_me: number
      type: number | null
      text: string | null
      sender: string | null
      chat_jid: string
      target_id: number | null
      caption: string | null
      file_name: string | null
      has_thumb: number
    }>(
      `SELECT q.message_row_id AS id, q.from_me, q.message_type AS type, q.text_data AS text,
         sj.raw_string AS sender, cj.raw_string AS chat_jid,
         (SELECT t._id FROM message t
           WHERE t.chat_row_id = q.parent_message_chat_row_id AND t.from_me = q.from_me AND t.key_id = q.key_id
           LIMIT 1) AS target_id,
         ${qm ? "qm.media_caption" : "NULL"} AS caption,
         ${qm ? "qm.media_name" : "NULL"} AS file_name,
         (${qm ? "qm.thumbnail IS NOT NULL" : "0"} OR ${qt ? "qt.thumbnail IS NOT NULL" : "0"}) AS has_thumb
       FROM message_quoted q
       JOIN chat c ON c._id = q.parent_message_chat_row_id
       JOIN jid cj ON cj._id = c.jid_row_id
       LEFT JOIN jid sj ON sj._id = q.sender_jid_row_id
       ${qm ? "LEFT JOIN message_quoted_media qm ON qm.message_row_id = q.message_row_id" : ""}
       ${qt ? "LEFT JOIN message_quoted_text qt ON qt.message_row_id = q.message_row_id" : ""}
       WHERE q.message_row_id IN (${placeholders(keys.length)})`,
      ...keys
    )
    for (const r of rows) {
      const individual = chatKind(r.chat_jid) === "individual"
      out.set(r.id, {
        targetId: r.target_id,
        fromMe: !!r.from_me,
        senderJid: r.from_me ? null : (r.sender ?? (individual ? r.chat_jid : null)),
        chatJid: r.chat_jid,
        type: r.type ?? -1,
        text: r.text,
        caption: r.caption,
        fileName: r.file_name,
        thumbnailUrl: r.has_thumb ? `/api/messages/${r.id}/thumbnail?quoted=1` : null,
      })
    }
    return out
  },

  thumbnail(id) {
    if (hasTable("message_thumbnail")) {
      const th = get<{ thumbnail: Uint8Array | null }>(`SELECT thumbnail FROM message_thumbnail WHERE message_row_id = ?`, id)
      if (th?.thumbnail?.length) return th.thumbnail
    }
    if (hasTable("message_media") && hasTable("media_hash_thumbnail")) {
      const h = get<{ thumbnail: Uint8Array | null }>(
        `SELECT h.thumbnail FROM message_media mm JOIN media_hash_thumbnail h ON h.media_hash = mm.file_hash
         WHERE mm.message_row_id = ?`,
        id
      )
      if (h?.thumbnail?.length) return h.thumbnail
    }
    return null
  },

  quotedThumbnail(key) {
    for (const table of ["message_quoted_media", "message_quoted_text"]) {
      if (!hasTable(table)) continue
      const t = get<{ thumbnail: Uint8Array | null }>(`SELECT thumbnail FROM ${table} WHERE message_row_id = ?`, key)
      if (t?.thumbnail?.length) return t.thumbnail
    }
    return null
  },

  mediaFile(id) {
    if (!hasTable("message_media")) return null
    return get<{ path: string | null; mime: string | null }>(`SELECT file_path AS path, mime_type AS mime FROM message_media WHERE message_row_id = ?`, id) ?? null
  },
}
