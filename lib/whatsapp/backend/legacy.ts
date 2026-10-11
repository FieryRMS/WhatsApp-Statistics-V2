import "server-only"

/**
 * Legacy layout: all messages in `messages` (Android, until ~2022).
 * See docs/msgstore-schema.md.
 */
import { all, get, hasTable, placeholders } from "../db"
import { Status, SystemAction, WaType } from "../constants"
import { decodeJava, javaStrings } from "../decoders"
import { chatKind } from "../jid"
import { emptyRow, type Backend, type ChatStat, type RawQuote, type RawReaction, type RawRow, type RawSystem } from "./types"

interface Row {
  id: number
  jid: string
  from_me: number
  key_id: string
  status: number | null
  data: string | null
  timestamp: number
  wa_type: number
  mime: string | null
  media_size: number | null
  media_name: string | null
  media_caption: string | null
  media_duration: number | null
  origin: number | null
  latitude: number | null
  longitude: number | null
  remote_resource: string | null
  starred: number | null
  quoted_row_id: number | null
  mentioned_jids: string | null
  forwarded: number | null
  sys_payload: Uint8Array | null
  raw: Uint8Array | null
  has_link_thumb: number
  has_thumb: number
  mm_width: number | null
  mm_height: number | null
  mm_file_path: string | null
  mm_file_size: number | null
  mm_page_count: number | null
  mm_animated: number | null
  mm_name: string | null
  mm_mime: string | null
  mm_duration: number | null
  view_once_state: number | null
  missed_video: number | null
  forward_score: number | null
}

const opt = (table: string, sql: string, fallback: string) => (hasTable(table) ? sql : fallback)

function selectSql() {
  return `
SELECT
  m._id AS id, m.key_remote_jid AS jid, m.key_from_me AS from_me, m.key_id, m.status, m.data,
  m.timestamp, CAST(m.media_wa_type AS INTEGER) AS wa_type, m.media_mime_type AS mime,
  m.media_size, m.media_name, m.media_caption, m.media_duration, m.origin,
  m.latitude, m.longitude, m.remote_resource, m.starred, m.quoted_row_id, m.mentioned_jids, m.forwarded,
  CASE WHEN m.status = 6 THEN m.thumb_image END AS sys_payload,
  CASE WHEN m.media_wa_type IN ('12', '14') THEN m.raw_data END AS raw,
  (m.media_wa_type = '0' AND substr(m.thumb_image, 1, 6) = X'ACED00057572') AS has_link_thumb,
  (${opt("message_thumbnails", "th.key_id IS NOT NULL", "0")} OR ${opt("media_hash_thumbnail", "h.media_hash IS NOT NULL", "0")}) AS has_thumb,
  ${opt(
    "message_media",
    `mm.width AS mm_width, mm.height AS mm_height, mm.file_path AS mm_file_path, mm.file_size AS mm_file_size,
     mm.page_count AS mm_page_count, mm.is_animated_sticker AS mm_animated, mm.media_name AS mm_name,
     mm.mime_type AS mm_mime, mm.media_duration AS mm_duration`,
    `NULL AS mm_width, NULL AS mm_height, NULL AS mm_file_path, NULL AS mm_file_size, NULL AS mm_page_count,
     NULL AS mm_animated, NULL AS mm_name, NULL AS mm_mime, NULL AS mm_duration`
  )},
  ${opt("message_view_once_media", "vo.state", "NULL")} AS view_once_state,
  ${opt("missed_call_logs", "mc.video_call", "NULL")} AS missed_video,
  ${opt("message_forwarded", "mf.forward_score", "NULL")} AS forward_score
FROM messages m
${opt("message_media", "LEFT JOIN message_media mm ON mm.message_row_id = m._id", "")}
${opt(
  "message_thumbnails",
  `LEFT JOIN message_thumbnails th
     ON th.key_remote_jid = m.key_remote_jid AND th.key_from_me = m.key_from_me AND th.key_id = m.key_id`,
  ""
)}
${opt("media_hash_thumbnail", "LEFT JOIN media_hash_thumbnail h ON m.media_wa_type = '20' AND h.media_hash = m.media_hash", "")}
${opt("message_view_once_media", "LEFT JOIN message_view_once_media vo ON vo.message_row_id = m._id", "")}
${opt("missed_call_logs", "LEFT JOIN missed_call_logs mc ON mc.message_row_id = m._id", "")}
${opt("message_forwarded", "LEFT JOIN message_forwarded mf ON mf.message_row_id = m._id", "")}`
}

function systemOf(r: Row): RawSystem {
  const code = r.media_size ?? -1
  const listed = javaStrings(r.sys_payload)
  const base: RawSystem = {
    action: code,
    actor: r.remote_resource || null,
    targets: [],
    value: r.data,
    oldValue: null,
    numberFrom: null,
    numberTo: null,
    video: false,
    duration: null,
    count: null,
  }
  switch (code) {
    case SystemAction.SUBJECT_CHANGED:
      return { ...base, oldValue: listed[0] ?? null }
    case SystemAction.NUMBER_CHANGED_GROUP:
      return { ...base, numberFrom: r.remote_resource || null, numberTo: listed[0] ?? null }
    case SystemAction.NUMBER_CHANGED:
      return { ...base, numberFrom: r.media_name, numberTo: r.media_caption }
    case SystemAction.GROUP_CALL:
      return { ...base, video: r.media_caption === "video" }
    case SystemAction.BROADCAST_CREATED:
      return { ...base, count: listed.length }
    default:
      return { ...base, targets: listed }
  }
}

function toRaw(r: Row): RawRow {
  const isSystem = r.status === Status.SYSTEM && r.wa_type !== WaType.MISSED_CALL
  const isMissedCall = r.wa_type === WaType.MISSED_CALL
  // Missed calls are stored as from_me = 1 like other system rows, but the caller is the other side.
  const fromMe = !!r.from_me && !isMissedCall
  const individual = chatKind(r.jid) === "individual"
  const hasMedia = r.mm_file_path != null || r.mm_mime != null || (r.mime != null && r.wa_type !== WaType.TEXT)
  const isText = r.wa_type === WaType.TEXT

  return emptyRow({
    id: r.id,
    chatJid: r.jid,
    fromMe,
    keyId: r.key_id,
    status: r.status,
    timestamp: r.timestamp,
    type: r.wa_type,
    text: r.data || null,
    senderJid: fromMe || isSystem ? null : individual ? r.jid : r.remote_resource || null,
    // Documents mirror the filename in media_caption; it's only a caption if it differs.
    caption:
      r.wa_type === WaType.DOCUMENT
        ? r.media_caption && r.media_caption !== (r.mm_name ?? r.media_name)
          ? r.media_caption
          : null
        : isText
          ? null
          : r.media_caption || null,
    media: hasMedia
      ? {
          mime: r.mm_mime ?? r.mime,
          size: r.mm_file_size || r.media_size || null,
          width: r.mm_width || null,
          height: r.mm_height || null,
          duration: r.mm_duration || r.media_duration || null,
          name: r.mm_name ?? r.media_caption ?? r.media_name,
          path: r.mm_file_path,
          pageCount: r.mm_page_count || null,
          animated: !!r.mm_animated,
        }
      : null,
    hasThumb: !!r.has_thumb || !!r.has_link_thumb,
    location:
      r.wa_type === WaType.LOCATION
        ? {
            latitude: r.latitude ?? 0,
            longitude: r.longitude ?? 0,
            // media_name is "name\naddress"
            name: r.media_name?.split("\n")[0] || null,
            address: r.media_name?.split("\n").slice(1).join(", ") || r.media_caption || null,
          }
        : null,
    link:
      isText && !isSystem && (r.media_caption || r.media_name || r.has_link_thumb)
        ? { title: r.media_caption, description: r.media_name, url: null }
        : null,
    starred: !!r.starred,
    forwarded: !!r.forwarded || r.forward_score != null,
    forwardScore: r.forward_score ?? (r.forwarded ? r.forwarded : null),
    viewOnceState: r.view_once_state,
    // origin 1 = recorded push-to-talk; opus is usually a voice note too
    voiceNote: r.origin === 1 || (r.mime ?? "").includes("opus"),
    missedVideo: isMissedCall ? !!r.missed_video : null,
    vcards:
      r.wa_type === WaType.CONTACT && r.data ? [r.data] : r.wa_type === WaType.CONTACT_LIST ? javaStrings(r.raw) : [],
    payment: r.wa_type === WaType.PAYMENT ? r.raw : null,
    system: isSystem ? systemOf(r) : null,
    revoked: r.wa_type === WaType.REVOKED,
    quoteKey: r.quoted_row_id && r.quoted_row_id > 0 ? r.quoted_row_id : null,
    mentions: r.mentioned_jids ? r.mentioned_jids.split(",").map((j) => j.trim()).filter(Boolean) : [],
  })
}

export const legacyBackend: Backend = {
  name: "legacy",

  chats(): ChatStat[] {
    const meta = new Map(
      all<{ jid: string; subject: string | null; archived: number | null; created: number | null }>(
        `SELECT j.raw_string AS jid, c.subject, c.archived, c.created_timestamp AS created
         FROM chat c JOIN jid j ON j._id = c.jid_row_id`
      ).map((r) => [r.jid, r])
    )
    const real = `(status IS NOT 6 OR media_wa_type = '${WaType.MISSED_CALL}')`
    return all<{ jid: string; n: number; real_n: number; last_id: number; last_real_id: number | null }>(
      `SELECT key_remote_jid AS jid, COUNT(*) AS n, MAX(_id) AS last_id,
         SUM(${real}) AS real_n, MAX(CASE WHEN ${real} THEN _id END) AS last_real_id
       FROM messages WHERE key_remote_jid != '-1' GROUP BY key_remote_jid`
    ).map((c) => {
      const info = meta.get(c.jid)
      return {
        jid: c.jid,
        count: c.n,
        realCount: c.real_n,
        lastId: c.last_id,
        lastRealId: c.last_real_id,
        subject: info?.subject || null,
        archived: !!info?.archived,
        createdAt: info?.created || null,
      }
    })
  },

  range(jid) {
    return (
      get<{ first: number | null; last: number | null }>(
        `SELECT
           (SELECT timestamp FROM messages WHERE key_remote_jid = ?1 ORDER BY _id ASC LIMIT 1) AS first,
           (SELECT timestamp FROM messages WHERE key_remote_jid = ?1 ORDER BY _id DESC LIMIT 1) AS last`,
        jid
      ) ?? { first: null, last: null }
    )
  },

  byIds(ids) {
    if (!ids.length) return []
    return all<Row>(`${selectSql()} WHERE m._id IN (${placeholders(ids.length)})`, ...ids).map(toRaw)
  },

  older(jid, id, limit) {
    return all<Row>(`${selectSql()} WHERE m.key_remote_jid = ? AND m._id < ? ORDER BY m._id DESC LIMIT ?`, jid, id, limit).map(toRaw)
  },

  newer(jid, id, limit) {
    return all<Row>(`${selectSql()} WHERE m.key_remote_jid = ? AND m._id > ? ORDER BY m._id ASC LIMIT ?`, jid, id, limit).map(toRaw)
  },

  locate(jid, at) {
    const hit = get<{ id: number | null }>(`SELECT MIN(_id) AS id FROM messages WHERE key_remote_jid = ? AND timestamp >= ?`, jid, at)
    if (hit?.id != null) return hit.id
    return get<{ id: number | null }>(`SELECT MAX(_id) AS id FROM messages WHERE key_remote_jid = ?`, jid)?.id ?? null
  },

  contains(jid, id) {
    return !!get(`SELECT 1 FROM messages WHERE _id = ? AND key_remote_jid = ?`, id, jid)
  },

  reactions(ids): RawReaction[] {
    if (!ids.length || !hasTable("message_add_on") || !hasTable("message_add_on_reaction")) return []
    return all<{ pid: number; from_me: number; sender: string | null; reaction: string; ts: number }>(
      `SELECT a.parent_message_row_id AS pid, a.from_me, j.raw_string AS sender, r.reaction, a.timestamp AS ts
       FROM message_add_on a
       JOIN message_add_on_reaction r ON r.message_add_on_row_id = a._id
       LEFT JOIN jid j ON j._id = a.sender_jid_row_id
       WHERE a.parent_message_row_id IN (${placeholders(ids.length)}) AND r.reaction != ''
       ORDER BY a.timestamp`,
      ...ids
    ).map((r) => ({ messageId: r.pid, emoji: r.reaction, senderJid: r.from_me ? null : r.sender, timestamp: r.ts }))
  },

  quotes(keys) {
    const out = new Map<number, RawQuote>()
    if (!keys.length || !hasTable("messages_quotes")) return out
    // messages_quotes is a copy of the quoted row with the same columns as `messages`.
    const rows = all<{
      id: number
      jid: string
      from_me: number
      remote_resource: string | null
      wa_type: number
      data: string | null
      media_caption: string | null
      media_name: string | null
      target_id: number | null
      target_has_thumb: number
    }>(
      `SELECT q._id AS id, q.key_remote_jid AS jid, q.key_from_me AS from_me, q.remote_resource,
         CAST(q.media_wa_type AS INTEGER) AS wa_type, q.data, q.media_caption, q.media_name,
         t._id AS target_id,
         ${opt(
           "message_thumbnails",
           `(t._id IS NOT NULL AND EXISTS (
              SELECT 1 FROM message_thumbnails th
              WHERE th.key_remote_jid = t.key_remote_jid AND th.key_from_me = t.key_from_me AND th.key_id = t.key_id))`,
           "0"
         )} AS target_has_thumb
       FROM messages_quotes q
       LEFT JOIN messages t
         ON t.key_remote_jid = q.key_remote_jid AND t.key_from_me = q.key_from_me AND t.key_id = q.key_id
       WHERE q._id IN (${placeholders(keys.length)})`,
      ...keys
    )
    for (const r of rows) {
      out.set(r.id, {
        targetId: r.target_id,
        fromMe: !!r.from_me,
        senderJid: r.from_me ? null : chatKind(r.jid) === "individual" ? r.jid : r.remote_resource,
        chatJid: r.jid,
        type: r.wa_type,
        text: r.data,
        caption: r.media_caption,
        fileName: r.media_name,
        thumbnailUrl: r.target_id && r.target_has_thumb ? `/api/messages/${r.target_id}/thumbnail` : null,
      })
    }
    return out
  },

  thumbnail(id) {
    const row = get<{ jid: string; from_me: number; key_id: string; media_hash: string | null; thumb: Uint8Array | null }>(
      `SELECT key_remote_jid AS jid, key_from_me AS from_me, key_id, media_hash,
         CASE WHEN media_wa_type = '0' THEN thumb_image END AS thumb
       FROM messages WHERE _id = ?`,
      id
    )
    if (!row) return null
    if (hasTable("message_thumbnails")) {
      const th = get<{ thumbnail: Uint8Array | null }>(
        `SELECT thumbnail FROM message_thumbnails WHERE key_remote_jid = ? AND key_from_me = ? AND key_id = ?`,
        row.jid,
        row.from_me,
        row.key_id
      )
      if (th?.thumbnail?.length) return th.thumbnail
    }
    if (row.media_hash && hasTable("media_hash_thumbnail")) {
      const h = get<{ thumbnail: Uint8Array | null }>(`SELECT thumbnail FROM media_hash_thumbnail WHERE media_hash = ?`, row.media_hash)
      if (h?.thumbnail?.length) return h.thumbnail
    }
    // Link previews: Java-serialized byte[] JPEG in thumb_image
    const decoded = decodeJava(row.thumb)
    return decoded?.type === "bytes" ? decoded.value : null
  },

  quotedThumbnail() {
    return null
  },

  mediaFile(id) {
    return (
      get<{ path: string | null; mime: string | null }>(
        hasTable("message_media")
          ? `SELECT mm.file_path AS path, COALESCE(mm.mime_type, m.media_mime_type) AS mime
             FROM messages m LEFT JOIN message_media mm ON mm.message_row_id = m._id WHERE m._id = ?`
          : `SELECT NULL AS path, media_mime_type AS mime FROM messages WHERE _id = ?`,
        id
      ) ?? null
    )
  },
}
