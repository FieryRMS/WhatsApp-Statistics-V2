import type { RawQuote, RawRow, RawSystem } from "./backend"
import { emptyRow } from "./backend/types"
import { ModernType } from "./backend/modern"
import { participant, me } from "./contacts"
import { Status, SystemAction, WaType } from "./constants"
import { decodePayment, parseVCard } from "./decoders"
import { messagePreview } from "./format"
import { chatKind, jidUser } from "./jid"
import { mediaExists } from "./media"
import type { MediaInfo, Message, MessageStatus, QuotedMessage, Reaction, SystemEvent } from "./types"

export interface MapContext {
  reactions: Map<number, Reaction[]>
  quotes: Map<number, QuotedMessage>
}

export const thumbnailUrl = (id: number) => `/api/messages/${id}/thumbnail`
export const mediaUrl = (id: number) => `/api/messages/${id}/media`

function mapStatus(fromMe: boolean, status: number | null): MessageStatus {
  if (!fromMe) return "received"
  switch (status) {
    case Status.SERVER_ACK:
      return "sent"
    case Status.DELIVERED:
      return "delivered"
    case Status.READ:
      return "read"
    case Status.PLAYED_BY_RECIPIENT:
      return "played"
    default:
      return "pending"
  }
}

function mediaInfo(row: RawRow): MediaInfo {
  const m = row.media
  return {
    mimeType: m?.mime ?? null,
    size: m?.size ?? null,
    width: m?.width ?? null,
    height: m?.height ?? null,
    duration: m?.duration ?? null,
    fileName: m?.name ?? null,
    filePath: m?.path ?? null,
    pageCount: m?.pageCount ?? null,
    thumbnailUrl: row.hasThumb ? thumbnailUrl(row.id) : null,
    url: mediaExists(m?.path) ? mediaUrl(row.id) : null,
  }
}

function systemEvent(row: RawRow, s: RawSystem): SystemEvent {
  const actor = s.actor ? participant(s.actor) : null
  const targets = s.targets.map((j) => participant(j))
  const targetsOrActor = targets.length ? targets : actor ? [actor] : []

  switch (s.action) {
    case SystemAction.E2E_ENCRYPTED:
      return { type: "e2e_encrypted" }
    case SystemAction.SECURITY_CODE_CHANGED:
      return {
        type: "security_code_changed",
        who: actor ?? (chatKind(row.chatJid) === "individual" ? participant(row.chatJid) : null),
      }
    case SystemAction.GROUP_CREATED:
      return { type: "group_created", actor, subject: s.value }
    case SystemAction.SUBJECT_CHANGED:
      return { type: "subject_changed", actor, subject: s.value }
    case SystemAction.DESCRIPTION_CHANGED:
      return { type: "description_changed", actor }
    case SystemAction.ICON_CHANGED:
      return { type: "icon_changed", actor }
    case SystemAction.PARTICIPANTS_ADDED:
      return { type: "participants_added", actor, targets }
    case SystemAction.PARTICIPANTS_REMOVED:
      return { type: "participants_removed", actor, targets }
    case SystemAction.PARTICIPANT_JOINED:
      return { type: "participant_joined", targets: targetsOrActor }
    case SystemAction.JOINED_VIA_LINK:
      return { type: "participant_joined_via_link", targets: targetsOrActor }
    case SystemAction.PARTICIPANT_LEFT:
    case SystemAction.PARTICIPANTS_LEFT_LIST:
      return { type: "participant_left", targets: targetsOrActor }
    case SystemAction.ADMIN_PROMOTED:
      return { type: "admin_promoted", targets: targetsOrActor }
    case SystemAction.ADMIN_DEMOTED:
      return { type: "admin_demoted", targets: targetsOrActor }
    case SystemAction.NUMBER_CHANGED_GROUP:
    case SystemAction.NUMBER_CHANGED:
      return {
        type: "number_changed",
        from: s.numberFrom ? participant(s.numberFrom) : actor,
        to: s.numberTo ? participant(s.numberTo) : null,
      }
    case SystemAction.GROUP_SETTING_29:
    case SystemAction.GROUP_SETTING_31:
    case SystemAction.GROUP_SETTING_32:
    case SystemAction.GROUP_SETTING_33:
      return { type: "group_settings_changed", actor, code: s.action }
    case SystemAction.DISAPPEARING_TIMER:
      return { type: "disappearing_timer_changed", actor, seconds: s.duration }
    case SystemAction.GROUP_CALL:
      return { type: "group_call_started", actor, video: s.video }
    case SystemAction.BROADCAST_CREATED:
      return { type: "broadcast_created", recipients: s.count ?? s.targets.length }
    case SystemAction.BLOCKED:
      return { type: "blocked", blocked: s.value === "true" }
    case SystemAction.SECURE_SERVICE:
    case SystemAction.SECURE_SERVICE_2:
    case SystemAction.SECURE_SERVICE_3:
      return { type: "business_notice", variant: "secure_service" }
    case SystemAction.BUSINESS_TO_STANDARD:
      return { type: "business_notice", variant: "account_type_changed" }
    case SystemAction.BUSINESS_ACCOUNT:
      return { type: "business_notice", variant: "business_account" }
    case SystemAction.TEMP_GROUP_SUBJECT:
      return { type: "temp_group_subject", subject: s.value }
    default:
      return { type: "unknown", code: s.action }
  }
}

function mentionsOf(row: RawRow): Record<string, string> {
  const out: Record<string, string> = {}
  for (const jid of row.mentions) out[jidUser(jid)] = participant(jid).name
  return out
}

/** Media kind for unknown type codes, from the MIME type or else the file extension. */
function inferMediaKind(media: NonNullable<RawRow["media"]>): "image" | "video" | "audio" | "document" {
  const m = media.mime ?? ""
  if (m.startsWith("image/")) return "image"
  if (m.startsWith("video/")) return "video"
  if (m.startsWith("audio/")) return "audio"
  if (m) return "document"
  const ext = (media.path ?? media.name ?? "").split(".").pop()?.toLowerCase() ?? ""
  if (["jpg", "jpeg", "png", "webp", "heic", "gif"].includes(ext)) return "image"
  if (["mp4", "3gp", "mov", "mkv", "webm"].includes(ext)) return "video"
  if (["opus", "ogg", "m4a", "mp3", "aac", "amr", "wav"].includes(ext)) return "audio"
  return "document"
}

export function mapMessage(row: RawRow, ctx?: MapContext): Message {
  const isSystem = row.system != null || row.type === ModernType.SYSTEM
  const base = {
    id: row.id,
    keyId: row.keyId,
    chatJid: row.chatJid,
    fromMe: row.fromMe,
    sender: isSystem ? null : row.fromMe ? me() : participant(row.senderJid),
    timestamp: row.timestamp,
    status: mapStatus(row.fromMe, row.status),
    starred: row.starred,
    forwarded: row.forwarded,
    frequentlyForwarded: (row.forwardScore ?? 0) >= 5,
    quoted: (row.quoteKey != null && ctx?.quotes.get(row.quoteKey)) || null,
    reactions: ctx?.reactions.get(row.id) ?? [],
    mentions: mentionsOf(row),
    edited: row.edited,
    waType: row.type,
  }

  if (row.system) return { ...base, kind: "system", event: systemEvent(row, row.system) }
  if (isSystem) return { ...base, kind: "system", event: { type: "unknown", code: -1 } }

  if (row.revoked || row.type === WaType.REVOKED || row.type === ModernType.ADMIN_REVOKED)
    return { ...base, kind: "deleted", byAdmin: row.revokedByAdmin ? participant(row.revokedByAdmin) : null }

  switch (row.type) {
    case WaType.TEXT:
      return {
        ...base,
        kind: "text",
        text: row.text ?? "",
        linkPreview: row.link
          ? {
              url: row.link.url ?? row.text?.match(/\bhttps?:\/\/[^\s<>"]+/i)?.[0] ?? null,
              title: row.link.title,
              description: row.link.description,
              thumbnailUrl: row.hasThumb ? thumbnailUrl(row.id) : null,
            }
          : null,
      }
    case WaType.IMAGE:
      return { ...base, kind: "image", media: mediaInfo(row), caption: row.caption }
    case WaType.VIDEO:
      return { ...base, kind: "video", media: mediaInfo(row), caption: row.caption }
    case WaType.GIF:
      return { ...base, kind: "gif", media: mediaInfo(row), caption: row.caption }
    case WaType.AUDIO:
      return { ...base, kind: "audio", media: mediaInfo(row), voiceNote: row.voiceNote }
    case WaType.DOCUMENT:
      return { ...base, kind: "document", media: mediaInfo(row), caption: row.caption }
    case WaType.STICKER:
      return { ...base, kind: "sticker", media: mediaInfo(row), animated: !!row.media?.animated }
    case WaType.CONTACT:
    case WaType.CONTACT_LIST:
      return { ...base, kind: "contact", contacts: row.vcards.map(parseVCard) }
    case WaType.LOCATION:
    case ModernType.LIVE_LOCATION:
      return {
        ...base,
        kind: "location",
        live: row.type === ModernType.LIVE_LOCATION,
        latitude: row.location?.latitude ?? 0,
        longitude: row.location?.longitude ?? 0,
        name: row.location?.name ?? null,
        address: row.location?.address ?? null,
        thumbnailUrl: row.hasThumb ? thumbnailUrl(row.id) : null,
      }
    case WaType.VIEW_ONCE_IMAGE:
    case WaType.VIEW_ONCE_VIDEO:
      return {
        ...base,
        kind: "view_once",
        mediaType: row.type === WaType.VIEW_ONCE_VIDEO ? "video" : "image",
        opened: (row.viewOnceState ?? 0) > 0,
        caption: row.caption,
        media: mediaInfo(row),
      }
    case WaType.MISSED_CALL:
      return { ...base, fromMe: false, status: "received", kind: "missed_call", video: !!row.missedVideo }
    case WaType.PAYMENT: {
      const p = decodePayment(row.payment)
      return { ...base, kind: "payment", amount: p?.amount ?? null, currency: p?.currency ?? null, note: p?.note ?? null }
    }
    case WaType.DISAPPEARING_NOTICE:
      return { ...base, kind: "disappearing_notice" }
    case ModernType.POLL:
      return {
        ...base,
        kind: "poll",
        question: row.text ?? "Poll",
        options: row.poll?.options ?? [],
        multipleAnswers: (row.poll?.selectable ?? 1) !== 1,
      }
    case ModernType.CALL: {
      const c = row.call
      // call_result 5 = connected; anything else didn't connect.
      const answered = c?.result === 5 || (c?.duration ?? 0) > 0
      return {
        ...base,
        status: "received", // call bubbles carry the system status code; they show no ticks
        kind: "call",
        video: !!c?.video,
        duration: answered ? (c?.duration ?? null) : null,
        outcome: answered ? "answered" : row.fromMe ? "unanswered" : "missed",
      }
    }
    case ModernType.ALBUM:
      return { ...base, kind: "album", images: row.album?.images ?? 0, videos: row.album?.videos ?? 0 }
  }

  // Unknown codes: fall back on whatever content the row carries.
  if (row.poll) return mapMessage({ ...row, type: ModernType.POLL }, ctx)
  if (row.location) return mapMessage({ ...row, type: WaType.LOCATION }, ctx)
  if (row.media) {
    const kind = inferMediaKind(row.media)
    if (kind === "audio") return { ...base, kind, media: mediaInfo(row), voiceNote: row.voiceNote }
    return { ...base, kind, media: mediaInfo(row), caption: row.caption }
  }
  if (row.text) return { ...base, kind: "text", text: row.text, linkPreview: null }
  return { ...base, kind: "unsupported", text: `Unsupported message (type ${row.type})` }
}

const MEDIA_TYPES: number[] = [WaType.IMAGE, WaType.VIDEO, WaType.GIF, WaType.AUDIO, WaType.DOCUMENT, WaType.STICKER]

/** Turn a quoted-message stub into the preview shown above a reply. */
export function mapQuote(q: RawQuote): QuotedMessage {
  const isMedia = MEDIA_TYPES.includes(q.type)
  const msg = mapMessage(
    emptyRow({
      id: -1,
      chatJid: q.chatJid,
      keyId: "",
      timestamp: 0,
      type: q.type,
      fromMe: q.fromMe,
      senderJid: q.senderJid,
      text: isMedia ? null : q.text,
      caption: isMedia ? q.caption || q.text || null : null,
      media: isMedia
        ? { mime: null, size: null, width: null, height: null, duration: null, name: q.fileName, path: null, pageCount: null, animated: false }
        : null,
      voiceNote: q.type === WaType.AUDIO,
    })
  )
  return {
    targetId: q.targetId,
    sender: q.fromMe ? me() : participant(q.senderJid),
    kind: msg.kind,
    preview: messagePreview(msg),
    thumbnailUrl: q.thumbnailUrl,
  }
}
