/**
 * Client-safe domain types for the WhatsApp explorer.
 *
 * Everything the UI renders is described here. The server maps raw msgstore.db
 * rows into these shapes (see `mapper.ts`), so components never touch the
 * database schema directly.
 */

export type ChatKind = "individual" | "group" | "broadcast" | "status" | "channel" | "bot" | "other"

/** A person as they appear in a chat. `jid: null` means the device owner ("You"). */
export interface Participant {
  jid: string | null
  name: string
  /** E.164-ish phone number derived from the JID, if any. */
  phone: string | null
  /** True when `name` came from a real contact source rather than the phone number. */
  known: boolean
}

export interface ChatSummary {
  jid: string
  name: string
  kind: ChatKind
  messageCount: number
  archived: boolean
  lastMessage: {
    id: number
    timestamp: number
    fromMe: boolean
    senderName: string | null
    preview: string
  } | null
}

export interface ChatDetails extends ChatSummary {
  firstMessageAt: number | null
  lastMessageAt: number | null
  createdAt: number | null
}

/** Outgoing delivery state. Incoming messages are always `received`. */
export type MessageStatus =
  | "pending"
  | "sent"
  | "delivered"
  | "read"
  | "played"
  | "received"

export interface Reaction {
  emoji: string
  sender: Participant
  timestamp: number
}

export interface MediaInfo {
  mimeType: string | null
  /** Bytes. */
  size: number | null
  width: number | null
  height: number | null
  /** Seconds. */
  duration: number | null
  fileName: string | null
  /** Path relative to the WhatsApp folder on the phone, e.g. "Media/WhatsApp Images/IMG-….jpg". */
  filePath: string | null
  pageCount: number | null
  /** URL of an embedded low-res thumbnail, or null if the DB has none. */
  thumbnailUrl: string | null
  /** URL of the original file, or null if it isn't present in the media folder. */
  url: string | null
}

export interface LinkPreview {
  url: string | null
  title: string | null
  description: string | null
  thumbnailUrl: string | null
}

export interface ContactCard {
  name: string
  phones: { number: string; waId: string | null; label: string | null }[]
}

export interface QuotedMessage {
  /** Row id of the original message in this chat, if it still exists (enables jump-to). */
  targetId: number | null
  sender: Participant
  kind: MessageKind
  preview: string
  thumbnailUrl: string | null
}

/**
 * Structured system event. Rendered to text on the client by
 * `formatSystemEvent` so names stay consistent with the rest of the UI.
 */
export type SystemEvent =
  | { type: "e2e_encrypted" }
  | { type: "security_code_changed"; who: Participant | null }
  | { type: "group_created"; actor: Participant | null; subject: string | null }
  | { type: "subject_changed"; actor: Participant | null; subject: string | null }
  | { type: "description_changed"; actor: Participant | null }
  | { type: "icon_changed"; actor: Participant | null }
  | { type: "participants_added"; actor: Participant | null; targets: Participant[] }
  | { type: "participants_removed"; actor: Participant | null; targets: Participant[] }
  | { type: "participant_joined"; targets: Participant[] }
  | { type: "participant_joined_via_link"; targets: Participant[] }
  | { type: "participant_left"; targets: Participant[] }
  | { type: "admin_promoted"; targets: Participant[] }
  | { type: "admin_demoted"; targets: Participant[] }
  | { type: "number_changed"; from: Participant | null; to: Participant | null }
  | { type: "group_settings_changed"; actor: Participant | null; code: number }
  | { type: "disappearing_timer_changed"; actor: Participant | null; seconds: number | null }
  | { type: "group_call_started"; actor: Participant | null; video: boolean }
  | { type: "broadcast_created"; recipients: number }
  | { type: "blocked"; blocked: boolean }
  | { type: "business_notice"; variant: "secure_service" | "account_type_changed" | "business_account" }
  | { type: "temp_group_subject"; subject: string | null }
  | { type: "unknown"; code: number }

interface MessageBase {
  id: number
  keyId: string
  chatJid: string
  fromMe: boolean
  /** Who sent it. For system events this is null. */
  sender: Participant | null
  /** Milliseconds since epoch. */
  timestamp: number
  status: MessageStatus
  starred: boolean
  forwarded: boolean
  /** Forwarded many times (WhatsApp's double-arrow label). */
  frequentlyForwarded: boolean
  quoted: QuotedMessage | null
  reactions: Reaction[]
  /** JID user part (the digits after "@" in the text) → display name, for rendering @mentions. */
  mentions: Record<string, string>
  edited: boolean
  /** Raw `media_wa_type` — kept for debugging and future renderers. */
  waType: number
}

export type TextMessage = MessageBase & {
  kind: "text"
  text: string
  linkPreview: LinkPreview | null
}
export type ImageMessage = MessageBase & { kind: "image"; media: MediaInfo; caption: string | null }
export type VideoMessage = MessageBase & { kind: "video"; media: MediaInfo; caption: string | null }
export type GifMessage = MessageBase & { kind: "gif"; media: MediaInfo; caption: string | null }
export type AudioMessage = MessageBase & { kind: "audio"; media: MediaInfo; voiceNote: boolean }
export type DocumentMessage = MessageBase & { kind: "document"; media: MediaInfo; caption: string | null }
export type StickerMessage = MessageBase & { kind: "sticker"; media: MediaInfo; animated: boolean }
export type ContactMessage = MessageBase & { kind: "contact"; contacts: ContactCard[] }
export type LocationMessage = MessageBase & {
  kind: "location"
  live: boolean
  latitude: number
  longitude: number
  name: string | null
  address: string | null
  thumbnailUrl: string | null
}
export type ViewOnceMessage = MessageBase & {
  kind: "view_once"
  mediaType: "image" | "video"
  opened: boolean
  caption: string | null
  media: MediaInfo
}
export type DeletedMessage = MessageBase & { kind: "deleted"; byAdmin: Participant | null }
export type MissedCallMessage = MessageBase & { kind: "missed_call"; video: boolean }
export type PaymentMessage = MessageBase & {
  kind: "payment"
  amount: number | null
  currency: string | null
  note: string | null
}
export type PollMessage = MessageBase & {
  kind: "poll"
  question: string
  options: { name: string; votes: number }[]
  multipleAnswers: boolean
}
export type CallMessage = MessageBase & {
  kind: "call"
  video: boolean
  /** Seconds, when the call connected. */
  duration: number | null
  outcome: "answered" | "missed" | "unanswered"
}
export type AlbumMessage = MessageBase & { kind: "album"; images: number; videos: number }
export type SystemMessage = MessageBase & { kind: "system"; event: SystemEvent }
export type DisappearingNoticeMessage = MessageBase & { kind: "disappearing_notice" }
export type UnsupportedMessage = MessageBase & { kind: "unsupported"; text: string | null }

export type Message =
  | TextMessage
  | ImageMessage
  | VideoMessage
  | GifMessage
  | AudioMessage
  | DocumentMessage
  | StickerMessage
  | ContactMessage
  | LocationMessage
  | ViewOnceMessage
  | DeletedMessage
  | MissedCallMessage
  | PaymentMessage
  | PollMessage
  | CallMessage
  | AlbumMessage
  | SystemMessage
  | DisappearingNoticeMessage
  | UnsupportedMessage

export type MessageKind = Message["kind"]

export type MessageOfKind<K extends MessageKind> = Extract<Message, { kind: K }>

/** Where to open a chat. */
export type ChatAnchor =
  | { type: "latest" }
  | { type: "message"; id: number }
  | { type: "time"; at: number }

export interface MessagePage {
  messages: Message[]
  hasOlder: boolean
  hasNewer: boolean
  /** Message the page was centred on (for `message`/`time` anchors). */
  targetId: number | null
}
