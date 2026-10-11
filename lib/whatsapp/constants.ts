/**
 * Codes reverse-engineered from Android msgstore.db (shared by the legacy and modern layouts).
 * See docs/msgstore-schema.md for the evidence behind each one.
 */

/** `messages.media_wa_type` (legacy) / `message.message_type` (modern). Modern-only codes are in backend/modern.ts. */
export const WaType = {
  PLACEHOLDER: -1,
  TEXT: 0,
  IMAGE: 1,
  AUDIO: 2,
  VIDEO: 3,
  CONTACT: 4,
  LOCATION: 5,
  DOCUMENT: 9,
  MISSED_CALL: 10,
  /** Seen once, no payload. Unknown. */
  UNKNOWN_11: 11,
  /** Payment request; protobuf in `raw_data`. */
  PAYMENT: 12,
  GIF: 13,
  CONTACT_LIST: 14,
  /** Deleted for everyone. `edit_version` is 7, `media_name` holds the original key id. */
  REVOKED: 15,
  STICKER: 20,
  /** Disappearing-messages notice. */
  DISAPPEARING_NOTICE: 36,
  VIEW_ONCE_IMAGE: 42,
  VIEW_ONCE_VIDEO: 43,
} as const

/** `messages.status` */
export const Status = {
  RECEIVED: 0,
  SERVER_ACK: 4, // one grey tick
  DELIVERED: 5, // two grey ticks
  SYSTEM: 6, // control / system message
  PLAYED_BY_RECIPIENT: 8, // voice note played (outgoing)
  READ: 13, // blue ticks
} as const

/**
 * System event codes. Legacy: `messages.media_size` on rows with `status = 6`.
 * Modern: `message_system.action_type`, payloads in message_system_* side tables.
 *
 * Legacy payloads live in `data`, `remote_resource` (actor), `thumb_image`
 * (Java-serialized String or ArrayList<String> of JIDs), and for
 * number changes `media_name` / `media_caption`.
 */
export const SystemAction = {
  SUBJECT_CHANGED: 1, // data = new subject, thumb_image = old subject
  TEMP_GROUP_SUBJECT: 2,
  PARTICIPANT_JOINED: 4, // actor = remote_resource
  PARTICIPANT_LEFT: 5, // actor = remote_resource
  ICON_CHANGED: 6, // data = photo id; thumb_image = ProfilePhotoChange object
  BROADCAST_CREATED: 9, // thumb_image = ArrayList of recipients
  NUMBER_CHANGED_GROUP: 10, // remote_resource = old jid, thumb_image = new jid
  GROUP_CREATED: 11, // data = subject
  PARTICIPANTS_ADDED: 12, // thumb_image = ArrayList of added JIDs
  PARTICIPANTS_LEFT_LIST: 13,
  PARTICIPANTS_REMOVED: 14, // thumb_image = ArrayList of removed JIDs
  ADMIN_PROMOTED: 15,
  ADMIN_DEMOTED: 16,
  SECURITY_CODE_CHANGED: 18,
  E2E_ENCRYPTED: 19,
  JOINED_VIA_LINK: 20, // thumb_image = ArrayList of JIDs that joined
  DESCRIPTION_CHANGED: 27,
  NUMBER_CHANGED: 28, // media_name = old jid, media_caption = new jid
  GROUP_SETTING_29: 29,
  GROUP_SETTING_31: 31,
  GROUP_SETTING_32: 32,
  GROUP_SETTING_33: 33,
  BUSINESS_ACCOUNT: 46,
  BUSINESS_TO_STANDARD: 50,
  DISAPPEARING_TIMER: 56,
  BLOCKED: 58, // data = "true" | "false"
  SECURE_SERVICE: 67,
  SECURE_SERVICE_2: 68,
  SECURE_SERVICE_3: 69,
  GROUP_CALL: 70, // media_caption = "audio" | "video"
} as const
