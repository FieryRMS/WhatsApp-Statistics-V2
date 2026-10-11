/**
 * Pure, client-safe formatting helpers.
 */
import type { Message, Participant, SystemEvent } from "./types"

export function joinNames(people: Participant[]): string {
  const names = people.map((p) => p.name)
  if (names.length === 0) return "someone"
  if (names.length === 1) return names[0]
  return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`
}

const who = (p: Participant | null | undefined) => p?.name ?? "Someone"

export function formatSystemEvent(e: SystemEvent): string {
  switch (e.type) {
    case "e2e_encrypted":
      return "Messages and calls are end-to-end encrypted. No one outside of this chat, not even WhatsApp, can read or listen to them."
    case "security_code_changed":
      return e.who ? `Your security code with ${e.who.name} changed.` : "Your security code changed."
    case "group_created":
      return `${who(e.actor)} created group "${e.subject ?? ""}"`
    case "subject_changed":
      return `${who(e.actor)} changed the subject to "${e.subject ?? ""}"`
    case "description_changed":
      return `${who(e.actor)} changed the group description`
    case "icon_changed":
      return `${who(e.actor)} changed this group's icon`
    case "participants_added":
      return `${who(e.actor)} added ${joinNames(e.targets)}`
    case "participants_removed":
      return `${who(e.actor)} removed ${joinNames(e.targets)}`
    case "participant_joined":
      return `${joinNames(e.targets)} joined`
    case "participant_joined_via_link":
      return `${joinNames(e.targets)} joined using this group's invite link`
    case "participant_left":
      return `${joinNames(e.targets)} left`
    case "admin_promoted":
      return `${joinNames(e.targets)} ${e.targets.length > 1 ? "are" : "is"} now an admin`
    case "admin_demoted":
      return `${joinNames(e.targets)} ${e.targets.length > 1 ? "are" : "is"} no longer an admin`
    case "number_changed":
      return `${who(e.from)} changed their phone number to a new number${e.to?.phone ? ` (${e.to.phone})` : ""}`
    case "group_settings_changed":
      return `${who(e.actor)} changed the group settings`
    case "disappearing_timer_changed":
      if (e.seconds === 0) return `${who(e.actor)} turned off disappearing messages`
      if (e.seconds) return `${who(e.actor)} turned on disappearing messages. New messages will disappear ${formatPeriod(e.seconds)} after they're sent.`
      return `${who(e.actor)} changed the disappearing messages setting`
    case "group_call_started":
      return `${who(e.actor)} started a ${e.video ? "video" : "voice"} call`
    case "broadcast_created":
      return `You created a broadcast list with ${e.recipients} recipient${e.recipients === 1 ? "" : "s"}`
    case "blocked":
      return e.blocked ? "You blocked this contact." : "You unblocked this contact."
    case "business_notice":
      if (e.variant === "secure_service")
        return "This business uses a secure service from Meta to manage this chat."
      if (e.variant === "account_type_changed") return "This account changed from a business account to a standard account."
      return "This chat is with a business account."
    case "temp_group_subject":
      return e.subject ?? "Group"
    case "unknown":
      return `System message (code ${e.code})`
  }
}

/** One-line summary, as shown in the chat list and in quoted replies. */
export function messagePreview(m: Message): string {
  switch (m.kind) {
    case "text":
      return m.text
    case "image":
      return m.caption ? `📷 ${m.caption}` : "📷 Photo"
    case "video":
      return m.caption ? `🎥 ${m.caption}` : "🎥 Video"
    case "gif":
      return m.caption ? `GIF ${m.caption}` : "GIF"
    case "audio":
      return m.voiceNote
        ? `🎤 Voice message${m.media.duration ? ` (${formatDuration(m.media.duration)})` : ""}`
        : `🎵 Audio`
    case "document":
      return `📄 ${m.media.fileName ?? "Document"}`
    case "sticker":
      return "Sticker"
    case "contact":
      return m.contacts.length > 1 ? `👤 ${m.contacts.length} contacts` : `👤 ${m.contacts[0]?.name ?? "Contact"}`
    case "location":
      return `📍 ${m.live ? "Live location" : (m.name ?? "Location")}`
    case "view_once":
      return m.mediaType === "video" ? "Video (view once)" : "Photo (view once)"
    case "deleted":
      if (m.byAdmin) return `This message was deleted by admin ${m.byAdmin.name}`
      return m.fromMe ? "You deleted this message" : "This message was deleted"
    case "missed_call":
      return m.video ? "Missed video call" : "Missed voice call"
    case "payment":
      return `💸 Payment request${m.amount != null ? `: ${formatMoney(m.amount, m.currency)}` : ""}`
    case "poll":
      return `📊 ${m.question}`
    case "call": {
      const type = m.video ? "video" : "voice"
      if (m.outcome === "answered") return `${m.video ? "📹" : "📞"} ${m.fromMe ? "Outgoing" : "Incoming"} ${type} call`
      return m.outcome === "missed" ? `Missed ${type} call` : `${m.video ? "📹" : "📞"} ${type[0].toUpperCase()}${type.slice(1)} call, no answer`
    }
    case "album": {
      const parts = [m.images && `${m.images} photo${m.images === 1 ? "" : "s"}`, m.videos && `${m.videos} video${m.videos === 1 ? "" : "s"}`]
      return `🖼️ Album: ${parts.filter(Boolean).join(", ") || "media"}`
    }
    case "system":
      return formatSystemEvent(m.event)
    case "disappearing_notice":
      return "Disappearing messages setting changed"
    case "unsupported":
      return m.text ?? "Unsupported message"
  }
}

export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds))
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = String(s % 60).padStart(2, "0")
  return h ? `${h}:${String(m).padStart(2, "0")}:${sec}` : `${m}:${sec}`
}

/** 86400 → "24 hours", 604800 → "7 days". */
export function formatPeriod(seconds: number): string {
  const days = seconds / 86400
  if (days >= 1 && Number.isInteger(days)) return `${days} day${days === 1 ? "" : "s"}`
  const hours = Math.round(seconds / 3600)
  if (hours >= 1) return `${hours} hour${hours === 1 ? "" : "s"}`
  const minutes = Math.round(seconds / 60)
  return `${minutes} minute${minutes === 1 ? "" : "s"}`
}

export function formatBytes(bytes: number | null | undefined): string {
  if (!bytes) return ""
  const units = ["B", "kB", "MB", "GB"]
  let v = bytes
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`
}

export function formatMoney(amount: number, currency: string | null) {
  try {
    return new Intl.NumberFormat(undefined, { style: "currency", currency: currency ?? "USD" }).format(amount)
  } catch {
    return `${currency ?? ""} ${amount.toFixed(2)}`.trim()
  }
}

export function formatTime(ts: number) {
  return new Date(ts).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })
}

const DAY = 86_400_000

function startOfDay(ts: number) {
  const d = new Date(ts)
  d.setHours(0, 0, 0, 0)
  return d.getTime()
}

export function isSameDay(a: number, b: number) {
  return startOfDay(a) === startOfDay(b)
}

/** "Today" / "Yesterday" / "Monday" / "12 March 2021", like the chat date chips. */
export function formatDayLabel(ts: number, now = Date.now()) {
  const diff = Math.round((startOfDay(now) - startOfDay(ts)) / DAY)
  if (diff === 0) return "Today"
  if (diff === 1) return "Yesterday"
  if (diff > 1 && diff < 7) return new Date(ts).toLocaleDateString(undefined, { weekday: "long" })
  return new Date(ts).toLocaleDateString(undefined, { day: "numeric", month: "long", year: "numeric" })
}

/** Chat list timestamp: time today, "Yesterday", weekday, else short date. */
export function formatListTimestamp(ts: number, now = Date.now()) {
  const diff = Math.round((startOfDay(now) - startOfDay(ts)) / DAY)
  if (diff === 0) return formatTime(ts)
  if (diff === 1) return "Yesterday"
  if (diff > 1 && diff < 7) return new Date(ts).toLocaleDateString(undefined, { weekday: "long" })
  return new Date(ts).toLocaleDateString(undefined, { day: "2-digit", month: "2-digit", year: "numeric" })
}
