/** Stable per-person colours for group sender names and avatars. */
const SENDER_COLORS = [
  "text-[#1f7aec] dark:text-[#53bdeb]",
  "text-[#c13584] dark:text-[#ff72a1]",
  "text-[#dd6a00] dark:text-[#ffa95c]",
  "text-[#029d00] dark:text-[#25d366]",
  "text-[#8b44c7] dark:text-[#c79ef5]",
  "text-[#b6142c] dark:text-[#ff6b7f]",
  "text-[#007b83] dark:text-[#36cdd6]",
  "text-[#a86d00] dark:text-[#ffd279]",
  "text-[#5f6ad8] dark:text-[#a5abff]",
  "text-[#2e8a5d] dark:text-[#7fe0b0]",
]

const AVATAR_COLORS = [
  "bg-[#1f7aec]",
  "bg-[#c13584]",
  "bg-[#dd6a00]",
  "bg-[#029d00]",
  "bg-[#8b44c7]",
  "bg-[#b6142c]",
  "bg-[#007b83]",
  "bg-[#a86d00]",
  "bg-[#5f6ad8]",
  "bg-[#2e8a5d]",
]

function hash(s: string) {
  let h = 0
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0
  return Math.abs(h)
}

export const senderColor = (key: string | null) => SENDER_COLORS[hash(key ?? "me") % SENDER_COLORS.length]
export const avatarColor = (key: string | null) => AVATAR_COLORS[hash(key ?? "me") % AVATAR_COLORS.length]

export function initials(name: string) {
  const clean = name.replace(/^[~+]/, "").trim()
  if (/^\d/.test(clean)) return "#"
  const parts = clean.split(/\s+/).filter(Boolean)
  const chars = parts.length > 1 ? [parts[0], parts[parts.length - 1]] : [parts[0] ?? "?"]
  return chars.map((p) => Array.from(p)[0]?.toUpperCase() ?? "").join("")
}
