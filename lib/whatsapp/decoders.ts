/**
 * Decoders for the binary blobs found in msgstore.db.
 */

// ---------------------------------------------------------------------------
// Java serialization (java.io.ObjectOutputStream)
//
// Legacy WhatsApp stores small payloads in `thumb_image` / `raw_data` this way:
//   - a single String            (e.g. new JID on number change, old subject)
//   - an ArrayList<String>       (participants added/removed, vCard lists)
//   - a byte[]                   (JPEG thumbnail of link previews)
//   - a com.whatsapp.MediaData   (media metadata — not needed, message_media has it)
// ---------------------------------------------------------------------------

export type JavaValue =
  | { type: "string"; value: string }
  | { type: "list"; items: string[] }
  | { type: "bytes"; value: Uint8Array }
  | { type: "object"; className: string }

const TC_NULL = 0x70
const TC_REFERENCE = 0x71
const TC_CLASSDESC = 0x72
const TC_OBJECT = 0x73
const TC_STRING = 0x74
const TC_ARRAY = 0x75
const TC_BLOCKDATA = 0x77
const TC_ENDBLOCKDATA = 0x78
const TC_LONGSTRING = 0x7c

const utf8 = new TextDecoder("utf-8")

class JavaReader {
  pos = 0
  handles: unknown[] = []
  constructor(private buf: Uint8Array) {}

  u8() {
    if (this.pos >= this.buf.length) throw new Error("EOF")
    return this.buf[this.pos++]
  }
  u16() {
    return (this.u8() << 8) | this.u8()
  }
  i32() {
    return ((this.u8() << 24) | (this.u8() << 16) | (this.u8() << 8) | this.u8()) >>> 0
  }
  bytes(n: number) {
    if (this.pos + n > this.buf.length) throw new Error("EOF")
    const out = this.buf.subarray(this.pos, this.pos + n)
    this.pos += n
    return out
  }
  utf(n: number) {
    return utf8.decode(this.bytes(n))
  }

  /** Reads a class descriptor and returns its class name. */
  classDesc(): string {
    const tag = this.u8()
    if (tag === TC_NULL) return ""
    if (tag === TC_REFERENCE) {
      const ref = this.handles[this.i32() - 0x7e0000]
      return typeof ref === "string" ? ref : ""
    }
    if (tag !== TC_CLASSDESC) throw new Error(`Unexpected classDesc tag ${tag}`)
    const name = this.utf(this.u16())
    this.bytes(8) // serialVersionUID
    this.handles.push(name)
    this.u8() // flags
    const fields = this.u16()
    for (let i = 0; i < fields; i++) {
      const typeCode = String.fromCharCode(this.u8())
      this.utf(this.u16()) // field name
      if (typeCode === "L" || typeCode === "[") this.content() // class name string
    }
    this.skipAnnotation()
    this.classDesc() // super class
    return name
  }

  skipAnnotation() {
    while (true) {
      const tag = this.buf[this.pos]
      if (tag === TC_ENDBLOCKDATA) {
        this.pos++
        return
      }
      if (tag === TC_BLOCKDATA) {
        this.pos++
        this.bytes(this.u8())
      } else {
        this.content()
      }
    }
  }

  content(): unknown {
    const tag = this.u8()
    switch (tag) {
      case TC_NULL:
        return null
      case TC_REFERENCE:
        return this.handles[this.i32() - 0x7e0000]
      case TC_STRING: {
        const s = this.utf(this.u16())
        this.handles.push(s)
        return s
      }
      case TC_LONGSTRING: {
        this.bytes(4) // high 32 bits; strings > 4GB don't happen here
        const s = this.utf(this.i32())
        this.handles.push(s)
        return s
      }
      case TC_ARRAY: {
        const cls = this.classDesc()
        const len = this.i32()
        if (cls === "[B") {
          const b = this.bytes(len)
          this.handles.push(b)
          return b
        }
        throw new Error(`Unsupported array ${cls}`)
      }
      case TC_OBJECT: {
        const cls = this.classDesc()
        const handle = this.handles.length
        this.handles.push(null)
        if (cls === "java.util.ArrayList") {
          this.i32() // size field
          // writeObject block: capacity, then elements, then end marker
          if (this.u8() !== TC_BLOCKDATA) throw new Error("Bad ArrayList")
          this.bytes(this.u8())
          const items: string[] = []
          while (this.buf[this.pos] !== TC_ENDBLOCKDATA) {
            const v = this.content()
            if (typeof v === "string") items.push(v)
          }
          this.pos++
          this.handles[handle] = items
          return items
        }
        // Other objects: don't decode the field values.
        return { className: cls }
      }
      default:
        throw new Error(`Unsupported tag 0x${tag.toString(16)}`)
    }
  }
}

export function decodeJava(buf: Uint8Array | null | undefined): JavaValue | null {
  if (!buf || buf.length < 5 || buf[0] !== 0xac || buf[1] !== 0xed) return null
  const r = new JavaReader(buf)
  r.pos = 4
  const tag = buf[4]
  try {
    if (tag === TC_OBJECT) {
      // Peek the class name so unknown objects don't need full decoding.
      const v = r.content()
      if (Array.isArray(v)) return { type: "list", items: v }
      const cls = (v as { className?: string })?.className ?? ""
      return { type: "object", className: cls }
    }
    const v = r.content()
    if (typeof v === "string") return { type: "string", value: v }
    if (v instanceof Uint8Array) return { type: "bytes", value: v }
  } catch {
    // Fall back to the class name for objects we can't walk.
    if (tag === TC_OBJECT && buf[5] === TC_CLASSDESC) {
      const len = (buf[6] << 8) | buf[7]
      return { type: "object", className: utf8.decode(buf.subarray(8, 8 + len)) }
    }
  }
  return null
}

export function javaStrings(buf: Uint8Array | null | undefined): string[] {
  const v = decodeJava(buf)
  if (!v) return []
  if (v.type === "string") return [v.value]
  if (v.type === "list") return v.items
  return []
}

// ---------------------------------------------------------------------------
// Protobuf (just enough to walk fields)
// ---------------------------------------------------------------------------

export type ProtoField =
  | { field: number; wire: 0; value: number }
  | { field: number; wire: 2; value: Uint8Array }

export function decodeProto(buf: Uint8Array): ProtoField[] {
  const out: ProtoField[] = []
  let pos = 0
  const varint = () => {
    let result = 0
    let shift = 0
    while (pos < buf.length) {
      const b = buf[pos++]
      result += (b & 0x7f) * 2 ** shift
      if (!(b & 0x80)) return result
      shift += 7
    }
    throw new Error("Truncated varint")
  }
  while (pos < buf.length) {
    const key = varint()
    const field = Math.floor(key / 8)
    const wire = key & 7
    if (wire === 0) out.push({ field, wire, value: varint() })
    else if (wire === 2) {
      const len = varint()
      out.push({ field, wire, value: buf.subarray(pos, pos + len) })
      pos += len
    } else if (wire === 1) pos += 8
    else if (wire === 5) pos += 4
    else throw new Error(`Unsupported wire type ${wire}`)
  }
  return out
}

export function protoGet(fields: ProtoField[], n: number, wire: 0): number | undefined
export function protoGet(fields: ProtoField[], n: number, wire: 2): Uint8Array | undefined
export function protoGet(fields: ProtoField[], n: number, wire: 0 | 2) {
  return fields.find((f) => f.field === n && f.wire === wire)?.value
}

export const protoString = (b: Uint8Array | undefined) => (b ? utf8.decode(b) : undefined)

/**
 * Payment request (`media_wa_type = 12`). Observed layout of `raw_data`:
 *   field 22 {
 *     1: currency ("BDT")
 *     3: requested-from JID
 *     4: note message { 1: text }
 *     6: Money { 1: value, 2: offset (default 1000), 3: currency }
 *   }
 */
export function decodePayment(buf: Uint8Array | null) {
  if (!buf) return null
  try {
    const top = decodeProto(buf)
    const body = top.find((f) => f.wire === 2)?.value
    if (!body) return null
    const f = decodeProto(body)
    let currency = protoString(protoGet(f, 1, 2)) ?? null
    let amount: number | null = null
    const amount1000 = protoGet(f, 2, 0)
    if (amount1000 !== undefined) amount = amount1000 / 1000
    const money = protoGet(f, 6, 2)
    if (money) {
      const m = decodeProto(money)
      const value = protoGet(m, 1, 0)
      const offset = protoGet(m, 2, 0) ?? 1000
      if (value !== undefined) amount = value / offset
      currency = protoString(protoGet(m, 3, 2)) ?? currency
    }
    const noteMsg = protoGet(f, 4, 2)
    const note = noteMsg ? (protoString(protoGet(decodeProto(noteMsg), 1, 2)) ?? null) : null
    return { amount, currency, note }
  } catch {
    return null
  }
}

// ---------------------------------------------------------------------------
// vCard
// ---------------------------------------------------------------------------

export function parseVCard(text: string) {
  // Unfold continuation lines
  const lines = text.replace(/\r\n/g, "\n").replace(/\n[ \t]/g, "").split("\n")
  let name: string | null = null
  let structured: string | null = null
  const phones: { number: string; waId: string | null; label: string | null }[] = []
  const labels = new Map<string, string>()
  for (const line of lines) {
    const colon = line.indexOf(":")
    if (colon === -1) continue
    const rawKey = line.slice(0, colon)
    const value = line.slice(colon + 1).trim()
    // Strip "item1." grouping prefix
    const key = rawKey.replace(/^item\d+\./i, "")
    const group = rawKey.match(/^(item\d+)\./i)?.[1]
    const [prop, ...params] = key.split(";")
    switch (prop.toUpperCase()) {
      case "FN":
        name = value
        break
      case "N":
        structured = value.split(";").filter(Boolean).reverse().join(" ")
        break
      case "TEL": {
        const waId = params.find((p) => p.toLowerCase().startsWith("waid="))?.slice(5) ?? null
        const type = params.find((p) => p.toLowerCase().startsWith("type="))?.slice(5) ?? null
        phones.push({ number: value, waId, label: group ?? type })
        break
      }
      case "X-ABLABEL":
        if (group) labels.set(group, value)
        break
    }
  }
  for (const p of phones) {
    if (p.label && labels.has(p.label)) p.label = labels.get(p.label)!
    else if (p.label?.startsWith("item")) p.label = null
  }
  return { name: name || structured || "Unknown contact", phones }
}

// ---------------------------------------------------------------------------
// Image sniffing
// ---------------------------------------------------------------------------

export function sniffImageType(b: Uint8Array): string {
  if (b[0] === 0xff && b[1] === 0xd8) return "image/jpeg"
  if (b[0] === 0x89 && b[1] === 0x50) return "image/png"
  if (b[0] === 0x52 && b[1] === 0x49 && b[8] === 0x57) return "image/webp"
  if (b[0] === 0x47 && b[1] === 0x49) return "image/gif"
  return "application/octet-stream"
}
