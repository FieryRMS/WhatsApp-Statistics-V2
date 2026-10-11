# WhatsApp Stats

A read-only explorer for an Android WhatsApp `msgstore.db`, laid out like WhatsApp.
Works with both the legacy (`messages` table, until ~2022) and modern (`message` table) database layouts.
Built with Next.js, shadcn/ui and Node's built-in `node:sqlite`.

```bash
npm run dev   # http://localhost:3000
```

## Data

Everything lives in `whatsapp-data/`. Its contents are gitignored; only the folder is tracked:

| File | Required | Purpose |
| --- | --- | --- |
| `whatsapp-data/msgstore.db` | yes | the decrypted message database |
| `whatsapp-data/wa.db` | no | WhatsApp's contact DB (same folder on the phone). Adds address-book names and push names. |
| `whatsapp-data/contacts.json` | no | manual `{ "<jid>": "Name" }` overrides |
| `whatsapp-data/Media/…` | no | the phone's `WhatsApp/Media` folder. Originals are served when present, thumbnails otherwise. |

Names are extracted automatically from whatever is present (see `lib/whatsapp/contacts.ts`).
msgstore.db alone provides group names, business names, and names from shared contact cards.
Address-book names for individual chats only exist in `wa.db`. Replacing any of these files is
picked up without restarting.

You can override these paths with `WHATSAPP_DATA_DIR`, `WHATSAPP_DB_PATH`, `WHATSAPP_CONTACTS`, `WHATSAPP_WA_DB_PATH` and `WHATSAPP_MEDIA_DIR`.

See [docs/msgstore-schema.md](docs/msgstore-schema.md) for the reverse-engineered schema and message types.

## Opening a chat at a point in time

```ts
import { chatHref } from "@/lib/whatsapp/anchor"

chatHref(jid)                                    // latest messages
chatHref(jid, { at: new Date("2021-03-05") })    // around a moment
chatHref(jid, { messageId: 123456 })             // around a message
```

URLs: `/chats/<jid>`, `/chats/<jid>?at=<ms|ISO date>` and `/chats/<jid>?message=<id>`.

To embed a chat anywhere:

```tsx
<ChatView jid={jid} anchor={{ type: "time", at }} syncUrl={false} />
```

For lower-level access, use `useMessageStream(jid, anchor)` to stream pages yourself, or
`<MessageView message={m} />` to render any single message.

## Layout

```
lib/whatsapp/
  types.ts        domain types (Message union, ChatSummary, ChatAnchor…). Client-safe.
  constants.ts    media_wa_type / status / system action codes
  decoders.ts     Java-serialization, protobuf and vCard decoders
  backend/        one adapter per DB layout (legacy.ts, modern.ts) → normalized RawRow
  mapper.ts       RawRow → Message
  queries.ts      server data access: listChats, getChat, getMessages, locateMessage, thumbnails, media
  contacts.ts     name resolution
  anchor.ts       chat URLs and anchors. Client-safe.
  format.ts       previews, system-event text, dates. Client-safe.
  client.ts       browser fetchers for the API
app/api/          chats, messages (cursor paging), thumbnails, media (Range streaming)
hooks/use-message-stream.ts   bidirectional windowed paging
components/messages/          one renderer per message kind + registry.ts
components/chat/              sidebar, header, message list, ChatView
```

To add a new message kind: extend `Message` in `types.ts`, map it in `mapper.ts`,
add a component under `components/messages/kinds/`, and register it in `registry.ts`.
TypeScript will flag any kind that's missing a renderer.
