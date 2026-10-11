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

## Statistics

`/stats` (chart icon in the sidebar) is a graph builder:

1. **What to count:** messages, words, characters, average length, media, emojis, a word or phrase,
   reactions given or received, tags made or received, or active people. Add several to compare
   them on one chart.
2. **Group by:** day, week, month or year, hour of day, day of week, person, chat, message type,
   emoji, or a single total. Time axes can also rank the busiest periods.
3. **Filters:** chats, people, message types, dates, words and emojis, for the whole chart or per series.
   Each series can be **split** into one line per person, chat, type, emoji, weekday, hour or year.
   The top 7 are shown and the rest fold into "Other".
4. **Display:** lines, area, bars, stacked bars or a heatmap. Values can be raw, a running total,
   a share of the total, smoothed, or **indexed to each series' peak**. Use indexing to compare
   series of different sizes on one axis; there are never two y-axes.

Click any point, bar, cell or table row to open that moment in the chat. **Back** returns to exactly
the chart you were on, including edits, the table view and scroll position. A time bucket opens at its
first matching message, a category at its latest. When a point spans several chats, you pick one.

"Ideas" holds about 30 ready-made statistics. **Save** keeps a statistic, and **Share with everyone**
lists it for all accounts. **Copy link** encodes it in the URL.

Statistics are JSON definitions, never SQL, validated in `lib/stats/schema.ts`. Each account runs
them against its own data, so a shared statistic shows every viewer their own numbers. Chats or
people the viewer can't access are removed from it before it reaches their browser.

The first run builds an in-memory index of every message, about 2–3 s for 850k messages. After
that, queries take milliseconds. Days are bucketed in the server's timezone, and messages with
impossible timestamps are left out.

## Accounts and access

On first start the server prints a one-time **setup token** to its console. Open `/setup` and use it
to create the admin account. Until an admin exists, a new token is printed on every start.

- **Admins** see every chat and manage accounts at `/admin`.
- **Other accounts** see only the chats the admin grants them. A grant can cover a whole chat or a
  date range within it, and there is a per-account switch for opening media files.

How access is enforced:

- **A filtered copy per account.** Each restricted account reads its own SQLite copy containing only
  its granted messages, people and names, built by allowlist into `app-data/snapshots/`. Data
  outside the grant isn't hidden; it isn't there. Every copy is verified before it's used, and it's
  rebuilt automatically when grants or the WhatsApp files change.
- **One gate for all data access.** Every page, API route and server action resolves the session and
  that account's data source in `lib/access/dal.ts`. Query helpers throw if called outside it, so
  they never default to the full database. `proxy.ts` only redirects signed-out visitors.
- **Sessions:** a random token in an HttpOnly, SameSite cookie, which is marked Secure when the
  request arrived over HTTPS. Only its hash is stored. Sessions are revocable, and disabling an
  account or resetting its password ends them immediately.
- **Passwords:** hashed with scrypt. Sign-in is rate-limited per username, and responses don't
  reveal whether a username exists.

### Who's viewing

The data comes from one phone, so "you" in the raw data means the phone owner. A login may be
shared, so every sign-in starts at **Who's viewing?**, where you pick yourself from the people in
the chats. You can change it anytime in **Settings** (or from the name in the sidebar footer). It is
stored only on the session.

When someone other than the owner is viewing, their messages show as "You" on the right. The
owner's messages show under the **phone owner's name**, set in Settings and stored in the account's
name mapping under the key `"me"`. This applies to chats, previews, quotes, reactions, system events
and statistics. In the people filter, "You (whoever is viewing)" follows the viewer. The adjustment
is applied after caching (`lib/whatsapp/viewer.ts`), so sessions never see each other's view.

### Contact names

Each account has its own name mapping, `app-data/names/u<id>.json`, shaped `{ "<jid>": "Name" | null }`.
A string renames that person; `null` hides the name so the phone number shows instead.
The account edits it on its **Contact names** page, and admins edit the same file from `/admin`.

- **Restricted accounts** get names *only* from their mapping. wa.db, push names, contact cards and
  display names are never used for them, and their filtered copy contains no wa.db. The mapping is
  seeded with the names they'd have seen automatically. When their access grows, new people are
  added; existing entries are never overwritten.
- **Admins** see automatic names, with their own mapping applied on top as overrides. An admin's
  mapping starts empty, and "reset" returns a person to their automatic name.

**Same person, several numbers:** give two or more numbers *exactly the same name* in an account's
mapping and they count as one person for that account. Statistics combine them (rankings, splits,
filters, active-people counts), and choosing either one in "Who's viewing" makes all of them "You".
Only names set in the mapping merge people; automatic names never do. For the same reason, when a
mapping is seeded, a name that's already taken gets the number's last digits appended, e.g.
`Mohsin Hossain (…4521)`. The Contact names editor lists merged people under **Same person**.

Accounts live in `app-data/app.db` (gitignored). Use `WHATSAPP_APP_DATA_DIR` to put them elsewhere.

`next dev` and `next start` listen on every network interface by default. Use `-H 127.0.0.1` to keep
the server local, and put a reverse proxy with HTTPS in front if you expose it.

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
lib/auth/         accounts, passwords, sessions, setup token, server actions (app-data/app.db)
lib/access/       dal.ts (session → data source gate), snapshot.ts (per-account filtered copies)
lib/names/        per-account contact-name mappings (store, editor rows, server action)
lib/stats/        statistics: query types/schema, index, engine, presets, saved queries
lib/admin/        admin-only server actions
app/(auth)/       /login, /setup
app/(explorer)/   everything behind sign-in, incl. /admin
app/api/          chats, messages (cursor paging), thumbnails, media (Range streaming)
hooks/use-message-stream.ts   bidirectional windowed paging
components/messages/          one renderer per message kind + registry.ts
components/chat/              sidebar, header, message list, ChatView
```

To add a new message kind: extend `Message` in `types.ts`, map it in `mapper.ts`,
add a component under `components/messages/kinds/`, and register it in `registry.ts`.
TypeScript will flag any kind that's missing a renderer.
