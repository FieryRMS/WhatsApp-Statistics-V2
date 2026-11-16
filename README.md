# WhatsApp Stats

A read-only explorer for an Android WhatsApp `msgstore.db`, laid out like WhatsApp.
Works with both the legacy (`messages` table, until ~2022) and modern (`message` table) database layouts.
Built with Next.js, shadcn/ui and Node's built-in `node:sqlite`.

```bash
npm run dev   # http://localhost:3000
```

## Setup

```bash
git clone --recurse-submodules <repo>   # or, in an existing clone:
git submodule update --init             # fetches vendor/whatsapp-emoji-linux (~80 MB of emoji PNGs)
npm install
```

## Emoji

Emoji are drawn with WhatsApp's own artwork from the
[`whatsapp-emoji-linux`](https://github.com/dmlls/whatsapp-emoji-linux) submodule (`vendor/`),
not the operating system's emoji font, so they look the same on every device. `/emoji/<codepoints>.png`
serves its PNGs with a one-year cache, and `components/emoji/emoji-text.tsx` swaps emoji in text for
those images. Anything missing from the set falls back to the system emoji. The submodule's code is
Apache-2.0; the emoji artwork itself is WhatsApp's.

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

## Getting the data

### Media

Copy the phone's media folder over USB (or any file manager) into `whatsapp-data/Media/`. No root
is needed. On Android 11 and later it's at `Android/media/com.whatsapp/WhatsApp/Media`; older
versions keep it at `WhatsApp/Media` in internal storage.

### Databases

> [!WARNING]
> You follow these steps at your own risk. They move your WhatsApp account to another device and
> back, and I'm not responsible for lost chats, bans or anything else that goes wrong. Make sure
> your Google Drive backup is up to date before you start.

`msgstore.db` and `wa.db` live in WhatsApp's private app directory, which only root can read. The
`msgstore.db.crypt*` files in the phone's storage are encrypted backups and won't work. You can root
your phone, or you can restore your Google Drive backup into an Android emulator on your computer
and copy the files out of it. This is how it was done with [Waydroid](https://waydro.id) on Linux.
Waydroid's Android data is a plain folder on the host, so the emulator doesn't need to be rooted:

1. **Set up Waydroid with Google Play.** `sudo waydroid init -s GAPPS` downloads the GApps image.
   Add `-f` to start over.
2. **Make it look like a Pixel.** This step is required. Otherwise WhatsApp detects Waydroid's
   LineageOS image and refuses to run, saying custom ROMs aren't supported. Add this to `/var/lib/waydroid/waydroid.cfg`, then run `sudo waydroid upgrade --offline` so the
   values are written into `waydroid_base.prop`, and restart the container:

   ```ini
   [properties]
   ro.product.brand = google
   ro.product.manufacturer = Google
   ro.product.model = Pixel 5
   ro.product.device = redfin
   ro.product.name = redfin
   ro.build.product = redfin
   ro.build.flavor = redfin-user
   ro.build.type = eng
   ro.build.tags = release-keys
   ro.build.description = redfin-user 13 TQ3A.230901.001 10750268 release-keys
   ro.build.display.id = TQ3A.230901.001
   ro.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.build.version.incremental = 10750268
   ro.build.host = abfarm-01234
   ro.build.user = android-build
   ro.vendor_dlkm.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.system.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.vendor.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.odm.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.product.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.system_ext.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ro.bootimage.build.fingerprint = google/redfin/redfin:13/TQ3A.230901.001/10750268:user/release-keys
   ```

3. **Back up on your phone.** In WhatsApp, go to Settings → Chats → Chat backup and back up to Google
   Drive.
4. **Sign in to Google in Waydroid.** Use the Google account that holds the backup. WhatsApp only
   finds a Drive backup through a signed-in account.
5. **Install WhatsApp and register.** Install it from Play Store. If it opens on "link as a companion
   device", pick the option to register instead from the menu.
   - Enter your phone number.
   - WhatsApp asks to restore your messages. **Skip** it, unless Google Drive is already offered.
   - WhatsApp then sends a code. Enter it to log in. This signs WhatsApp out on your phone. The
     phone will offer to delete the account or log back in. **Don't touch either option**; leave the
     phone alone until you're done.
   - Once you're logged in, WhatsApp asks to restore again, this time with **Google Drive** as an
     option. Choose it. The other option, restoring from another device, doesn't work on Waydroid
     because it has no Wi-Fi.
   - Wait for the restore to finish. WhatsApp restores your messages first. If your backup includes
     media, it downloads that afterwards.
6. **Copy the databases out.** Stop Waydroid first so everything is flushed to the `.db` files, then
   copy them straight from the host:

   ```bash
   waydroid session stop
   sudo cp ~/.local/share/waydroid/data/data/com.whatsapp/databases/{msgstore,wa}.db whatsapp-data/
   sudo chown "$USER" whatsapp-data/*.db
   ```

   You only need the message restore for the databases, so you can copy them once it's done. If your
   backup includes media, you can wait for that download too and copy it from
   `~/.local/share/waydroid/data/media/0/Android/media/com.whatsapp/WhatsApp/Media/` (with `sudo`)
   instead of from the phone.
7. **Log back in on your phone.** Choose to log back in and verify your number again. Your chats on
   the phone stay as they were. This signs the emulator out.

## Statistics

`/stats` (chart icon in the sidebar) is a graph builder:

1. **What to count:** messages, words, characters, average length, media, emojis, a word or phrase,
   reactions given or received, tags made or received, or active people. Add several to compare
   them on one chart.
2. **Group by:** day, week, month or year, hour of day, day of week, person, chat, message type,
   emoji, word, or a single total. Time axes can also rank the busiest periods.
3. **Filters:** chats, people, message types, dates, words and emojis, for the whole chart or per series.
   Each series can be **split** into one line per person, chat, type, emoji, word, weekday, hour or year.
   The top 7 are shown and the rest fold into "Other".
4. **Display:** lines, area, bars, stacked bars, a heatmap or a word cloud. Values can be raw, a running total,
   a share of the total, smoothed, or **indexed to each series' peak**. Use indexing to compare
   series of different sizes on one axis; there are never two y-axes.

**Words and clouds:** group a Words count by word to rank the most used words, or show them as a
cloud (emoji counts can be drawn as a cloud too). Words are lowercased runs of letters, so Bangla
script works; links, @tags and bare numbers are left out. Each account has a **hidden words** list
(`app-data/hidden-words/u<id>.json`). It starts with common English, Banglish and Bangla filler
words ("in", "to", "the", "ki", "na", …) and is edited in the builder whenever word grouping is on.
Clicking a word can also chart it over time or hide it.

Click any point, bar, cell or table row to open that moment in the chat. **Back** returns to exactly
the chart you were on, including edits, zoom, the table view and scroll position.

**Zoom:** on timelines, drag across the chart to zoom into that stretch, Ctrl+scroll or pinch to zoom
around the cursor, or use the range slider underneath. Shift+drag (or a sideways trackpad swipe) pans.
Double-click or press "Reset zoom" to see everything again.

Graphs always use real names, never "You", so a chart reads the same whoever is viewing. The phone
owner appears under the name set in Contact names. A time bucket opens at its
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

When someone other than the owner is viewing, their messages show as "You" on the right in chats.
The owner's messages show under the **phone owner's name**. You set it in Contact names (the pinned
"Phone owner" row), and it's stored in the account's mapping under the key `"me"`. This applies to
chats, previews, quotes, reactions and system events. Graphs use real names throughout. In the people
filter, "You (whoever is viewing)" follows the viewer. The adjustment
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
This includes the phone owner: give the owner's own number the owner's name, and it merges into the
owner. Only names set in the mapping merge people; automatic names never do. For the same reason, when a
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
