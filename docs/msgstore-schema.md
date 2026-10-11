# msgstore.db — reverse-engineered notes

Android WhatsApp has shipped two layouts, and the app supports both
([`lib/whatsapp/backend/`](../lib/whatsapp/backend/) picks one automatically):

- **Legacy** (until ~2022): every message in the `messages` table. Databases from the
  migration period contain both tables; the one holding the messages wins.
- **Modern**: the `message` table plus side tables. See [Modern layout](#modern-layout) below.

Both use the same type codes, status codes and system-event action codes.

Code: [`lib/whatsapp/constants.ts`](../lib/whatsapp/constants.ts) (codes),
[`backend/legacy.ts`](../lib/whatsapp/backend/legacy.ts) / [`backend/modern.ts`](../lib/whatsapp/backend/modern.ts)
(rows → normalized `RawRow`), [`lib/whatsapp/mapper.ts`](../lib/whatsapp/mapper.ts) (`RawRow` → `Message`).

The sections up to "Timestamps & ordering" describe the legacy layout (counts are from a mid-2022 backup).

## Tables that matter

| Table | Key | Used for |
| --- | --- | --- |
| `messages` | `_id`, chat = `key_remote_jid` | every message (index `messages_jid_id_index (key_remote_jid, _id)` drives paging) |
| `chat` + `jid` | `chat.jid_row_id → jid._id` | group subject, archived flag |
| `message_media` | `message_row_id = messages._id` | width/height, file path, size, page count, animated sticker |
| `message_thumbnails` | `(key_remote_jid, key_from_me, key_id)` | JPEG thumbnails for images/videos/GIFs/docs/location |
| `media_hash_thumbnail` | `media_hash` | PNG thumbnails for stickers |
| `messages_quotes` | `messages.quoted_row_id → _id` | copy of the quoted message (same columns as `messages`) |
| `message_add_on` + `message_add_on_reaction` | `parent_message_row_id = messages._id` | reactions (`sender_jid_row_id = -1` means you) |
| `message_view_once_media` | `message_row_id` | view-once state (`> 0` = opened) |
| `missed_call_logs` | `message_row_id` | `video_call` flag for missed calls |
| `message_forwarded` | `message_row_id` | `forward_score` (≥ 5 is "forwarded many times") |
| `messages_vcards` | `message_row_id` | shared vCards (also used as a fallback name source) |

There are **no contact names or push names** in msgstore.db (they live in `wa.db`).
Names are built automatically from an optional `wa.db`, group subjects and subject
history, group invites, business names, and shared vCards. See `lib/whatsapp/contacts.ts`.

## `media_wa_type`

| Code | Kind | Count | Notes |
| ---: | --- | ---: | --- |
| -1 | placeholder | 1 | `key_remote_jid = '-1'`, skipped |
| 0 | text | 633,433 | `data` = text. Link previews: `media_caption` = title, `media_name` = description, `thumb_image` = serialized `byte[]` JPEG. **System events also use 0** (see below). |
| 1 | image | 21,465 | `media_caption` = caption |
| 2 | audio | 629 | `origin = 1` → recorded voice note (PTT), opus |
| 3 | video | 962 | `media_caption` = caption |
| 4 | contact | 41 | `data` = vCard |
| 5 | location | 1 | `latitude`/`longitude`; `media_name` = "name\naddress" |
| 9 | document | 1,756 | `media_caption` mirrors the filename |
| 10 | missed call | 369 | stored as `status = 6`, `key_from_me = 1`, but the caller is the other side |
| 11 | unknown | 1 | no payload; rendered as "unsupported" |
| 12 | payment request | 3 | protobuf in `raw_data`: currency, note, `Money{value, offset=1000}` |
| 13 | GIF | 802 | MP4 file; may have a caption |
| 14 | contact list | 2 | `raw_data` = Java-serialized `ArrayList<String>` of vCards |
| 15 | deleted for everyone | 6,814 | `edit_version = 7`, `media_name` = original key id |
| 20 | sticker | 1,636 | WebP; `message_media.is_animated_sticker` |
| 36 | disappearing-messages notice | 5 | no payload |
| 42 | view-once photo | 13 | |
| 43 | view-once video | 2 | |

Your earlier notes were right, apart from these corrections: **11** is still unknown
(a single empty row), **12** is a payment request, **36** is a disappearing-messages
notice, and **43** (view-once video) was missing.

## `status`

| Value | Meaning |
| ---: | --- |
| 0 | received |
| 4 | sent (one tick) |
| 5 | delivered (two grey ticks) |
| 6 | **system/control message** |
| 8 | voice note played by recipient |
| 13 | read (blue ticks) |
| 10, 12, 16 | incoming only, local playback/read flags. Not shown. |

## System events (`status = 6`, action code in `media_size`)

Payloads: `remote_resource` = actor JID; `data` = text value;
`thumb_image` = Java-serialized `String` or `ArrayList<String>` of JIDs.

| Code | Event | Payload |
| ---: | --- | --- |
| 1 | subject changed | `data` = new subject, `thumb_image` = old subject |
| 2 | temp group subject | `data` |
| 4 | participant joined | actor |
| 5 | participant left | actor |
| 6 | group icon changed | `thumb_image` = `ProfilePhotoChange` object |
| 9 | broadcast list created | `thumb_image` = recipient list |
| 10 | number changed (group) | actor = old JID, `thumb_image` = new JID |
| 11 | group created | `data` = subject |
| 12 | participants added | actor + `thumb_image` list |
| 13 | participants left | `thumb_image` list |
| 14 | participants removed | actor + `thumb_image` list |
| 15 / 16 | promoted / demoted admin | `thumb_image` list |
| 18 | security code changed | actor (groups) or the chat (DMs) |
| 19 | end-to-end encryption notice | — |
| 20 | joined via invite link | `thumb_image` list |
| 27 | description changed | `data` |
| 28 | number changed (DM) | `media_name` = old JID, `media_caption` = new JID |
| 29, 31, 32, 33 | group settings changed | exact setting unknown |
| 46 / 50 | business account / changed to standard | — |
| 56 | disappearing-messages timer changed | actor |
| 58 | blocked / unblocked | `data` = "true" / "false" |
| 67, 68 | business uses a secure service from Meta | — |
| 70 | group call started | `media_caption` = "audio" / "video" |

## Timestamps & ordering

`timestamp` is epoch **milliseconds**. `_id` is global across chats but monotonic
within a chat, so the app pages by `_id` (keyset pagination) and finds a moment
with `MIN(_id) WHERE key_remote_jid = ? AND timestamp >= ?`.

## Modern layout

Researched on an October 2026 backup (880k messages).

| Table | Key | Used for |
| --- | --- | --- |
| `message` | `_id`, `chat_row_id → chat._id` | every message: `message_type`, `text_data` (text, or the caption for media), `sender_jid_row_id → jid` |
| `message_media` | `message_row_id` | as in legacy. Captions are in `message.text_data`; documents repeat the filename there. |
| `message_thumbnail` | `message_row_id` | link previews, locations, some media. **Image/video thumbnails are no longer stored in the DB.** |
| `media_hash_thumbnail` | `message_media.file_hash` | sticker thumbnails |
| `message_text` | `message_row_id` | link preview: `page_title`, `description`, `url` |
| `message_quoted` (+ `_media`, `_text`) | `message_row_id` = the reply | quoted message stub; target found by `(parent_message_chat_row_id, from_me, key_id)`; `thumbnail` columns hold the quote's thumbnail |
| `message_system` | `message_row_id` | system event `action_type` (same codes as legacy) |
| `message_system_chat_participant` | `message_row_id` | event targets (added/removed/joined…) |
| `message_system_value_change` | `message_row_id` | `old_data` (e.g. old subject) |
| `message_system_number_change` | `message_row_id` | `old_jid_row_id`, `new_jid_row_id` |
| `message_system_linked_group_call` | `message_row_id` | `is_video_call` for action 70 |
| `message_ephemeral_setting` | `message_row_id` | disappearing-messages `setting_duration` (seconds) for action 56 / type 36 |
| `message_revoked` | `message_row_id` | deletions; `admin_jid_row_id` = deleted by a group admin |
| `message_edit_info` | `message_row_id` | message was edited |
| `message_location` | `message_row_id` | `latitude`, `longitude`, `place_name`, `place_address` |
| `message_vcard` | `message_row_id` | vCards for types 4 and 14 |
| `message_mentions` | `message_row_id → jid` | @mentions (the text contains `@<jid user>`) |
| `message_poll` + `message_poll_option` | `message_row_id` | poll question = `text_data`; options with `vote_total` |
| `message_call_log → call_log` | `message_row_id` | call bubbles: `video_call`, `duration`, `call_result` (5 = connected) |
| `message_album` | `message_row_id` | album header: `image_count`, `video_count` (the items follow as separate messages) |
| `jid_map` | `lid_row_id → jid_row_id` | maps privacy IDs (`…@lid`) to phone JIDs |
| `lid_display_name` | `lid_row_id` | the name a LID user set for themselves |
| `newsletter` | `chat_row_id` | channel names |

### Modern-only `message_type` codes

| Code | Kind | Notes |
| ---: | --- | --- |
| 7 | system event | details in `message_system*` |
| 16 | live location | `message_location` |
| 64 | deleted by admin | `message_revoked.admin_jid_row_id` |
| 66 | poll | |
| 90 | call bubble | `message_call_log` |
| 99 | album header | |
| 28, 62 | WhatsApp tip videos | rendered via the generic media fallback |
| 32, 49 | button / list replies | rendered as text |
| 11, 27, 45, 98, 110, 112, 116, 118 | unknown | rare, usually no payload; shown as "unsupported" |

Unknown codes fall back on whatever content the row carries: a media row is shown by its MIME type
(or file extension), a row with text is shown as text.

### Notes

- Chats that contain only system notices are hidden from the chat list, as WhatsApp does.
  In the 2026 backup that's about 2,800 of 3,200 chats (mostly security-code and business notices).
- The chat list is ordered by the last non-system message, which matches `chat.sort_timestamp`.
- Senders and DM chats can be LIDs (`…@lid`); `contacts.ts` maps them to phone JIDs via `jid_map`,
  so a person has one identity and one name everywhere.
- System action codes seen only in the modern backup and not yet identified: 79, 84, 85, 91, 101–115, 118, 129, 132, 134, 140, 165.
  These show as "System message (code N)".
