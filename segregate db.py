import sqlite3
from configparser import ConfigParser
import os

config = ConfigParser()
config.read("settings.ini")

ChatJID = config.get("DEFAULT", "ChatJID", fallback="None")

if(ChatJID == "None"):
    print("No ChatJID found in settings.ini")
    config['DEFAULT'] = {"ChatJID": "None"}
    with open('settings.ini', 'w') as configfile:
        config.write(configfile)
    print("Config file generated, Please edit ChatJID with the Chat ID you want to generate a statistic for")
    print("exiting...")
    exit()

if(not os.path.isfile("db/msgstore.db")):
    print("Error: db/msgstore.db not found")
    print("exiting...")
    exit()

SegregatedData = sqlite3.connect("db/SegregatedData.db")

SegregatedData.executescript("""
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS messages_quotes;
DROP TABLE IF EXISTS NameLookup;
ATTACH DATABASE "db/msgstore.db" AS msgstore;
""")

SegregatedData.execute("""
CREATE TABLE messages AS
SELECT
	-- _id,
	-- key_remote_jid,
	key_from_me,
	key_id,
	status,
	-- needs_push,
	data,
	timestamp,
	-- media_url,
	-- media_mime_type,
	media_wa_type,
	media_size,
	media_name,
	media_caption,
	-- media_hash,
	media_duration,
	-- origin,
	latitude,
	longitude,
	thumb_image,
	remote_resource,
	-- received_timestamp,
	-- send_timestamp,
	-- receipt_server_timestamp,
	-- receipt_device_timestamp,
	-- read_device_timestamp,
	-- played_device_timestamp,
	-- raw_data, -- I dont know how to parse
	recipient_count,
	-- participant_hash,
	-- starred,
	quoted_row_id,
	mentioned_jids,
	-- multicast_id,
	-- edit_version,
	-- media_enc_hash,
	-- payment_transaction_id,
	forwarded
	-- preview_type,
	-- send_count,
	-- lookup_tables,
	-- future_message_type,
	-- message_add_on_flags
FROM msgstore.messages
WHERE key_remote_jid==?
""", (ChatJID,))
SegregatedData.commit()

SegregatedData.execute("""
CREATE TABLE messages_quotes AS
SELECT
	msgstore.messages_quotes._id,
	-- key_remote_jid,
	msgstore.messages_quotes.key_from_me,
	msgstore.messages_quotes.key_id,
	-- status,
	-- needs_push,
	msgstore.messages_quotes.data,
	-- timestamp,
	-- media_url,
	-- media_mime_type,
	msgstore.messages_quotes.media_wa_type,
	-- media_size,
	msgstore.messages_quotes.media_name,
	msgstore.messages_quotes.media_caption,
	-- media_hash,
	-- media_duration,
	-- origin,
	msgstore.messages_quotes.latitude,
	msgstore.messages_quotes.longitude,
	msgstore.messages_quotes.thumb_image,
	msgstore.messages_quotes.remote_resource,
	-- received_timestamp,
	-- send_timestamp,
	-- receipt_server_timestamp,
	-- receipt_device_timestamp,
	-- read_device_timestamp,
	-- played_device_timestamp,
	-- raw_data, -- I dont know how to parse
	msgstore.messages_quotes.recipient_count,
	-- participant_hash,
	-- starred,
	-- quoted_row_id,
	msgstore.messages_quotes.mentioned_jids,
	-- multicast_id,
	-- edit_version,
	-- media_enc_hash,
	-- payment_transaction_id,
	msgstore.messages_quotes.forwarded
	-- preview_type,
	-- send_count,
	-- lookup_tables,
	-- future_message_type,
	-- message_add_on_flags
FROM messages
LEFT JOIN msgstore.messages_quotes ON messages.quoted_row_id==msgstore.messages_quotes._id 
WHERE messages.quoted_row_id!=0;
""")
SegregatedData.commit()

SegregatedData.executescript("""
CREATE TABLE NameLookup(
"JID" TEXT UNIQUE,
"Name" TEXT
);
INSERT INTO NameLookup(JID)
SELECT DISTINCT remote_resource
FROM messages
WHERE remote_resource IS NOT NULL
""")
SegregatedData.commit()

SegregatedData.execute("""
INSERT INTO NameLookup(JID, Name)
SELECT raw_string_jid, subject
FROM chat_view
WHERE raw_string_jid==?;
""", (ChatJID,))
SegregatedData.commit()

SegregatedData.execute("""
INSERT INTO NameLookup(JID, Name)
VALUES ("CHATJID", ?)
""", (ChatJID,))
SegregatedData.commit()

print("""
Database segregation complete!!

Open the file with a database explorer and edit the "Name" column in the "NameLookup" table with names of the given numbers. This name will be used to show the statistics. Satistics of JIDS with the same Name will be combined together. If the field is kept NULL, then the phone number will be used as the name.

database browser
https://sqlitebrowser.org/dl/
""")