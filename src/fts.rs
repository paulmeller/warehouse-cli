use anyhow::Result;
use rusqlite::Connection;

use crate::db;

/// Rebuild all FTS5 indexes and return counts.
pub fn rebuild_all_fts(conn: &Connection) -> Result<Vec<(String, i64)>> {
    let mut counts = Vec::new();

    counts.push(("messages".into(), populate_messages_fts(conn)?));
    counts.push(("notes".into(), populate_notes_fts(conn)?));
    counts.push(("contacts".into(), populate_contacts_fts(conn)?));
    counts.push(("photos".into(), populate_photos_fts(conn)?));
    counts.push(("documents".into(), populate_documents_fts(conn)?));
    counts.push(("reminders".into(), populate_reminders_fts(conn)?));

    Ok(counts)
}

fn populate_messages_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "imessage_messages") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM messages_fts; DELETE FROM messages_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO messages_fts(rowid, sender_name, chat_name, text)
        SELECT
            m.message_id,
            CASE
                WHEN m.is_from_me = 1 THEN 'Me'
                ELSE COALESCE(
                    MAX(TRIM(COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, ''))),
                    h.identifier,
                    'Unknown'
                )
            END,
            COALESCE(ch.display_name, ch.chat_identifier, ''),
            m.text
        FROM imessage_messages m
        LEFT JOIN imessage_handles h ON m.handle_id = h.handle_id
        LEFT JOIN contact_phones cp ON h.identifier = cp.phone_number
        LEFT JOIN contact_emails ce ON h.identifier = ce.email
        LEFT JOIN contacts c ON c.identifier = COALESCE(
            cp.contact_identifier, ce.contact_identifier
        )
        LEFT JOIN imessage_chats ch ON m.chat_id = ch.chat_id
        WHERE m.text IS NOT NULL AND m.text != ''
        GROUP BY m.message_id;

        INSERT INTO messages_fts_map(fts_rowid, message_id)
        SELECT message_id, message_id FROM imessage_messages
        WHERE text IS NOT NULL AND text != '';
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

fn populate_notes_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "obsidian_notes") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM notes_fts; DELETE FROM notes_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO notes_fts(rowid, title, body, tags)
        SELECT
            n.id,
            COALESCE(n.title, ''),
            COALESCE(n.body, ''),
            COALESCE(
                (SELECT GROUP_CONCAT(tag, ' ')
                 FROM obsidian_tags
                 WHERE note_id = n.id),
                ''
            )
        FROM obsidian_notes n;

        INSERT INTO notes_fts_map(fts_rowid, note_id)
        SELECT id, id FROM obsidian_notes;
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM notes_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

fn populate_contacts_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "contacts") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM contacts_fts; DELETE FROM contacts_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO contacts_fts(rowid, full_name, organization, note)
        SELECT
            rowid,
            TRIM(
                COALESCE(given_name, '') || ' ' ||
                COALESCE(family_name, '') || ' ' ||
                COALESCE(nickname, '')
            ),
            TRIM(COALESCE(organization, '') || ' ' || COALESCE(job_title, '')),
            COALESCE(note, '')
        FROM contacts;

        INSERT INTO contacts_fts_map(fts_rowid, contact_identifier)
        SELECT rowid, identifier FROM contacts;
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM contacts_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

fn populate_photos_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "photos_assets") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM photos_fts; DELETE FROM photos_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO photos_fts(rowid, title, filename, people, album)
        SELECT
            a.asset_id,
            COALESCE(a.title, ''),
            COALESCE(a.filename, ''),
            COALESCE(
                (SELECT GROUP_CONCAT(COALESCE(p.full_name, p.display_name), ' ')
                 FROM photos_faces f
                 JOIN photos_people p ON f.person_id = p.person_id
                 WHERE f.asset_id = a.asset_id),
                ''
            ),
            ''
        FROM photos_assets a;

        INSERT INTO photos_fts_map(fts_rowid, asset_id)
        SELECT asset_id, asset_id FROM photos_assets;
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM photos_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

fn populate_documents_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "documents") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM documents_fts; DELETE FROM documents_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO documents_fts(rowid, title, filename, content, file_type)
        SELECT
            id,
            COALESCE(title, ''),
            COALESCE(filename, ''),
            COALESCE(content, ''),
            COALESCE(file_type, '')
        FROM documents;

        INSERT INTO documents_fts_map(fts_rowid, document_id)
        SELECT id, id FROM documents;
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM documents_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

fn populate_reminders_fts(conn: &Connection) -> Result<i64> {
    if !db::table_exists(conn, "reminders") {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("DELETE FROM reminders_fts; DELETE FROM reminders_fts_map;")?;

    tx.execute_batch(
        "
        INSERT INTO reminders_fts(rowid, title, notes, list_name, location)
        SELECT
            id,
            COALESCE(title, ''),
            COALESCE(notes, ''),
            COALESCE(list_name, ''),
            COALESCE(location, '')
        FROM reminders;

        INSERT INTO reminders_fts_map(fts_rowid, reminder_id)
        SELECT id, reminder_id FROM reminders;
        ",
    )?;

    let count: i64 = tx.query_row("SELECT COUNT(*) FROM reminders_fts", [], |r| r.get(0))?;
    tx.commit()?;
    Ok(count)
}

