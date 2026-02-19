use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};

use crate::config::{self, Config};

/// Extract iMessages from chat.db into warehouse.
pub fn extract(conn: &Connection, _config: &Config) -> Result<usize> {
    let chat_db_path = config::get_imessages_db_path()
        .ok_or_else(|| anyhow::anyhow!("iMessages database not found"))?;

    let src = Connection::open_with_flags(&chat_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("Cannot open chat.db (Full Disk Access required)")?;

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "DELETE FROM imessage_handles;
         DELETE FROM imessage_chats;
         DELETE FROM imessage_messages;
         DELETE FROM imessage_attachments;",
    )?;

    let handles = extract_handles(&src, &tx)?;
    let chats = extract_chats(&src, &tx)?;
    let messages = extract_messages(&src, &tx)?;
    let attachments = extract_attachments(&src, &tx)?;

    tx.commit()?;

    eprintln!(
        "  handles: {handles}, chats: {chats}, messages: {messages}, attachments: {attachments}"
    );
    Ok(messages)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS imessage_handles (
            handle_id INTEGER PRIMARY KEY,
            identifier TEXT,
            service TEXT,
            country TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS imessage_chats (
            chat_id INTEGER PRIMARY KEY,
            guid TEXT,
            chat_identifier TEXT,
            service_name TEXT,
            display_name TEXT,
            group_id TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS imessage_messages (
            message_id INTEGER PRIMARY KEY,
            guid TEXT,
            text TEXT,
            handle_id INTEGER,
            chat_id INTEGER,
            service TEXT,
            is_from_me INTEGER,
            is_read INTEGER,
            message_date TEXT,
            date_read TEXT,
            date_delivered TEXT,
            cache_has_attachments INTEGER,
            is_audio_message INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS imessage_attachments (
            attachment_id INTEGER PRIMARY KEY,
            guid TEXT,
            filename TEXT,
            mime_type TEXT,
            total_bytes INTEGER,
            transfer_name TEXT,
            created_date TEXT,
            message_id INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;
    Ok(())
}

fn extract_handles(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare("SELECT ROWID, id, service, country FROM handle")?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO imessage_handles (handle_id, identifier, service, country)
         VALUES (?1, ?2, ?3, ?4)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    for row in rows {
        let (id, identifier, service, country) = row?;
        insert.execute(params![id, identifier, service, country])?;
        count += 1;
    }
    Ok(count)
}

fn extract_chats(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare(
        "SELECT ROWID, guid, chat_identifier, service_name, display_name, group_id FROM chat",
    )?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO imessage_chats
         (chat_id, guid, chat_identifier, service_name, display_name, group_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;

    for row in rows {
        let (id, guid, ci, sn, dn, gi) = row?;
        insert.execute(params![id, guid, ci, sn, dn, gi])?;
        count += 1;
    }
    Ok(count)
}

fn extract_messages(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare(
        "SELECT
            m.ROWID,
            m.guid,
            m.text,
            m.handle_id,
            cmj.chat_id,
            m.service,
            m.is_from_me,
            m.is_read,
            CASE WHEN m.date > 0
                THEN datetime(m.date / 1000000000 + 978307200, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN m.date_read > 0
                THEN datetime(m.date_read / 1000000000 + 978307200, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN m.date_delivered > 0
                THEN datetime(m.date_delivered / 1000000000 + 978307200, 'unixepoch', 'localtime')
                ELSE NULL END,
            m.cache_has_attachments,
            m.is_audio_message
         FROM message m
         LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id",
    )?;

    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO imessage_messages
         (message_id, guid, text, handle_id, chat_id, service, is_from_me, is_read,
          message_date, date_read, date_delivered, cache_has_attachments, is_audio_message)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<i64>>(11)?,
            row.get::<_, Option<i64>>(12)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9, r.10, r.11, r.12
        ])?;
        count += 1;
    }
    Ok(count)
}

fn extract_attachments(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare(
        "SELECT
            a.ROWID,
            a.guid,
            a.filename,
            a.mime_type,
            a.total_bytes,
            a.transfer_name,
            CASE WHEN a.created_date > 0
                THEN datetime(a.created_date / 1000000000 + 978307200, 'unixepoch', 'localtime')
                ELSE NULL END,
            maj.message_id
         FROM attachment a
         LEFT JOIN message_attachment_join maj ON a.ROWID = maj.attachment_id",
    )?;

    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO imessage_attachments
         (attachment_id, guid, filename, mime_type, total_bytes, transfer_name,
          created_date, message_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<i64>>(7)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7])?;
        count += 1;
    }
    Ok(count)
}
