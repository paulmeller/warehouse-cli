use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use std::path::PathBuf;

use crate::config::Config;
use crate::connector::Connector;
use crate::db;

pub struct ContactsConnector;

impl Connector for ContactsConnector {
    fn name(&self) -> &str {
        "contacts"
    }

    fn description(&self) -> &str {
        "macOS AddressBook contacts"
    }

    fn create_source_tables(&self, conn: &Connection) -> Result<()> {
        create_tables(conn)
    }

    fn extract(&self, conn: &Connection, config: &Config) -> Result<usize> {
        extract(conn, config)
    }

    fn fts_schema_sql(&self) -> Option<&str> {
        Some(
            "CREATE VIRTUAL TABLE IF NOT EXISTS contacts_fts USING fts5(
                full_name,
                organization,
                note,
                tokenize='porter unicode61'
            );

            CREATE TABLE IF NOT EXISTS contacts_fts_map (
                fts_rowid INTEGER PRIMARY KEY,
                contact_identifier TEXT NOT NULL,
                UNIQUE(contact_identifier)
            );",
        )
    }

    fn populate_fts(&self, conn: &Connection) -> Result<i64> {
        if !db::table_exists(conn, "contacts") {
            return Ok(0);
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute_batch("DELETE FROM contacts_fts; DELETE FROM contacts_fts_map;")?;

        tx.execute_batch(
            "INSERT INTO contacts_fts(rowid, full_name, organization, note)
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
            SELECT rowid, identifier FROM contacts;",
        )?;

        let count: i64 = tx.query_row("SELECT COUNT(*) FROM contacts_fts", [], |r| r.get(0))?;
        tx.commit()?;
        Ok(count)
    }
}

/// Extract contacts from macOS AddressBook database into warehouse.
pub fn extract(conn: &Connection, _config: &Config) -> Result<usize> {
    let ab_path =
        find_addressbook_db().ok_or_else(|| anyhow::anyhow!("AddressBook database not found"))?;

    let src = Connection::open_with_flags(&ab_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("Cannot open AddressBook database (Full Disk Access required)")?;

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "DELETE FROM contact_relations;
         DELETE FROM contact_social_profiles;
         DELETE FROM contact_urls;
         DELETE FROM contact_addresses;
         DELETE FROM contact_emails;
         DELETE FROM contact_phones;
         DELETE FROM contacts;",
    )?;

    let contacts = extract_contacts(&src, &tx)?;
    let phones = extract_phones(&src, &tx)?;
    let emails = extract_emails(&src, &tx)?;

    tx.commit()?;

    eprintln!("  contacts: {contacts}, phones: {phones}, emails: {emails}");
    Ok(contacts)
}

fn find_addressbook_db() -> Option<PathBuf> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let home = dirs::home_dir()?;

    // Primary location on modern macOS
    let sources_dir = home.join("Library/Application Support/AddressBook/Sources");
    if sources_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&sources_dir) {
            for entry in entries.flatten() {
                let db_path = entry.path().join("AddressBook-v22.abcddb");
                if db_path.exists() {
                    return Some(db_path);
                }
            }
        }
    }

    // Fallback: direct path
    let direct = home.join("Library/Application Support/AddressBook/AddressBook-v22.abcddb");
    if direct.exists() {
        return Some(direct);
    }

    None
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS contacts (
            identifier TEXT PRIMARY KEY,
            given_name TEXT,
            family_name TEXT,
            middle_name TEXT,
            name_prefix TEXT,
            name_suffix TEXT,
            nickname TEXT,
            organization TEXT,
            department TEXT,
            job_title TEXT,
            birthday TEXT,
            note TEXT,
            has_image INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS contact_phones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            phone_number TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );

        CREATE TABLE IF NOT EXISTS contact_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            email TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );

        CREATE TABLE IF NOT EXISTS contact_addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            street TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            country TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );

        CREATE TABLE IF NOT EXISTS contact_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            url TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );

        CREATE TABLE IF NOT EXISTS contact_social_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            service TEXT,
            username TEXT,
            url TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );

        CREATE TABLE IF NOT EXISTS contact_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_identifier TEXT,
            label TEXT,
            name TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_identifier) REFERENCES contacts(identifier)
        );
        ",
    )?;
    Ok(())
}

fn extract_contacts(src: &Connection, dst: &Connection) -> Result<usize> {
    // ZABCDRECORD is the main contacts table; notes are in ZABCDNOTE
    let mut stmt = src.prepare(
        "SELECT
            r.ZUNIQUEID,
            r.ZFIRSTNAME,
            r.ZLASTNAME,
            r.ZMIDDLENAME,
            r.ZTITLE,
            r.ZSUFFIX,
            r.ZNICKNAME,
            r.ZORGANIZATION,
            r.ZDEPARTMENT,
            r.ZJOBTITLE,
            CASE WHEN r.ZBIRTHDAY IS NOT NULL
                THEN date(r.ZBIRTHDAY + 978307200, 'unixepoch')
                ELSE NULL END,
            n.ZTEXT,
            CASE WHEN r.ZIMAGEDATA IS NOT NULL THEN 1 ELSE 0 END
         FROM ZABCDRECORD r
         LEFT JOIN ZABCDNOTE n ON n.ZCONTACT = r.Z_PK
         WHERE r.ZUNIQUEID IS NOT NULL",
    )?;

    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO contacts
         (identifier, given_name, family_name, middle_name, name_prefix, name_suffix,
          nickname, organization, department, job_title, birthday, note, has_image)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, i64>(12)?,
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

fn extract_phones(src: &Connection, dst: &Connection) -> Result<usize> {
    let has_table: bool = src
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='ZABCDPHONENUMBER'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);

    if !has_table {
        return Ok(0);
    }

    let mut stmt = src.prepare(
        "SELECT
            r.ZUNIQUEID,
            p.ZLABEL,
            p.ZFULLNUMBER
         FROM ZABCDPHONENUMBER p
         JOIN ZABCDRECORD r ON p.ZOWNER = r.Z_PK
         WHERE r.ZUNIQUEID IS NOT NULL",
    )?;

    let mut insert = dst.prepare(
        "INSERT INTO contact_phones (contact_identifier, label, phone_number)
         VALUES (?1, ?2, ?3)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    for row in rows {
        let (identifier, label, phone) = row?;
        // Clean up Apple-style labels like "_$!<Home>!$_"
        let clean_label = label.map(|l| clean_apple_label(&l));
        insert.execute(params![identifier, clean_label, phone])?;
        count += 1;
    }
    Ok(count)
}

fn extract_emails(src: &Connection, dst: &Connection) -> Result<usize> {
    let has_table: bool = src
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='ZABCDEMAILADDRESS'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);

    if !has_table {
        return Ok(0);
    }

    let mut stmt = src.prepare(
        "SELECT
            r.ZUNIQUEID,
            e.ZLABEL,
            e.ZADDRESS
         FROM ZABCDEMAILADDRESS e
         JOIN ZABCDRECORD r ON e.ZOWNER = r.Z_PK
         WHERE r.ZUNIQUEID IS NOT NULL",
    )?;

    let mut insert = dst.prepare(
        "INSERT INTO contact_emails (contact_identifier, label, email)
         VALUES (?1, ?2, ?3)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    for row in rows {
        let (identifier, label, email) = row?;
        let clean_label = label.map(|l| clean_apple_label(&l));
        insert.execute(params![identifier, clean_label, email])?;
        count += 1;
    }
    Ok(count)
}

/// Clean Apple-style labels like "_$!<Home>!$_" to just "Home"
fn clean_apple_label(label: &str) -> String {
    label
        .trim_start_matches("_$!<")
        .trim_end_matches(">!$_")
        .to_string()
}
