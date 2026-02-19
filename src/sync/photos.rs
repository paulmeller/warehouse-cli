use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};

use crate::config::{self, Config};

/// Extract Apple Photos metadata into warehouse.
pub fn extract(conn: &Connection, _config: &Config) -> Result<usize> {
    let photos_db_path =
        config::get_photos_db_path().ok_or_else(|| anyhow::anyhow!("Photos database not found"))?;

    let src = Connection::open_with_flags(&photos_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("Cannot open Photos.sqlite")?;

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "DELETE FROM photos_faces;
         DELETE FROM photos_people;
         DELETE FROM photos_albums;
         DELETE FROM photos_assets;",
    )?;

    let assets = extract_assets(&src, &tx)?;
    let people = extract_people(&src, &tx)?;
    let faces = extract_faces(&src, &tx)?;
    let albums = extract_albums(&src, &tx)?;

    tx.commit()?;

    eprintln!("  assets: {assets}, people: {people}, faces: {faces}, albums: {albums}");
    Ok(assets)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS photos_assets (
            asset_id INTEGER PRIMARY KEY,
            uuid TEXT,
            filename TEXT,
            directory TEXT,
            title TEXT,
            date_created TEXT,
            date_modified TEXT,
            date_added TEXT,
            latitude REAL,
            longitude REAL,
            width INTEGER,
            height INTEGER,
            duration REAL,
            is_favorite INTEGER,
            is_hidden INTEGER,
            kind INTEGER,
            media_type TEXT,
            camera_make TEXT,
            camera_model TEXT,
            lens_model TEXT,
            iso INTEGER,
            aperture REAL,
            shutter_speed REAL,
            focal_length REAL,
            focal_length_35mm INTEGER,
            flash_fired INTEGER,
            timezone TEXT,
            file_size INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS photos_people (
            person_id INTEGER PRIMARY KEY,
            uuid TEXT,
            full_name TEXT,
            display_name TEXT,
            face_count INTEGER,
            gender_type INTEGER,
            age_type INTEGER,
            verified_type INTEGER,
            is_me_confidence REAL,
            contact_identifier TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS photos_faces (
            face_id INTEGER PRIMARY KEY,
            asset_id INTEGER,
            person_id INTEGER,
            quality REAL,
            center_x REAL,
            center_y REAL,
            size REAL,
            has_smile INTEGER,
            left_eye_closed INTEGER,
            right_eye_closed INTEGER,
            gender_type INTEGER,
            age_type INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES photos_assets(asset_id),
            FOREIGN KEY (person_id) REFERENCES photos_people(person_id)
        );

        CREATE TABLE IF NOT EXISTS photos_albums (
            album_id INTEGER PRIMARY KEY,
            uuid TEXT,
            title TEXT,
            kind INTEGER,
            date_created TEXT,
            start_date TEXT,
            end_date TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;
    Ok(())
}

const APPLE_TS: &str = "978307200";

fn extract_assets(src: &Connection, dst: &Connection) -> Result<usize> {
    // Photos.sqlite schema: title is in ZADDITIONALASSETATTRIBUTES, EXIF in ZEXTENDEDATTRIBUTES
    // The join column between ZASSET and ZEXTENDEDATTRIBUTES varies by macOS version
    let ea_join = if has_column_in(src, "ZASSET", "ZEXTENDEDATTRIBUTES") {
        "LEFT JOIN ZEXTENDEDATTRIBUTES ea ON a.ZEXTENDEDATTRIBUTES = ea.Z_PK"
    } else {
        "LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK"
    };

    let sql = format!(
        "SELECT
            a.Z_PK,
            a.ZUUID,
            a.ZFILENAME,
            a.ZDIRECTORY,
            aa.ZTITLE,
            CASE WHEN a.ZDATECREATED IS NOT NULL
                THEN datetime(a.ZDATECREATED + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN a.ZMODIFICATIONDATE IS NOT NULL
                THEN datetime(a.ZMODIFICATIONDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN a.ZADDEDDATE IS NOT NULL
                THEN datetime(a.ZADDEDDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN a.ZLATITUDE > -180.0 AND a.ZLATITUDE < 180.0
                THEN a.ZLATITUDE ELSE NULL END,
            CASE WHEN a.ZLONGITUDE > -180.0 AND a.ZLONGITUDE < 180.0
                THEN a.ZLONGITUDE ELSE NULL END,
            a.ZWIDTH,
            a.ZHEIGHT,
            a.ZDURATION,
            a.ZFAVORITE,
            a.ZHIDDEN,
            a.ZKIND,
            CASE a.ZKIND WHEN 0 THEN 'photo' WHEN 1 THEN 'video' ELSE 'other' END,
            ea.ZISO,
            ea.ZAPERTURE,
            ea.ZSHUTTERSPEED,
            ea.ZFOCALLENGTH,
            ea.ZFOCALLENGTHIN35MM,
            ea.ZFLASHFIRED,
            aa.ZORIGINALFILESIZE
        FROM ZASSET a
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aa ON a.ZADDITIONALATTRIBUTES = aa.Z_PK
        {ea_join}"
    );

    let mut stmt = src.prepare(&sql)?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO photos_assets
         (asset_id, uuid, filename, directory, title,
          date_created, date_modified, date_added,
          latitude, longitude, width, height, duration,
          is_favorite, is_hidden, kind, media_type,
          iso, aperture, shutter_speed, focal_length,
          focal_length_35mm, flash_fired, file_size)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)",
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
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<f64>>(8)?,
            row.get::<_, Option<f64>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, Option<i64>>(11)?,
            row.get::<_, Option<f64>>(12)?,
            row.get::<_, Option<i64>>(13)?,
            row.get::<_, Option<i64>>(14)?,
            row.get::<_, Option<i64>>(15)?,
            row.get::<_, Option<String>>(16)?,
            row.get::<_, Option<i64>>(17)?,
            row.get::<_, Option<f64>>(18)?,
            row.get::<_, Option<f64>>(19)?,
            row.get::<_, Option<f64>>(20)?,
            row.get::<_, Option<i64>>(21)?,
            row.get::<_, Option<i64>>(22)?,
            row.get::<_, Option<i64>>(23)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9, r.10, r.11, r.12, r.13, r.14, r.15,
            r.16, r.17, r.18, r.19, r.20, r.21, r.22, r.23
        ])?;
        count += 1;
    }
    Ok(count)
}

fn has_column_in(conn: &Connection, table: &str, column: &str) -> bool {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let rows = stmt.query_map([], |row| row.get::<_, String>(1));
    match rows {
        Ok(rows) => rows.flatten().any(|name| name == column),
        Err(_) => false,
    }
}

fn extract_people(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare(
        "SELECT
            Z_PK, ZPERSONUUID, ZFULLNAME, ZDISPLAYNAME, ZFACECOUNT,
            ZGENDERTYPE, ZAGETYPE, ZVERIFIEDTYPE,
            ZISMECONFIDENCE, NULL
         FROM ZPERSON",
    )?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO photos_people
         (person_id, uuid, full_name, display_name, face_count,
          gender_type, age_type, verified_type, is_me_confidence, contact_identifier)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<f64>>(8)?,
            row.get::<_, Option<String>>(9)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9])?;
        count += 1;
    }
    Ok(count)
}

fn extract_faces(src: &Connection, dst: &Connection) -> Result<usize> {
    let mut stmt = src.prepare(
        "SELECT
            Z_PK, ZASSETFORFACE, ZPERSONFORFACE, ZQUALITY,
            ZCENTERX, ZCENTERY, ZSIZE,
            ZHASSMILE, ZISLEFTEYECLOSED, ZISRIGHTEYECLOSED,
            ZGENDERTYPE, ZAGETYPE
         FROM ZDETECTEDFACE",
    )?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO photos_faces
         (face_id, asset_id, person_id, quality,
          center_x, center_y, size,
          has_smile, left_eye_closed, right_eye_closed,
          gender_type, age_type)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, Option<f64>>(5)?,
            row.get::<_, Option<f64>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<i64>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, Option<i64>>(11)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9, r.10, r.11
        ])?;
        count += 1;
    }
    Ok(count)
}

fn extract_albums(src: &Connection, dst: &Connection) -> Result<usize> {
    let sql = format!(
        "SELECT
            Z_PK, ZUUID, ZTITLE, ZKIND,
            CASE WHEN ZCREATIONDATE IS NOT NULL
                THEN datetime(ZCREATIONDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN ZSTARTDATE IS NOT NULL
                THEN datetime(ZSTARTDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN ZENDDATE IS NOT NULL
                THEN datetime(ZENDDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END
         FROM ZGENERICALBUM
         WHERE ZTITLE IS NOT NULL"
    );
    let mut stmt = src.prepare(&sql)?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO photos_albums
         (album_id, uuid, title, kind, date_created, start_date, end_date)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![r.0, r.1, r.2, r.3, r.4, r.5, r.6])?;
        count += 1;
    }
    Ok(count)
}
