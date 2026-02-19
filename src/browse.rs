use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;

use crate::cli;
use crate::search::SearchResult;

// ========== Messages ==========

pub fn browse_messages(conn: &Connection, args: &cli::MessagesArgs) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            CAST(m.message_id AS TEXT) as id,
            COALESCE(ch.display_name, ch.chat_identifier, 'Chat') as title,
            m.text as snippet,
            m.message_date,
            m.is_from_me,
            COALESCE(h.identifier, '') as sender
        FROM imessage_messages m
        LEFT JOIN imessage_handles h ON m.handle_id = h.handle_id
        LEFT JOIN imessage_chats ch ON m.chat_id = ch.chat_id
        WHERE m.text IS NOT NULL AND m.text != ''",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if args.from_me {
        sql.push_str(" AND m.is_from_me = 1");
    }

    if let Some(ref from) = args.date_from {
        sql.push_str(&format!(" AND m.message_date >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = args.date_to {
        sql.push_str(&format!(" AND m.message_date <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    if let Some(ref contact) = args.contact {
        sql.push_str(&format!(
            " AND m.handle_id IN (
                SELECT DISTINCT h2.handle_id
                FROM contacts c
                JOIN contact_phones cp ON c.identifier = cp.contact_identifier
                JOIN imessage_handles h2 ON
                    REPLACE(REPLACE(REPLACE(REPLACE(h2.identifier, ' ', ''), '-', ''), '(', ''), ')', '') =
                    REPLACE(REPLACE(REPLACE(REPLACE(cp.phone_number, ' ', ''), '-', ''), '(', ''), ')', '')
                WHERE LOWER(COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, '')) LIKE LOWER(?{0})
                   OR LOWER(c.given_name) LIKE LOWER(?{0})
                   OR LOWER(c.family_name) LIKE LOWER(?{0})
                UNION
                SELECT DISTINCT h2.handle_id
                FROM contacts c
                JOIN contact_emails ce ON c.identifier = ce.contact_identifier
                JOIN imessage_handles h2 ON LOWER(h2.identifier) = LOWER(ce.email)
                WHERE LOWER(COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, '')) LIKE LOWER(?{0})
                   OR LOWER(c.given_name) LIKE LOWER(?{0})
                   OR LOWER(c.family_name) LIKE LOWER(?{0})
            )",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{contact}%")));
    }

    if let Some(ref search) = args.search {
        sql.push_str(&format!(" AND m.text LIKE ?{}", params.len() + 1));
        params.push(Box::new(format!("%{search}%")));
    }

    let order = match args.sort.as_str() {
        "contact" => "sender",
        _ => "m.message_date",
    };
    let dir = if args.reverse { "ASC" } else { "DESC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let date: Option<String> = row.get("message_date")?;
        let from_me: Option<bool> = row.get("is_from_me")?;
        let sender: String = row.get("sender")?;
        let mut metadata = HashMap::new();
        if let Some(d) = date {
            metadata.insert("date".into(), serde_json::Value::String(d));
        }
        if let Some(fm) = from_me {
            metadata.insert("from_me".into(), serde_json::json!(fm));
        }
        metadata.insert("sender".into(), serde_json::Value::String(sender));
        Ok(SearchResult {
            result_type: "message".into(),
            id: row.get("id")?,
            title: row.get("title")?,
            snippet: truncate_str(
                &row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                200,
            ),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ========== Notes ==========

pub fn browse_notes(conn: &Connection, args: &cli::NotesArgs) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            CAST(n.id AS TEXT) as id,
            n.title,
            COALESCE(n.body, n.content, '') as body,
            n.modified_at,
            n.file_path,
            n.vault_name
        FROM obsidian_notes n
        WHERE 1=1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(ref vault) = args.vault {
        sql.push_str(&format!(" AND n.vault_name LIKE ?{}", params.len() + 1));
        params.push(Box::new(format!("%{vault}%")));
    }

    if let Some(ref tag) = args.tag {
        sql.push_str(&format!(
            " AND n.id IN (SELECT note_id FROM obsidian_tags WHERE tag LIKE ?{})",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{tag}%")));
    }

    if let Some(ref search) = args.search {
        sql.push_str(&format!(
            " AND (n.title LIKE ?{0} OR n.body LIKE ?{0})",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{search}%")));
    }

    if let Some(ref from) = args.date_from {
        sql.push_str(&format!(" AND n.modified_at >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = args.date_to {
        sql.push_str(&format!(" AND n.modified_at <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    let order = match args.sort.as_str() {
        "created" => "n.created_at",
        "title" => "n.title",
        _ => "n.modified_at",
    };
    let dir = if args.reverse { "ASC" } else { "DESC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let modified: Option<String> = row.get("modified_at")?;
        let path: Option<String> = row.get("file_path")?;
        let vault: Option<String> = row.get("vault_name")?;
        let mut metadata = HashMap::new();
        if let Some(m) = modified {
            metadata.insert("modified".into(), serde_json::Value::String(m));
        }
        if let Some(p) = path {
            metadata.insert("path".into(), serde_json::Value::String(p));
        }
        if let Some(v) = vault {
            metadata.insert("vault".into(), serde_json::Value::String(v));
        }
        Ok(SearchResult {
            result_type: "note".into(),
            id: row.get("id")?,
            title: row
                .get::<_, Option<String>>("title")?
                .unwrap_or_else(|| "Untitled".into()),
            snippet: truncate_str(
                &row.get::<_, Option<String>>("body")?.unwrap_or_default(),
                200,
            ),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ========== Contacts ==========

pub fn browse_contacts(conn: &Connection, args: &cli::ContactsArgs) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            c.identifier as id,
            COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, '') as name,
            c.organization,
            c.job_title,
            c.note
        FROM contacts c
        WHERE (c.given_name IS NOT NULL OR c.family_name IS NOT NULL)",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(ref search) = args.search {
        sql.push_str(&format!(
            " AND (LOWER(COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, '')) LIKE LOWER(?{0})
             OR LOWER(c.organization) LIKE LOWER(?{0}))",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{search}%")));
    }

    if let Some(ref org) = args.org {
        sql.push_str(&format!(
            " AND LOWER(c.organization) LIKE LOWER(?{})",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{org}%")));
    }

    if args.has_email {
        sql.push_str(" AND c.identifier IN (SELECT contact_identifier FROM contact_emails)");
    }

    if args.has_phone {
        sql.push_str(" AND c.identifier IN (SELECT contact_identifier FROM contact_phones)");
    }

    let order = match args.sort.as_str() {
        "org" => "c.organization",
        _ => "name",
    };
    let dir = if args.reverse { "DESC" } else { "ASC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let org: Option<String> = row.get("organization")?;
        let job: Option<String> = row.get("job_title")?;
        let mut metadata = HashMap::new();
        if let Some(o) = &org {
            metadata.insert("org".into(), serde_json::Value::String(o.clone()));
        }
        if let Some(j) = job {
            metadata.insert("job_title".into(), serde_json::Value::String(j));
        }
        let name: String = row.get("name")?;
        Ok(SearchResult {
            result_type: "contact".into(),
            id: row.get("id")?,
            title: name.trim().to_string(),
            snippet: org.unwrap_or_default(),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ========== Documents ==========

pub fn browse_documents(conn: &Connection, args: &cli::DocumentsArgs) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            CAST(d.id AS TEXT) as id,
            COALESCE(d.title, d.filename) as title,
            COALESCE(d.content, '') as content,
            d.file_path,
            d.file_type,
            d.modified_at,
            d.file_size
        FROM documents d
        WHERE 1=1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(ref ft) = args.file_type {
        sql.push_str(&format!(
            " AND LOWER(d.file_type) = LOWER(?{})",
            params.len() + 1
        ));
        params.push(Box::new(format!(".{ft}")));
    }

    if let Some(ref search) = args.search {
        sql.push_str(&format!(
            " AND (d.title LIKE ?{0} OR d.filename LIKE ?{0} OR d.content LIKE ?{0})",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{search}%")));
    }

    if let Some(ref from) = args.date_from {
        sql.push_str(&format!(" AND d.modified_at >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = args.date_to {
        sql.push_str(&format!(" AND d.modified_at <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    let order = match args.sort.as_str() {
        "size" => "d.file_size",
        "name" => "d.filename",
        "type" => "d.file_type",
        _ => "d.modified_at",
    };
    let dir = if args.reverse { "ASC" } else { "DESC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let path: Option<String> = row.get("file_path")?;
        let ftype: Option<String> = row.get("file_type")?;
        let modified: Option<String> = row.get("modified_at")?;
        let size: Option<i64> = row.get("file_size")?;
        let mut metadata = HashMap::new();
        if let Some(p) = path {
            metadata.insert("path".into(), serde_json::Value::String(p));
        }
        if let Some(ft) = ftype {
            metadata.insert("file_type".into(), serde_json::Value::String(ft));
        }
        if let Some(m) = modified {
            metadata.insert("modified".into(), serde_json::Value::String(m));
        }
        if let Some(s) = size {
            metadata.insert("size".into(), serde_json::json!(s));
        }
        Ok(SearchResult {
            result_type: "document".into(),
            id: row.get("id")?,
            title: row
                .get::<_, Option<String>>("title")?
                .unwrap_or_else(|| "Untitled".into()),
            snippet: truncate_str(
                &row.get::<_, Option<String>>("content")?.unwrap_or_default(),
                200,
            ),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ========== Reminders ==========

pub fn browse_reminders(conn: &Connection, args: &cli::RemindersArgs) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            r.reminder_id as id,
            r.title,
            COALESCE(r.notes, '') as notes,
            r.list_name,
            r.due_date,
            r.is_completed,
            r.priority,
            r.creation_date,
            r.location
        FROM reminders r
        WHERE 1=1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if args.completed {
        sql.push_str(" AND r.is_completed = 1");
    } else if !args.all {
        sql.push_str(" AND r.is_completed = 0");
    }

    if let Some(ref list) = args.list {
        sql.push_str(&format!(
            " AND LOWER(r.list_name) LIKE LOWER(?{})",
            params.len() + 1
        ));
        params.push(Box::new(format!("%{list}%")));
    }

    if args.due_today {
        sql.push_str(" AND r.due_date >= date('now') AND r.due_date < date('now', '+1 day')");
    }

    if args.due_week {
        sql.push_str(" AND r.due_date >= date('now') AND r.due_date < date('now', '+7 days')");
    }

    if args.overdue {
        sql.push_str(" AND r.due_date < date('now') AND r.is_completed = 0");
    }

    if let Some(ref priority) = args.priority {
        let p = match priority.to_lowercase().as_str() {
            "high" => 1,
            "medium" => 5,
            "low" => 9,
            _ => 0,
        };
        if p > 0 {
            sql.push_str(&format!(" AND r.priority = ?{}", params.len() + 1));
            params.push(Box::new(p));
        }
    }

    let order = match args.sort.as_str() {
        "priority" => "r.priority",
        "created" => "r.creation_date",
        "title" => "r.title",
        _ => "COALESCE(r.due_date, '9999-12-31')",
    };
    let dir = if args.reverse { "DESC" } else { "ASC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let list: Option<String> = row.get("list_name")?;
        let due: Option<String> = row.get("due_date")?;
        let completed: Option<bool> = row.get("is_completed")?;
        let priority: Option<i32> = row.get("priority")?;
        let location: Option<String> = row.get("location")?;
        let mut metadata = HashMap::new();
        if let Some(l) = list {
            metadata.insert("list".into(), serde_json::Value::String(l));
        }
        if let Some(d) = due {
            metadata.insert("due".into(), serde_json::Value::String(d));
        }
        if let Some(c) = completed {
            metadata.insert("completed".into(), serde_json::json!(c));
        }
        if let Some(p) = priority {
            metadata.insert("priority".into(), serde_json::json!(p));
        }
        if let Some(loc) = location {
            metadata.insert("location".into(), serde_json::Value::String(loc));
        }
        Ok(SearchResult {
            result_type: "reminder".into(),
            id: row.get("id")?,
            title: row
                .get::<_, Option<String>>("title")?
                .unwrap_or_else(|| "Untitled".into()),
            snippet: truncate_str(
                &row.get::<_, Option<String>>("notes")?.unwrap_or_default(),
                200,
            ),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ========== Photos ==========

pub fn browse_photos(conn: &Connection, args: &cli::PhotosArgs) -> Result<Vec<SearchResult>> {
    if let Some(ref name) = args.name {
        return photos_with_person(conn, name, args);
    }

    if let Some(ref near) = args.near {
        let parts: Vec<&str> = near.split(',').collect();
        if parts.len() == 2 {
            if let (Ok(lat), Ok(lng)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                return photos_near_location(conn, lat, lng, args.radius, args.limit);
            }
        }
        anyhow::bail!("Invalid --near format. Use: lat,lng (e.g., -33.87,151.21)");
    }

    let mut sql = String::from(
        "SELECT
            CAST(a.asset_id AS TEXT) as id,
            COALESCE(a.title, a.filename) as title,
            a.filename,
            a.date_created,
            a.latitude,
            a.longitude
        FROM photos_assets a
        WHERE 1=1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(ref from) = args.date_from {
        sql.push_str(&format!(" AND a.date_created >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = args.date_to {
        sql.push_str(&format!(" AND a.date_created <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    let order = match args.sort.as_str() {
        "name" => "a.filename",
        _ => "a.date_created",
    };
    let dir = if args.reverse { "ASC" } else { "DESC" };
    sql.push_str(&format!(
        " ORDER BY {order} {dir} LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let date: Option<String> = row.get("date_created")?;
        let lat: Option<f64> = row.get("latitude")?;
        let lng: Option<f64> = row.get("longitude")?;
        let mut metadata = HashMap::new();
        if let Some(d) = date {
            metadata.insert("date".into(), serde_json::Value::String(d));
        }
        if let Some(la) = lat {
            metadata.insert("lat".into(), serde_json::json!(la));
        }
        if let Some(lo) = lng {
            metadata.insert("lng".into(), serde_json::json!(lo));
        }
        Ok(SearchResult {
            result_type: "photo".into(),
            id: row.get("id")?,
            title: row
                .get::<_, Option<String>>("title")?
                .unwrap_or_else(|| "Untitled".into()),
            snippet: row
                .get::<_, Option<String>>("filename")?
                .unwrap_or_default(),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn photos_with_person(
    conn: &Connection,
    name: &str,
    args: &cli::PhotosArgs,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            CAST(a.asset_id AS TEXT) as id,
            COALESCE(a.title, a.filename) as title,
            p.full_name,
            a.date_created,
            a.latitude,
            a.longitude
        FROM photos_assets a
        JOIN photos_faces f ON a.asset_id = f.asset_id
        JOIN photos_people p ON f.person_id = p.person_id
        WHERE (p.full_name LIKE ?1 OR p.display_name LIKE ?1)",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(format!("%{name}%"))];

    if let Some(ref from) = args.date_from {
        sql.push_str(&format!(" AND a.date_created >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = args.date_to {
        sql.push_str(&format!(" AND a.date_created <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(&format!(
        " ORDER BY a.date_created DESC LIMIT ?{}",
        params.len() + 1
    ));
    params.push(Box::new(args.limit as i64));

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let full_name: String = row.get("full_name")?;
        let date: Option<String> = row.get("date_created")?;
        let lat: Option<f64> = row.get("latitude")?;
        let lng: Option<f64> = row.get("longitude")?;
        let mut metadata = HashMap::new();
        if let Some(d) = date {
            metadata.insert("date".into(), serde_json::Value::String(d));
        }
        if let Some(la) = lat {
            metadata.insert("lat".into(), serde_json::json!(la));
        }
        if let Some(lo) = lng {
            metadata.insert("lng".into(), serde_json::json!(lo));
        }
        Ok(SearchResult {
            result_type: "photo".into(),
            id: row.get("id")?,
            title: row
                .get::<_, Option<String>>("title")?
                .unwrap_or_else(|| "Untitled".into()),
            snippet: format!("Photo with {full_name}"),
            score: 1.0,
            metadata,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Haversine distance in kilometers between two lat/lng points.
fn haversine_km(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6371.0; // Earth radius in km
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

fn photos_near_location(
    conn: &Connection,
    lat: f64,
    lng: f64,
    radius_km: f64,
    limit: usize,
) -> Result<Vec<SearchResult>> {
    // Use bounding box for initial SQL filter, then refine with Haversine
    let lat_delta = radius_km / 111.0;
    let cos_lat = (lat * std::f64::consts::PI / 180.0).cos().abs();
    let lng_delta = if cos_lat > 0.001 {
        radius_km / (111.0 * cos_lat)
    } else {
        radius_km / 111.0
    };

    let mut stmt = conn.prepare(
        "SELECT
            CAST(asset_id AS TEXT) as id,
            COALESCE(title, filename) as title,
            filename,
            date_created,
            latitude,
            longitude
        FROM photos_assets
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN ?1 AND ?2
          AND longitude BETWEEN ?3 AND ?4",
    )?;

    let rows = stmt.query_map(
        rusqlite::params![
            lat - lat_delta,
            lat + lat_delta,
            lng - lng_delta,
            lng + lng_delta,
        ],
        |row| {
            let plat: f64 = row.get("latitude")?;
            let plng: f64 = row.get("longitude")?;
            let date: Option<String> = row.get("date_created")?;
            let filename: Option<String> = row.get("filename")?;
            Ok((
                row.get::<_, String>("id")?,
                row.get::<_, Option<String>>("title")?,
                filename,
                date,
                plat,
                plng,
            ))
        },
    )?;

    // Calculate actual Haversine distance and filter
    let mut results: Vec<SearchResult> = rows
        .filter_map(|r| r.ok())
        .filter_map(|(id, title, filename, date, plat, plng)| {
            let distance = haversine_km(lat, lng, plat, plng);
            if distance > radius_km {
                return None;
            }
            let mut metadata = HashMap::new();
            if let Some(d) = date {
                metadata.insert("date".into(), serde_json::Value::String(d));
            }
            metadata.insert("lat".into(), serde_json::json!(plat));
            metadata.insert("lng".into(), serde_json::json!(plng));
            metadata.insert(
                "distance_km".into(),
                serde_json::json!((distance * 100.0).round() / 100.0),
            );
            Some(SearchResult {
                result_type: "photo".into(),
                id,
                title: title.unwrap_or_else(|| "Untitled".into()),
                snippet: format!("{:.2} km away — {}", distance, filename.unwrap_or_default()),
                score: 1.0,
                metadata,
            })
        })
        .collect();

    // Sort by distance (ascending) instead of date
    results.sort_by(|a, b| {
        let da = a
            .metadata
            .get("distance_km")
            .and_then(|v| v.as_f64())
            .unwrap_or(f64::MAX);
        let db = b
            .metadata
            .get("distance_km")
            .and_then(|v| v.as_f64())
            .unwrap_or(f64::MAX);
        da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(limit);

    Ok(results)
}

// ========== Person View ==========

pub fn person_view(conn: &Connection, name: &str, limit: usize) -> Result<serde_json::Value> {
    let mut result = serde_json::json!({
        "contact": null,
        "messages": [],
        "photos": [],
        "notes": [],
    });

    // Find contact
    if let Ok(mut stmt) = conn.prepare(
        "SELECT
            identifier,
            COALESCE(given_name, '') || ' ' || COALESCE(family_name, '') as full_name,
            organization, job_title, note, birthday
        FROM contacts
        WHERE LOWER(COALESCE(given_name, '') || ' ' || COALESCE(family_name, '')) LIKE LOWER(?1)
           OR LOWER(given_name) LIKE LOWER(?1)
           OR LOWER(family_name) LIKE LOWER(?1)
        LIMIT 1",
    ) {
        let pattern = format!("%{name}%");
        if let Ok(Some(row)) = stmt
            .query_row([&pattern], |row| {
                let id: String = row.get("identifier")?;
                let full_name: String = row.get("full_name")?;
                let org: Option<String> = row.get("organization")?;
                let job: Option<String> = row.get("job_title")?;
                let note: Option<String> = row.get("note")?;
                let birthday: Option<String> = row.get("birthday")?;
                Ok(serde_json::json!({
                    "id": id,
                    "name": full_name.trim(),
                    "organization": org,
                    "job_title": job,
                    "note": note,
                    "birthday": birthday,
                }))
            })
            .map(Some)
            .or_else(|e| {
                if matches!(e, rusqlite::Error::QueryReturnedNoRows) {
                    Ok(None)
                } else {
                    Err(e)
                }
            })
        {
            result["contact"] = row;
        }
    }

    // Messages
    let args = cli::MessagesArgs {
        contact: Some(name.to_string()),
        date_from: None,
        date_to: None,
        from_me: false,
        search: None,
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(msgs) = browse_messages(conn, &args) {
        result["messages"] = serde_json::json!(msgs);
    }

    // Photos
    let photo_args = cli::PhotosArgs {
        name: Some(name.to_string()),
        near: None,
        radius: 10.0,
        date_from: None,
        date_to: None,
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(photos) = browse_photos(conn, &photo_args) {
        result["photos"] = serde_json::json!(photos);
    }

    // Notes mentioning person
    let note_args = cli::NotesArgs {
        vault: None,
        tag: None,
        search: Some(name.to_string()),
        date_from: None,
        date_to: None,
        sort: "modified".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(notes) = browse_notes(conn, &note_args) {
        result["notes"] = serde_json::json!(notes);
    }

    Ok(result)
}

// ========== Timeline View ==========

pub fn timeline_view(
    conn: &Connection,
    date: &str,
    days: usize,
    limit: usize,
) -> Result<serde_json::Value> {
    let start = date.to_string();
    let end = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map(|d| d + chrono::Duration::days(days as i64))
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|_| date.to_string());

    let mut result = serde_json::json!({
        "date": start,
        "days": days,
        "messages": [],
        "notes": [],
        "photos": [],
        "documents": [],
        "reminders": [],
    });

    // Messages
    let msg_args = cli::MessagesArgs {
        contact: None,
        date_from: Some(start.clone()),
        date_to: Some(end.clone()),
        from_me: false,
        search: None,
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(msgs) = browse_messages(conn, &msg_args) {
        result["messages"] = serde_json::json!(msgs);
    }

    // Notes
    let note_args = cli::NotesArgs {
        vault: None,
        tag: None,
        search: None,
        date_from: Some(start.clone()),
        date_to: Some(end.clone()),
        sort: "modified".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(notes) = browse_notes(conn, &note_args) {
        result["notes"] = serde_json::json!(notes);
    }

    // Photos
    let photo_args = cli::PhotosArgs {
        name: None,
        near: None,
        radius: 10.0,
        date_from: Some(start.clone()),
        date_to: Some(end.clone()),
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(photos) = browse_photos(conn, &photo_args) {
        result["photos"] = serde_json::json!(photos);
    }

    // Documents
    let doc_args = cli::DocumentsArgs {
        file_type: None,
        search: None,
        date_from: Some(start.clone()),
        date_to: Some(end.clone()),
        sort: "modified".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(docs) = browse_documents(conn, &doc_args) {
        result["documents"] = serde_json::json!(docs);
    }

    // Reminders
    let rem_args = cli::RemindersArgs {
        all: false,
        completed: false,
        list: None,
        due_today: false,
        due_week: false,
        overdue: false,
        priority: None,
        sort: "due".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(rems) = browse_reminders(conn, &rem_args) {
        result["reminders"] = serde_json::json!(rems);
    }

    Ok(result)
}

// ========== Recent Activity ==========

pub fn recent_activity(conn: &Connection, limit: usize) -> Result<serde_json::Value> {
    let mut result = serde_json::json!({
        "messages": [],
        "notes": [],
        "photos": [],
        "documents": [],
        "reminders_upcoming": [],
        "reminders_overdue": [],
    });

    // Recent messages
    let msg_args = cli::MessagesArgs {
        contact: None,
        date_from: None,
        date_to: None,
        from_me: false,
        search: None,
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(msgs) = browse_messages(conn, &msg_args) {
        result["messages"] = serde_json::json!(msgs);
    }

    // Recent notes
    let note_args = cli::NotesArgs {
        vault: None,
        tag: None,
        search: None,
        date_from: None,
        date_to: None,
        sort: "modified".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(notes) = browse_notes(conn, &note_args) {
        result["notes"] = serde_json::json!(notes);
    }

    // Recent photos
    let photo_args = cli::PhotosArgs {
        name: None,
        near: None,
        radius: 10.0,
        date_from: None,
        date_to: None,
        sort: "date".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(photos) = browse_photos(conn, &photo_args) {
        result["photos"] = serde_json::json!(photos);
    }

    // Recent documents
    let doc_args = cli::DocumentsArgs {
        file_type: None,
        search: None,
        date_from: None,
        date_to: None,
        sort: "modified".into(),
        reverse: false,
        limit,
        format: "text".into(),
    };
    if let Ok(docs) = browse_documents(conn, &doc_args) {
        result["documents"] = serde_json::json!(docs);
    }

    // Upcoming reminders
    if let Ok(mut stmt) = conn.prepare(
        "SELECT reminder_id as id, title, COALESCE(notes, '') as notes,
                list_name, due_date
         FROM reminders
         WHERE is_completed = 0 AND due_date IS NOT NULL AND due_date >= date('now')
         ORDER BY due_date LIMIT ?1",
    ) {
        if let Ok(rows) = stmt.query_map([limit as i64], |row| {
            let mut metadata = HashMap::new();
            if let Ok(Some(l)) = row.get::<_, Option<String>>("list_name") {
                metadata.insert("list".into(), serde_json::Value::String(l));
            }
            if let Ok(Some(d)) = row.get::<_, Option<String>>("due_date") {
                metadata.insert("due".into(), serde_json::Value::String(d));
            }
            Ok(SearchResult {
                result_type: "reminder".into(),
                id: row.get("id")?,
                title: row.get::<_, Option<String>>("title")?.unwrap_or_default(),
                snippet: truncate_str(
                    &row.get::<_, Option<String>>("notes")?.unwrap_or_default(),
                    150,
                ),
                score: 1.0,
                metadata,
            })
        }) {
            let upcoming: Vec<SearchResult> = rows.filter_map(|r| r.ok()).collect();
            result["reminders_upcoming"] = serde_json::json!(upcoming);
        }
    }

    // Overdue reminders
    if let Ok(mut stmt) = conn.prepare(
        "SELECT reminder_id as id, title, COALESCE(notes, '') as notes,
                list_name, due_date
         FROM reminders
         WHERE is_completed = 0 AND due_date IS NOT NULL AND due_date < date('now')
         ORDER BY due_date DESC LIMIT ?1",
    ) {
        if let Ok(rows) = stmt.query_map([limit as i64], |row| {
            let mut metadata = HashMap::new();
            if let Ok(Some(l)) = row.get::<_, Option<String>>("list_name") {
                metadata.insert("list".into(), serde_json::Value::String(l));
            }
            if let Ok(Some(d)) = row.get::<_, Option<String>>("due_date") {
                metadata.insert("due".into(), serde_json::Value::String(d));
            }
            Ok(SearchResult {
                result_type: "reminder".into(),
                id: row.get("id")?,
                title: row.get::<_, Option<String>>("title")?.unwrap_or_default(),
                snippet: truncate_str(
                    &row.get::<_, Option<String>>("notes")?.unwrap_or_default(),
                    150,
                ),
                score: 1.0,
                metadata,
            })
        }) {
            let overdue: Vec<SearchResult> = rows.filter_map(|r| r.ok()).collect();
            result["reminders_overdue"] = serde_json::json!(overdue);
        }
    }

    Ok(result)
}

// ========== Message Context ==========

pub fn message_context(
    conn: &Connection,
    message_id: &str,
    before: usize,
    after: usize,
) -> Result<Vec<SearchResult>> {
    // Get reference message
    let (chat_id, ref_date): (i64, String) = conn.query_row(
        "SELECT chat_id, message_date FROM imessage_messages WHERE message_id = ?1",
        [message_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let mut results = Vec::new();

    // Messages before
    let mut stmt = conn.prepare(
        "SELECT
            CAST(m.message_id AS TEXT) as id,
            CASE WHEN m.is_from_me = 1 THEN 'Me'
                 ELSE COALESCE(h.identifier, 'Unknown')
            END as sender,
            m.text,
            m.message_date,
            m.is_from_me
        FROM imessage_messages m
        LEFT JOIN imessage_handles h ON m.handle_id = h.handle_id
        WHERE m.chat_id = ?1 AND m.message_date < ?2 AND m.text IS NOT NULL
        ORDER BY m.message_date DESC
        LIMIT ?3",
    )?;

    let before_rows = stmt.query_map(
        rusqlite::params![chat_id, &ref_date, before as i64],
        |row| {
            let date: Option<String> = row.get("message_date")?;
            let from_me: Option<bool> = row.get("is_from_me")?;
            let mut metadata = HashMap::new();
            if let Some(d) = date {
                metadata.insert("date".into(), serde_json::Value::String(d));
            }
            if let Some(fm) = from_me {
                metadata.insert("from_me".into(), serde_json::json!(fm));
            }
            Ok(SearchResult {
                result_type: "message".into(),
                id: row.get("id")?,
                title: row.get("sender")?,
                snippet: row.get::<_, Option<String>>("text")?.unwrap_or_default(),
                score: 0.5,
                metadata,
            })
        },
    )?;

    let mut before_vec: Vec<SearchResult> = before_rows.filter_map(|r| r.ok()).collect();
    before_vec.reverse();
    results.extend(before_vec);

    // Target message and after
    let mut stmt = conn.prepare(
        "SELECT
            CAST(m.message_id AS TEXT) as id,
            CASE WHEN m.is_from_me = 1 THEN 'Me'
                 ELSE COALESCE(h.identifier, 'Unknown')
            END as sender,
            m.text,
            m.message_date,
            m.is_from_me
        FROM imessage_messages m
        LEFT JOIN imessage_handles h ON m.handle_id = h.handle_id
        WHERE m.chat_id = ?1 AND m.message_date >= ?2 AND m.text IS NOT NULL
        ORDER BY m.message_date
        LIMIT ?3",
    )?;

    let after_rows = stmt.query_map(
        rusqlite::params![chat_id, &ref_date, (after + 1) as i64],
        |row| {
            let id: String = row.get("id")?;
            let is_target = id == message_id;
            let date: Option<String> = row.get("message_date")?;
            let from_me: Option<bool> = row.get("is_from_me")?;
            let mut metadata = HashMap::new();
            if let Some(d) = date {
                metadata.insert("date".into(), serde_json::Value::String(d));
            }
            if let Some(fm) = from_me {
                metadata.insert("from_me".into(), serde_json::json!(fm));
            }
            Ok(SearchResult {
                result_type: "message".into(),
                id,
                title: row.get("sender")?,
                snippet: row.get::<_, Option<String>>("text")?.unwrap_or_default(),
                score: if is_target { 1.0 } else { 0.5 },
                metadata,
            })
        },
    )?;

    results.extend(after_rows.filter_map(|r| r.ok()));

    Ok(results)
}

// ========== Show Full Content ==========

pub fn get_full_content(
    conn: &Connection,
    content_type: &str,
    content_id: &str,
) -> Result<Option<serde_json::Value>> {
    match content_type {
        "message" => {
            let row = conn.query_row(
                "SELECT m.message_id, m.text, m.message_date, m.is_from_me,
                        COALESCE(ch.display_name, ch.chat_identifier) as chat_name,
                        COALESCE(h.identifier, 'Unknown') as sender
                 FROM imessage_messages m
                 LEFT JOIN imessage_chats ch ON m.chat_id = ch.chat_id
                 LEFT JOIN imessage_handles h ON m.handle_id = h.handle_id
                 WHERE m.message_id = ?1",
                [content_id],
                |row| {
                    Ok(serde_json::json!({
                        "message_id": row.get::<_, i64>(0)?,
                        "text": row.get::<_, Option<String>>(1)?,
                        "message_date": row.get::<_, Option<String>>(2)?,
                        "is_from_me": row.get::<_, Option<bool>>(3)?,
                        "chat_name": row.get::<_, Option<String>>(4)?,
                        "sender": row.get::<_, Option<String>>(5)?,
                    }))
                },
            );
            Ok(row.ok())
        }
        "note" => {
            let row = conn.query_row(
                "SELECT id, title, body, file_path, created_at, modified_at
                 FROM obsidian_notes WHERE id = ?1",
                [content_id],
                |row| {
                    Ok(serde_json::json!({
                        "id": row.get::<_, i64>(0)?,
                        "title": row.get::<_, Option<String>>(1)?,
                        "body": row.get::<_, Option<String>>(2)?,
                        "file_path": row.get::<_, Option<String>>(3)?,
                        "created_at": row.get::<_, Option<String>>(4)?,
                        "modified_at": row.get::<_, Option<String>>(5)?,
                    }))
                },
            );
            Ok(row.ok())
        }
        "contact" => {
            let row = conn.query_row(
                "SELECT identifier, given_name, family_name, organization,
                        job_title, note, birthday, nickname
                 FROM contacts WHERE identifier = ?1",
                [content_id],
                |row| {
                    Ok(serde_json::json!({
                        "identifier": row.get::<_, Option<String>>(0)?,
                        "given_name": row.get::<_, Option<String>>(1)?,
                        "family_name": row.get::<_, Option<String>>(2)?,
                        "organization": row.get::<_, Option<String>>(3)?,
                        "job_title": row.get::<_, Option<String>>(4)?,
                        "note": row.get::<_, Option<String>>(5)?,
                        "birthday": row.get::<_, Option<String>>(6)?,
                        "nickname": row.get::<_, Option<String>>(7)?,
                    }))
                },
            );
            Ok(row.ok())
        }
        "document" => {
            let row = conn.query_row(
                "SELECT id, title, filename, content, file_path, file_type,
                        file_size, modified_at
                 FROM documents WHERE id = ?1",
                [content_id],
                |row| {
                    Ok(serde_json::json!({
                        "id": row.get::<_, i64>(0)?,
                        "title": row.get::<_, Option<String>>(1)?,
                        "filename": row.get::<_, Option<String>>(2)?,
                        "content": row.get::<_, Option<String>>(3)?,
                        "file_path": row.get::<_, Option<String>>(4)?,
                        "file_type": row.get::<_, Option<String>>(5)?,
                        "file_size": row.get::<_, Option<i64>>(6)?,
                        "modified_at": row.get::<_, Option<String>>(7)?,
                    }))
                },
            );
            Ok(row.ok())
        }
        "reminder" => {
            let row = conn.query_row(
                "SELECT reminder_id, title, notes, list_name, due_date,
                        is_completed, priority, creation_date, completion_date, location
                 FROM reminders WHERE reminder_id = ?1",
                [content_id],
                |row| {
                    Ok(serde_json::json!({
                        "reminder_id": row.get::<_, Option<String>>(0)?,
                        "title": row.get::<_, Option<String>>(1)?,
                        "notes": row.get::<_, Option<String>>(2)?,
                        "list_name": row.get::<_, Option<String>>(3)?,
                        "due_date": row.get::<_, Option<String>>(4)?,
                        "is_completed": row.get::<_, Option<bool>>(5)?,
                        "priority": row.get::<_, Option<i32>>(6)?,
                        "creation_date": row.get::<_, Option<String>>(7)?,
                        "completion_date": row.get::<_, Option<String>>(8)?,
                        "location": row.get::<_, Option<String>>(9)?,
                    }))
                },
            );
            Ok(row.ok())
        }
        _ => Ok(None),
    }
}

// ========== Text Output Helpers ==========

pub fn print_results(results: &[SearchResult], format: &str, _limit: usize) {
    match format {
        "json" => {
            if let Ok(json) = serde_json::to_string_pretty(results) {
                println!("{json}");
            }
        }
        "markdown" => {
            for r in results {
                println!("## [{}] {}", r.result_type.to_uppercase(), r.title);
                if !r.snippet.is_empty() {
                    println!("{}", r.snippet);
                }
                let meta: Vec<String> = r
                    .metadata
                    .iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| {
                        let val = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        format!("**{k}:** {val}")
                    })
                    .collect();
                if !meta.is_empty() {
                    println!("{}", meta.join(" | "));
                }
                println!();
            }
        }
        _ => {
            if results.is_empty() {
                println!("No results found.");
                return;
            }
            for r in results {
                println!();
                println!("[{}] {}", r.result_type.to_uppercase(), r.title);
                if !r.snippet.is_empty() {
                    println!("{}", r.snippet);
                }
                let mut meta_parts = Vec::new();
                for key in &[
                    "date",
                    "modified",
                    "due",
                    "path",
                    "org",
                    "list",
                    "file_type",
                    "sender",
                ] {
                    if let Some(val) = r.metadata.get(*key) {
                        if let Some(s) = val.as_str() {
                            if !s.is_empty() {
                                meta_parts
                                    .push(format!("{}: {s}", key[..1].to_uppercase() + &key[1..]));
                            }
                        }
                    }
                }
                if !meta_parts.is_empty() {
                    println!("  {}", meta_parts.join(" | "));
                }
            }
        }
    }
}

pub fn print_person_text(data: &serde_json::Value) {
    if let Some(contact) = data.get("contact") {
        if !contact.is_null() {
            println!("=== Contact ===");
            if let Some(name) = contact.get("name").and_then(|v| v.as_str()) {
                println!("  Name: {name}");
            }
            if let Some(org) = contact.get("organization").and_then(|v| v.as_str()) {
                println!("  Organization: {org}");
            }
            if let Some(job) = contact.get("job_title").and_then(|v| v.as_str()) {
                println!("  Job: {job}");
            }
            if let Some(bday) = contact.get("birthday").and_then(|v| v.as_str()) {
                println!("  Birthday: {bday}");
            }
            println!();
        }
    }

    print_section("Messages", data.get("messages"));
    print_section("Photos", data.get("photos"));
    print_section("Notes", data.get("notes"));
}

pub fn print_timeline_text(data: &serde_json::Value) {
    if let Some(date) = data.get("date").and_then(|v| v.as_str()) {
        let days = data.get("days").and_then(|v| v.as_i64()).unwrap_or(1);
        println!(
            "=== Timeline: {date} ({days} day{}) ===",
            if days > 1 { "s" } else { "" }
        );
        println!();
    }

    print_section("Messages", data.get("messages"));
    print_section("Notes", data.get("notes"));
    print_section("Photos", data.get("photos"));
    print_section("Documents", data.get("documents"));
    print_section("Reminders", data.get("reminders"));
}

pub fn print_recent_text(data: &serde_json::Value) {
    println!("=== Recent Activity ===");
    println!();

    print_section("Messages", data.get("messages"));
    print_section("Notes", data.get("notes"));
    print_section("Photos", data.get("photos"));
    print_section("Documents", data.get("documents"));

    if let Some(upcoming) = data.get("reminders_upcoming") {
        if let Some(arr) = upcoming.as_array() {
            if !arr.is_empty() {
                println!("--- Upcoming Reminders ---");
                for item in arr {
                    let title = item
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Untitled");
                    let due = item
                        .get("metadata")
                        .and_then(|m| m.get("due"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    println!("  {title} (due: {due})");
                }
                println!();
            }
        }
    }

    if let Some(overdue) = data.get("reminders_overdue") {
        if let Some(arr) = overdue.as_array() {
            if !arr.is_empty() {
                println!("--- Overdue Reminders ---");
                for item in arr {
                    let title = item
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Untitled");
                    let due = item
                        .get("metadata")
                        .and_then(|m| m.get("due"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    println!("  {title} (was due: {due})");
                }
                println!();
            }
        }
    }
}

fn print_section(label: &str, data: Option<&serde_json::Value>) {
    if let Some(arr) = data.and_then(|v| v.as_array()) {
        if !arr.is_empty() {
            println!("--- {label} ---");
            for item in arr {
                let title = item
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Untitled");
                let snippet = item.get("snippet").and_then(|v| v.as_str()).unwrap_or("");
                let display: String = snippet.chars().take(100).collect();
                println!("  {title}");
                if !display.is_empty() {
                    println!("    {display}");
                }
            }
            println!();
        }
    }
}

fn truncate_str(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== truncate_str ==========

    #[test]
    fn truncate_str_normal() {
        assert_eq!(truncate_str("hello world", 5), "hello");
    }

    #[test]
    fn truncate_str_empty() {
        assert_eq!(truncate_str("", 10), "");
    }

    #[test]
    fn truncate_str_longer_than_max() {
        assert_eq!(truncate_str("abcdefghij", 3), "abc");
    }

    #[test]
    fn truncate_str_exact_length() {
        assert_eq!(truncate_str("hello", 5), "hello");
    }

    #[test]
    fn truncate_str_shorter_than_max() {
        assert_eq!(truncate_str("hi", 10), "hi");
    }

    // ========== haversine_km ==========

    #[test]
    fn haversine_known_distance() {
        // Sydney (-33.8688, 151.2093) to Melbourne (-37.8136, 144.9631)
        // Known distance ~714 km
        let distance = haversine_km(-33.8688, 151.2093, -37.8136, 144.9631);
        assert!(
            (distance - 714.0).abs() < 20.0,
            "Expected ~714km, got {distance}"
        );
    }

    #[test]
    fn haversine_same_point() {
        let distance = haversine_km(40.0, -74.0, 40.0, -74.0);
        assert!((distance - 0.0).abs() < 0.001);
    }

    #[test]
    fn haversine_equator_one_degree() {
        // One degree of longitude at equator ~111 km
        let distance = haversine_km(0.0, 0.0, 0.0, 1.0);
        assert!(
            (distance - 111.0).abs() < 1.0,
            "Expected ~111km, got {distance}"
        );
    }
}
