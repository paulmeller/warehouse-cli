use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::collections::HashSet;

use crate::config::Config;

// X's public web app bearer token (constant, not a secret)
const BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA";
const GRAPHQL_BASE: &str = "https://x.com/i/api/graphql";

// Hardcoded bookmark query ID (not discoverable from JS bundles)
const BOOKMARKS_QUERY_ID: &str = "tmd4ifV8RHltzn8ymGg1aw";

/// Extract Twitter bookmarks and likes into warehouse.
pub fn extract(conn: &Connection, config: &Config) -> Result<usize> {
    let tw_config = &config.twitter;
    if !tw_config.enabled {
        anyhow::bail!("Twitter is disabled in config");
    }

    // Read cookies from config or environment
    let (auth_token, ct0) = get_cookies(tw_config)?;

    create_tables(conn)?;

    let client = build_client(&auth_token, &ct0)?;
    let mut total = 0;

    // Discover query IDs from X's JS bundles
    let query_ids = discover_query_ids(&client)?;

    // Extract bookmarks
    if tw_config.extract_bookmarks {
        let existing = get_existing_ids(conn, "twitter_bookmarks");
        let count = fetch_bookmarks(&client, conn, tw_config, &existing)?;
        eprintln!("  bookmarks: {count}");
        total += count;
    }

    // Extract likes (requires user_screen_name in config)
    if tw_config.extract_likes && !tw_config.user_screen_name.is_empty() {
        let user_id = get_user_id(&client, &query_ids, &tw_config.user_screen_name)?;
        let existing = get_existing_ids(conn, "twitter_likes");
        let count = fetch_likes(&client, conn, tw_config, &query_ids, &user_id, &existing)?;
        eprintln!("  likes: {count}");
        total += count;
    }

    Ok(total)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS twitter_bookmarks (
            tweet_id TEXT PRIMARY KEY,
            text TEXT,
            author_handle TEXT,
            author_name TEXT,
            author_id TEXT,
            created_at TEXT,
            favorite_count INTEGER,
            retweet_count INTEGER,
            reply_count INTEGER,
            quote_count INTEGER,
            has_media INTEGER,
            media_types TEXT,
            conversation_id TEXT,
            in_reply_to_tweet_id TEXT,
            lang TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS twitter_likes (
            tweet_id TEXT PRIMARY KEY,
            text TEXT,
            author_handle TEXT,
            author_name TEXT,
            author_id TEXT,
            created_at TEXT,
            favorite_count INTEGER,
            retweet_count INTEGER,
            reply_count INTEGER,
            quote_count INTEGER,
            has_media INTEGER,
            media_types TEXT,
            conversation_id TEXT,
            in_reply_to_tweet_id TEXT,
            lang TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_bookmarks_created ON twitter_bookmarks(created_at);
        CREATE INDEX IF NOT EXISTS idx_bookmarks_author ON twitter_bookmarks(author_handle);
        CREATE INDEX IF NOT EXISTS idx_likes_created ON twitter_likes(created_at);
        CREATE INDEX IF NOT EXISTS idx_likes_author ON twitter_likes(author_handle);
        ",
    )?;
    Ok(())
}

fn get_cookies(tw_config: &crate::config::TwitterConfig) -> Result<(String, String)> {
    // Try environment variables first
    if let (Ok(auth), Ok(ct0)) = (
        std::env::var("TWITTER_AUTH_TOKEN"),
        std::env::var("TWITTER_CT0"),
    ) {
        return Ok((auth, ct0));
    }

    // Try reading from Safari cookies (macOS)
    if tw_config.cookie_source == "safari" {
        return read_safari_cookies();
    }

    // Try reading from Chrome cookie database
    if tw_config.cookie_source == "chrome" {
        return read_chrome_cookies();
    }

    anyhow::bail!(
        "Twitter cookies not available. Set TWITTER_AUTH_TOKEN and TWITTER_CT0 environment variables, \
         or configure cookie_source in config.toml"
    )
}

fn read_safari_cookies() -> Result<(String, String)> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;
    let cookies_path = home.join("Library/Cookies/Cookies.binarycookies");

    if !cookies_path.exists() {
        anyhow::bail!("Safari cookies file not found");
    }

    let data = std::fs::read(&cookies_path)
        .context("Cannot read Safari cookies (Full Disk Access required)")?;

    // Parse Safari's binary cookie format
    let mut auth_token = None;
    let mut ct0 = None;

    // Simple string search for cookie values in binary data
    let data_str = String::from_utf8_lossy(&data);
    for line in data_str.split('\0') {
        if line == "auth_token" {
            // The value follows after some binary data
        }
        // This is a simplified parser - the actual binary format is complex
        // For production use, consider using the Python version
    }

    // Try a regex-based approach on the binary data
    let text = String::from_utf8_lossy(&data);
    if let Some(pos) = text.find("auth_token") {
        // Look for hex-like token value nearby
        let after = &text[pos..std::cmp::min(pos + 200, text.len())];
        for segment in after.split(|c: char| !c.is_alphanumeric()) {
            if segment.len() == 40 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
                auth_token = Some(segment.to_string());
                break;
            }
        }
    }
    if let Some(pos) = text.find("ct0") {
        let after = &text[pos..std::cmp::min(pos + 200, text.len())];
        for segment in after.split(|c: char| !c.is_alphanumeric()) {
            if segment.len() >= 32 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
                ct0 = Some(segment.to_string());
                break;
            }
        }
    }

    match (auth_token, ct0) {
        (Some(auth), Some(ct0)) => Ok((auth, ct0)),
        _ => anyhow::bail!(
            "Could not extract Twitter cookies from Safari. \
             Set TWITTER_AUTH_TOKEN and TWITTER_CT0 environment variables instead."
        ),
    }
}

fn read_chrome_cookies() -> Result<(String, String)> {
    // Chrome cookies are encrypted on macOS; reading them requires decryption
    // which is complex. Recommend environment variables instead.
    anyhow::bail!(
        "Chrome cookie extraction not yet supported in Rust. \
         Set TWITTER_AUTH_TOKEN and TWITTER_CT0 environment variables, \
         or use cookie_source = \"safari\" in config.toml"
    )
}

fn build_client(auth_token: &str, ct0: &str) -> Result<reqwest::blocking::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {BEARER_TOKEN}").parse()?,
    );
    headers.insert("x-csrf-token", ct0.parse()?);
    headers.insert(
        reqwest::header::COOKIE,
        format!("auth_token={auth_token}; ct0={ct0}").parse()?,
    );
    headers.insert(
        reqwest::header::USER_AGENT,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36".parse()?,
    );
    headers.insert("x-twitter-active-user", "yes".parse()?);
    headers.insert("x-twitter-auth-type", "OAuth2Session".parse()?);
    headers.insert("x-twitter-client-language", "en".parse()?);

    reqwest::blocking::Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("Failed to build HTTP client")
}

fn discover_query_ids(client: &reqwest::blocking::Client) -> Result<std::collections::HashMap<String, String>> {
    let mut ids = std::collections::HashMap::new();
    ids.insert("Bookmarks".into(), BOOKMARKS_QUERY_ID.into());

    // Try to discover Likes and UserByScreenName query IDs from JS bundles
    let resp = client
        .get("https://x.com/")
        .send()
        .context("Failed to fetch x.com")?;

    let html = resp.text().unwrap_or_default();

    // Find JS bundle URLs
    let bundle_re = regex::Regex::new(
        r#"(?:src|href)="((?:https?://[^"]+|/[^"]+)(?:main|api|client)[^"]*\.js)""#,
    )?;

    let bundles: Vec<String> = bundle_re
        .captures_iter(&html)
        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
        .take(10)
        .collect();

    let id_re = regex::Regex::new(r#"queryId:"([^"]+)",operationName:"([^"]+)""#)?;
    let needed = ["Likes", "UserByScreenName"];

    for bundle_path in &bundles {
        if needed.iter().all(|n| ids.contains_key(*n)) {
            break;
        }

        let url = if bundle_path.starts_with("http") {
            bundle_path.clone()
        } else {
            format!("https://x.com{bundle_path}")
        };

        if let Ok(resp) = client.get(&url).send() {
            if let Ok(js) = resp.text() {
                for caps in id_re.captures_iter(&js) {
                    let query_id = &caps[1];
                    let operation = &caps[2];
                    if needed.contains(&operation) {
                        ids.insert(operation.to_string(), query_id.to_string());
                    }
                }
            }
        }
    }

    Ok(ids)
}

fn get_existing_ids(conn: &Connection, table: &str) -> HashSet<String> {
    let sql = format!("SELECT tweet_id FROM {table}");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(_) => return HashSet::new(),
    };
    let rows = stmt.query_map([], |row| row.get::<_, String>(0));
    match rows {
        Ok(rows) => rows.flatten().collect(),
        Err(_) => HashSet::new(),
    }
}

fn default_features() -> serde_json::Value {
    serde_json::json!({
        "graphql_timeline_v2_bookmark_timeline": true,
        "rweb_video_screen_enabled": true,
        "responsive_web_graphql_timeline_navigation_enabled": true,
        "responsive_web_graphql_exclude_directive_enabled": true,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": false,
        "creator_subscriptions_tweet_preview_api_enabled": true,
        "communities_web_enable_tweet_community_results_fetch": true,
        "longform_notetweets_consumption_enabled": true,
        "responsive_web_twitter_article_tweet_consumption_enabled": true,
        "view_counts_everywhere_api_enabled": true,
        "responsive_web_edit_tweet_api_enabled": true,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": true,
        "longform_notetweets_rich_text_read_enabled": true,
        "longform_notetweets_inline_media_enabled": true,
        "freedom_of_speech_not_reach_fetch_enabled": true,
        "standardized_nudges_misinfo": true,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
        "responsive_web_enhance_cards_enabled": false,
        "tweetypie_unmention_optimization_enabled": true,
        "vibe_api_enabled": true,
        "responsive_web_twitter_blue_verified_badge_is_enabled": true,
        "interactive_text_enabled": true,
        "longform_notetweets_richtext_consumption_enabled": true,
        "verified_phone_label_enabled": false,
    })
}

fn fetch_bookmarks(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    tw_config: &crate::config::TwitterConfig,
    existing: &HashSet<String>,
) -> Result<usize> {
    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut consecutive_seen = 0;
    let delay = std::time::Duration::from_secs_f64(tw_config.page_delay_seconds);

    for page in 0..tw_config.max_pages {
        let mut variables = serde_json::json!({
            "count": 20,
            "includePromotedContent": false,
        });
        if let Some(ref c) = cursor {
            variables["cursor"] = serde_json::Value::String(c.clone());
        }

        let url = format!("{GRAPHQL_BASE}/{BOOKMARKS_QUERY_ID}/Bookmarks");
        let resp = client
            .get(&url)
            .query(&[
                ("variables", serde_json::to_string(&variables)?),
                ("features", serde_json::to_string(&default_features())?),
            ])
            .send();

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  request error on page {}: {e}", page + 1);
                break;
            }
        };

        if resp.status().as_u16() == 429 {
            eprintln!("  rate limited after {page} pages");
            break;
        }
        if resp.status().as_u16() == 401 || resp.status().as_u16() == 403 {
            anyhow::bail!("Twitter authentication failed. Check cookies.");
        }

        let data: serde_json::Value = resp.json().unwrap_or_default();
        let (tweets, next_cursor) = parse_bookmark_response(&data);

        if tweets.is_empty() {
            break;
        }

        for tweet in &tweets {
            let id = tweet["tweet_id"].as_str().unwrap_or("");
            if tw_config.incremental && existing.contains(id) {
                consecutive_seen += 1;
            } else {
                consecutive_seen = 0;
                insert_tweet(conn, "twitter_bookmarks", tweet)?;
                total += 1;
            }
        }

        if tw_config.incremental && consecutive_seen >= 5 {
            break;
        }

        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }

        std::thread::sleep(delay);
    }

    Ok(total)
}

fn fetch_likes(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    tw_config: &crate::config::TwitterConfig,
    query_ids: &std::collections::HashMap<String, String>,
    user_id: &str,
    existing: &HashSet<String>,
) -> Result<usize> {
    let likes_id = query_ids
        .get("Likes")
        .ok_or_else(|| anyhow::anyhow!("Likes query ID not found"))?;

    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut consecutive_seen = 0;
    let delay = std::time::Duration::from_secs_f64(tw_config.page_delay_seconds);

    for page in 0..tw_config.max_pages {
        let mut variables = serde_json::json!({
            "userId": user_id,
            "count": 20,
            "includePromotedContent": false,
        });
        if let Some(ref c) = cursor {
            variables["cursor"] = serde_json::Value::String(c.clone());
        }

        let url = format!("{GRAPHQL_BASE}/{likes_id}/Likes");
        let resp = client
            .get(&url)
            .query(&[
                ("variables", serde_json::to_string(&variables)?),
                ("features", serde_json::to_string(&default_features())?),
            ])
            .send();

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  request error on page {}: {e}", page + 1);
                break;
            }
        };

        if resp.status().as_u16() == 429 {
            eprintln!("  rate limited after {page} pages");
            break;
        }

        let data: serde_json::Value = resp.json().unwrap_or_default();
        let (tweets, next_cursor) = parse_likes_response(&data);

        if tweets.is_empty() {
            break;
        }

        for tweet in &tweets {
            let id = tweet["tweet_id"].as_str().unwrap_or("");
            if tw_config.incremental && existing.contains(id) {
                consecutive_seen += 1;
            } else {
                consecutive_seen = 0;
                insert_tweet(conn, "twitter_likes", tweet)?;
                total += 1;
            }
        }

        if tw_config.incremental && consecutive_seen >= 5 {
            break;
        }

        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }

        std::thread::sleep(delay);
    }

    Ok(total)
}

fn get_user_id(
    client: &reqwest::blocking::Client,
    query_ids: &std::collections::HashMap<String, String>,
    screen_name: &str,
) -> Result<String> {
    let query_id = query_ids
        .get("UserByScreenName")
        .ok_or_else(|| anyhow::anyhow!("UserByScreenName query ID not found"))?;

    let variables = serde_json::json!({
        "screen_name": screen_name,
        "withSafetyModeUserFields": true,
    });
    let features = serde_json::json!({
        "hidden_profile_subscriptions_enabled": true,
        "responsive_web_graphql_exclude_directive_enabled": true,
        "verified_phone_label_enabled": false,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": false,
        "responsive_web_graphql_timeline_navigation_enabled": true,
    });

    let url = format!("{GRAPHQL_BASE}/{query_id}/UserByScreenName");
    let resp: serde_json::Value = client
        .get(&url)
        .query(&[
            ("variables", serde_json::to_string(&variables)?),
            ("features", serde_json::to_string(&features)?),
        ])
        .send()?
        .json()?;

    resp["data"]["user"]["result"]["rest_id"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Could not get user ID for @{screen_name}"))
}

fn parse_tweet(result: &serde_json::Value) -> Option<serde_json::Value> {
    let mut result = result;

    // Handle wrapper types
    let typename = result["__typename"].as_str().unwrap_or("");
    if typename == "TweetWithVisibilityResults" {
        result = &result["tweet"];
    }
    if result["__typename"].as_str() == Some("TweetTombstone") {
        return None;
    }

    let legacy = &result["legacy"];
    let core = &result["core"];
    let user_results = &core["user_results"]["result"];
    let user_legacy = &user_results["legacy"];

    let tweet_id = result["rest_id"]
        .as_str()
        .or_else(|| legacy["id_str"].as_str())?;

    // Extract media types
    let media_types: Vec<String> = legacy["extended_entities"]["media"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m["type"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    Some(serde_json::json!({
        "tweet_id": tweet_id,
        "text": legacy["full_text"].as_str().unwrap_or(""),
        "author_handle": user_legacy["screen_name"].as_str().unwrap_or(""),
        "author_name": user_legacy["name"].as_str().unwrap_or(""),
        "author_id": user_results["rest_id"].as_str().unwrap_or(""),
        "created_at": legacy["created_at"].as_str().unwrap_or(""),
        "favorite_count": legacy["favorite_count"].as_i64().unwrap_or(0),
        "retweet_count": legacy["retweet_count"].as_i64().unwrap_or(0),
        "reply_count": legacy["reply_count"].as_i64().unwrap_or(0),
        "quote_count": legacy["quote_count"].as_i64().unwrap_or(0),
        "has_media": !media_types.is_empty(),
        "media_types": media_types,
        "conversation_id": legacy["conversation_id_str"].as_str().unwrap_or(""),
        "in_reply_to_tweet_id": legacy["in_reply_to_status_id_str"].as_str(),
        "lang": legacy["lang"].as_str().unwrap_or(""),
    }))
}

fn parse_timeline_entries(entries: &[serde_json::Value]) -> (Vec<serde_json::Value>, Option<String>) {
    let mut tweets = Vec::new();
    let mut bottom_cursor = None;

    for entry in entries {
        let entry_id = entry["entryId"].as_str().unwrap_or("");
        let content = &entry["content"];

        if entry_id.starts_with("tweet-") {
            let tweet_result = &content["itemContent"]["tweet_results"]["result"];
            if let Some(parsed) = parse_tweet(tweet_result) {
                tweets.push(parsed);
            }
        } else if entry_id.starts_with("cursor-bottom") {
            if let Some(val) = content["value"].as_str() {
                bottom_cursor = Some(val.to_string());
            }
        } else {
            // TimelineTimelineItem format
            let item_content = &content["itemContent"];
            if item_content["itemType"].as_str() == Some("TimelineTweet") {
                let tweet_result = &item_content["tweet_results"]["result"];
                if let Some(parsed) = parse_tweet(tweet_result) {
                    tweets.push(parsed);
                }
            }
        }
    }

    (tweets, bottom_cursor)
}

fn parse_bookmark_response(data: &serde_json::Value) -> (Vec<serde_json::Value>, Option<String>) {
    // Navigate the nested response structure
    let timeline = if let Some(bt) = data["data"]["bookmark_timeline_v2"]["timeline"].as_object() {
        serde_json::Value::Object(bt.clone())
    } else if let Some(bt) = data["data"]["bookmarkTimeline"]["timeline"].as_object() {
        serde_json::Value::Object(bt.clone())
    } else {
        // Search all keys for timeline
        let mut found = None;
        if let Some(obj) = data["data"].as_object() {
            for (_key, val) in obj {
                if let Some(tl) = val["timeline"].as_object() {
                    found = Some(serde_json::Value::Object(tl.clone()));
                    break;
                }
            }
        }
        match found {
            Some(tl) => tl,
            None => return (Vec::new(), None),
        }
    };

    let instructions = timeline["instructions"].as_array();
    let instructions = match instructions {
        Some(i) => i,
        None => return (Vec::new(), None),
    };

    for instruction in instructions {
        if instruction["type"].as_str() == Some("TimelineAddEntries") {
            if let Some(entries) = instruction["entries"].as_array() {
                return parse_timeline_entries(entries);
            }
        }
    }

    (Vec::new(), None)
}

fn parse_likes_response(data: &serde_json::Value) -> (Vec<serde_json::Value>, Option<String>) {
    let instructions = data["data"]["user"]["result"]["timeline_v2"]["timeline"]["instructions"]
        .as_array();

    let instructions = match instructions {
        Some(i) => i,
        None => return (Vec::new(), None),
    };

    for instruction in instructions {
        if instruction["type"].as_str() == Some("TimelineAddEntries") {
            if let Some(entries) = instruction["entries"].as_array() {
                return parse_timeline_entries(entries);
            }
        }
    }

    (Vec::new(), None)
}

fn insert_tweet(conn: &Connection, table: &str, tweet: &serde_json::Value) -> Result<()> {
    let media_types = tweet["media_types"]
        .as_array()
        .map(|arr| serde_json::to_string(arr).unwrap_or_default());

    let sql = format!(
        "INSERT OR REPLACE INTO {table}
         (tweet_id, text, author_handle, author_name, author_id,
          created_at, favorite_count, retweet_count, reply_count,
          quote_count, has_media, media_types, conversation_id,
          in_reply_to_tweet_id, lang)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)"
    );

    conn.execute(
        &sql,
        params![
            tweet["tweet_id"].as_str(),
            tweet["text"].as_str(),
            tweet["author_handle"].as_str(),
            tweet["author_name"].as_str(),
            tweet["author_id"].as_str(),
            tweet["created_at"].as_str(),
            tweet["favorite_count"].as_i64().unwrap_or(0),
            tweet["retweet_count"].as_i64().unwrap_or(0),
            tweet["reply_count"].as_i64().unwrap_or(0),
            tweet["quote_count"].as_i64().unwrap_or(0),
            tweet["has_media"].as_bool().map(|b| b as i64).unwrap_or(0),
            media_types,
            tweet["conversation_id"].as_str(),
            tweet["in_reply_to_tweet_id"].as_str(),
            tweet["lang"].as_str(),
        ],
    )?;
    Ok(())
}
