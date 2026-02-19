use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::config::Config;

const API_BASE: &str = "https://api.pocketsmith.com/v2";

/// Extract PocketSmith data via REST API into warehouse.
pub fn extract(conn: &Connection, config: &Config) -> Result<usize> {
    let ps_config = &config.pocketsmith;
    if !ps_config.enabled {
        anyhow::bail!("PocketSmith is disabled in config");
    }
    if ps_config.api_key.is_empty() {
        anyhow::bail!("PocketSmith API key not found in config");
    }

    create_tables(conn)?;

    let client = reqwest::blocking::Client::new();
    let api_key = &ps_config.api_key;

    // Get user info to find user_id
    let user: serde_json::Value = client
        .get(format!("{API_BASE}/me"))
        .header("X-Developer-Key", api_key)
        .header("Accept", "application/json")
        .send()
        .context("Failed to connect to PocketSmith API")?
        .json()
        .context("Failed to parse PocketSmith user response")?;

    let user_id = user["id"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("Cannot get PocketSmith user ID"))?;

    let mut total = 0;

    // Extract accounts
    if ps_config.extract_accounts {
        let count = extract_accounts(&client, conn, api_key, user_id)?;
        eprintln!("  accounts: {count}");
        total += count;
    }

    // Extract categories
    if ps_config.extract_categories {
        let count = extract_categories(&client, conn, api_key, user_id)?;
        eprintln!("  categories: {count}");
    }

    // Extract transactions
    if ps_config.extract_transactions {
        let count = extract_transactions(&client, conn, api_key, user_id, ps_config)?;
        eprintln!("  transactions: {count}");
        total += count;
    }

    Ok(total)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS pocketsmith_accounts (
            id TEXT PRIMARY KEY,
            title TEXT,
            type TEXT,
            currency_code TEXT,
            current_balance REAL,
            current_balance_date TEXT,
            safe_balance REAL,
            is_net_worth INTEGER,
            institution_id TEXT,
            institution_name TEXT,
            transaction_account_id TEXT,
            created_at TEXT,
            updated_at TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pocketsmith_categories (
            id TEXT PRIMARY KEY,
            title TEXT,
            colour TEXT,
            parent_id TEXT,
            is_transfer INTEGER,
            is_bill INTEGER,
            roll_up INTEGER,
            refund_behaviour TEXT,
            children_count INTEGER,
            created_at TEXT,
            updated_at TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pocketsmith_transactions (
            id TEXT PRIMARY KEY,
            date TEXT,
            payee TEXT,
            original_payee TEXT,
            amount REAL,
            amount_in_base_currency REAL,
            type TEXT,
            is_transfer INTEGER,
            category_id TEXT,
            category_name TEXT,
            memo TEXT,
            note TEXT,
            labels TEXT,
            transaction_account_id TEXT,
            transaction_account_name TEXT,
            closing_balance REAL,
            cheque_number TEXT,
            needs_review INTEGER,
            created_at TEXT,
            updated_at TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES pocketsmith_categories(id)
        );
        ",
    )?;
    Ok(())
}

fn extract_accounts(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    api_key: &str,
    user_id: i64,
) -> Result<usize> {
    let accounts: Vec<serde_json::Value> = client
        .get(format!("{API_BASE}/users/{user_id}/accounts"))
        .header("X-Developer-Key", api_key)
        .header("Accept", "application/json")
        .send()?
        .json()?;

    conn.execute("DELETE FROM pocketsmith_accounts", [])?;

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO pocketsmith_accounts
         (id, title, type, currency_code, current_balance, current_balance_date,
          safe_balance, is_net_worth, institution_id, institution_name,
          transaction_account_id, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
    )?;

    let mut count = 0;
    for acct in &accounts {
        let ta = &acct["transaction_accounts"];
        let ta_list = ta.as_array().map(|a| a.as_slice()).unwrap_or(&[]);

        for ta_item in ta_list {
            let inst = &acct["institution"];
            insert.execute(params![
                ta_item["id"].as_i64().map(|v| v.to_string()),
                acct["title"].as_str(),
                acct["type"].as_str(),
                acct["currency_code"].as_str(),
                acct["current_balance"].as_f64(),
                acct["current_balance_date"].as_str(),
                acct["safe_balance"].as_f64(),
                acct["is_net_worth"].as_bool().map(|b| b as i64),
                inst["id"].as_i64().map(|v| v.to_string()),
                inst["title"].as_str(),
                ta_item["id"].as_i64().map(|v| v.to_string()),
                acct["created_at"].as_str(),
                acct["updated_at"].as_str(),
            ])?;
            count += 1;
        }
    }
    Ok(count)
}

fn extract_categories(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    api_key: &str,
    user_id: i64,
) -> Result<usize> {
    let categories: Vec<serde_json::Value> = client
        .get(format!("{API_BASE}/users/{user_id}/categories"))
        .header("X-Developer-Key", api_key)
        .header("Accept", "application/json")
        .send()?
        .json()?;

    conn.execute("DELETE FROM pocketsmith_categories", [])?;

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO pocketsmith_categories
         (id, title, colour, parent_id, is_transfer, is_bill, roll_up,
          refund_behaviour, children_count, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
    )?;

    let mut count = 0;
    fn insert_categories(
        categories: &[serde_json::Value],
        insert: &mut rusqlite::Statement,
        count: &mut usize,
    ) -> Result<()> {
        for cat in categories {
            insert.execute(params![
                cat["id"].as_i64().map(|v| v.to_string()),
                cat["title"].as_str(),
                cat["colour"].as_str(),
                cat["parent_id"].as_i64().map(|v| v.to_string()),
                cat["is_transfer"].as_bool().map(|b| b as i64),
                cat["is_bill"].as_bool().map(|b| b as i64),
                cat["roll_up"].as_bool().map(|b| b as i64),
                cat["refund_behaviour"].as_str(),
                cat["children"].as_array().map(|a| a.len() as i64),
                cat["created_at"].as_str(),
                cat["updated_at"].as_str(),
            ])?;
            *count += 1;

            // Recurse into children
            if let Some(children) = cat["children"].as_array() {
                insert_categories(children, insert, count)?;
            }
        }
        Ok(())
    }

    insert_categories(&categories, &mut insert, &mut count)?;
    Ok(count)
}

fn extract_transactions(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    api_key: &str,
    user_id: i64,
    ps_config: &crate::config::PocketSmithConfig,
) -> Result<usize> {
    // Determine date range
    let end_date = chrono::Local::now().format("%Y-%m-%d").to_string();

    let start_date = if ps_config.incremental {
        // Check for existing transactions
        let last_date: Option<String> = conn
            .query_row(
                "SELECT MAX(date) FROM pocketsmith_transactions",
                [],
                |r| r.get(0),
            )
            .unwrap_or(None);

        if let Some(last) = last_date {
            // Go back 1 day from last transaction for overlap
            if let Ok(parsed) = chrono::NaiveDate::parse_from_str(&last, "%Y-%m-%d") {
                let start = parsed - chrono::Duration::days(1);
                start.format("%Y-%m-%d").to_string()
            } else {
                default_start_date(ps_config.transaction_days)
            }
        } else {
            default_start_date(ps_config.transaction_days)
        }
    } else {
        conn.execute("DELETE FROM pocketsmith_transactions", [])?;
        default_start_date(ps_config.transaction_days)
    };

    eprintln!("  fetching transactions from {start_date} to {end_date}");

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO pocketsmith_transactions
         (id, date, payee, original_payee, amount, amount_in_base_currency,
          type, is_transfer, category_id, category_name, memo, note, labels,
          transaction_account_id, transaction_account_name, closing_balance,
          cheque_number, needs_review, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
    )?;

    let mut count = 0;
    let mut page = 1;
    let page_delay = std::time::Duration::from_secs_f64(ps_config.page_delay_seconds);

    loop {
        if page > ps_config.max_pages as usize {
            break;
        }

        let url = format!(
            "{API_BASE}/users/{user_id}/transactions?start_date={start_date}&end_date={end_date}&page={page}&per_page=100"
        );

        let resp: Vec<serde_json::Value> = client
            .get(&url)
            .header("X-Developer-Key", api_key)
            .header("Accept", "application/json")
            .send()?
            .json()?;

        if resp.is_empty() {
            break;
        }

        for txn in &resp {
            let category = &txn["category"];
            let labels = txn["labels"]
                .as_array()
                .map(|arr| serde_json::to_string(arr).unwrap_or_default());

            insert.execute(params![
                txn["id"].as_i64().map(|v| v.to_string()),
                txn["date"].as_str(),
                txn["payee"].as_str(),
                txn["original_payee"].as_str(),
                txn["amount"].as_f64(),
                txn["amount_in_base_currency"].as_f64(),
                txn["type"].as_str(),
                txn["is_transfer"].as_bool().map(|b| b as i64),
                category["id"].as_i64().map(|v| v.to_string()),
                category["title"].as_str(),
                txn["memo"].as_str(),
                txn["note"].as_str(),
                labels,
                txn["transaction_account"]["id"].as_i64().map(|v| v.to_string()),
                txn["transaction_account"]["name"].as_str(),
                txn["closing_balance"].as_f64(),
                txn["cheque_number"].as_str(),
                txn["needs_review"].as_bool().map(|b| b as i64),
                txn["created_at"].as_str(),
                txn["updated_at"].as_str(),
            ])?;
            count += 1;
        }

        if resp.len() < 100 {
            break;
        }

        page += 1;
        std::thread::sleep(page_delay);
    }

    Ok(count)
}

fn default_start_date(days_back: u32) -> String {
    let start = chrono::Local::now() - chrono::Duration::days(days_back as i64);
    start.format("%Y-%m-%d").to_string()
}
