use anyhow::{Context, Result};
use chrono::Datelike;
use rusqlite::{params, Connection};
use std::collections::HashSet;

use crate::config::Config;

const GRAPHQL_URL: &str = "https://api.monarch.com/graphql";

/// Extract Monarch Money data via GraphQL API into warehouse.
pub fn extract(conn: &Connection, config: &Config) -> Result<usize> {
    let mc = &config.monarch;
    if !mc.enabled {
        anyhow::bail!("Monarch is disabled in config");
    }

    // Get token from environment variable
    let token = std::env::var("MONARCH_TOKEN")
        .map_err(|_| anyhow::anyhow!("Monarch not available: set MONARCH_TOKEN env var"))?;

    if token.is_empty() {
        anyhow::bail!("MONARCH_TOKEN is empty");
    }

    create_tables(conn)?;

    let client = reqwest::blocking::Client::new();
    let mut total = 0;

    // Extract accounts
    if mc.extract_accounts {
        let count = extract_accounts(&client, conn, &token)?;
        eprintln!("  accounts: {count}");
        total += count;
    }

    // Extract transactions
    if mc.extract_transactions {
        let count = extract_transactions(&client, conn, &token, mc)?;
        eprintln!("  transactions: {count}");
        total += count;
    }

    // Extract recurring
    if mc.extract_recurring {
        match extract_recurring(&client, conn, &token) {
            Ok(count) => {
                eprintln!("  recurring: {count}");
                total += count;
            }
            Err(e) => eprintln!("  recurring: skipped ({e})"),
        }
    }

    // Extract budgets (current month)
    if mc.extract_budgets {
        match extract_budgets(&client, conn, &token) {
            Ok(count) => {
                eprintln!("  budgets: {count}");
                total += count;
            }
            Err(e) => eprintln!("  budgets: skipped ({e})"),
        }
    }

    Ok(total)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS monarch_accounts (
            id TEXT PRIMARY KEY,
            display_name TEXT,
            type_name TEXT,
            type_display TEXT,
            type_group TEXT,
            subtype TEXT,
            institution_id TEXT,
            institution_name TEXT,
            institution_url TEXT,
            current_balance REAL,
            display_balance TEXT,
            is_asset INTEGER,
            is_manual INTEGER,
            is_hidden INTEGER,
            include_in_net_worth INTEGER,
            sync_disabled INTEGER,
            mask TEXT,
            logo_url TEXT,
            created_at TEXT,
            updated_at TEXT,
            transactions_count INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS monarch_transactions (
            id TEXT PRIMARY KEY,
            account_id TEXT,
            account_name TEXT,
            date TEXT,
            amount REAL,
            pending INTEGER,
            merchant_id TEXT,
            merchant_name TEXT,
            category_id TEXT,
            category_name TEXT,
            category_group_id TEXT,
            category_group_name TEXT,
            category_group_type TEXT,
            notes TEXT,
            is_recurring INTEGER,
            is_split INTEGER,
            needs_review INTEGER,
            hide_from_reports INTEGER,
            plaid_name TEXT,
            tags TEXT,
            created_at TEXT,
            updated_at TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS monarch_recurring (
            id TEXT PRIMARY KEY,
            merchant_id TEXT,
            merchant_name TEXT,
            category_id TEXT,
            category_name TEXT,
            amount REAL,
            frequency TEXT,
            is_active INTEGER,
            last_date TEXT,
            next_date TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS monarch_budgets (
            category_id TEXT,
            month TEXT,
            category_name TEXT,
            category_group_id TEXT,
            category_group_name TEXT,
            category_group_type TEXT,
            planned_amount REAL,
            actual_amount REAL,
            remaining_amount REAL,
            rollover_amount REAL,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (category_id, month)
        );

        CREATE INDEX IF NOT EXISTS idx_monarch_txn_date ON monarch_transactions(date);
        CREATE INDEX IF NOT EXISTS idx_monarch_txn_merchant ON monarch_transactions(merchant_name);
        CREATE INDEX IF NOT EXISTS idx_monarch_txn_category ON monarch_transactions(category_name);
        CREATE INDEX IF NOT EXISTS idx_monarch_txn_account ON monarch_transactions(account_id);
        CREATE INDEX IF NOT EXISTS idx_monarch_budgets_month ON monarch_budgets(month);
        ",
    )?;
    Ok(())
}

/// Make a GraphQL request to Monarch API.
fn graphql_request(
    client: &reqwest::blocking::Client,
    token: &str,
    query: &str,
    variables: Option<serde_json::Value>,
    operation_name: Option<&str>,
) -> Result<serde_json::Value> {
    let mut payload = serde_json::json!({ "query": query });
    if let Some(vars) = variables {
        payload["variables"] = vars;
    }
    if let Some(op) = operation_name {
        payload["operationName"] = serde_json::json!(op);
    }

    let resp = client
        .post(GRAPHQL_URL)
        .header("Authorization", format!("Token {token}"))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("Client-Platform", "web")
        .json(&payload)
        .send()
        .context("Failed to connect to Monarch API")?;

    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        anyhow::bail!("Monarch authentication failed — token may be expired");
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        anyhow::bail!("Monarch rate limit exceeded");
    }
    if !status.is_success() {
        anyhow::bail!("Monarch API error: {status}");
    }

    let body: serde_json::Value = resp.json().context("Failed to parse Monarch response")?;

    if let Some(errors) = body.get("errors").and_then(|e| e.as_array()) {
        let msgs: Vec<&str> = errors
            .iter()
            .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
            .collect();
        anyhow::bail!("GraphQL errors: {}", msgs.join("; "));
    }

    Ok(body.get("data").cloned().unwrap_or(serde_json::json!({})))
}

fn extract_accounts(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    token: &str,
) -> Result<usize> {
    let query = r#"
        query GetAccounts {
          accounts {
            id displayName syncDisabled isHidden isAsset mask
            createdAt updatedAt currentBalance displayBalance
            includeInNetWorth isManual transactionsCount logoUrl
            type { name display group }
            subtype { name }
            institution { id name url }
          }
        }
    "#;

    let data = graphql_request(client, token, query, None, Some("GetAccounts"))?;
    let accounts = data
        .get("accounts")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO monarch_accounts
         (id, display_name, type_name, type_display, type_group, subtype,
          institution_id, institution_name, institution_url,
          current_balance, display_balance, is_asset, is_manual, is_hidden,
          include_in_net_worth, sync_disabled, mask, logo_url,
          created_at, updated_at, transactions_count)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
    )?;

    let mut count = 0;
    for acct in &accounts {
        let acct_type = acct.get("type").unwrap_or(&serde_json::Value::Null);
        let subtype = acct.get("subtype").unwrap_or(&serde_json::Value::Null);
        let inst = acct.get("institution").unwrap_or(&serde_json::Value::Null);

        insert.execute(params![
            acct["id"].as_str(),
            acct["displayName"].as_str(),
            acct_type["name"].as_str(),
            acct_type["display"].as_str(),
            acct_type["group"].as_str(),
            subtype["name"].as_str(),
            inst["id"].as_str(),
            inst["name"].as_str(),
            inst["url"].as_str(),
            acct["currentBalance"].as_f64(),
            acct["displayBalance"].as_str(),
            acct["isAsset"].as_bool().map(|b| b as i64),
            acct["isManual"].as_bool().map(|b| b as i64),
            acct["isHidden"].as_bool().map(|b| b as i64),
            acct["includeInNetWorth"].as_bool().map(|b| b as i64),
            acct["syncDisabled"].as_bool().map(|b| b as i64),
            acct["mask"].as_str(),
            acct["logoUrl"].as_str(),
            acct["createdAt"].as_str(),
            acct["updatedAt"].as_str(),
            acct["transactionsCount"].as_i64(),
        ])?;
        count += 1;
    }
    Ok(count)
}

fn extract_transactions(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    token: &str,
    mc: &crate::config::MonarchConfig,
) -> Result<usize> {
    let query = r#"
        query GetTransactionsList($offset: Int, $limit: Int, $filters: TransactionFilterInput, $orderBy: TransactionOrdering) {
          allTransactions(filters: $filters) {
            totalCount
            results(offset: $offset, limit: $limit, orderBy: $orderBy) {
              id amount pending date hideFromReports plaidName notes
              isRecurring needsReview isSplitTransaction createdAt updatedAt
              category { id name group { id name type } }
              merchant { id name }
              account { id displayName }
              tags { id name }
            }
          }
        }
    "#;

    // Determine date range for incremental sync
    let existing_ids = get_existing_transaction_ids(conn);
    let incremental = mc.incremental && !existing_ids.is_empty();

    let start_date = if incremental {
        let days_back = mc.transaction_days as i64;
        let start = chrono::Local::now() - chrono::Duration::days(days_back);
        Some(start.format("%Y-%m-%d").to_string())
    } else {
        None // First sync: fetch all history
    };

    let page_delay = std::time::Duration::from_secs_f64(mc.page_delay_seconds);
    let mut count = 0;
    let mut offset: i64 = 0;
    let limit: i64 = 100;
    let mut consecutive_seen = 0;

    for page in 0..mc.max_pages {
        let mut filters = serde_json::json!({});
        if let Some(ref sd) = start_date {
            filters["startDate"] = serde_json::json!(sd);
        }

        let variables = serde_json::json!({
            "limit": limit,
            "offset": offset,
            "filters": filters,
            "orderBy": "date",
        });

        let data = graphql_request(
            client,
            token,
            query,
            Some(variables),
            Some("GetTransactionsList"),
        )?;

        let all_txns = data.get("allTransactions").unwrap_or(&serde_json::Value::Null);
        let total = all_txns.get("totalCount").and_then(|v| v.as_i64()).unwrap_or(0);
        let results = all_txns
            .get("results")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();

        if results.is_empty() {
            break;
        }

        let page_count = insert_transactions(conn, &results, &existing_ids, incremental, &mut consecutive_seen)?;
        count += page_count;

        if page > 0 {
            eprint!("\r  transactions: page {} ({count}/{total})  ", page + 1);
        }

        // Stop if caught up with existing data
        if incremental && consecutive_seen >= 10 {
            break;
        }

        offset += limit;
        if offset >= total {
            break;
        }

        std::thread::sleep(page_delay);
    }

    Ok(count)
}

fn get_existing_transaction_ids(conn: &Connection) -> HashSet<String> {
    let has_table: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='monarch_transactions'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);

    if !has_table {
        return HashSet::new();
    }

    let mut ids = HashSet::new();
    if let Ok(mut stmt) = conn.prepare("SELECT id FROM monarch_transactions") {
        if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
            for row in rows.flatten() {
                ids.insert(row);
            }
        }
    }
    ids
}

fn insert_transactions(
    conn: &Connection,
    transactions: &[serde_json::Value],
    existing_ids: &HashSet<String>,
    incremental: bool,
    consecutive_seen: &mut u32,
) -> Result<usize> {
    let mut insert = conn.prepare_cached(
        "INSERT OR REPLACE INTO monarch_transactions
         (id, account_id, account_name, date, amount, pending,
          merchant_id, merchant_name, category_id, category_name,
          category_group_id, category_group_name, category_group_type,
          notes, is_recurring, is_split, needs_review, hide_from_reports,
          plaid_name, tags, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
    )?;

    let mut count = 0;
    for txn in transactions {
        let txn_id = txn["id"].as_str().unwrap_or("");

        // Track consecutive seen for incremental sync
        if incremental && existing_ids.contains(txn_id) {
            *consecutive_seen += 1;
            continue;
        }
        *consecutive_seen = 0;

        let category = txn.get("category").unwrap_or(&serde_json::Value::Null);
        let cat_group = category.get("group").unwrap_or(&serde_json::Value::Null);
        let merchant = txn.get("merchant").unwrap_or(&serde_json::Value::Null);
        let account = txn.get("account").unwrap_or(&serde_json::Value::Null);

        let tags = txn
            .get("tags")
            .and_then(|t| t.as_array())
            .map(|arr| {
                let names: Vec<&str> = arr
                    .iter()
                    .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                    .collect();
                serde_json::to_string(&names).unwrap_or_default()
            });

        insert.execute(params![
            txn["id"].as_str(),
            account["id"].as_str(),
            account["displayName"].as_str(),
            txn["date"].as_str(),
            txn["amount"].as_f64(),
            txn["pending"].as_bool().map(|b| b as i64),
            merchant["id"].as_str(),
            merchant["name"].as_str(),
            category["id"].as_str(),
            category["name"].as_str(),
            cat_group["id"].as_str(),
            cat_group["name"].as_str(),
            cat_group["type"].as_str(),
            txn["notes"].as_str(),
            txn["isRecurring"].as_bool().map(|b| b as i64),
            txn["isSplitTransaction"].as_bool().map(|b| b as i64),
            txn["needsReview"].as_bool().map(|b| b as i64),
            txn["hideFromReports"].as_bool().map(|b| b as i64),
            txn["plaidName"].as_str(),
            tags,
            txn["createdAt"].as_str(),
            txn["updatedAt"].as_str(),
        ])?;
        count += 1;
    }
    Ok(count)
}

fn extract_recurring(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    token: &str,
) -> Result<usize> {
    let query = r#"
        query GetRecurringTransactions {
          recurringTransactionStreams {
            id frequency isActive amount
            merchant { id name }
            category { id name }
            lastTransaction { id date }
            nextExpectedTransaction { date }
          }
        }
    "#;

    let data = graphql_request(client, token, query, None, Some("GetRecurringTransactions"))?;
    let items = data
        .get("recurringTransactionStreams")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO monarch_recurring
         (id, merchant_id, merchant_name, category_id, category_name,
          amount, frequency, is_active, last_date, next_date)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )?;

    let mut count = 0;
    for item in &items {
        let merchant = item.get("merchant").unwrap_or(&serde_json::Value::Null);
        let category = item.get("category").unwrap_or(&serde_json::Value::Null);
        let last_txn = item.get("lastTransaction").unwrap_or(&serde_json::Value::Null);
        let next_txn = item.get("nextExpectedTransaction").unwrap_or(&serde_json::Value::Null);

        insert.execute(params![
            item["id"].as_str(),
            merchant["id"].as_str(),
            merchant["name"].as_str(),
            category["id"].as_str(),
            category["name"].as_str(),
            item["amount"].as_f64(),
            item["frequency"].as_str(),
            item["isActive"].as_bool().map(|b| b as i64),
            last_txn["date"].as_str(),
            next_txn["date"].as_str(),
        ])?;
        count += 1;
    }
    Ok(count)
}

fn extract_budgets(
    client: &reqwest::blocking::Client,
    conn: &Connection,
    token: &str,
) -> Result<usize> {
    let query = r#"
        query GetBudgets($startDate: Date!, $endDate: Date!) {
          budgetData(startDate: $startDate, endDate: $endDate) {
            monthlyAmountsByCategory {
              category { id name group { id name type } }
              monthlyAmounts { month plannedAmount actualAmount remainingAmount rolloverAmount }
            }
          }
        }
    "#;

    let today = chrono::Local::now().date_naive();
    let start_date = today.with_day(1).unwrap_or(today);
    let end_date = if today.month() == 12 {
        chrono::NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap()
    } else {
        chrono::NaiveDate::from_ymd_opt(today.year(), today.month() + 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap()
    };

    let month = today.format("%Y-%m").to_string();

    let variables = serde_json::json!({
        "startDate": start_date.format("%Y-%m-%d").to_string(),
        "endDate": end_date.format("%Y-%m-%d").to_string(),
    });

    let data = graphql_request(client, token, query, Some(variables), Some("GetBudgets"))?;
    let categories = data
        .get("budgetData")
        .and_then(|b| b.get("monthlyAmountsByCategory"))
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();

    let mut insert = conn.prepare(
        "INSERT OR REPLACE INTO monarch_budgets
         (category_id, month, category_name, category_group_id,
          category_group_name, category_group_type,
          planned_amount, actual_amount, remaining_amount, rollover_amount)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )?;

    let mut count = 0;
    for entry in &categories {
        let category = entry.get("category").unwrap_or(&serde_json::Value::Null);
        let cat_group = category.get("group").unwrap_or(&serde_json::Value::Null);
        let cat_id = category["id"].as_str();

        if cat_id.is_none() {
            continue;
        }

        let monthly = entry
            .get("monthlyAmounts")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();

        // Find the amount for the current month
        let amount = monthly.iter().find(|ma| {
            ma.get("month").and_then(|m| m.as_str()) == Some(&month)
        });

        if let Some(ma) = amount {
            insert.execute(params![
                cat_id,
                &month,
                category["name"].as_str(),
                cat_group["id"].as_str(),
                cat_group["name"].as_str(),
                cat_group["type"].as_str(),
                ma["plannedAmount"].as_f64(),
                ma["actualAmount"].as_f64(),
                ma["remainingAmount"].as_f64(),
                ma["rolloverAmount"].as_f64(),
            ])?;
            count += 1;
        }
    }
    Ok(count)
}
