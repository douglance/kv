use crate::commands::list::format_size;
use crate::db::Database;
use crate::error::KvError;
use serde::Serialize;

#[derive(Serialize)]
struct StatsJson {
    total_size: i64,
    total_entries: i64,
    active_keys: i64,
    deleted_keys: i64,
    expired_keys: i64,
    oldest_key: Option<String>,
    oldest_date: Option<String>,
    largest_key: Option<String>,
    largest_size: i64,
    scopes: Vec<ScopeJson>,
}

#[derive(Serialize)]
struct ScopeJson {
    scope: Option<String>,
    size: i64,
    keys: i64,
}

pub fn execute(json: bool) -> Result<(), KvError> {
    let db = Database::open()?;
    let stats = db.stats()?;

    if json {
        let output = StatsJson {
            total_size: stats.total_size,
            total_entries: stats.total_entries,
            active_keys: stats.active_keys,
            deleted_keys: stats.deleted_keys,
            expired_keys: stats.expired_keys,
            oldest_key: stats.oldest_key.clone(),
            oldest_date: stats.oldest_date.map(|dt| dt.to_rfc3339()),
            largest_key: stats.largest_key.clone(),
            largest_size: stats.largest_size,
            scopes: stats.scopes.iter().map(|s| ScopeJson {
                scope: s.scope.clone(),
                size: s.size,
                keys: s.keys,
            }).collect(),
        };
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
        return Ok(());
    }

    // Human-readable output
    println!("Storage: {} ({} entries)", format_size(stats.total_size), stats.total_entries);
    println!(
        "Keys: {} active, {} deleted, {} expired",
        stats.active_keys, stats.deleted_keys, stats.expired_keys
    );

    if let Some(oldest) = &stats.oldest_key {
        let date = stats.oldest_date
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "unknown".into());
        println!("Oldest: {} ({})", date, oldest);
    }

    if let Some(largest) = &stats.largest_key {
        println!("Largest: {} ({})", format_size(stats.largest_size), largest);
    }

    if !stats.scopes.is_empty() {
        println!();
        println!("By scope:");
        for scope_stat in &stats.scopes {
            let scope_name = scope_stat.scope.as_deref().unwrap_or("global");
            println!(
                "  {:<14} {} ({} keys)",
                format!("{}:", scope_name),
                format_size(scope_stat.size),
                scope_stat.keys
            );
        }
    }

    Ok(())
}
