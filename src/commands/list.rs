use crate::db::Database;
use crate::error::KvError;
use crate::scope::current_scope;
use serde::Serialize;

#[derive(Serialize)]
struct KeyJson {
    key: String,
    versions: i64,
    size: i64,
    last_updated: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
}

#[derive(Serialize)]
struct HistoryJson {
    version: i64,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

pub fn execute(key: Option<&str>, limit: Option<usize>, global: bool, all: bool, json: bool) -> Result<(), KvError> {
    let scope = if global || all {
        None
    } else {
        current_scope()
    };

    let db = Database::open()?;

    match key {
        Some(k) => list_key_history(&db, k, limit, scope.as_deref(), json),
        None => list_all_keys(&db, limit, scope.as_deref(), all, json),
    }
}

fn list_all_keys(db: &Database, limit: Option<usize>, scope: Option<&str>, all: bool, json: bool) -> Result<(), KvError> {
    let keys = db.list_keys(limit, scope, all)?;

    if keys.is_empty() {
        if !json {
            eprintln!("no keys found");
        } else {
            println!("[]");
        }
        return Ok(());
    }

    if json {
        let output: Vec<KeyJson> = keys.iter().map(|s| KeyJson {
            key: s.key.clone(),
            versions: s.versions,
            size: s.total_size,
            last_updated: s.last_updated.to_rfc3339(),
            scope: s.scope.clone(),
        }).collect();
        println!("{}", serde_json::to_string(&output).unwrap());
        return Ok(());
    }

    if all {
        // Show scope column when listing all scopes
        println!("{:<30} {:>8} {:>12} {:<14} {}", "KEY", "VERSIONS", "SIZE", "SCOPE", "LAST UPDATED");
        println!("{}", "-".repeat(85));

        for summary in keys {
            let scope_display = summary.scope.as_deref().unwrap_or("global");
            println!(
                "{:<30} {:>8} {:>12} {:<14} {}",
                truncate(&summary.key, 30),
                summary.versions,
                format_size(summary.total_size),
                scope_display,
                summary.last_updated.format("%Y-%m-%d %H:%M:%S")
            );
        }
    } else {
        println!("{:<30} {:>8} {:>12} {}", "KEY", "VERSIONS", "SIZE", "LAST UPDATED");
        println!("{}", "-".repeat(70));

        for summary in keys {
            println!(
                "{:<30} {:>8} {:>12} {}",
                truncate(&summary.key, 30),
                summary.versions,
                format_size(summary.total_size),
                summary.last_updated.format("%Y-%m-%d %H:%M:%S")
            );
        }
    }

    Ok(())
}

fn list_key_history(db: &Database, key: &str, limit: Option<usize>, scope: Option<&str>, json: bool) -> Result<(), KvError> {
    let entries = db.list_key_history(key, limit, scope)?;

    if json {
        let output: Vec<HistoryJson> = entries.iter().map(|e| HistoryJson {
            version: e.version,
            size: e.size_bytes,
            content_type: e.content_type.clone(),
            created_at: e.created_at.to_rfc3339(),
            original_filename: e.original_filename.clone(),
            deleted_at: e.deleted_at.map(|dt| dt.to_rfc3339()),
            expires_at: e.expires_at.map(|dt| dt.to_rfc3339()),
        }).collect();
        println!("{}", serde_json::to_string(&output).unwrap());
        return Ok(());
    }

    println!("{:>8} {:>12} {:<20} {:<20} {}", "VERSION", "SIZE", "TYPE", "CREATED", "FILENAME");
    println!("{}", "-".repeat(80));

    let now = chrono::Utc::now();
    for entry in entries {
        let content_type = entry.content_type.as_deref().unwrap_or("-");
        let filename = entry.original_filename.as_deref().unwrap_or("-");

        let status = if entry.deleted_at.is_some() {
            " (deleted)"
        } else if entry.expires_at.map(|e| e < now).unwrap_or(false) {
            " (expired)"
        } else {
            ""
        };

        println!(
            "{:>8} {:>12} {:<20} {:<20} {}{}",
            entry.version,
            format_size(entry.size_bytes),
            truncate(content_type, 20),
            entry.created_at.format("%Y-%m-%d %H:%M:%S"),
            truncate(filename, 20),
            status
        );
    }

    Ok(())
}

pub fn format_size(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
