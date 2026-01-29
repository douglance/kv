use crate::db::Database;
use crate::error::KvError;
use crate::scope::current_scope;
use serde::Serialize;
use std::io::{self, IsTerminal, Write};

#[derive(Serialize)]
struct JsonOutput {
    key: String,
    value: String,
    version: i64,
    scope: Option<String>,
    content_type: Option<String>,
    original_filename: Option<String>,
    size_bytes: i64,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
}

pub fn execute(key: &str, version: Option<i64>, verbose: bool, global: bool, json: bool) -> Result<(), KvError> {
    let scope = if global {
        None
    } else {
        current_scope()
    };

    let db = Database::open()?;
    let entry = db.get(key, version, scope.as_deref())?;

    if json {
        // JSON output mode
        let value_str = String::from_utf8_lossy(&entry.value).to_string();
        let output = JsonOutput {
            key: entry.key.clone(),
            value: value_str,
            version: entry.version,
            scope: entry.scope.clone(),
            content_type: entry.content_type.clone(),
            original_filename: entry.original_filename.clone(),
            size_bytes: entry.size_bytes,
            created_at: entry.created_at.to_rfc3339(),
            expires_at: entry.expires_at.map(|dt| dt.to_rfc3339()),
        };
        println!("{}", serde_json::to_string(&output).unwrap());
        return Ok(());
    }

    if verbose {
        eprintln!("Key: {}", entry.key);
        eprintln!("Version: {}", entry.version);
        eprintln!("Size: {} bytes", entry.size_bytes);
        if let Some(scope) = &entry.scope {
            eprintln!("Scope: {}", scope);
        } else {
            eprintln!("Scope: global");
        }
        if let Some(ct) = &entry.content_type {
            eprintln!("Content-Type: {}", ct);
        }
        if let Some(filename) = &entry.original_filename {
            eprintln!("Original Filename: {}", filename);
        }
        eprintln!("Created: {}", entry.created_at.format("%Y-%m-%d %H:%M:%S UTC"));
        if let Some(expires) = &entry.expires_at {
            eprintln!("Expires: {}", expires.format("%Y-%m-%d %H:%M:%S UTC"));
        }
        if let Some(deleted) = &entry.deleted_at {
            eprintln!("Deleted: {}", deleted.format("%Y-%m-%d %H:%M:%S UTC"));
        }
        eprintln!("---");
    }

    // Write raw value to stdout
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(&entry.value)?;

    // Add newline for terminal display, but not when piping (preserves exact data)
    if io::stdout().is_terminal() && !entry.value.ends_with(b"\n") {
        handle.write_all(b"\n")?;
    }

    Ok(())
}
