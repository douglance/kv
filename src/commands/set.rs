use crate::db::Database;
use crate::detection::detect_input;
use crate::error::KvError;
use crate::scope::current_scope;
use chrono::{Duration, Utc};

const SIZE_LIMIT: u64 = 100 * 1024 * 1024; // 100 MB

pub fn execute(
    key: &str,
    value: Option<&str>,
    literal: bool,
    force: bool,
    global: bool,
    ttl: Option<&str>,
) -> Result<(), KvError> {
    let input = detect_input(value, literal)?;

    let content = input.content();
    let size = content.len() as u64;

    // Check size limit
    if size > SIZE_LIMIT && !force {
        return Err(KvError::SizeLimitExceeded {
            size,
            limit: SIZE_LIMIT,
        });
    }

    // Determine scope
    let scope = if global {
        None
    } else {
        current_scope()
    };

    // Parse TTL
    let expires_at = if let Some(ttl_str) = ttl {
        Some(parse_ttl(ttl_str)?)
    } else {
        None
    };

    let db = Database::open()?;
    let (version, was_saved) = db.set(
        key,
        content,
        input.content_type(),
        input.original_filename(),
        scope.as_deref(),
        expires_at,
    )?;

    if was_saved {
        let scope_info = if global { " (global)" } else { "" };
        let ttl_info = if let Some(exp) = expires_at {
            format!(" expires {}", exp.format("%Y-%m-%d %H:%M:%S UTC"))
        } else {
            String::new()
        };
        eprintln!("set {}{} (version {}, {} bytes){}", key, scope_info, version, size, ttl_info);
    } else {
        eprintln!("{} unchanged (version {})", key, version);
    }

    Ok(())
}

/// Parse a TTL string like "30s", "5m", "1h", "7d" into a DateTime
fn parse_ttl(ttl: &str) -> Result<chrono::DateTime<Utc>, KvError> {
    let ttl = ttl.trim();
    if ttl.is_empty() {
        return Err(KvError::InvalidTtl("empty TTL".into()));
    }

    let (num_str, unit) = ttl.split_at(ttl.len() - 1);
    let num: i64 = num_str.parse().map_err(|_| KvError::InvalidTtl(format!("invalid number in TTL: {}", ttl)))?;

    let duration = match unit {
        "s" => Duration::seconds(num),
        "m" => Duration::minutes(num),
        "h" => Duration::hours(num),
        "d" => Duration::days(num),
        _ => return Err(KvError::InvalidTtl(format!("invalid unit in TTL: {} (use s/m/h/d)", ttl))),
    };

    Ok(Utc::now() + duration)
}
