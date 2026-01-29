use crate::error::KvError;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::PathBuf;

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    value BLOB NOT NULL,
    version INTEGER NOT NULL,
    content_type TEXT,
    original_filename TEXT,
    size_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_key_active ON entries(key) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_created ON entries(created_at);
"#;

const SCHEMA_V2_MIGRATIONS: &[&str] = &[
    "ALTER TABLE entries ADD COLUMN scope TEXT",
    "ALTER TABLE entries ADD COLUMN expires_at TEXT",
    "DROP INDEX IF EXISTS idx_key_version",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_key_version_scope ON entries(key, version, scope)",
    "CREATE INDEX IF NOT EXISTS idx_scope ON entries(scope)",
    "CREATE INDEX IF NOT EXISTS idx_expires ON entries(expires_at) WHERE expires_at IS NOT NULL",
];

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Entry {
    pub id: i64,
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
    pub content_type: Option<String>,
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub scope: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct KeySummary {
    pub key: String,
    pub versions: i64,
    pub total_size: i64,
    pub last_updated: DateTime<Utc>,
    pub scope: Option<String>,
}

/// Statistics about the key-value store
#[derive(Debug, Clone)]
pub struct Stats {
    pub total_size: i64,
    pub total_entries: i64,
    pub active_keys: i64,
    pub deleted_keys: i64,
    pub expired_keys: i64,
    pub oldest_key: Option<String>,
    pub oldest_date: Option<DateTime<Utc>>,
    pub largest_key: Option<String>,
    pub largest_size: i64,
    pub scopes: Vec<ScopeStats>,
}

#[derive(Debug, Clone)]
pub struct ScopeStats {
    pub scope: Option<String>,
    pub size: i64,
    pub keys: i64,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open() -> Result<Self, KvError> {
        let db_path = Self::db_path()?;

        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&db_path)?;

        // Run initial schema
        conn.execute_batch(SCHEMA_V1)?;

        // Run migrations for v2
        Self::migrate_v2(&conn)?;

        Ok(Self { conn })
    }

    fn migrate_v2(conn: &Connection) -> Result<(), KvError> {
        // Check if scope column exists
        let has_scope: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM pragma_table_info('entries') WHERE name = 'scope'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !has_scope {
            for migration in SCHEMA_V2_MIGRATIONS {
                // Ignore errors for index creation (might already exist)
                let _ = conn.execute(migration, []);
            }
        }

        Ok(())
    }

    fn db_path() -> Result<PathBuf, KvError> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| KvError::Database("could not find config directory".into()))?;
        Ok(config_dir.join("kv").join("kv.db"))
    }

    /// Returns (version, was_saved) - was_saved is false if value unchanged
    pub fn set(
        &self,
        key: &str,
        value: &[u8],
        content_type: Option<&str>,
        original_filename: Option<&str>,
        scope: Option<&str>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(i64, bool), KvError> {
        // Check if current value is identical - skip save if unchanged
        if let Ok(Some(existing)) = self.get_latest(key, scope) {
            if existing.value == value {
                return Ok((existing.version, false));
            }
        }

        let next_version = self.next_version(key, scope)?;
        let now = Utc::now().to_rfc3339();
        let size = value.len() as i64;
        let expires_str = expires_at.map(|dt| dt.to_rfc3339());

        self.conn.execute(
            "INSERT INTO entries (key, value, version, content_type, original_filename, size_bytes, created_at, scope, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![key, value, next_version, content_type, original_filename, size, now, scope, expires_str],
        )?;

        Ok((next_version, true))
    }

    fn next_version(&self, key: &str, scope: Option<&str>) -> Result<i64, KvError> {
        let max: Option<i64> = if scope.is_some() {
            self.conn.query_row(
                "SELECT MAX(version) FROM entries WHERE key = ?1 AND scope = ?2",
                params![key, scope],
                |row| row.get(0),
            )?
        } else {
            self.conn.query_row(
                "SELECT MAX(version) FROM entries WHERE key = ?1 AND scope IS NULL",
                [key],
                |row| row.get(0),
            )?
        };
        Ok(max.unwrap_or(0) + 1)
    }

    pub fn get(&self, key: &str, version: Option<i64>, scope: Option<&str>) -> Result<Entry, KvError> {
        let entry = match version {
            Some(v) => self.get_version(key, v, scope)?,
            None => self.get_latest(key, scope)?,
        };

        // Check for expiration
        if let Some(ref e) = entry {
            if let Some(expires) = e.expires_at {
                if expires < Utc::now() {
                    return Err(KvError::KeyNotFound(key.to_string()));
                }
            }
        }

        entry.ok_or_else(|| {
            if let Some(v) = version {
                KvError::VersionNotFound { key: key.to_string(), version: v }
            } else {
                KvError::KeyNotFound(key.to_string())
            }
        })
    }

    fn get_latest(&self, key: &str, scope: Option<&str>) -> Result<Option<Entry>, KvError> {
        let sql = if scope.is_some() {
            "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
             FROM entries
             WHERE key = ?1 AND scope = ?2 AND deleted_at IS NULL
             ORDER BY version DESC
             LIMIT 1"
        } else {
            "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
             FROM entries
             WHERE key = ?1 AND scope IS NULL AND deleted_at IS NULL
             ORDER BY version DESC
             LIMIT 1"
        };

        let result = if scope.is_some() {
            self.conn
                .query_row(sql, params![key, scope], |row| Ok(Self::row_to_entry(row)))
                .optional()
        } else {
            self.conn
                .query_row(sql, [key], |row| Ok(Self::row_to_entry(row)))
                .optional()
        };

        result
            .map(|opt| opt.flatten())
            .map_err(Into::into)
    }

    fn get_version(&self, key: &str, version: i64, scope: Option<&str>) -> Result<Option<Entry>, KvError> {
        let sql = if scope.is_some() {
            "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
             FROM entries
             WHERE key = ?1 AND version = ?2 AND scope = ?3"
        } else {
            "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
             FROM entries
             WHERE key = ?1 AND version = ?2 AND scope IS NULL"
        };

        let result = if scope.is_some() {
            self.conn
                .query_row(sql, params![key, version, scope], |row| Ok(Self::row_to_entry(row)))
                .optional()
        } else {
            self.conn
                .query_row(sql, params![key, version], |row| Ok(Self::row_to_entry(row)))
                .optional()
        };

        result
            .map(|opt| opt.flatten())
            .map_err(Into::into)
    }

    fn row_to_entry(row: &rusqlite::Row) -> Option<Entry> {
        let created_at_str: String = row.get(7).ok()?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .ok()?
            .with_timezone(&Utc);

        let deleted_at: Option<String> = row.get(8).ok()?;
        let deleted_at = deleted_at.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        let expires_at: Option<String> = row.get(10).ok().unwrap_or(None);
        let expires_at = expires_at.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        Some(Entry {
            id: row.get(0).ok()?,
            key: row.get(1).ok()?,
            value: row.get(2).ok()?,
            version: row.get(3).ok()?,
            content_type: row.get(4).ok()?,
            original_filename: row.get(5).ok()?,
            size_bytes: row.get(6).ok()?,
            created_at,
            deleted_at,
            scope: row.get(9).ok().unwrap_or(None),
            expires_at,
        })
    }

    /// List keys, optionally filtering by scope
    /// If scope is Some, filter to that scope
    /// If scope is None and all is false, show only global keys
    /// If all is true, show all keys regardless of scope
    pub fn list_keys(&self, limit: Option<usize>, scope: Option<&str>, all: bool) -> Result<Vec<KeySummary>, KvError> {
        let now = Utc::now().to_rfc3339();
        let limit_clause = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();

        let sql = if all {
            format!(
                "SELECT key, COUNT(*) as versions, SUM(size_bytes) as total_size, MAX(created_at) as last_updated, scope
                 FROM entries
                 WHERE deleted_at IS NULL AND (expires_at IS NULL OR expires_at > ?1)
                 GROUP BY key, scope
                 ORDER BY last_updated DESC{}",
                limit_clause
            )
        } else if scope.is_some() {
            format!(
                "SELECT key, COUNT(*) as versions, SUM(size_bytes) as total_size, MAX(created_at) as last_updated, scope
                 FROM entries
                 WHERE deleted_at IS NULL AND scope = ?2 AND (expires_at IS NULL OR expires_at > ?1)
                 GROUP BY key
                 ORDER BY last_updated DESC{}",
                limit_clause
            )
        } else {
            format!(
                "SELECT key, COUNT(*) as versions, SUM(size_bytes) as total_size, MAX(created_at) as last_updated, scope
                 FROM entries
                 WHERE deleted_at IS NULL AND scope IS NULL AND (expires_at IS NULL OR expires_at > ?1)
                 GROUP BY key
                 ORDER BY last_updated DESC{}",
                limit_clause
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;

        let rows = if all {
            stmt.query_map([&now], Self::row_to_key_summary)?
        } else if scope.is_some() {
            stmt.query_map(params![&now, scope], Self::row_to_key_summary)?
        } else {
            stmt.query_map([&now], Self::row_to_key_summary)?
        };

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn row_to_key_summary(row: &rusqlite::Row) -> rusqlite::Result<KeySummary> {
        let last_updated_str: String = row.get(3)?;
        let last_updated = DateTime::parse_from_rfc3339(&last_updated_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(KeySummary {
            key: row.get(0)?,
            versions: row.get(1)?,
            total_size: row.get(2)?,
            last_updated,
            scope: row.get(4).ok().unwrap_or(None),
        })
    }

    pub fn list_key_history(&self, key: &str, limit: Option<usize>, scope: Option<&str>) -> Result<Vec<Entry>, KvError> {
        let limit_clause = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();

        let entries: Vec<Entry> = if scope.is_some() {
            let sql = format!(
                "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
                 FROM entries
                 WHERE key = ?1 AND scope = ?2
                 ORDER BY version DESC{}",
                limit_clause
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params![key, scope], |row| Ok(Self::row_to_entry(row)))?;
            rows.filter_map(|r| r.ok().flatten()).collect()
        } else {
            let sql = format!(
                "SELECT id, key, value, version, content_type, original_filename, size_bytes, created_at, deleted_at, scope, expires_at
                 FROM entries
                 WHERE key = ?1 AND scope IS NULL
                 ORDER BY version DESC{}",
                limit_clause
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map([key], |row| Ok(Self::row_to_entry(row)))?;
            rows.filter_map(|r| r.ok().flatten()).collect()
        };

        if entries.is_empty() {
            return Err(KvError::KeyNotFound(key.to_string()));
        }

        Ok(entries)
    }

    pub fn delete(&self, key: &str, hard: bool, scope: Option<&str>) -> Result<u64, KvError> {
        // First check if key exists
        let exists: bool = if scope.is_some() {
            self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM entries WHERE key = ?1 AND scope = ?2 AND deleted_at IS NULL)",
                params![key, scope],
                |row| row.get(0),
            )?
        } else {
            self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM entries WHERE key = ?1 AND scope IS NULL AND deleted_at IS NULL)",
                [key],
                |row| row.get(0),
            )?
        };

        if !exists {
            return Err(KvError::KeyNotFound(key.to_string()));
        }

        let affected = if hard {
            if scope.is_some() {
                self.conn.execute("DELETE FROM entries WHERE key = ?1 AND scope = ?2", params![key, scope])?
            } else {
                self.conn.execute("DELETE FROM entries WHERE key = ?1 AND scope IS NULL", [key])?
            }
        } else {
            let now = Utc::now().to_rfc3339();
            if scope.is_some() {
                self.conn.execute(
                    "UPDATE entries SET deleted_at = ?1 WHERE key = ?2 AND scope = ?3 AND deleted_at IS NULL",
                    params![now, key, scope],
                )?
            } else {
                self.conn.execute(
                    "UPDATE entries SET deleted_at = ?1 WHERE key = ?2 AND scope IS NULL AND deleted_at IS NULL",
                    params![now, key],
                )?
            }
        };

        Ok(affected as u64)
    }

    /// Get statistics about the store
    pub fn stats(&self) -> Result<Stats, KvError> {
        let now = Utc::now().to_rfc3339();

        // Total size and entries
        let (total_size, total_entries): (i64, i64) = self.conn.query_row(
            "SELECT COALESCE(SUM(size_bytes), 0), COUNT(*) FROM entries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // Active keys (not deleted, not expired)
        let active_keys: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT key || COALESCE(scope, '')) FROM entries
             WHERE deleted_at IS NULL AND (expires_at IS NULL OR expires_at > ?1)",
            [&now],
            |row| row.get(0),
        )?;

        // Deleted keys
        let deleted_keys: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT key || COALESCE(scope, '')) FROM entries WHERE deleted_at IS NOT NULL",
            [],
            |row| row.get(0),
        )?;

        // Expired keys (not deleted but expired)
        let expired_keys: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT key || COALESCE(scope, '')) FROM entries
             WHERE deleted_at IS NULL AND expires_at IS NOT NULL AND expires_at <= ?1",
            [&now],
            |row| row.get(0),
        )?;

        // Oldest key
        let oldest: Option<(String, String)> = self.conn.query_row(
            "SELECT key, created_at FROM entries WHERE deleted_at IS NULL ORDER BY created_at ASC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;

        let (oldest_key, oldest_date) = match oldest {
            Some((key, date_str)) => {
                let date = DateTime::parse_from_rfc3339(&date_str)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc));
                (Some(key), date)
            }
            None => (None, None),
        };

        // Largest key (by total size across versions)
        let largest: Option<(String, i64)> = self.conn.query_row(
            "SELECT key, SUM(size_bytes) as total FROM entries GROUP BY key ORDER BY total DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;

        let (largest_key, largest_size) = match largest {
            Some((k, s)) => (Some(k), s),
            None => (None, 0),
        };

        // Stats by scope
        let mut stmt = self.conn.prepare(
            "SELECT scope, SUM(size_bytes), COUNT(DISTINCT key) FROM entries
             WHERE deleted_at IS NULL
             GROUP BY scope
             ORDER BY SUM(size_bytes) DESC"
        )?;

        let scope_rows = stmt.query_map([], |row| {
            Ok(ScopeStats {
                scope: row.get(0).ok().unwrap_or(None),
                size: row.get(1)?,
                keys: row.get(2)?,
            })
        })?;

        let scopes: Vec<ScopeStats> = scope_rows.filter_map(|r| r.ok()).collect();

        Ok(Stats {
            total_size,
            total_entries,
            active_keys,
            deleted_keys,
            expired_keys,
            oldest_key,
            oldest_date,
            largest_key,
            largest_size,
            scopes,
        })
    }

    /// Garbage collect entries based on filters
    /// Returns count of entries that would be (or were) deleted
    pub fn gc(
        &self,
        run: bool,
        older_than_days: Option<u64>,
        keep_versions: Option<i64>,
        expired_only: bool,
        deleted_only: bool,
    ) -> Result<GcResult, KvError> {
        let now = Utc::now();
        let mut total_bytes = 0i64;

        // Collect IDs to delete
        let mut ids_to_delete: Vec<i64> = Vec::new();

        // Expired entries
        if expired_only || (!deleted_only && !expired_only) {
            let now_str = now.to_rfc3339();
            let mut stmt = self.conn.prepare(
                "SELECT id, size_bytes FROM entries WHERE expires_at IS NOT NULL AND expires_at <= ?1"
            )?;
            let rows = stmt.query_map([&now_str], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?;
            for row in rows.flatten() {
                ids_to_delete.push(row.0);
                total_bytes += row.1;
            }
        }

        // Deleted entries
        if deleted_only || (!deleted_only && !expired_only) {
            let mut stmt = self.conn.prepare(
                "SELECT id, size_bytes FROM entries WHERE deleted_at IS NOT NULL"
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?;
            for row in rows.flatten() {
                if !ids_to_delete.contains(&row.0) {
                    ids_to_delete.push(row.0);
                    total_bytes += row.1;
                }
            }
        }

        // Older than N days
        if let Some(days) = older_than_days {
            let cutoff = now - chrono::Duration::days(days as i64);
            let cutoff_str = cutoff.to_rfc3339();
            let mut stmt = self.conn.prepare(
                "SELECT id, size_bytes FROM entries WHERE created_at < ?1"
            )?;
            let rows = stmt.query_map([&cutoff_str], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?;
            for row in rows.flatten() {
                if !ids_to_delete.contains(&row.0) {
                    ids_to_delete.push(row.0);
                    total_bytes += row.1;
                }
            }
        }

        // Keep only N versions per key
        if let Some(keep) = keep_versions {
            // Get all key+scope combinations
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT key, scope FROM entries"
            )?;
            let key_scopes: Vec<(String, Option<String>)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1).ok().unwrap_or(None))))?
                .filter_map(|r| r.ok())
                .collect();

            for (key, scope) in key_scopes {
                // Get IDs to delete (versions beyond the keep limit)
                let version_rows: Vec<(i64, i64)> = if scope.is_some() {
                    let sql = "SELECT id, size_bytes FROM entries WHERE key = ?1 AND scope = ?2 ORDER BY version DESC";
                    let mut stmt = self.conn.prepare(sql)?;
                    let result = stmt.query_map(params![&key, &scope], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                        .flatten()
                        .collect();
                    result
                } else {
                    let sql = "SELECT id, size_bytes FROM entries WHERE key = ?1 AND scope IS NULL ORDER BY version DESC";
                    let mut stmt = self.conn.prepare(sql)?;
                    let result = stmt.query_map([&key], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                        .flatten()
                        .collect();
                    result
                };

                for (i, (id, size)) in version_rows.into_iter().enumerate() {
                    if i >= keep as usize && !ids_to_delete.contains(&id) {
                        ids_to_delete.push(id);
                        total_bytes += size;
                    }
                }
            }
        }

        let total_deleted = ids_to_delete.len() as i64;

        // Actually delete if run is true
        if run && !ids_to_delete.is_empty() {
            for id in &ids_to_delete {
                self.conn.execute("DELETE FROM entries WHERE id = ?1", [id])?;
            }
        }

        Ok(GcResult {
            entries_count: total_deleted,
            bytes_freed: total_bytes,
            was_run: run,
        })
    }
}

#[derive(Debug, Clone)]
pub struct GcResult {
    pub entries_count: i64,
    pub bytes_freed: i64,
    pub was_run: bool,
}
