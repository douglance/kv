use std::fmt;

#[derive(Debug)]
pub enum KvError {
    KeyNotFound(String),
    VersionNotFound { key: String, version: i64 },
    Database(String),
    Io(std::io::Error),
    SizeLimitExceeded { size: u64, limit: u64 },
    InvalidTtl(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::KeyNotFound(key) => write!(f, "key not found: {}", key),
            KvError::VersionNotFound { key, version } => {
                write!(f, "version {} not found for key: {}", version, key)
            }
            KvError::Database(msg) => write!(f, "database error: {}", msg),
            KvError::Io(err) => write!(f, "io error: {}", err),
            KvError::SizeLimitExceeded { size, limit } => {
                write!(
                    f,
                    "size {} bytes exceeds limit {} bytes (use --force to override)",
                    size, limit
                )
            }
            KvError::InvalidTtl(msg) => write!(f, "invalid TTL: {}", msg),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> Self {
        KvError::Io(err)
    }
}

impl From<rusqlite::Error> for KvError {
    fn from(err: rusqlite::Error) -> Self {
        KvError::Database(err.to_string())
    }
}
