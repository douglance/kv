use sha2::{Digest, Sha256};
use std::env;
use std::path::Path;

/// Generate a scope hash from the current working directory.
/// Returns first 12 characters of SHA256 hash of the canonical path.
/// Returns None for global scope (when --global flag is used).
pub fn current_scope() -> Option<String> {
    let cwd = env::current_dir().ok()?;
    Some(hash_path(&cwd))
}

/// Hash a path to a 12-character scope identifier.
pub fn hash_path(path: &Path) -> String {
    // Canonicalize to resolve symlinks and get absolute path
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let path_str = canonical.to_string_lossy();

    let mut hasher = Sha256::new();
    hasher.update(path_str.as_bytes());
    let result = hasher.finalize();

    // Take first 12 characters of hex representation
    hex::encode(&result[..6])
}

/// Simple hex encoding (to avoid adding hex crate)
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0xf) as usize] as char);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_path_consistency() {
        let path = Path::new("/tmp/test");
        let hash1 = hash_path(path);
        let hash2 = hash_path(path);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 12);
    }

    #[test]
    fn test_different_paths_different_hashes() {
        let hash1 = hash_path(Path::new("/tmp/a"));
        let hash2 = hash_path(Path::new("/tmp/b"));
        assert_ne!(hash1, hash2);
    }
}
