# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo run -- <args>      # Run with arguments (e.g., cargo run -- set foo bar)
```

## Testing

No test suite yet. Manual testing:

```bash
# Basic operations
./target/release/kv set mykey "myvalue"
./target/release/kv get mykey
./target/release/kv list

# Scoped operations (CWD-based)
./target/release/kv set -g globalkey "value"  # Global scope
./target/release/kv list --all                # Show all scopes

# TTL
./target/release/kv set temp "expires" --ttl 5s
```

## Architecture

**CLI Layer** (`main.rs`): Clap-based command parsing. Each subcommand (set/get/list/delete/stats/gc) dispatches to `commands/<cmd>.rs`.

**Command Layer** (`commands/*.rs`): Each command handles its own argument processing, calls Database methods, and formats output. Commands resolve scope via `scope::current_scope()` unless `--global` flag is set.

**Database Layer** (`db.rs`): SQLite storage at `~/Library/Application Support/kv/kv.db` (macOS) or equivalent. Single `entries` table with versioning, soft deletes, scoping, and TTL. Schema migrations run automatically on open.

**Key Concepts**:
- **Scoping**: Keys are namespaced by CWD (12-char SHA256 hash). `--global` bypasses scoping (scope=NULL).
- **Versioning**: Every `set` creates a new version. Identical values are deduplicated.
- **Soft Deletes**: `delete` sets `deleted_at` timestamp; `--hard` removes permanently.
- **TTL**: `expires_at` timestamp checked on read; expired entries filtered out.

**Data Flow**: `main.rs` → `commands/*.rs` → `db.rs` (Database struct) → SQLite

**Input Detection** (`detection.rs`): Auto-detects if value is stdin, file path, or literal string. Infers content-type from file extension.

**Error Handling** (`error.rs`): Custom `KvError` enum with variants for key-not-found, version-not-found, database errors, IO errors, size limits, and invalid TTL.
