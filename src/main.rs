use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod db;
mod detection;
mod error;
mod scope;

#[derive(Parser)]
#[command(name = "kv")]
#[command(about = "A universal key-value store for agentic tools")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key to a value (reads from stdin if piped, detects files)
    Set {
        /// The key to set
        key: String,

        /// The value (string, file path, or omit for stdin)
        value: Option<String>,

        /// Treat value as literal string, skip file detection
        #[arg(long)]
        literal: bool,

        /// Allow values larger than 100MB
        #[arg(long)]
        force: bool,

        /// Use global scope instead of CWD-scoped
        #[arg(short, long)]
        global: bool,

        /// Time-to-live (e.g., 30s, 5m, 1h, 7d)
        #[arg(long)]
        ttl: Option<String>,
    },

    /// Get the value for a key
    Get {
        /// The key to retrieve
        key: String,

        /// Get a specific version
        #[arg(long)]
        version: Option<i64>,

        /// Show metadata along with value
        #[arg(short, long)]
        verbose: bool,

        /// Use global scope instead of CWD-scoped
        #[arg(short, long)]
        global: bool,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// List all keys or history of a specific key
    List {
        /// Optional key to show history for
        key: Option<String>,

        /// Limit number of results
        #[arg(long)]
        limit: Option<usize>,

        /// Use global scope instead of CWD-scoped
        #[arg(short, long)]
        global: bool,

        /// Show all scopes
        #[arg(short, long)]
        all: bool,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Delete a key
    Delete {
        /// The key to delete
        key: String,

        /// Permanently delete (default is soft delete)
        #[arg(long)]
        hard: bool,

        /// Use global scope instead of CWD-scoped
        #[arg(short, long)]
        global: bool,
    },

    /// Show storage statistics
    Stats {
        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Garbage collect old/expired/deleted entries
    Gc {
        /// Actually delete (default is dry run)
        #[arg(long)]
        run: bool,

        /// Only delete entries older than N days
        #[arg(long, value_name = "DAYS")]
        older_than: Option<u64>,

        /// Keep only last N versions per key
        #[arg(long, value_name = "N")]
        keep_versions: Option<i64>,

        /// Only clean expired entries
        #[arg(long)]
        expired: bool,

        /// Only clean soft-deleted entries
        #[arg(long)]
        deleted: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Set {
            key,
            value,
            literal,
            force,
            global,
            ttl,
        } => commands::set::execute(&key, value.as_deref(), literal, force, global, ttl.as_deref()),

        Commands::Get {
            key,
            version,
            verbose,
            global,
            json,
        } => commands::get::execute(&key, version, verbose, global, json),

        Commands::List { key, limit, global, all, json } => {
            commands::list::execute(key.as_deref(), limit, global, all, json)
        }

        Commands::Delete { key, hard, global } => commands::delete::execute(&key, hard, global),

        Commands::Stats { json } => commands::stats::execute(json),

        Commands::Gc {
            run,
            older_than,
            keep_versions,
            expired,
            deleted,
        } => commands::gc::execute(run, older_than, keep_versions, expired, deleted),
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
