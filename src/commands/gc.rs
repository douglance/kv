use crate::commands::list::format_size;
use crate::db::Database;
use crate::error::KvError;

pub fn execute(
    run: bool,
    older_than: Option<u64>,
    keep_versions: Option<i64>,
    expired: bool,
    deleted: bool,
) -> Result<(), KvError> {
    let db = Database::open()?;

    // If no filters specified and not running, show help
    if !run && older_than.is_none() && keep_versions.is_none() && !expired && !deleted {
        eprintln!("Garbage collection (dry run by default)");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --run              Actually delete (default is dry run)");
        eprintln!("  --older-than DAYS  Delete entries older than N days");
        eprintln!("  --keep-versions N  Keep only last N versions per key");
        eprintln!("  --expired          Only clean expired entries");
        eprintln!("  --deleted          Only clean soft-deleted entries");
        eprintln!();

        // Show what would be cleaned with default settings (expired + deleted)
        let result = db.gc(false, None, None, false, false)?;
        if result.entries_count > 0 {
            eprintln!(
                "Without filters: {} entries ({}) would be cleaned",
                result.entries_count,
                format_size(result.bytes_freed)
            );
        } else {
            eprintln!("No entries to clean.");
        }
        return Ok(());
    }

    let result = db.gc(run, older_than, keep_versions, expired, deleted)?;

    if result.was_run {
        if result.entries_count > 0 {
            eprintln!(
                "Deleted {} entries, freed {}",
                result.entries_count,
                format_size(result.bytes_freed)
            );
        } else {
            eprintln!("No entries to clean.");
        }
    } else {
        if result.entries_count > 0 {
            eprintln!(
                "Would delete {} entries, freeing {} (dry run, use --run to execute)",
                result.entries_count,
                format_size(result.bytes_freed)
            );
        } else {
            eprintln!("No entries to clean.");
        }
    }

    Ok(())
}
