use crate::db::Database;
use crate::error::KvError;
use crate::scope::current_scope;

pub fn execute(key: &str, hard: bool, global: bool) -> Result<(), KvError> {
    let scope = if global {
        None
    } else {
        current_scope()
    };

    let db = Database::open()?;
    let affected = db.delete(key, hard, scope.as_deref())?;

    if hard {
        eprintln!("permanently deleted {} entries for key '{}'", affected, key);
    } else {
        eprintln!("soft-deleted {} entries for key '{}'", affected, key);
    }

    Ok(())
}
