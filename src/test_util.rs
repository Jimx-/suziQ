#![cfg(test)]

use crate::{storage::StorageManager, DBConfig, DB};

pub fn get_temp_smgr() -> (StorageManager, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let smgr = StorageManager::new(&db_dir.path());

    (smgr, db_dir)
}

pub fn get_temp_db() -> (DB, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let config = DBConfig::new().root_path(&db_dir.path());
    let db = DB::open(&config).unwrap();

    (db, db_dir)
}
