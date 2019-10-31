use crate::storage::*;
use crate::*;

use std::{
    collections::HashMap,
    fs::{DirBuilder, File},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub struct StorageHandleInner {
    file_ref: RelFileRef,
    file: Mutex<Option<File>>,
}

#[derive(Clone)]
pub struct StorageHandle(Arc<StorageHandleInner>);

impl StorageHandle {
    pub fn new(file_ref: RelFileRef) -> Self {
        Self(Arc::new(StorageHandleInner {
            file_ref: file_ref,
            file: Mutex::new(None),
        }))
    }
    pub fn file_ref(&self) -> RelFileRef {
        self.0.file_ref
    }
}

impl Deref for StorageHandle {
    type Target = StorageHandleInner;

    fn deref(&self) -> &StorageHandleInner {
        &self.0
    }
}

pub struct StorageManager {
    base_path: PathBuf,
    dir_builder: Mutex<DirBuilder>,
    shandles: Mutex<HashMap<RelFileRef, StorageHandle>>,
}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(base_path: P) -> Self {
        let dir_builder = Mutex::new(DirBuilder::new());
        dir_builder.lock().unwrap().recursive(true);
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            dir_builder: dir_builder,
            shandles: Mutex::new(HashMap::new()),
        }
    }

    pub fn open(&self, db: OID, rel_id: OID) -> Result<StorageHandle> {
        let file_ref = RelFileRef { db, rel_id };
        let mut guard = self.shandles.lock().unwrap();
        let handle = guard
            .entry(file_ref)
            .or_insert(StorageHandle::new(file_ref));
        Ok(handle.clone())
    }

    pub fn create(&self, shandle: &StorageHandle, redo: bool) -> Result<()> {
        let mut guard = shandle.file.lock().unwrap();
        let RelFileRef { db, rel_id } = shandle.file_ref();
        match &*guard {
            Some(_) => Ok(()),
            None => {
                self.ensure_database_path(db)?;
                let rel_path = self.rel_path(RelFileRef { db, rel_id });

                let file = if rel_path.exists() {
                    if rel_path.is_file() {
                        if redo {
                            File::open(rel_path)?
                        } else {
                            return Err(Error::FileAccess(format!(
                                "cannot create file '{}'",
                                rel_path.as_path().display()
                            )));
                        }
                    } else {
                        return Err(Error::WrongObjectType(format!(
                            "'{}' exists but is not a regular file",
                            rel_path.as_path().display()
                        )));
                    }
                } else {
                    File::create(rel_path)?
                };

                *guard = Some(file);
                Ok(())
            }
        }
    }

    pub fn read(&self, _shandle: &StorageHandle, _page_num: usize) {}

    pub fn write(&self, _shandle: &StorageHandle, _page_num: usize) {}

    fn ensure_database_path(&self, db: OID) -> Result<()> {
        let db_path = self.db_path(db);

        if !db_path.is_dir() {
            let guard = self.dir_builder.lock().unwrap();

            if !db_path.is_dir() {
                guard.create(db_path)?;
            }
            Ok(())
        } else if db_path.is_file() {
            Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a directory",
                db_path.as_path().display()
            )))
        } else {
            Ok(())
        }
    }

    fn db_path(&self, db: OID) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(db.to_string());
        path
    }
    fn rel_path(&self, file_ref: RelFileRef) -> PathBuf {
        let mut path = self.base_path.clone();
        let RelFileRef { db, rel_id } = file_ref;
        path.push(db.to_string());
        path.push(rel_id.to_string());
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_temp_smgr() -> (StorageManager, tempfile::TempDir) {
        let db_path = tempfile::tempdir().unwrap();
        let smgr = StorageManager::new(&db_path.path());

        (smgr, db_path)
    }

    #[test]
    fn can_create_relation() {
        let (smgr, db_path) = get_temp_smgr();
        let shandle = smgr.open(0, 0).unwrap();
        assert!(smgr.create(&shandle, false).is_ok());

        let mut rel_path = db_path.into_path().to_path_buf();
        rel_path.push("0");
        rel_path.push("0");

        assert!(rel_path.is_file());
    }
}
