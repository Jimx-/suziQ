use crate::storage::*;
use crate::*;

use std::{
    collections::HashMap,
    fs::{DirBuilder, File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ForkType {
    Main = 0,
    Last = 1,
}

const MAX_FORKS: usize = ForkType::Last as usize;

pub struct StorageHandleInner {
    file_ref: RelFileRef,
    forks: [Mutex<Option<File>>; MAX_FORKS],
}

#[derive(Clone)]
pub struct StorageHandle(Arc<StorageHandleInner>);

impl StorageHandle {
    pub fn new(file_ref: RelFileRef) -> Self {
        Self(Arc::new(StorageHandleInner {
            file_ref,
            forks: [Mutex::new(None); MAX_FORKS],
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
            dir_builder,
            shandles: Mutex::new(HashMap::new()),
        }
    }

    pub fn open(&self, db: OID, rel_id: OID) -> Result<StorageHandle> {
        let file_ref = RelFileRef { db, rel_id };
        let mut guard = self.shandles.lock().unwrap();
        let handle = guard
            .entry(file_ref)
            .or_insert_with(|| StorageHandle::new(file_ref));
        Ok(handle.clone())
    }

    pub fn create(&self, shandle: &StorageHandle, fork: ForkType, redo: bool) -> Result<()> {
        let mut guard = shandle.forks[fork as usize].lock().unwrap();
        let RelFileRef { db, rel_id } = shandle.file_ref();
        match &*guard {
            Some(_) => Ok(()),
            None => {
                self.ensure_database_path(db)?;
                let rel_path = self.rel_path(RelFileRef { db, rel_id }, fork);

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
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(rel_path)?
                };

                *guard = Some(file);
                Ok(())
            }
        }
    }

    fn close_fork(&self, shandle: &StorageHandle, fork: ForkType) -> Result<()> {
        let mut guard = shandle.forks[fork as usize].lock().unwrap();

        match &*guard {
            None => {}
            Some(_) => *guard = None,
        }

        Ok(())
    }

    pub fn close(&self, shandle: StorageHandle) -> Result<()> {
        let mut guard = self.shandles.lock().unwrap();
        self.close_fork(&shandle, ForkType::Main)?;

        let file_ref = shandle.file_ref();
        guard.remove(&file_ref);

        Ok(())
    }

    pub fn read(
        &self,
        shandle: &StorageHandle,
        fork: ForkType,
        page_num: usize,
        buffer: &mut PageBuffer,
    ) -> Result<()> {
        self.with_fork(shandle, fork, |file| {
            file.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
            match file.read_exact(buffer) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        Err(Error::DataCorrupted(format!(
                            "could not read page {} of relation {}: unexpected EOF",
                            page_num,
                            shandle.file_ref()
                        )))
                    } else {
                        Err(Error::FileAccess(format!(
                            "could not read page {} of relation {}",
                            page_num,
                            shandle.file_ref()
                        )))
                    }
                }
                _ => Ok(()),
            }
        })
    }

    pub fn write(
        &self,
        shandle: &StorageHandle,
        fork: ForkType,
        page_num: usize,
        buffer: &PageBuffer,
    ) -> Result<()> {
        self.with_fork(shandle, fork, |file| {
            file.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
            match file.write_all(buffer) {
                Err(_) => Err(Error::FileAccess(format!(
                    "could not write page {} of relation {}",
                    page_num,
                    shandle.file_ref()
                ))),
                _ => Ok(()),
            }
        })
    }

    pub fn file_size_in_page(&self, shandle: &StorageHandle, fork: ForkType) -> Result<usize> {
        self.with_fork(shandle, fork, |file| {
            let metadata = file.metadata()?;

            Ok(metadata.len() as usize / PAGE_SIZE)
        })
    }

    pub fn truncate(&self, shandle: &StorageHandle, fork: ForkType, nr_pages: usize) -> Result<()> {
        self.with_fork(shandle, fork, |file| {
            let metadata = file.metadata()?;
            let cur_pages = metadata.len() as usize / PAGE_SIZE;

            if cur_pages >= nr_pages {
                file.set_len(nr_pages as u64 * PAGE_SIZE as u64)?;
            }

            Ok(())
        })
    }

    pub fn sync(&self, shandle: &StorageHandle, fork: ForkType) -> Result<()> {
        self.with_fork(shandle, fork, |file| Ok(file.sync_data()?))
    }
    fn with_fork<F, R>(&self, shandle: &StorageHandle, fork: ForkType, f: F) -> Result<R>
    where
        F: FnOnce(&mut File) -> Result<R>,
    {
        let mut guard = shandle.forks[fork as usize].lock().unwrap();

        match &mut *guard {
            Some(file) => f(file),
            guard_ref @ None => {
                let rel_path = self.rel_path(shandle.file_ref, fork);
                let file = File::open(rel_path)?;

                *guard_ref = Some(file);

                f(guard_ref.as_mut().expect("impossbile"))
            }
        }
    }

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
    fn rel_path(&self, file_ref: RelFileRef, fork: ForkType) -> PathBuf {
        let mut path = self.base_path.clone();
        let RelFileRef { db, rel_id } = file_ref;
        path.push(db.to_string());
        path.push(format!("{}_{}", rel_id, fork as usize));
        path
    }
}

#[cfg(test)]
pub fn get_temp_smgr() -> (StorageManager, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let smgr = StorageManager::new(&db_dir.path());

    (smgr, db_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn can_create_relation() {
        let (smgr, db_dir) = super::get_temp_smgr();
        let shandle = smgr.open(0, 0).unwrap();
        assert!(smgr.create(&shandle, ForkType::Main, false).is_ok());
        assert!(shandle.forks[0].lock().unwrap().is_some());

        let mut rel_path = db_dir.path().to_path_buf();
        rel_path.push("0");
        rel_path.push("0_0");

        assert!(rel_path.is_file());
    }

    #[test]
    fn can_read_write() {
        let (smgr, db_dir) = get_temp_smgr();
        let shandle = smgr.open(0, 0).unwrap();
        assert!(smgr.create(&shandle, ForkType::Main, false).is_ok());

        let wbuf = [1u8; PAGE_SIZE];
        let mut rbuf = [0u8; PAGE_SIZE];

        assert!(smgr.write(&shandle, ForkType::Main, 0, &wbuf).is_ok());
        assert!(smgr.read(&shandle, ForkType::Main, 0, &mut rbuf).is_ok());
        assert_eq!(&wbuf[..], &rbuf[..]);

        assert!(db_dir.close().is_ok());
    }

    #[test]
    fn can_truncate() {
        let (smgr, db_dir) = get_temp_smgr();
        let shandle = smgr.open(0, 0).unwrap();
        assert!(smgr.create(&shandle, ForkType::Main, false).is_ok());

        let wbuf = [1u8; PAGE_SIZE];

        assert!(smgr.write(&shandle, ForkType::Main, 0, &wbuf).is_ok());
        assert!(smgr.write(&shandle, ForkType::Main, 1, &wbuf).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(2)
        );

        assert!(smgr.truncate(&shandle, ForkType::Main, 1).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(1)
        );

        assert!(smgr.truncate(&shandle, ForkType::Main, 3).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(1)
        );

        assert!(db_dir.close().is_ok());
    }
}
