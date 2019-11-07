use crate::{Error, Result};

use super::{segment::Segment, LogPointer};

use std::{
    fs::{self, DirBuilder, File},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Mutex,
};

use fs2::FileExt;

pub struct WalConfig {
    pub segment_capacity: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_capacity: 16 * 1024 * 1024,
        }
    }
}

impl WalConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct Wal {
    #[allow(dead_code)]
    dir: File,
    segment_creator: Mutex<SegmentCreator>,
    open_segment: Mutex<Segment>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P, config: &WalConfig) -> Result<Self> {
        if !path.as_ref().exists() {
            DirBuilder::new().recursive(true).create(&path)?;
        } else {
            if !path.as_ref().is_dir() {
                return Err(Error::WrongObjectType(format!(
                    "'{}' exists but is not a directory",
                    path.as_ref().display()
                )));
            }
        }

        let dir = File::open(&path)?;
        dir.try_lock_exclusive()?;

        let mut last_segno: u32 = 0;
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if !metadata.is_file() {
                return Err(Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                )));
            }

            let filename = entry.file_name().into_string().map_err(|_| {
                Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                ))
            })?;

            let segno = filename_to_segno(&filename)?;

            if segno > last_segno {
                last_segno = segno;
            }
        }

        let mut segment_creator = SegmentCreator::new(&path, config.segment_capacity, last_segno);
        let segment = if last_segno == 0 {
            segment_creator.next_segment()
        } else {
            segment_creator.open_segment(last_segno, true)
        }?;

        Ok(Wal {
            dir,
            segment_creator: Mutex::new(segment_creator),
            open_segment: Mutex::new(segment),
        })
    }

    pub fn append<T>(&self, record: &T) -> Result<LogPointer>
    where
        T: Deref<Target = [u8]>,
    {
        let mut guard = self.open_segment.lock().unwrap();

        if !guard.sufficient_capacity(record.len()) {
            if guard.dirty() {
                guard.flush_page(true)?;
            }

            let mut creator = self.segment_creator.lock().unwrap();
            *guard = creator.next_segment()?;
        }

        match guard.append(record)? {
            Some(lsn) => Ok(lsn),
            _ => unreachable!(),
        }
    }
}

fn filename_to_segno(filename: &str) -> Result<u32> {
    u32::from_str_radix(filename, 16).map_err(|_| {
        Error::WrongObjectType(format!(
            "unexpected segment in wal directory: '{}'",
            filename
        ))
    })
}

struct SegmentCreator {
    path: PathBuf,
    last_segno: u32,
    capacity: usize,
}

impl SegmentCreator {
    fn new<P: AsRef<Path>>(path: P, capacity: usize, last_segno: u32) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            last_segno: last_segno,
            capacity,
        }
    }

    fn open_segment(&self, segno: u32, write: bool) -> Result<Segment> {
        Segment::open(segno, self.segno_to_path(segno), self.capacity, write)
    }

    fn next_segment(&mut self) -> Result<Segment> {
        self.last_segno += 1;
        Segment::create(
            self.last_segno,
            self.segno_to_path(self.last_segno),
            self.capacity,
        )
    }
    fn segno_to_path(&self, segno: u32) -> PathBuf {
        let mut path = self.path.clone();
        path.push(format!("{:08X}", segno));
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_wal() -> (Wal, tempfile::TempDir) {
        let db_dir = tempfile::tempdir().unwrap();
        let config = WalConfig::new();
        let wal = Wal::open(db_dir.path(), &config).unwrap();
        (wal, db_dir)
    }

    #[test]
    fn can_create_wal() {
        let (_, db_dir) = create_wal();

        let mut path = db_dir.path().to_path_buf();
        path.push("00000001");
        assert!(path.is_file());

        assert!(db_dir.close().is_ok());
    }

    #[test]
    fn can_append_wal() {
        let (wal, db_dir) = create_wal();

        let record: &[u8] = &[42u8; 4096];
        for _ in 1..10 {
            assert!(wal.append(&record).is_ok());
        }

        assert!(db_dir.close().is_ok());
    }
}
