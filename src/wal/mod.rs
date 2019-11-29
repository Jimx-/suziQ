mod checkpoint_manager;
mod log_record;
mod reader;
mod segment;
mod wal_log;

pub use self::{
    checkpoint_manager::{CheckpointManager, DBState},
    log_record::LogRecord,
    wal_log::WalLogRecord,
};

use self::{reader::WalReader, segment::Segment};

use crate::{Error, Result, DB};

use std::{
    fs::{self, DirBuilder, File},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Mutex, RwLock},
};

use fs2::FileExt;

pub type LogPointer = u64;

pub fn is_valid_lsn(lsn: LogPointer) -> bool {
    lsn > 0
}

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
    path: PathBuf,
    capacity: usize,
    segment_creator: Mutex<SegmentCreator>,
    open_segment: RwLock<Segment>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P, config: &WalConfig) -> Result<Self> {
        if !path.as_ref().exists() {
            DirBuilder::new().recursive(true).create(&path)?;
        } else if !path.as_ref().is_dir() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a directory",
                path.as_ref().display()
            )));
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
            segment_creator.open_segment(last_segno)
        }?;

        Ok(Wal {
            dir,
            path: path.as_ref().to_path_buf(),
            capacity: config.segment_capacity,
            segment_creator: Mutex::new(segment_creator),
            open_segment: RwLock::new(segment),
        })
    }

    pub fn append(&self, record: &LogRecord) -> Result<(LogPointer, LogPointer)> {
        let buf = bincode::serialize(record).unwrap();
        self.append_raw(&buf)
    }

    fn append_raw<T>(&self, record: &T) -> Result<(LogPointer, LogPointer)>
    where
        T: Deref<Target = [u8]>,
    {
        let mut guard = self.open_segment.write().unwrap();

        if !guard.sufficient_capacity(record.len()) {
            if guard.dirty() {
                guard.flush_page(true)?;
            }

            let mut creator = self.segment_creator.lock().unwrap();
            *guard = creator.next_segment()?;
        }

        let start_pos = guard.current_lsn();
        match guard.append(record)? {
            Some(end_pos) => Ok((start_pos, end_pos)),
            _ => unreachable!(),
        }
    }

    pub fn flush(&self, lsn: Option<LogPointer>) -> Result<()> {
        let mut guard = self.open_segment.write().unwrap();

        if let Some(lsn) = lsn {
            if guard.flushed_lsn() >= lsn {
                return Ok(());
            }
        }
        guard.flush_page(false)
    }

    pub fn current_lsn(&self) -> LogPointer {
        let guard = self.open_segment.read().unwrap();

        guard.current_lsn()
    }

    pub fn get_reader(&self, start_pos: LogPointer) -> Result<WalReader> {
        WalReader::open(&self.path, self.capacity, start_pos)
    }

    pub fn startup(&self, db: &DB) -> Result<()> {
        db.with_checkpoint_manager(|ckptmgr| {
            let master_record = ckptmgr.read_master_record()?;
            let last_checkpoint_pos = master_record.last_checkpoint_pos;
            let redo_pos = if is_valid_lsn(last_checkpoint_pos) {
                let reader = self.get_reader(last_checkpoint_pos)?;
                match reader.read_record(last_checkpoint_pos)? {
                    None => {
                        return Err(Error::DataCorrupted(
                            "cannot load the checkpoint log record".to_owned(),
                        ))
                    }
                    Some((_, recbuf)) => match bincode::deserialize::<LogRecord>(&recbuf) {
                        Ok(LogRecord::Wal(WalLogRecord::Checkpoint(ckpt_log))) => ckpt_log.redo_pos,
                        Ok(_) => {
                            return Err(Error::DataCorrupted(
                                "last checkpoint pos points to non checkpoint record".to_owned(),
                            ))
                        }
                        _ => {
                            return Err(Error::DataCorrupted(
                                "cannot deserialize the checkpoint log record".to_owned(),
                            ))
                        }
                    },
                }
            } else {
                0
            };

            let current_lsn = self.current_lsn();
            if current_lsn < redo_pos {
                return Err(Error::DataCorrupted(
                    "invalid redo point in checkpoint record".to_owned(),
                ));
            }

            let need_recovery =
                current_lsn > redo_pos || master_record.db_state != DBState::Shutdowned;

            if need_recovery {
                ckptmgr.set_db_state(DBState::InCrashRecovery)?;

                let reader = self.get_reader(redo_pos)?;
                for rec in reader.iter() {
                    // this is the main redo apply loop
                    let (lsn, recbuf) = rec?;
                    let redo = match bincode::deserialize::<LogRecord>(&recbuf) {
                        Ok(record) => record,
                        _ => {
                            return Err(Error::DataCorrupted(
                                "invalid log record during recovery".to_owned(),
                            ))
                        }
                    };

                    redo.apply(db, lsn)?;
                }
            }
            Ok(())
        })
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
            last_segno,
            capacity,
        }
    }

    fn open_segment(&self, segno: u32) -> Result<Segment> {
        Segment::open(segno, self.segno_to_path(segno), self.capacity)
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
        for _ in 0..10 {
            assert!(wal.append_raw(&record).is_ok());
        }

        assert!(db_dir.close().is_ok());
    }

    #[test]
    fn can_read_wal() {
        let (wal, db_dir) = create_wal();

        let record: &[u8] = &[42u8; 100];
        for _ in 0..10 {
            assert!(wal.append_raw(&record).is_ok());
        }

        wal.flush(None).unwrap();

        let reader = wal.get_reader(0).unwrap();
        let mut count = 0;
        for rec in reader.iter() {
            let (_, recbuf) = rec.unwrap();
            count += 1;
            assert_eq!(record, &recbuf[..]);
        }

        assert_eq!(count, 10);
        db_dir.close().unwrap();
    }
}
