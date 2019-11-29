use crate::{
    wal::{LogPointer, WalLogRecord},
    Error, Result, DB, OID,
};

use std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    path::{Path, PathBuf},
    time::SystemTime,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DBState {
    Shutdowned,
    Shutdowning,
    InCrashRecovery,
    InProduction,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MasterRecord {
    pub db_state: DBState,
    pub last_checkpoint_pos: LogPointer,
    pub next_oid: OID,
    pub time: SystemTime,
}

impl Default for MasterRecord {
    fn default() -> Self {
        Self {
            db_state: DBState::Shutdowned,
            last_checkpoint_pos: 0,
            next_oid: 0,
            time: SystemTime::now(),
        }
    }
}

struct MasterRecordFile {
    file_path: PathBuf,
}

impl MasterRecordFile {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
        }
    }

    pub fn read_master_record(&self) -> Result<Option<MasterRecord>> {
        if !self.file_path.exists() {
            return Ok(None);
        }

        if !self.file_path.is_file() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a regular file",
                self.file_path.as_path().display()
            )));
        }

        let mut file = File::open(&self.file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.len() < 4 {
            return Err(Error::DataCorrupted(
                "master record is corrupted".to_owned(),
            ));
        }

        let crc_buf = buffer.split_off(buffer.len() - 4);
        let crc_file = (&crc_buf[..]).read_u32::<LittleEndian>().unwrap();
        let crc = crc32::checksum_ieee(&buffer);

        if crc != crc_file {
            return Err(Error::DataCorrupted(
                "master record is corrupted (checksum does not match)".to_owned(),
            ));
        }

        let record = match bincode::deserialize::<MasterRecord>(&buffer) {
            Ok(record) => record,
            _ => {
                return Err(Error::DataCorrupted(
                    "cannot deserialize the master record".to_owned(),
                ));
            }
        };

        Ok(Some(record))
    }

    pub fn write_master_record(&self, record: &MasterRecord) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(false)
            .open(&self.file_path)?;
        let mut buffer = bincode::serialize(record).unwrap();
        let crc = crc32::checksum_ieee(&buffer);
        buffer.write_u32::<LittleEndian>(crc).unwrap();

        file.write_all(&buffer)?;

        Ok(())
    }
}

pub struct CheckpointManager {
    master_record_file: MasterRecordFile,
    master_record: MasterRecord,
}

impl CheckpointManager {
    pub fn open<P: AsRef<Path>>(master_record_path: P) -> Result<Self> {
        let master_record_file = MasterRecordFile::new(master_record_path);
        let mut ckptmgr = Self {
            master_record_file,
            master_record: Default::default(),
        };

        ckptmgr.read_master_record()?;

        Ok(ckptmgr)
    }

    pub fn create_checkpoint(&mut self, db: &DB) -> Result<()> {
        let wal = db.get_wal();
        let redo_lsn = wal.current_lsn();

        // record all information needed for the checkpoint
        let next_oid = db.get_state_manager().max_allocated_oid();

        // sync all buffers
        let bufmgr = db.get_buffer_manager();
        bufmgr.sync_pages(db)?;

        // write checkpoint log
        let checkpoint_log = WalLogRecord::create_checkpoint_log(redo_lsn, next_oid);
        let (checkpoint, checkpoint_lsn) = wal.append(&checkpoint_log)?;
        wal.flush(Some(checkpoint_lsn))?;

        // update the master record
        let master_record = &mut self.master_record;
        master_record.time = SystemTime::now();
        master_record.last_checkpoint_pos = checkpoint;
        master_record.next_oid = next_oid;
        self.master_record_file.write_master_record(master_record)?;
        Ok(())
    }

    pub fn read_master_record(&mut self) -> Result<&MasterRecord> {
        self.master_record = match self.master_record_file.read_master_record()? {
            Some(record) => record,
            _ => {
                // the master record file is not yet initialized
                let record = MasterRecord::default();
                self.master_record_file.write_master_record(&record)?;
                record
            }
        };
        Ok(&self.master_record)
    }
    pub fn set_db_state(&mut self, state: DBState) -> Result<()> {
        self.master_record.db_state = state;
        self.master_record.time = SystemTime::now();
        self.master_record_file
            .write_master_record(&self.master_record)
    }
}

#[cfg(test)]
mod tests {
    use crate::{catalog::Schema, test_util::get_temp_db};

    #[test]
    fn can_create_checkpoint() {
        let (db, db_dir) = get_temp_db();
        let txn = db.start_transaction().unwrap();
        let heap = db.create_table(0, 0, Schema::new()).unwrap();

        let data: &[u8] = &[1u8; 100];
        heap.insert_tuple(&db, &txn, data).unwrap();

        db.commit_transaction(txn).unwrap();

        assert!(db.create_checkpoint().is_ok());

        db_dir.close().unwrap();
    }
}
