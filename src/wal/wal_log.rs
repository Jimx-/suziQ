use crate::{
    wal::{LogPointer, LogRecord},
    Result, DB,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointLog {
    pub redo_pos: LogPointer,
}

impl CheckpointLog {
    pub fn apply(self, _db: &DB, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WalLogRecord {
    Checkpoint(CheckpointLog),
}

impl WalLogRecord {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        match self {
            WalLogRecord::Checkpoint(checkpoint_log) => checkpoint_log.apply(db, lsn),
        }
    }

    pub fn create_checkpoint_log<'a>(redo_pos: LogPointer) -> LogRecord<'a> {
        let checkpoint_record = CheckpointLog { redo_pos };
        LogRecord::create_wal_record(WalLogRecord::Checkpoint(checkpoint_record))
    }
}
