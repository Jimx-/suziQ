use crate::wal::{LogPointer, LogRecord};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointLog {
    redo_lsn: LogPointer,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WalLogRecord {
    Checkpoint(CheckpointLog),
}

impl WalLogRecord {
    pub fn create_checkpoint_log<'a>(redo_lsn: LogPointer) -> LogRecord<'a> {
        let checkpoint_record = CheckpointLog { redo_lsn };
        LogRecord::create_wal_record(WalLogRecord::Checkpoint(checkpoint_record))
    }
}
