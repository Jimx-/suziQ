use crate::{
    wal::{LogPointer, LogRecord},
    Result, DB, OID,
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
pub struct NextOidLog {
    next_oid: OID,
}

impl NextOidLog {
    pub fn apply(self, db: &DB, _lsn: LogPointer) -> Result<()> {
        db.set_next_oid(self.next_oid);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WalLogRecord {
    Checkpoint(CheckpointLog),
    NextOid(NextOidLog),
}

impl WalLogRecord {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        match self {
            WalLogRecord::Checkpoint(checkpoint_log) => checkpoint_log.apply(db, lsn),
            WalLogRecord::NextOid(next_oid_log) => next_oid_log.apply(db, lsn),
        }
    }

    pub fn create_checkpoint_log<'a>(redo_pos: LogPointer) -> LogRecord<'a> {
        let checkpoint_record = CheckpointLog { redo_pos };
        LogRecord::create_wal_record(WalLogRecord::Checkpoint(checkpoint_record))
    }

    pub fn create_next_oid_log<'a>(next_oid: OID) -> LogRecord<'a> {
        let next_oid_record = NextOidLog { next_oid };
        LogRecord::create_wal_record(WalLogRecord::NextOid(next_oid_record))
    }
}
