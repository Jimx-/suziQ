use crate::{
    concurrency::XID,
    wal::{LogPointer, LogRecord},
    Result, DB, OID,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointLog {
    pub redo_pos: LogPointer,
    pub next_oid: OID,
    pub next_xid: XID,
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
        db.get_state_manager().set_next_oid(self.next_oid);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WalLogRecord {
    Checkpoint(CheckpointLog),
    NextOid(NextOidLog),
}

impl WalLogRecord {
    pub fn apply(self, db: &DB, _xid: XID, lsn: LogPointer) -> Result<()> {
        match self {
            WalLogRecord::Checkpoint(checkpoint_log) => checkpoint_log.apply(db, lsn),
            WalLogRecord::NextOid(next_oid_log) => next_oid_log.apply(db, lsn),
        }
    }

    pub fn create_checkpoint_log<'a>(
        redo_pos: LogPointer,
        next_oid: OID,
        next_xid: XID,
    ) -> LogRecord<'a> {
        let checkpoint_record = CheckpointLog {
            redo_pos,
            next_oid,
            next_xid,
        };
        LogRecord::create_wal_record(WalLogRecord::Checkpoint(checkpoint_record))
    }

    pub fn create_next_oid_log<'a>(next_oid: OID) -> LogRecord<'a> {
        let next_oid_record = NextOidLog { next_oid };
        LogRecord::create_wal_record(WalLogRecord::NextOid(next_oid_record))
    }
}
