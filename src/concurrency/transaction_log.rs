use crate::{
    wal::{LogPointer, LogRecord},
    Result, DB,
};

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TxnCommitLog {
    commit_time: SystemTime,
}

impl TxnCommitLog {
    pub fn apply(self, _db: &DB, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionLogRecord {
    Commit(TxnCommitLog),
}

impl TransactionLogRecord {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        match self {
            TransactionLogRecord::Commit(commit_log) => commit_log.apply(db, lsn),
        }
    }

    pub fn create_transaction_commit_log<'a>(commit_time: SystemTime) -> LogRecord<'a> {
        let txn_commit_record = TxnCommitLog { commit_time };
        LogRecord::create_transaction_record(TransactionLogRecord::Commit(txn_commit_record))
    }
}
