use crate::wal::LogRecord;

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TxnCommitLog {
    commit_time: SystemTime,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionLogRecord {
    Commit(TxnCommitLog),
}

impl TransactionLogRecord {
    pub fn create_transaction_commit_log<'a>(commit_time: SystemTime) -> LogRecord<'a> {
        let txn_commit_record = TxnCommitLog { commit_time };
        LogRecord::create_transaction_record(TransactionLogRecord::Commit(txn_commit_record))
    }
}
