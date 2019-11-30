use crate::{
    concurrency::XID,
    wal::{LogPointer, LogRecord},
    Result, DB,
};

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TxnCommitLog {
    pub(super) commit_time: SystemTime,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TxnTableZeroPageLog {
    pub(super) page_num: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionLogRecord {
    Commit(TxnCommitLog),
    ZeroPage(TxnTableZeroPageLog),
}

impl TransactionLogRecord {
    pub fn apply(self, db: &DB, xid: XID, lsn: LogPointer) -> Result<()> {
        db.get_transaction_manager()
            .redo_txn_log(db, xid, lsn, self)
    }

    pub fn create_transaction_commit_log<'a>(commit_time: SystemTime) -> LogRecord<'a> {
        let txn_commit_record = TxnCommitLog { commit_time };
        LogRecord::create_transaction_record(TransactionLogRecord::Commit(txn_commit_record))
    }

    pub fn create_transaction_zero_page_log<'a>(page_num: usize) -> LogRecord<'a> {
        let txn_zero_page_record = TxnTableZeroPageLog { page_num };
        LogRecord::create_transaction_record(TransactionLogRecord::ZeroPage(txn_zero_page_record))
    }
}
