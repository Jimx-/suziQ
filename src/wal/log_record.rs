use crate::{
    am::{btree::BTreeLogRecord, heap::HeapLogRecord},
    concurrency::{TransactionLogRecord, XID},
    wal::{LogPointer, WalLogRecord},
    Result, DB,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogRecord<'a> {
    #[serde(borrow)]
    Heap(HeapLogRecord<'a>),
    Transaction(TransactionLogRecord),
    Wal(WalLogRecord),
    BTree(BTreeLogRecord<'a>),
}

impl<'a> LogRecord<'a> {
    pub fn apply(self, db: &DB, xid: XID, lsn: LogPointer) -> Result<()> {
        match self {
            LogRecord::Heap(heap_log) => heap_log.apply(db, xid, lsn),
            LogRecord::Transaction(txn_log) => txn_log.apply(db, xid, lsn),
            LogRecord::Wal(wal_log) => wal_log.apply(db, xid, lsn),
            LogRecord::BTree(btree_log) => btree_log.apply(db, xid, lsn),
        }
    }
    pub fn create_heap_record(heap_log_record: HeapLogRecord) -> LogRecord {
        LogRecord::Heap(heap_log_record)
    }

    pub fn create_transaction_record(txn_log_record: TransactionLogRecord) -> LogRecord<'a> {
        LogRecord::Transaction(txn_log_record)
    }

    pub fn create_wal_record(wal_log_record: WalLogRecord) -> LogRecord<'a> {
        LogRecord::Wal(wal_log_record)
    }

    pub fn create_btree_record(btree_log_record: BTreeLogRecord) -> LogRecord {
        LogRecord::BTree(btree_log_record)
    }
}
