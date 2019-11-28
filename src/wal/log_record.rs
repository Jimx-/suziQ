use crate::{am::heap::HeapLogRecord, concurrency::TransactionLogRecord, wal::WalLogRecord};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogRecord<'a> {
    #[serde(borrow)]
    Heap(HeapLogRecord<'a>),
    Transaction(TransactionLogRecord),
    Wal(WalLogRecord),
}

impl<'a> LogRecord<'a> {
    pub fn create_heap_record(heap_log_record: HeapLogRecord) -> LogRecord {
        LogRecord::Heap(heap_log_record)
    }

    pub fn create_transaction_record(txn_log_record: TransactionLogRecord) -> LogRecord<'a> {
        LogRecord::Transaction(txn_log_record)
    }

    pub fn create_wal_record(wal_log_record: WalLogRecord) -> LogRecord<'a> {
        LogRecord::Wal(wal_log_record)
    }
}
