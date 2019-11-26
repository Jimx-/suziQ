use crate::{
    concurrency::{is_invalid_xid, Transaction, TransactionLogRecord, XID},
    Result, DB,
};

use std::{sync::Mutex, time::SystemTime};

pub struct TransactionManager {
    next_xid: Mutex<XID>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_xid: Mutex::new(0),
        }
    }

    pub fn start_transaction(&self) -> Result<Transaction> {
        let xid = self.get_next_xid();

        Ok(Transaction::new(xid))
    }

    pub fn commit_transaction(&self, db: &DB, _txn: Transaction) -> Result<()> {
        let wal = db.get_wal();
        let commit_time = SystemTime::now();

        // write txn commit log
        let txn_commit_log = TransactionLogRecord::create_transaction_commit_log(commit_time);
        wal.append(&txn_commit_log)?;

        // flush the log
        wal.flush()?;
        Ok(())
    }

    fn get_next_xid(&self) -> XID {
        let mut guard = self.next_xid.lock().unwrap();
        loop {
            *guard += 1;

            if !is_invalid_xid(*guard) {
                break;
            }
        }

        *guard
    }
}
