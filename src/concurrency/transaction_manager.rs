use crate::{
    concurrency::{compare_xid, is_invalid_xid, Transaction, TransactionLogRecord, XID},
    Result, DB,
};

use std::{cmp::Ordering, sync::Mutex, time::SystemTime};

pub struct TransactionManager {
    next_xid: Mutex<XID>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_xid: Mutex::new(1),
        }
    }

    pub fn start_transaction(&self) -> Result<Transaction> {
        let xid = self.get_next_xid();

        Ok(Transaction::new(xid))
    }

    pub fn commit_transaction(&self, db: &DB, txn: Transaction) -> Result<()> {
        let wal = db.get_wal();
        let commit_time = SystemTime::now();

        // write txn commit log
        let txn_commit_log = TransactionLogRecord::create_transaction_commit_log(commit_time);
        let (_, lsn) = wal.append(txn.xid(), txn_commit_log)?;

        // flush the log
        wal.flush(Some(lsn))?;
        Ok(())
    }

    fn get_next_xid(&self) -> XID {
        let mut guard = self.next_xid.lock().unwrap();
        let xid = *guard;

        loop {
            *guard += 1;

            if !is_invalid_xid(*guard) {
                break;
            }
        }

        xid
    }

    pub fn read_next_id(&self) -> XID {
        let guard = self.next_xid.lock().unwrap();
        *guard
    }

    pub fn set_next_xid(&self, xid: XID) {
        let mut guard = self.next_xid.lock().unwrap();
        *guard = xid;
    }

    pub fn advance_next_xid_past(&self, xid: XID) {
        let mut guard = self.next_xid.lock().unwrap();

        match compare_xid(xid, *guard) {
            Ordering::Greater | Ordering::Equal => {
                let mut xid = xid;
                loop {
                    xid += 1;

                    if !is_invalid_xid(xid) {
                        break;
                    }
                }
                *guard = xid;
            }
            _ => {}
        }
    }
}
