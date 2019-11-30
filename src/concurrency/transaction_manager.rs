use crate::{
    concurrency::{
        compare_xid, is_invalid_xid, Transaction, TransactionLogRecord, TransactionStatus,
        TransactionTable, XID,
    },
    wal::LogPointer,
    Error, Result, DB,
};

use std::{
    cmp::Ordering,
    fs::DirBuilder,
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};

pub struct TransactionManager {
    next_xid: Mutex<XID>,
    txn_table: Mutex<TransactionTable>,
}

impl TransactionManager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            DirBuilder::new().recursive(true).create(&path)?;
        } else if !path.as_ref().is_dir() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a directory",
                path.as_ref().display()
            )));
        }

        let txn_table = TransactionTable::open(Self::get_txn_table_path(path))?;

        let txnmgr = Self {
            next_xid: Mutex::new(1),
            txn_table: Mutex::new(txn_table),
        };

        Ok(txnmgr)
    }

    pub fn init_state(&self) {
        let guard = self.next_xid.lock().unwrap();
        let mut table_guard = self.txn_table.lock().unwrap();

        table_guard.init_state(*guard);
    }

    pub fn start_transaction(&self, db: &DB) -> Result<Transaction> {
        let xid = self.get_next_xid(db)?;

        Ok(Transaction::new(xid))
    }

    pub fn commit_transaction(&self, db: &DB, txn: Transaction) -> Result<()> {
        let xid = txn.xid();
        let wal = db.get_wal();
        let commit_time = SystemTime::now();

        // write txn commit log
        let txn_commit_log = TransactionLogRecord::create_transaction_commit_log(commit_time);
        let (_, lsn) = wal.append(xid, txn_commit_log)?;

        // flush the log
        wal.flush(Some(lsn))?;

        // update status
        {
            let mut guard = self.txn_table.lock().unwrap();
            guard.set_transaction_status(xid, TransactionStatus::Committed)?;
        }

        Ok(())
    }

    fn get_next_xid(&self, db: &DB) -> Result<XID> {
        let mut guard = self.next_xid.lock().unwrap();
        let xid = *guard;

        {
            let mut table_guard = self.txn_table.lock().unwrap();
            table_guard.extend(db, xid)?;
        }

        loop {
            *guard += 1;

            if !is_invalid_xid(*guard) {
                break;
            }
        }

        Ok(xid)
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

    pub fn checkpoint(&self) -> Result<()> {
        let mut guard = self.txn_table.lock().unwrap();
        guard.checkpoint()
    }

    pub fn redo_txn_log(
        &self,
        db: &DB,
        xid: XID,
        lsn: LogPointer,
        redo: TransactionLogRecord,
    ) -> Result<()> {
        match redo {
            TransactionLogRecord::ZeroPage(zero_page_log) => {
                let mut guard = self.txn_table.lock().unwrap();
                guard.redo_table_zero_page(zero_page_log.page_num)
            }
            TransactionLogRecord::Commit(commit_log) => {
                self.redo_commit(db, xid, lsn, commit_log.commit_time)
            }
        }
    }

    fn redo_commit(
        &self,
        db: &DB,
        xid: XID,
        lsn: LogPointer,
        _commit_time: SystemTime,
    ) -> Result<()> {
        // update status
        {
            let mut guard = self.txn_table.lock().unwrap();
            guard.set_transaction_status(xid, TransactionStatus::Committed)?;
        }

        db.get_wal().flush(Some(lsn))?;
        Ok(())
    }

    fn get_txn_table_path<P: AsRef<Path>>(path: P) -> PathBuf {
        let mut dir = path.as_ref().to_path_buf();
        dir.push("txn_log");
        dir
    }
}
