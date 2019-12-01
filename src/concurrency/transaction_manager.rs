use crate::{
    concurrency::{
        IsolationLevel, Snapshot, Transaction, TransactionLogRecord, TransactionStatus,
        TransactionTable, XID,
    },
    wal::LogPointer,
    Error, Result, DB,
};

use std::{
    collections::HashSet,
    fs::DirBuilder,
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};

struct SnapshotData {
    active_xids: HashSet<XID>,
    latest_completed_xid: XID,
}

impl Default for SnapshotData {
    fn default() -> Self {
        Self {
            active_xids: HashSet::new(),
            latest_completed_xid: XID::default(),
        }
    }
}

pub struct TransactionManager {
    next_xid: Mutex<XID>,
    txn_table: Mutex<TransactionTable>,
    snapshot_data: Mutex<SnapshotData>,
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

        let snapshot_data = Default::default();

        let txnmgr = Self {
            next_xid: Mutex::new(XID::default().inc()),
            txn_table: Mutex::new(txn_table),
            snapshot_data: Mutex::new(snapshot_data),
        };

        Ok(txnmgr)
    }

    pub fn init_state(&self) {
        let guard = self.next_xid.lock().unwrap();

        {
            let mut snapshot_guard = self.snapshot_data.lock().unwrap();
            snapshot_guard.latest_completed_xid = (*guard).dec();
        }

        let mut table_guard = self.txn_table.lock().unwrap();
        table_guard.init_state(*guard);
    }

    pub fn start_transaction(&self, db: &DB) -> Result<Transaction> {
        let xid = self.get_next_xid(db)?;

        {
            let mut guard = self.snapshot_data.lock().unwrap();
            guard.active_xids.insert(xid);
        }

        Ok(Transaction::new(xid, IsolationLevel::ReadCommitted))
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

        self.mark_transaction_end(xid);

        Ok(())
    }

    pub fn get_snapshot<'a>(&self, txn: &'a mut Transaction) -> Result<&'a Snapshot> {
        let snapshot = txn.current_snapshot.take();
        match snapshot {
            None => {
                // first call
                let snapshot = self.record_snapshot(txn)?;
                txn.current_snapshot = Some(snapshot);
            }
            Some(snapshot) => {
                if txn.uses_transaction_snapshot() {
                    // for repeatable read, always use the first snapshot
                    txn.current_snapshot = Some(snapshot);
                } else {
                    let snapshot = self.record_snapshot(txn)?;
                    txn.current_snapshot = Some(snapshot);
                }
            }
        };

        match &txn.current_snapshot {
            Some(snapshot) => Ok(snapshot),
            _ => unreachable!(),
        }
    }

    fn record_snapshot(&self, txn: &Transaction) -> Result<Snapshot> {
        let guard = self.snapshot_data.lock().unwrap();

        let max_xid = guard.latest_completed_xid.inc();
        let mut min_xid = max_xid;
        let mut xips = HashSet::new();

        for xid in guard.active_xids.iter().copied() {
            if xid.is_invalid() {
                panic!("invalid XID in active transaction list");
            }

            if xid >= max_xid {
                continue;
            }

            if xid < min_xid {
                min_xid = xid;
            }

            if xid == txn.xid() {
                continue;
            }

            xips.insert(xid);
        }

        let snapshot = Snapshot {
            min_xid,
            max_xid,
            xips,
        };
        Ok(snapshot)
    }

    fn get_next_xid(&self, db: &DB) -> Result<XID> {
        let mut guard = self.next_xid.lock().unwrap();
        let xid = *guard;

        {
            let mut table_guard = self.txn_table.lock().unwrap();
            table_guard.extend(db, xid)?;
        }

        *guard = (*guard).inc();
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

        if xid >= *guard {
            *guard = xid.inc();
        }
    }

    pub fn get_transaction_status(&self, xid: XID) -> Result<TransactionStatus> {
        let mut guard = self.txn_table.lock().unwrap();

        guard.get_transaction_status(xid)
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
                guard.redo_zero_page(zero_page_log.page_num)
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

    fn mark_transaction_end(&self, xid: XID) {
        let mut guard = self.snapshot_data.lock().unwrap();

        guard.active_xids.remove(&xid); // XXX: sanity check

        if guard.latest_completed_xid < xid {
            guard.latest_completed_xid = xid;
        }
    }
}
