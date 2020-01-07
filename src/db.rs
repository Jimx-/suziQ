use crate::*;

use std::{
    option::Option,
    sync::{Arc, Mutex},
};

use crate::{
    am::{btree::BTree, heap::Heap, Index, IndexPtr},
    concurrency::{IsolationLevel, StateManager, Transaction, TransactionManager},
    storage::{BufferManager, ForkType, RelationWithStorage, StorageManager, TablePtr},
    wal::{CheckpointManager, DBState, Wal},
    Result,
};

pub struct DB {
    bufmgr: BufferManager,
    smgr: StorageManager,
    txnmgr: TransactionManager,
    wal: Wal,
    ckptmgr: Mutex<CheckpointManager>,
    statemgr: StateManager,
}

impl DB {
    pub fn open(config: &DBConfig) -> Result<Self> {
        let smgr = StorageManager::new(config.get_storage_path());
        let bufmgr = BufferManager::new(config.cache_capacity);
        let txnmgr = TransactionManager::open(config.get_transaction_path())?;
        let wal = Wal::open(config.get_wal_path(), &config.wal_config)?;
        let ckptmgr = CheckpointManager::open(config.get_master_record_path())?;
        let statemgr = StateManager::new();
        let db = Self {
            bufmgr,
            smgr,
            txnmgr,
            wal,
            ckptmgr: Mutex::new(ckptmgr),
            statemgr,
        };

        db.startup()?;

        Ok(db)
    }

    pub fn get_storage_manager(&self) -> &StorageManager {
        &self.smgr
    }

    pub fn get_buffer_manager(&self) -> &BufferManager {
        &self.bufmgr
    }

    pub fn get_transaction_manager(&self) -> &TransactionManager {
        &self.txnmgr
    }

    pub fn get_wal(&self) -> &Wal {
        &self.wal
    }

    pub fn get_state_manager(&self) -> &StateManager {
        &self.statemgr
    }

    pub fn startup(&self) -> Result<()> {
        let mut guard = self.ckptmgr.lock().unwrap();

        let master_record = guard.read_master_record()?;
        let last_checkpoint_pos = master_record.last_checkpoint_pos();
        let checkpoint_log = self.wal.read_checkpoint_record(last_checkpoint_pos)?;
        let redo_pos = match checkpoint_log {
            Some(checkpoint_log) => {
                self.statemgr.set_next_oid(checkpoint_log.next_oid);
                self.txnmgr.set_next_xid(checkpoint_log.next_xid);
                checkpoint_log.redo_pos
            }
            _ => 0,
        };

        let current_lsn = self.wal.current_lsn();
        if current_lsn < redo_pos {
            return Err(Error::DataCorrupted(
                "invalid redo point in checkpoint record".to_owned(),
            ));
        }

        let need_recovery =
            current_lsn > redo_pos || master_record.db_state() != DBState::Shutdowned;

        if need_recovery {
            guard.set_db_state(DBState::InCrashRecovery)?;

            self.wal.replay_logs(self, redo_pos)?;
        }

        self.txnmgr.init_state();
        guard.set_db_state(DBState::InProduction)?;
        Ok(())
    }

    pub fn create_table(&self, db: OID, rel_id: OID) -> Result<TablePtr> {
        let heap = Arc::new(Heap::new(rel_id, db));
        heap.create_storage(&self.smgr)?;
        Ok(heap)
    }

    pub fn open_table(&self, db: OID, rel_id: OID) -> Result<Option<TablePtr>> {
        if self.smgr.exists(db, rel_id, ForkType::Main)? {
            let heap = Arc::new(Heap::new(rel_id, db));
            Ok(Some(heap))
        } else {
            Ok(None)
        }
    }

    pub fn create_index<F>(&self, db: OID, rel_id: OID, key_comparator: F) -> Result<IndexPtr>
    where
        F: Fn(&[u8], &[u8]) -> Result<std::cmp::Ordering> + Sync + Send + 'static,
    {
        let btree = Arc::new(BTree::new(rel_id, db, key_comparator));
        btree.create_storage(&self.smgr)?;
        btree.build_empty(self)?;
        Ok(btree)
    }

    pub fn open_index<F>(&self, db: OID, rel_id: OID, key_comparator: F) -> Result<Option<IndexPtr>>
    where
        F: Fn(&[u8], &[u8]) -> Result<std::cmp::Ordering> + Sync + Send + 'static,
    {
        if self.smgr.exists(db, rel_id, ForkType::Main)? {
            let index = Arc::new(BTree::new(rel_id, db, key_comparator));
            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    pub fn start_transaction(&self, isolation_level: IsolationLevel) -> Result<Transaction> {
        self.txnmgr.start_transaction(self, isolation_level)
    }

    pub fn commit_transaction(&self, txn: Transaction) -> Result<()> {
        self.txnmgr.commit_transaction(self, txn)
    }

    pub fn create_checkpoint(&self) -> Result<()> {
        let mut guard = self.ckptmgr.lock().unwrap();

        guard.create_checkpoint(self)
    }

    pub fn get_next_oid(&self) -> Result<OID> {
        self.statemgr.get_next_oid(self)
    }
}
