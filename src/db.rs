use crate::*;

use std::{
    option::Option,
    sync::{Arc, Mutex},
};

use crate::{
    am::heap::Heap,
    catalog::{CatalogCache, Schema},
    concurrency::{StateManager, Transaction, TransactionManager},
    storage::{BufferManager, ForkType, RelationWithStorage, StorageManager, TablePtr},
    wal::{CheckpointManager, Wal},
    Result,
};

pub struct DB {
    bufmgr: BufferManager,
    smgr: StorageManager,
    catalog_cache: Mutex<CatalogCache>,
    txnmgr: TransactionManager,
    wal: Wal,
    ckptmgr: Mutex<CheckpointManager>,
    statemgr: StateManager,
}

impl DB {
    pub fn open(config: &DBConfig) -> Result<Self> {
        let smgr = StorageManager::new(config.get_storage_path());
        let bufmgr = BufferManager::new(config.cache_capacity);
        let catalog_cache = CatalogCache::new();
        let txnmgr = TransactionManager::new();
        let wal = Wal::open(config.get_wal_path(), &config.wal_config)?;
        let ckptmgr = CheckpointManager::open(config.get_master_record_path())?;
        let statemgr = StateManager::new();
        let db = Self {
            bufmgr,
            smgr,
            catalog_cache: Mutex::new(catalog_cache),
            txnmgr,
            wal,
            ckptmgr: Mutex::new(ckptmgr),
            statemgr,
        };

        db.wal.startup(&db)?;

        Ok(db)
    }

    pub fn get_storage_manager(&self) -> &StorageManager {
        &self.smgr
    }

    pub fn get_buffer_manager(&self) -> &BufferManager {
        &self.bufmgr
    }

    pub fn get_wal(&self) -> &Wal {
        &self.wal
    }

    pub fn with_checkpoint_manager<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut CheckpointManager) -> Result<R>,
    {
        let mut guard = self.ckptmgr.lock().unwrap();
        f(&mut *guard)
    }

    pub fn create_table(&self, db: OID, rel_id: OID, schema: Schema) -> Result<TablePtr> {
        let mut guard = self.catalog_cache.lock().unwrap();
        let heap = Arc::new(Heap::new(rel_id, db, schema));
        heap.create_storage(&self.smgr)?;
        guard.add_table(heap.clone());
        Ok(heap)
    }

    pub fn open_table(&self, db: OID, rel_id: OID) -> Result<Option<TablePtr>> {
        let mut guard = self.catalog_cache.lock().unwrap();
        match guard.lookup_table(rel_id) {
            Some(table) => Ok(Some(table)),
            None => {
                if self.smgr.exists(db, rel_id, ForkType::Main)? {
                    let heap = Arc::new(Heap::new(rel_id, db, Schema::new()));
                    guard.add_table(heap.clone());
                    Ok(Some(heap))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn start_transaction(&self) -> Result<Transaction> {
        self.txnmgr.start_transaction()
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

    pub fn set_next_oid(&self, oid: OID) {
        self.statemgr.set_next_oid(oid);
    }
}
