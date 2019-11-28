use crate::*;

use std::{
    option::Option,
    sync::{Arc, Mutex},
};

use crate::{
    am::heap::Heap,
    catalog::{CatalogCache, Schema},
    concurrency::{Transaction, TransactionManager},
    storage::{BufferManager, RelationWithStorage, StorageManager, TablePtr},
    wal::{CheckpointManager, Wal},
    Result,
};

pub struct DB {
    bufmgr: BufferManager,
    smgr: StorageManager,
    catalog_cache: CatalogCache,
    txnmgr: TransactionManager,
    wal: Wal,
    ckptmgr: Mutex<CheckpointManager>,
}

impl DB {
    pub fn open(config: &DBConfig) -> Result<Self> {
        let smgr = StorageManager::new(config.get_storage_path());
        let bufmgr = BufferManager::new(config.cache_capacity);
        let catalog_cache = CatalogCache::new();
        let txnmgr = TransactionManager::new();
        let wal = Wal::open(config.get_wal_path(), &config.wal_config)?;
        let ckptmgr = CheckpointManager::open(config.get_master_record_path())?;
        Ok(Self {
            bufmgr,
            smgr,
            catalog_cache,
            txnmgr,
            wal,
            ckptmgr: Mutex::new(ckptmgr),
        })
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

    pub fn create_table(&self, db: OID, rel_id: OID, schema: Schema) -> Result<TablePtr> {
        let heap = Arc::new(Heap::new(rel_id, db, schema));
        heap.create_storage(&self.smgr)?;
        self.catalog_cache.add_table(heap.clone());
        Ok(heap)
    }

    pub fn open_table(&self, rel_id: OID) -> Option<TablePtr> {
        self.catalog_cache.lookup_table(rel_id)
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
}
