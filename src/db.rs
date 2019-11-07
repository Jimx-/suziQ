use crate::*;

use std::{option::Option, rc::Rc, sync::Arc};

use crate::{
    am::Heap,
    catalog::{CatalogCache, Schema},
    storage::{BufferManager, RelationWithStorage, StorageManager, TablePtr},
    wal::Wal,
    Result,
};

pub struct DB {
    bufmgr: BufferManager,
    smgr: Rc<StorageManager>,
    catalog_cache: CatalogCache,
    wal: Wal,
}

impl DB {
    pub fn open(config: &DBConfig) -> Result<Self> {
        let smgr = Rc::new(StorageManager::new(config.get_storage_path()));
        let bufmgr = BufferManager::new(smgr.clone(), config.cache_capacity);
        let catalog_cache = CatalogCache::new();
        let wal = Wal::open(config.get_wal_path(), &config.wal_config)?;
        Ok(Self {
            bufmgr,
            smgr,
            catalog_cache,
            wal,
        })
    }

    pub fn get_storage_manager(&self) -> &StorageManager {
        &self.smgr
    }

    pub fn get_buffer_manager(&self) -> &BufferManager {
        &self.bufmgr
    }
    pub fn create_table(&self, db: OID, rel_id: OID, schema: Schema) -> Result<TablePtr> {
        let heap = Arc::new(Heap::new(rel_id, db, schema));
        heap.create_storage(&self.smgr)?;
        self.catalog_cache.add_table(heap.clone());
        Ok(heap)
    }
    fn _open_table(&self, rel_id: OID) -> Option<TablePtr> {
        self.catalog_cache.lookup_table(rel_id)
    }
}
