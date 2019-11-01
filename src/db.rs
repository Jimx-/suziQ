use crate::*;

use std::{option::Option, rc::Rc};

use self::{
    catalog::CatalogCache,
    storage::{BufferManager, StorageManager, TablePtr},
};

pub struct DB {
    _bufmgr: BufferManager,
    _smgr: Rc<StorageManager>,
    _catalog_cache: CatalogCache,
}

impl DB {
    pub fn new(config: DBConfig) -> Self {
        let smgr = Rc::new(StorageManager::new(config.get_storage_path()));
        let bufmgr = BufferManager::new(smgr.clone(), config.cache_capacity);
        let catalog_cache = CatalogCache::new();
        Self {
            _bufmgr: bufmgr,
            _smgr: smgr,
            _catalog_cache: catalog_cache,
        }
    }

    fn _open_table(&self, rel_id: OID) -> Option<TablePtr> {
        self._catalog_cache.lookup_table(rel_id)
    }
}
