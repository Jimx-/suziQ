use crate::*;

use std::rc::Rc;

use self::storage::{BufferManager, StorageManager};

pub struct DB {
    _bufmgr: BufferManager,
    _smgr: Rc<StorageManager>,
}

impl DB {
    pub fn new(config: DBConfig) -> Self {
        let smgr = Rc::new(StorageManager::new(config.get_storage_path()));
        let bufmgr = BufferManager::new(smgr.clone(), config.cache_capacity);
        Self {
            _bufmgr: bufmgr,
            _smgr: smgr,
        }
    }
}
