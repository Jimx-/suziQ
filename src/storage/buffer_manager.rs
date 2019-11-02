use crate::storage::page_cache::*;
use crate::storage::*;

use std::{rc::Rc, sync::Mutex};

pub struct BufferManager {
    smgr: Rc<StorageManager>,
    page_cache: Mutex<PageCache>,
}

impl BufferManager {
    pub fn new(smgr: Rc<StorageManager>, cache_capacity: usize) -> Self {
        let page_cache = Mutex::new(PageCache::new(cache_capacity));

        Self { smgr, page_cache }
    }

    pub fn new_page(&self, shandle: &StorageHandle, fork: ForkType) -> Result<PagePtr> {
        self.page_cache
            .lock()
            .unwrap()
            .new_page(&self.smgr, shandle, shandle.file_ref(), fork)
    }

    pub fn fetch_page(
        &self,
        shandle: &StorageHandle,
        fork: ForkType,
        page_num: usize,
    ) -> Result<PagePtr> {
        self.page_cache.lock().unwrap().fetch_page(
            &self.smgr,
            shandle,
            shandle.file_ref(),
            fork,
            page_num,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_manager::get_temp_smgr;

    fn get_temp_bufmgr(
        cache_capacity: usize,
    ) -> (BufferManager, Rc<StorageManager>, tempfile::TempDir) {
        let (smgr, db_dir) = get_temp_smgr();
        let smgr = Rc::new(smgr);
        let bufmgr = BufferManager::new(smgr.clone(), cache_capacity);
        (bufmgr, smgr, db_dir)
    }

    #[test]
    fn can_allocate_page() {
        let (bufmgr, smgr, db_dir) = get_temp_bufmgr(10);
        let shandle = smgr.open(0, 0).unwrap();
        assert!(smgr.create(&shandle, ForkType::Main, false).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(0)
        );

        assert!(bufmgr.new_page(&shandle, ForkType::Main).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(1)
        );

        assert!(db_dir.close().is_ok());
    }
}
