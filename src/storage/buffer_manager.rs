use crate::{
    storage::{page_cache::PageCache, ForkType, PinnedPagePtr, StorageHandle},
    Result, DB,
};

use std::sync::Mutex;

pub struct BufferManager {
    page_cache: Mutex<PageCache>,
}

impl BufferManager {
    pub fn new(cache_capacity: usize) -> Self {
        let page_cache = Mutex::new(PageCache::new(cache_capacity));

        Self { page_cache }
    }

    pub fn new_page(
        &self,
        db: &DB,
        shandle: &StorageHandle,
        fork: ForkType,
    ) -> Result<PinnedPagePtr> {
        self.page_cache
            .lock()
            .unwrap()
            .new_page(db, shandle, shandle.file_ref(), fork)
    }

    pub fn fetch_page(
        &self,
        db: &DB,
        shandle: &StorageHandle,
        fork: ForkType,
        page_num: usize,
    ) -> Result<PinnedPagePtr> {
        self.page_cache
            .lock()
            .unwrap()
            .fetch_page(db, shandle, shandle.file_ref(), fork, page_num)
    }

    pub fn release_page(&self, page_ptr: PinnedPagePtr) -> Result<()> {
        self.page_cache.lock().unwrap().release_page(page_ptr)
    }

    pub fn sync_pages(&self, db: &DB) -> Result<()> {
        let dirty_pages = {
            // get dirty pages with lock on page cache, then release the lock and proceed to write the pages
            let mut guard = self.page_cache.lock().unwrap();
            guard.get_dirty_pages()
        };

        for page_ptr in dirty_pages {
            page_ptr.with_write(|mut page| PageCache::flush_page(db, &mut page))?;
            self.release_page(page_ptr)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::RelFileRef, test_util::get_temp_db};

    #[test]
    fn can_allocate_page() {
        let (db, db_dir) = get_temp_db();
        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();
        let shandle = smgr.open(RelFileRef { db: 0, rel_id: 0 }).unwrap();
        assert!(smgr.create(&shandle, ForkType::Main, false).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(0)
        );

        assert!(bufmgr.new_page(&db, &shandle, ForkType::Main).is_ok());
        assert_eq!(
            smgr.file_size_in_page(&shandle, ForkType::Main).ok(),
            Some(1)
        );

        db_dir.close().unwrap();
    }
}
