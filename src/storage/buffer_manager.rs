use crate::storage::page_cache::*;
use crate::storage::*;

use std::{option::Option, rc::Rc, sync::Mutex};

pub struct BufferManager {
    smgr: Rc<StorageManager>,
    page_cache: Mutex<PageCache>,
}

impl BufferManager {
    pub fn new(smgr: Rc<StorageManager>, cache_capacity: usize) -> Self {
        let page_cache = Mutex::new(PageCache::new(cache_capacity));

        Self {
            smgr: smgr,
            page_cache: page_cache,
        }
    }

    pub fn new_page(&self, shandle: &StorageHandle) -> Result<PagePtr> {
        self.fetch_page_common(shandle, shandle.file_ref(), None)
    }

    pub fn fetch_page(
        &self,
        shandle: &StorageHandle,
        page_num: usize,
    ) -> Result<PagePtr> {
        self.fetch_page_common(shandle, shandle.file_ref(), Some(page_num))
    }

    fn fetch_page_common(
        &self,
        shandle: &StorageHandle,
        rel: RelFileRef,
        page_idx: Option<usize>,
    ) -> Result<PagePtr> {
        let page_ptr = match page_idx {
            None => self.page_cache.lock().unwrap().new_page(&self.smgr, rel, 0)?,

            Some(page_num) => self
                .page_cache
                .lock()
                .unwrap()
                .fetch_page(&self.smgr, shandle, rel, page_num)?,
        };

        Ok(page_ptr)
    }
}
