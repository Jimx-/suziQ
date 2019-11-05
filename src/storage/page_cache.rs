use crate::storage::*;
use crate::*;

use lru::LruCache;
use std::{collections::HashMap, vec::Vec};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct PageTag(RelFileRef, ForkType, usize);

pub struct PageCache {
    lru: LruCache<PageTag, usize>,
    page_hash: HashMap<PageTag, usize>,
    page_pool: Vec<PagePtr>,
    cache_capacity: usize,
}

impl PageCache {
    pub fn new(cache_capacity: usize) -> Self {
        PageCache {
            lru: LruCache::new(cache_capacity),
            page_hash: HashMap::new(),
            page_pool: Vec::new(),
            cache_capacity,
        }
    }

    /// Create a new page if the cache is not full. Otherwise select a victim and evict the page
    fn alloc_page(
        &mut self,
        smgr: &StorageManager,
        rel: RelFileRef,
        fork: ForkType,
        page_num: usize,
    ) -> Result<PagePtr> {
        let tag = PageTag(rel, fork, page_num);

        if self.page_pool.len() < self.cache_capacity {
            let slot = self.page_pool.len();
            let page_ptr = PagePtr::new(rel, fork, page_num, slot);
            self.page_pool.push(page_ptr.clone());
            self.page_hash.insert(tag, slot);

            Ok(page_ptr)
        } else {
            match self.evict(smgr) {
                Some(page_ptr) => {
                    page_ptr.with_write(|page| {
                        page.set_fork_and_num(tag.0, tag.1, tag.2);
                        self.page_hash.insert(tag, page.slot());
                        Ok(())
                    })?;

                    Ok(page_ptr.clone())
                }
                None => Err(Error::OutOfMemory),
            }
        }
    }

    pub fn new_page(
        &mut self,
        smgr: &StorageManager,
        shandle: &StorageHandle,
        rel: RelFileRef,
        fork: ForkType,
    ) -> Result<PinnedPagePtr> {
        let page_num = smgr.file_size_in_page(shandle, fork)?;
        let temp_buf = [0u8; PAGE_SIZE];
        smgr.write(shandle, fork, page_num, &temp_buf)?;
        let page_ptr = self.alloc_page(smgr, rel, fork, page_num)?;

        let (_, pinned_page) = page_ptr.pin()?;
        Ok(pinned_page)
    }

    pub fn fetch_page(
        &mut self,
        smgr: &StorageManager,
        shandle: &StorageHandle,
        rel: RelFileRef,
        fork: ForkType,
        page_num: usize,
    ) -> Result<PinnedPagePtr> {
        let tag = PageTag(rel, fork, page_num);

        match self.page_hash.get(&tag) {
            Some(slot) => {
                let page_ptr = self.page_pool[*slot].clone();

                let (pin_count, pinned_page) = page_ptr.pin()?;

                if pin_count == 1 {
                    self.lru.pop(&tag);
                }

                Ok(pinned_page)
            }
            None => {
                let page_ptr = self.alloc_page(smgr, rel, fork, page_num)?;
                page_ptr
                    .with_write(|page| smgr.read(shandle, fork, page_num, page.buffer_mut()))?;
                let (_, pinned_page) = page_ptr.pin()?;

                Ok(pinned_page)
            }
        }
    }

    pub fn release_page(&mut self, page_ptr: PinnedPagePtr) -> Result<()> {
        page_ptr.with_write(|page| {
            let pin_count = page.unpin();
            let (file_ref, fork, page_num) = page.get_fork_and_num();
            let slot = page.slot();

            if pin_count == 0 {
                self.lru.put(PageTag(file_ref, fork, page_num), slot);
            }

            Ok(())
        })
    }

    fn evict(&mut self, _smgr: &StorageManager) -> Option<PagePtr> {
        match self.lru.pop_lru() {
            Some((tag, victim)) => {
                let page_ptr = self.page_pool[victim].clone();
                self.page_hash.remove(&tag);
                Some(page_ptr)
            }
            None => None,
        }
    }
}
