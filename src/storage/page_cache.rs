use crate::storage::*;
use crate::*;

use lru::LruCache;
use std::{collections::HashMap, vec::Vec};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct PageTag(RelFileRef, usize);

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
        page_num: usize,
    ) -> Result<PagePtr> {
        let tag = PageTag(rel, page_num);

        if self.page_pool.len() < self.cache_capacity {
            let slot = self.page_pool.len();
            let page_ptr = PagePtr::new(rel, page_num, slot);
            self.page_pool.push(page_ptr.clone());
            self.page_hash.insert(tag, slot);

            Ok(page_ptr)
        } else {
            match self.evict(smgr) {
                Some(page_ptr) => {
                    page_ptr.with_write(|page| {
                        page.set_file_and_num(tag.0, tag.1);
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
        rel: RelFileRef,
        page_num: usize,
    ) -> Result<PagePtr> {
        let page_ptr = self.alloc_page(smgr, rel, page_num)?;

        page_ptr.with_write(|page| {
            page.pin();
            Ok(())
        })?;

        Ok(page_ptr)
    }

    pub fn fetch_page(
        &mut self,
        smgr: &StorageManager,
        shandle: &StorageHandle,
        rel: RelFileRef,
        page_num: usize,
    ) -> Result<PagePtr> {
        let tag = PageTag(rel, page_num);

        match self.page_hash.get(&tag) {
            Some(slot) => {
                let page_ptr = self.page_pool[*slot].clone();

                let pin_count = page_ptr.with_write(|page| Ok(page.pin()))?;

                if pin_count == 1 {
                    self.lru.pop(&tag);
                }

                Ok(page_ptr)
            }
            None => {
                let page_ptr = self.alloc_page(smgr, rel, page_num)?;

                page_ptr.with_write(|page| smgr.read(shandle, page_num, page.buffer_mut()))?;

                Ok(page_ptr)
            }
        }
    }

    pub fn _release_page(&mut self, page_ptr: PagePtr) -> Result<()> {
        let (pin_count, file_ref, page_num, slot) = page_ptr.with_write(|page| {
            let pin_count = page.unpin();
            let (file_ref, page_num) = page.get_file_and_num();
            let slot = page.slot();
            Ok((pin_count, file_ref, page_num, slot))
        })?;

        if pin_count == 0 {
            self.lru.put(PageTag(file_ref, page_num), slot);
        }

        Ok(())
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
