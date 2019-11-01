use std::{
    fmt,
    sync::{Arc, RwLock},
};

pub mod consts;

mod buffer_manager;
mod page_cache;
mod storage_manager;

use crate::*;

use std::ops::{Deref, DerefMut};

use self::consts::PAGE_SIZE;

pub use self::{
    buffer_manager::BufferManager,
    storage_manager::{StorageHandle, StorageManager},
};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct RelFileRef {
    pub db: OID,
    pub rel_id: OID,
}

impl fmt::Display for RelFileRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.db, self.rel_id)
    }
}

pub type PageBuffer = [u8; PAGE_SIZE];

pub struct Page {
    file_ref: RelFileRef,
    page_num: usize,
    slot: usize,
    buffer: PageBuffer,
    pin_count: i32,
    dirty: bool,
}

impl Page {
    pub fn pin(&mut self) -> i32 {
        self.pin_count += 1;
        self.pin_count
    }

    pub fn unpin(&mut self) -> i32 {
        self.pin_count -= 1;
        self.pin_count
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn get_file_and_num(&self) -> (RelFileRef, usize) {
        (self.file_ref, self.page_num)
    }

    pub fn set_file_and_num(&mut self, file_ref: RelFileRef, page_num: usize) {
        self.file_ref = file_ref;
        self.page_num = page_num;
    }

    pub fn slot(&self) -> usize {
        self.slot
    }

    pub fn buffer(&self) -> &PageBuffer {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut PageBuffer {
        &mut self.buffer
    }
}

#[derive(Clone)]
pub struct PagePtr(Arc<RwLock<Page>>);

impl Deref for PagePtr {
    type Target = RwLock<Page>;

    fn deref(&self) -> &RwLock<Page> {
        &self.0
    }
}

impl PagePtr {
    pub fn new(file_ref: RelFileRef, page_num: usize, slot: usize) -> Self {
        Self(Arc::new(RwLock::new(Page {
            file_ref,
            page_num,
            slot,
            buffer: [0u8; PAGE_SIZE],
            pin_count: 0,
            dirty: false,
        })))
    }

    pub fn with_read<F, R>(&self, f: F) -> Result<R>
    where
        F: Fn(&Page) -> Result<R>,
    {
        let guard = self.0.read().unwrap();
        f(guard.deref())
    }

    pub fn with_write<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Page) -> Result<R>,
    {
        let mut guard = self.0.write().unwrap();
        f(guard.deref_mut())
    }
}
