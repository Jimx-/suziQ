use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub mod consts;

mod buffer_manager;
mod page_cache;
mod storage_manager;

use crate::*;

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

pub struct Page {
    file_ref: RelFileRef,
    page_num: usize,
    slot: usize,
    _buffer: [u8; PAGE_SIZE],
    pin_count: usize,
    dirty: bool,
}

impl Page {
    pub fn pin(&mut self) -> usize {
        self.pin_count += 1;
        self.pin_count
    }

    pub fn unpin(&mut self) -> usize {
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
}

#[derive(Clone)]
pub struct PagePtr(Arc<RwLock<Page>>);

impl PagePtr {
    pub fn new(file_ref: RelFileRef, page_num: usize, slot: usize) -> Self {
        Self(Arc::new(RwLock::new(Page {
            file_ref: file_ref,
            page_num: page_num,
            slot: slot,
            _buffer: [0u8; PAGE_SIZE],
            pin_count: 0,
            dirty: false,
        })))
    }

    pub fn read(&self) -> LockResult<RwLockReadGuard<Page>> {
        self.0.read()
    }

    pub fn write(&self) -> LockResult<RwLockWriteGuard<Page>> {
        self.0.write()
    }

    pub fn with_read<F, T>(&self, f: F) -> T
    where
        F: Fn(&Page) -> T,
    {
        use std::ops::Deref;
        let guard = self.0.read().unwrap();
        f(guard.deref())
    }

    pub fn with_write<F, T>(&self, f: F) -> T
    where
        F: Fn(&mut Page) -> T,
    {
        use std::ops::DerefMut;
        let mut guard = self.0.write().unwrap();
        f(guard.deref_mut())
    }
}
