pub mod consts;

mod buffer_manager;
mod page_cache;
mod storage_manager;
mod table;

use crate::{wal::LogPointer, Relation, Result, OID};

use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};

use self::consts::PAGE_SIZE;

pub use self::{
    buffer_manager::BufferManager,
    storage_manager::{ForkType, StorageHandle, StorageManager},
    table::{ScanDirection, Table, TableData, TablePtr, TableScanIterator, Tuple},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, PartialEq, Eq)]
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
    fork: ForkType,
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

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn get_fork_and_num(&self) -> (RelFileRef, ForkType, usize) {
        (self.file_ref, self.fork, self.page_num)
    }

    pub fn set_fork_and_num(&mut self, file_ref: RelFileRef, fork: ForkType, page_num: usize) {
        self.file_ref = file_ref;
        self.fork = fork;
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
    pub fn new(file_ref: RelFileRef, fork: ForkType, page_num: usize, slot: usize) -> Self {
        Self(Arc::new(RwLock::new(Page {
            file_ref,
            fork,
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

    pub(self) fn pin(self) -> Result<(i32, PinnedPagePtr)> {
        let pin_count = self.with_write(|page| Ok(page.pin()))?;
        Ok((pin_count, PinnedPagePtr(self)))
    }

    pub(self) fn pin_if<F>(self, f: F) -> Result<Option<(i32, PinnedPagePtr)>>
    where
        F: FnOnce(&Page) -> bool,
    {
        let pin_count =
            self.with_write(|page| Ok(if f(page) { Some(page.pin()) } else { None }))?;
        Ok(pin_count.map(|pin_count| (pin_count, PinnedPagePtr(self))))
    }
}

pub trait RelationWithStorage: Relation {
    fn get_storage_handle(&self) -> &std::sync::Mutex<Option<StorageHandle>>;

    fn create_storage(&self, smgr: &StorageManager) -> Result<()> {
        self.with_storage(smgr, |storage| smgr.create(storage, ForkType::Main, false))
    }

    fn with_storage<F, R>(&self, smgr: &StorageManager, f: F) -> Result<R>
    where
        F: FnOnce(&StorageHandle) -> Result<R>,
    {
        let mut guard = self.get_storage_handle().lock().unwrap();

        match &*guard {
            Some(shandle) => f(shandle),
            None => {
                let shandle = smgr.open(RelFileRef {
                    db: self.rel_db(),
                    rel_id: self.rel_id(),
                })?;
                *guard = Some(shandle.clone());
                f(&shandle)
            }
        }
    }

    fn get_size_in_page(&self, smgr: &StorageManager) -> Result<usize> {
        self.with_storage(smgr, |storage| {
            smgr.file_size_in_page(storage, ForkType::Main)
        })
    }
}

pub struct PinnedPagePtr(PagePtr);

impl Deref for PinnedPagePtr {
    type Target = PagePtr;

    fn deref(&self) -> &PagePtr {
        &self.0
    }
}

impl Clone for PinnedPagePtr {
    fn clone(&self) -> Self {
        let page_ptr = self.0.clone();
        let (_, page) = page_ptr.pin().unwrap();
        page
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct ItemPointer {
    pub page_num: usize,
    pub offset: usize,
}

impl ItemPointer {
    pub fn new(page_num: usize, offset: usize) -> Self {
        Self { page_num, offset }
    }

    pub fn next_offset(&self) -> Self {
        Self {
            page_num: self.page_num,
            offset: self.offset + 1,
        }
    }

    pub fn prev_offset(&self) -> Option<Self> {
        if self.offset == 0 {
            None
        } else {
            Some(Self {
                page_num: self.page_num,
                offset: self.offset - 1,
            })
        }
    }
}

const P_LSN: usize = 0;
const P_PAYLOAD: usize = P_LSN + 8;

pub trait DiskPageReader {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE];

    fn get_disk_page_payload(&self) -> &[u8] {
        &self.get_page_buffer()[P_PAYLOAD..]
    }

    fn get_lsn(&self) -> LogPointer {
        let buf = self.get_page_buffer();
        (&buf[P_LSN..]).read_u64::<LittleEndian>().unwrap() as LogPointer
    }
}

pub trait DiskPageWriter {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE];

    fn get_disk_page_payload_mut(&mut self) -> &mut [u8] {
        &mut self.get_page_buffer_mut()[P_PAYLOAD..]
    }

    fn set_lsn(&mut self, lsn: LogPointer) {
        (&mut self.get_page_buffer_mut()[P_LSN..])
            .write_u64::<LittleEndian>(lsn as u64)
            .unwrap();
    }
}

pub struct DiskPageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

impl<'a> DiskPageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&DiskPageView) -> Result<R>,
    {
        page.with_read(|page| {
            let buffer = page.buffer();
            let page_view = DiskPageView::new(buffer);

            f(&page_view)
        })
    }
}

impl<'a> DiskPageReader for DiskPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

pub struct DiskPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> DiskPageReader for DiskPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> DiskPageWriter for DiskPageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}
