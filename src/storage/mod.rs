pub mod consts;

mod buffer_manager;
mod page_cache;
mod storage_manager;
mod table;

use crate::{wal::LogPointer, Error, Relation, Result, OID};

use std::{
    fmt,
    ops::Deref,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use self::consts::PAGE_SIZE;

pub use self::{
    buffer_manager::BufferManager,
    storage_manager::{ForkType, StorageHandle, StorageManager},
    table::{ScanDirection, Table, TablePtr, TableScanIterator, Tuple, TuplePtr},
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

pub type PageReadGuard<'a> = RwLockReadGuard<'a, Page>;
pub type PageWriteGuard<'a> = RwLockWriteGuard<'a, Page>;

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
        f(&*guard)
    }

    pub fn with_write<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Page) -> Result<R>,
    {
        let mut guard = self.0.write().unwrap();
        f(&mut *guard)
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
            self.with_write(|page| Ok(if f(&page) { Some(page.pin()) } else { None }))?;
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

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemPointer {
    pub page_num: usize,
    pub offset: usize,
}

impl ItemPointer {
    pub fn new(page_num: usize, offset: usize) -> Self {
        Self { page_num, offset }
    }

    pub fn next(&self) -> Self {
        Self {
            page_num: self.page_num,
            offset: self.offset + 1,
        }
    }

    pub fn prev(&self) -> Option<Self> {
        if self.offset <= 1 {
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

const P_LOWER: usize = 0;
const P_UPPER: usize = P_LOWER + 2;
const P_POINTERS: usize = P_UPPER + 2;

#[derive(Debug, Clone, Copy)]
pub struct LinePointer {
    off: u16,
    len: u16,
}

const LINE_POINTER_SIZE: usize = 4;

/// Item-based interface for pages
///
/// The items in the page are indexed with an offset number which starts from 1.
pub trait ItemPageReader {
    fn get_item_page_payload(&self) -> &[u8];

    fn get_lower(&self) -> u16 {
        let buf = self.get_item_page_payload();
        (&buf[P_LOWER..]).read_u16::<LittleEndian>().unwrap()
    }

    fn get_upper(&self) -> u16 {
        let buf = self.get_item_page_payload();
        (&buf[P_UPPER..]).read_u16::<LittleEndian>().unwrap()
    }

    fn is_new(&self) -> bool {
        self.get_upper() == 0
    }

    fn get_free_space(&self) -> usize {
        let size = self.get_upper() as usize - self.get_lower() as usize;

        if size < LINE_POINTER_SIZE {
            0
        } else {
            size - LINE_POINTER_SIZE
        }
    }

    fn num_line_pointers(&self) -> usize {
        let lower = self.get_lower() as usize;

        if lower < P_POINTERS {
            0
        } else {
            (lower - P_POINTERS) / LINE_POINTER_SIZE
        }
    }

    fn get_line_pointer(&self, offset: usize) -> LinePointer {
        let buf = self.get_item_page_payload();
        let off = (&buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE..])
            .read_u16::<LittleEndian>()
            .unwrap();
        let len = (&buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE + 2..])
            .read_u16::<LittleEndian>()
            .unwrap();

        LinePointer { off, len }
    }

    fn get_item(&self, offset: usize) -> &[u8] {
        let buf = self.get_item_page_payload();
        let LinePointer { off, len } = self.get_line_pointer(offset);
        &buf[off as usize..(off + len) as usize]
    }

    fn print_items(&self) {
        for offset in 1..=self.num_line_pointers() {
            let LinePointer { off, len } = self.get_line_pointer(offset);
            println!("{}({}, {}): {:?}", offset, off, len, self.get_item(offset));
        }
    }
}

pub trait ItemPageWriter: ItemPageReader {
    fn get_item_page_payload_mut(&mut self) -> &mut [u8];

    fn set_lower(&mut self, lower: u16) {
        (&mut self.get_item_page_payload_mut()[P_LOWER..])
            .write_u16::<LittleEndian>(lower)
            .unwrap();
    }

    fn set_upper(&mut self, upper: u16) {
        (&mut self.get_item_page_payload_mut()[P_UPPER..])
            .write_u16::<LittleEndian>(upper)
            .unwrap();
    }

    fn init_item_page(&mut self) {
        for i in self.get_item_page_payload_mut().iter_mut() {
            *i = 0;
        }

        let buffer_len = self.get_item_page_payload_mut().len();
        self.set_lower(P_POINTERS as u16);
        self.set_upper(buffer_len as u16);
    }

    fn put_line_pointer(&mut self, offset: usize, lp: LinePointer) {
        let buf = self.get_item_page_payload_mut();
        (&mut buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE..])
            .write_u16::<LittleEndian>(lp.off)
            .unwrap();
        (&mut buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE + 2..])
            .write_u16::<LittleEndian>(lp.len)
            .unwrap();
    }

    fn put_item(&mut self, item: &[u8], target: Option<usize>, overwrite: bool) -> Result<usize> {
        let mut lower = self.get_lower();
        let mut upper = self.get_upper();

        if lower < P_POINTERS as u16
            || lower > upper
            || upper > self.get_item_page_payload_mut().len() as u16
        {
            return Err(Error::DataCorrupted(format!(
                "heap page corrupted: lower = {}, upper = {}",
                lower, upper
            )));
        }

        upper -= item.len() as u16;
        let lp = LinePointer {
            off: upper,
            len: item.len() as u16,
        };

        let limit = self.num_line_pointers() + 1;
        let offset = target.unwrap_or(limit);

        if offset > limit {
            // reject putting items beyond the first unused slot
            // the insert should be in order even if we are redoing the log records
            return Err(Error::InvalidArgument(
                "target offset is too large".to_owned(),
            ));
        }

        let need_shuffle = !overwrite && offset < limit;
        if need_shuffle {
            let src = &mut self.get_item_page_payload_mut()
                [P_POINTERS + (offset - 1) * LINE_POINTER_SIZE..];

            unsafe {
                std::ptr::copy(
                    src.as_ptr(),
                    src.as_mut_ptr().add(LINE_POINTER_SIZE),
                    (limit - offset) * LINE_POINTER_SIZE,
                );
            }
        }

        self.put_line_pointer(offset, lp);
        if offset == limit || need_shuffle {
            lower += LINE_POINTER_SIZE as u16;
        }

        (&mut self.get_item_page_payload_mut()[upper as usize..upper as usize + item.len()])
            .copy_from_slice(item);

        self.set_lower(lower);
        self.set_upper(upper);

        Ok(offset)
    }

    fn set_item(&mut self, offset: usize, item: &[u8]) -> Result<()> {
        let LinePointer { off, len } = self.get_line_pointer(offset);

        if len as usize != item.len() {
            return Err(Error::InvalidArgument(
                "tuple size does not match".to_owned(),
            ));
        }
        (&mut self.get_item_page_payload_mut()[off as usize..off as usize + len as usize])
            .copy_from_slice(item);

        Ok(())
    }
}
