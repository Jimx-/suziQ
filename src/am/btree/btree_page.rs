use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    storage::{
        consts::PAGE_SIZE, DiskPageReader, DiskPageWriter, ItemPageReader, ItemPageWriter,
        PinnedPagePtr,
    },
    Result,
};

use bitflags::bitflags;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreePageType {
    Meta,
    Internal,
    Leaf,
}

bitflags! {
    pub struct BTreePageFlags: u32 {
        const IS_LEAF = 0b0000_0001;
        const IS_META = 0b0000_0010;
        const IS_ROOT = 0b0000_0100;
    }
}

const P_PREV: usize = 0;
const P_NEXT: usize = P_PREV + 8;
const P_LEVEL: usize = P_NEXT + 8;
const P_FLAGS: usize = P_LEVEL + 4;
const P_PAYLOAD: usize = P_FLAGS + 4;

const BTREE_META_MAGIC: u32 = 0x4254_7239u32;
const P_META_MAGIC: usize = 0;
const P_META_ROOT: usize = P_META_MAGIC + 4;

pub mod views {
    pub use super::{
        BTreeDataPageReader, BTreeDataPageView, BTreeDataPageViewMut, BTreeMetaPageReader,
        BTreeMetaPageView, BTreeMetaPageViewMut, BTreePageReader, BTreePageView, BTreePageViewMut,
        BTreePageWriter,
    };
}

pub trait BTreePageReader: DiskPageReader {
    fn get_btree_page_payload(&self) -> &[u8] {
        &self.get_disk_page_payload()[P_PAYLOAD..]
    }

    fn get_prev(&self) -> usize {
        let buf = self.get_disk_page_payload();
        (&buf[P_PREV..]).read_u64::<LittleEndian>().unwrap() as usize
    }

    fn get_next(&self) -> usize {
        let buf = self.get_disk_page_payload();
        (&buf[P_NEXT..]).read_u64::<LittleEndian>().unwrap() as usize
    }

    fn get_level(&self) -> u32 {
        let buf = self.get_disk_page_payload();
        (&buf[P_LEVEL..]).read_u32::<LittleEndian>().unwrap()
    }

    fn get_flags(&self) -> BTreePageFlags {
        let buf = self.get_disk_page_payload();
        BTreePageFlags::from_bits_truncate((&buf[P_FLAGS..]).read_u32::<LittleEndian>().unwrap())
    }

    fn page_type(&self) -> BTreePageType {
        let flags = self.get_flags();

        if flags.contains(BTreePageFlags::IS_META) {
            BTreePageType::Meta
        } else if flags.contains(BTreePageFlags::IS_LEAF) {
            BTreePageType::Leaf
        } else {
            BTreePageType::Internal
        }
    }

    fn is_root(&self) -> bool {
        self.get_flags().contains(BTreePageFlags::IS_ROOT)
    }

    fn is_rightmost(&self) -> bool {
        self.get_next() == 0
    }

    #[inline(always)]
    fn high_key_offset(&self) -> usize {
        1
    }

    fn first_key_offset(&self) -> usize {
        self.high_key_offset() + if self.is_rightmost() { 0 } else { 1 }
    }
}

pub trait BTreePageWriter: BTreePageReader + DiskPageWriter {
    fn get_btree_page_payload_mut(&mut self) -> &mut [u8] {
        &mut self.get_disk_page_payload_mut()[P_PAYLOAD..]
    }

    fn init_btree_page(&mut self) {
        for i in self.get_disk_page_payload_mut()[P_PREV..P_PAYLOAD].iter_mut() {
            *i = 0;
        }
    }
    fn set_prev(&mut self, prev: usize) {
        (&mut self.get_disk_page_payload_mut()[P_PREV..])
            .write_u64::<LittleEndian>(prev as u64)
            .unwrap();
    }

    fn set_next(&mut self, next: usize) {
        (&mut self.get_disk_page_payload_mut()[P_NEXT..])
            .write_u64::<LittleEndian>(next as u64)
            .unwrap();
    }

    fn set_level(&mut self, level: u32) {
        (&mut self.get_disk_page_payload_mut()[P_LEVEL..])
            .write_u32::<LittleEndian>(level)
            .unwrap();
    }

    fn set_flags(&mut self, flags: BTreePageFlags) {
        let flags = self.get_flags() | flags;
        (&mut self.get_disk_page_payload_mut()[P_FLAGS..])
            .write_u32::<LittleEndian>(flags.bits())
            .unwrap();
    }

    fn clear_flags(&mut self, flags: BTreePageFlags) {
        let flags = self.get_flags() & !flags;
        (&mut self.get_disk_page_payload_mut()[P_FLAGS..])
            .write_u32::<LittleEndian>(flags.bits())
            .unwrap();
    }

    fn set_page_type(&mut self, typ: BTreePageType) {
        let flags = self.get_flags();
        let type_flag = match typ {
            BTreePageType::Meta => BTreePageFlags::IS_META,
            BTreePageType::Leaf => BTreePageFlags::IS_LEAF,
            BTreePageType::Internal => BTreePageFlags::empty(),
        };
        self.set_flags(flags | type_flag);
    }

    fn set_as_root(&mut self) {
        let flags = self.get_flags();
        self.set_flags(flags | BTreePageFlags::IS_ROOT);
    }
}

pub trait BTreeMetaPageReader: BTreePageReader {
    fn get_magic(&self) -> u32 {
        let buf = self.get_btree_page_payload();
        (&buf[P_META_MAGIC..]).read_u32::<LittleEndian>().unwrap() as u32
    }

    fn get_root(&self) -> usize {
        let buf = self.get_btree_page_payload();
        (&buf[P_META_ROOT..]).read_u64::<LittleEndian>().unwrap() as usize
    }
}

pub trait BTreeDataPageReader: BTreePageReader + ItemPageReader {}

// TODO: cleanup this mess with macros

// =============== BTreePageView ===============
pub struct BTreePageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

impl<'a> BTreePageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    #[allow(dead_code)]
    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&BTreePageView) -> Result<R>,
    {
        page.with_read(|page| {
            let buffer = page.buffer();
            let page_view = BTreePageView::new(buffer);

            f(&page_view)
        })
    }
}

impl<'a> DiskPageReader for BTreePageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageReader for BTreePageView<'a> {}

pub struct BTreePageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> BTreePageViewMut<'a> {
    #[allow(dead_code)]
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }
}

impl<'a> DiskPageReader for BTreePageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageReader for BTreePageViewMut<'a> {}

impl<'a> DiskPageWriter for BTreePageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageWriter for BTreePageViewMut<'a> {}

// =============== BTreeMetaPageView ===============

pub struct BTreeMetaPageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

impl<'a> BTreeMetaPageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    #[allow(dead_code)]
    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&BTreeMetaPageView) -> Result<R>,
    {
        page.with_read(|page| {
            let buffer = page.buffer();
            let page_view = BTreeMetaPageView::new(buffer);

            f(&page_view)
        })
    }
}

impl<'a> DiskPageReader for BTreeMetaPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageReader for BTreeMetaPageView<'a> {}
impl<'a> BTreeMetaPageReader for BTreeMetaPageView<'a> {}

pub struct BTreeMetaPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> BTreeMetaPageViewMut<'a> {
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    fn set_magic(&mut self, magic: u32) {
        (&mut self.get_btree_page_payload_mut()[P_META_MAGIC..])
            .write_u32::<LittleEndian>(magic)
            .unwrap();
    }

    pub fn set_root(&mut self, root: usize) {
        (&mut self.get_btree_page_payload_mut()[P_META_ROOT..])
            .write_u64::<LittleEndian>(root as u64)
            .unwrap();
    }

    pub fn init_page(&mut self, root: usize) {
        self.init_btree_page();
        self.set_magic(BTREE_META_MAGIC);
        self.set_root(root);
        self.set_page_type(BTreePageType::Meta);
    }
}

impl<'a> DiskPageReader for BTreeMetaPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageReader for BTreeMetaPageViewMut<'a> {}
impl<'a> BTreeMetaPageReader for BTreeMetaPageViewMut<'a> {}

impl<'a> DiskPageWriter for BTreeMetaPageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageWriter for BTreeMetaPageViewMut<'a> {}

// =============== BTreeDataPageView ===============

pub struct BTreeDataPageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

impl<'a> BTreeDataPageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    #[allow(dead_code)]
    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&BTreeDataPageView) -> Result<R>,
    {
        page.with_read(|page| {
            let buffer = page.buffer();
            let page_view = BTreeDataPageView::new(buffer);

            f(&page_view)
        })
    }
}

impl<'a> DiskPageReader for BTreeDataPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> ItemPageReader for BTreeDataPageView<'a> {
    fn get_item_page_payload(&self) -> &[u8] {
        self.get_btree_page_payload()
    }
}

impl<'a> BTreePageReader for BTreeDataPageView<'a> {}
impl<'a> BTreeDataPageReader for BTreeDataPageView<'a> {}

pub struct BTreeDataPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> BTreeDataPageViewMut<'a> {
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn init_page(&mut self) {
        self.init_btree_page();
        self.init_item_page();
    }
}

impl<'a> DiskPageReader for BTreeDataPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> BTreePageReader for BTreeDataPageViewMut<'a> {}

impl<'a> DiskPageWriter for BTreeDataPageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> ItemPageReader for BTreeDataPageViewMut<'a> {
    fn get_item_page_payload(&self) -> &[u8] {
        self.get_btree_page_payload()
    }
}

impl<'a> ItemPageWriter for BTreeDataPageViewMut<'a> {
    fn get_item_page_payload_mut(&mut self) -> &mut [u8] {
        self.get_btree_page_payload_mut()
    }
}

impl<'a> BTreePageWriter for BTreeDataPageViewMut<'a> {}
