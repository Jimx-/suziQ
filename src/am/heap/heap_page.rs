use crate::{
    storage::{
        consts::PAGE_SIZE, DiskPageReader, DiskPageWriter, ItemPageReader, ItemPageWriter,
        PinnedPagePtr,
    },
    Result,
};

pub struct HeapPageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

impl<'a> HeapPageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&HeapPageView) -> Result<R>,
    {
        page.with_read(|page| {
            let buffer = page.buffer();
            let page_view = HeapPageView::new(buffer);

            f(&page_view)
        })
    }
}

impl<'a> DiskPageReader for HeapPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> ItemPageReader for HeapPageView<'a> {
    fn get_item_page_payload(&self) -> &[u8] {
        self.get_disk_page_payload()
    }
}

pub struct HeapPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> HeapPageViewMut<'a> {
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn init_page(&mut self) {
        self.init_item_page();
    }

    pub fn with_page<F, R>(page: &PinnedPagePtr, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&mut HeapPageViewMut) -> Result<(bool, R)>,
    {
        page.with_write(|page| {
            let buffer = page.buffer_mut();
            let mut page_view = HeapPageViewMut::new(buffer);

            let (dirty, result) = f(&mut page_view)?;
            if dirty {
                page.set_dirty(true);
            }
            Ok(result)
        })
    }
}

impl<'a> DiskPageReader for HeapPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> DiskPageWriter for HeapPageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> ItemPageReader for HeapPageViewMut<'a> {
    fn get_item_page_payload(&self) -> &[u8] {
        self.get_disk_page_payload()
    }
}

impl<'a> ItemPageWriter for HeapPageViewMut<'a> {
    fn get_item_page_payload_mut(&mut self) -> &mut [u8] {
        self.get_disk_page_payload_mut()
    }
}
