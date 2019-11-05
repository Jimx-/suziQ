use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    storage::{consts::PAGE_SIZE, PinnedPagePtr},
    Error, Result,
};

const P_LOWER: usize = 0;
const P_UPPER: usize = P_LOWER + 2;
const P_POINTERS: usize = P_UPPER + 2;

#[derive(Debug, Clone, Copy)]
pub struct LinePointer {
    off: u16,
    len: u16,
}

const LINE_POINTER_SIZE: usize = 4;

pub trait HeapPageReader {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE];

    fn get_lower(&self) -> u16 {
        let buf = self.get_page_buffer();
        (&buf[P_LOWER..]).read_u16::<LittleEndian>().unwrap()
    }

    fn get_upper(&self) -> u16 {
        let buf = self.get_page_buffer();
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
        let buf = self.get_page_buffer();
        let off = (&buf[P_POINTERS + offset * LINE_POINTER_SIZE..])
            .read_u16::<LittleEndian>()
            .unwrap();
        let len = (&buf[P_POINTERS + offset * LINE_POINTER_SIZE + 2..])
            .read_u16::<LittleEndian>()
            .unwrap();

        LinePointer { off, len }
    }

    fn get_item<'a>(&'a self, line_ptr: LinePointer) -> &'a [u8] {
        let buf = self.get_page_buffer();
        let LinePointer { off, len } = line_ptr;
        &buf[off as usize..(off + len) as usize]
    }
}

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

impl<'a> HeapPageReader for HeapPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        &self.buffer
    }
}

pub struct HeapPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> HeapPageViewMut<'a> {
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn set_lower(&mut self, lower: u16) {
        (&mut self.buffer[P_LOWER..])
            .write_u16::<LittleEndian>(lower)
            .unwrap();
    }

    pub fn set_upper(&mut self, upper: u16) {
        (&mut self.buffer[P_UPPER..])
            .write_u16::<LittleEndian>(upper)
            .unwrap();
    }

    pub fn init_page(&mut self) {
        for i in self.buffer.iter_mut() {
            *i = 0;
        }

        self.set_lower(P_POINTERS as u16);
        self.set_upper(PAGE_SIZE as u16);
    }

    fn put_line_pointer(&mut self, offset: usize, lp: &LinePointer) {
        (&mut self.buffer[P_POINTERS + offset * LINE_POINTER_SIZE..])
            .write_u16::<LittleEndian>(lp.off)
            .unwrap();
        (&mut self.buffer[P_POINTERS + offset * LINE_POINTER_SIZE + 2..])
            .write_u16::<LittleEndian>(lp.len)
            .unwrap();
    }

    pub fn put_tuple(&mut self, tuple: &[u8]) -> Result<usize> {
        let mut lower = self.get_lower();
        let mut upper = self.get_upper();

        if lower < P_POINTERS as u16 || lower > upper || upper > PAGE_SIZE as u16 {
            return Err(Error::DataCorrupted(format!(
                "heap page corrupted: lower = {}, upper = {}",
                lower, upper
            )));
        }

        upper -= tuple.len() as u16;
        let lp = LinePointer {
            off: upper,
            len: tuple.len() as u16,
        };

        let offset = self.num_line_pointers();
        self.put_line_pointer(offset, &lp);
        lower += LINE_POINTER_SIZE as u16;

        (&mut self.buffer[upper as usize..upper as usize + tuple.len()]).copy_from_slice(tuple);

        self.set_lower(lower);
        self.set_upper(upper);

        Ok(offset)
    }
}

impl<'a> HeapPageReader for HeapPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        &self.buffer
    }
}
