use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    storage::{consts::PAGE_SIZE, DiskPageReader, DiskPageWriter, PinnedPagePtr},
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

pub trait HeapPageReader: DiskPageReader {
    fn get_lower(&self) -> u16 {
        let buf = self.get_disk_page_payload();
        (&buf[P_LOWER..]).read_u16::<LittleEndian>().unwrap()
    }

    fn get_upper(&self) -> u16 {
        let buf = self.get_disk_page_payload();
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
        let buf = self.get_disk_page_payload();
        let off = (&buf[P_POINTERS + offset * LINE_POINTER_SIZE..])
            .read_u16::<LittleEndian>()
            .unwrap();
        let len = (&buf[P_POINTERS + offset * LINE_POINTER_SIZE + 2..])
            .read_u16::<LittleEndian>()
            .unwrap();

        LinePointer { off, len }
    }

    fn get_item(&self, line_ptr: LinePointer) -> &[u8] {
        let buf = self.get_disk_page_payload();
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

impl<'a> DiskPageReader for HeapPageView<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        self.buffer
    }
}

impl<'a> HeapPageReader for HeapPageView<'a> {}

pub struct HeapPageViewMut<'a> {
    buffer: &'a mut [u8; PAGE_SIZE],
}

impl<'a> HeapPageViewMut<'a> {
    pub fn new(buffer: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
    }

    pub fn set_lower(&mut self, lower: u16) {
        (&mut self.get_disk_page_payload_mut()[P_LOWER..])
            .write_u16::<LittleEndian>(lower)
            .unwrap();
    }

    pub fn set_upper(&mut self, upper: u16) {
        (&mut self.get_disk_page_payload_mut()[P_UPPER..])
            .write_u16::<LittleEndian>(upper)
            .unwrap();
    }

    pub fn init_page(&mut self) {
        for i in self.get_disk_page_payload_mut().iter_mut() {
            *i = 0;
        }

        let buffer_len = self.get_disk_page_payload_mut().len();
        self.set_lower(P_POINTERS as u16);
        self.set_upper(buffer_len as u16);
    }

    fn put_line_pointer(&mut self, offset: usize, lp: LinePointer) {
        let buf = self.get_disk_page_payload_mut();
        (&mut buf[P_POINTERS + offset * LINE_POINTER_SIZE..])
            .write_u16::<LittleEndian>(lp.off)
            .unwrap();
        (&mut buf[P_POINTERS + offset * LINE_POINTER_SIZE + 2..])
            .write_u16::<LittleEndian>(lp.len)
            .unwrap();
    }

    pub fn put_tuple(&mut self, tuple: &[u8], target: Option<usize>) -> Result<usize> {
        let mut lower = self.get_lower();
        let mut upper = self.get_upper();

        if lower < P_POINTERS as u16
            || lower > upper
            || upper > self.get_disk_page_payload().len() as u16
        {
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

        let limit = self.num_line_pointers();
        let offset = target.unwrap_or(limit);

        if offset > limit {
            // reject putting items beyond the first unused slot
            // the insert should be in order even if we are redoing the log records
            return Err(Error::InvalidArgument(
                "target offset is too large".to_owned(),
            ));
        }

        self.put_line_pointer(offset, lp);
        if offset == limit {
            lower += LINE_POINTER_SIZE as u16;
        }

        (&mut self.get_disk_page_payload_mut()[upper as usize..upper as usize + tuple.len()])
            .copy_from_slice(tuple);

        self.set_lower(lower);
        self.set_upper(upper);

        Ok(offset)
    }

    pub fn set_item(&mut self, tuple: &[u8], lp: LinePointer) -> Result<()> {
        let LinePointer { off, len } = lp;

        if len as usize != tuple.len() {
            return Err(Error::InvalidArgument(
                "tuple size does not match".to_owned(),
            ));
        }
        (&mut self.get_disk_page_payload_mut()[off as usize..off as usize + len as usize])
            .copy_from_slice(tuple);

        Ok(())
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

impl<'a> HeapPageReader for HeapPageViewMut<'a> {}

impl<'a> DiskPageWriter for HeapPageViewMut<'a> {
    fn get_page_buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buffer
    }
}
