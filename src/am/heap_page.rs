use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{storage::consts::PAGE_SIZE, Error, Result};

const P_LOWER: usize = 0;
const P_UPPER: usize = P_LOWER + 2;
const P_POINTERS: usize = P_UPPER + 2;

#[allow(dead_code)]
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
}

#[allow(dead_code)]
pub struct HeapPageView<'a> {
    buffer: &'a [u8; PAGE_SIZE],
}

#[allow(dead_code)]
impl<'a> HeapPageView<'a> {
    pub fn new(buffer: &'a [u8; PAGE_SIZE]) -> Self {
        Self { buffer }
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
        (&mut self.buffer[offset..])
            .write_u16::<LittleEndian>(lp.len)
            .unwrap();
        (&mut self.buffer[offset + 2..])
            .write_u16::<LittleEndian>(lp.len)
            .unwrap();
    }

    pub fn put_tuple(&mut self, tuple: &[u8]) -> Result<(u16, u16)> {
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

        self.put_line_pointer(lower as usize, &lp);
        lower += LINE_POINTER_SIZE as u16;

        (&mut self.buffer[upper as usize..upper as usize + tuple.len()]).copy_from_slice(tuple);

        self.set_lower(lower);
        self.set_upper(upper);

        Ok((lp.off, lp.len))
    }
}

impl<'a> HeapPageReader for HeapPageViewMut<'a> {
    fn get_page_buffer(&self) -> &[u8; PAGE_SIZE] {
        &self.buffer
    }
}
