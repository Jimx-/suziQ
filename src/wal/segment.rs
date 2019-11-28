use std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    ops::Deref,
    path::Path,
};

use byteorder::{LittleEndian, WriteBytesExt};
use crc::crc32;

use super::LogPointer;
use crate::{Error, Result};

const SEGMENT_PAGE_SIZE: usize = 0x2000;
const RECORD_HEADER_SIZE: usize = 7;

#[derive(Clone, Copy)]
enum RecordHeaderType {
    None = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

pub struct Segment {
    segno: u32,
    file: File,
    page: Option<[u8; SEGMENT_PAGE_SIZE]>,
    page_allocated: usize,
    page_flushed: usize,
    page_start: usize,
    capacity: usize,
}

fn check_capacity(capacity: usize) -> Result<()> {
    if capacity > RECORD_HEADER_SIZE {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "segment capacity is smalle than record header size".to_owned(),
        ))
    }
}

impl Segment {
    pub fn create<P: AsRef<Path>>(segno: u32, path: P, capacity: usize) -> Result<Self> {
        check_capacity(capacity)?;

        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&path)?;

        let segment = Segment {
            segno,
            file,
            page: Some([0u8; SEGMENT_PAGE_SIZE]),
            page_allocated: 0,
            page_flushed: 0,
            page_start: 0,
            capacity,
        };

        Ok(segment)
    }

    pub fn open<P: AsRef<Path>>(segno: u32, path: P, capacity: usize, write: bool) -> Result<Self> {
        check_capacity(capacity)?;

        let mut file = OpenOptions::new()
            .read(!write)
            .write(write)
            .create(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        if file_size > capacity {
            return Err(Error::InvalidArgument(format!(
                "invalid segment capacity: file size = {}, capacity = {}",
                file_size, capacity
            )));
        }

        let mut page_start = file_size;

        if write && file_size % SEGMENT_PAGE_SIZE != 0 {
            let padding = SEGMENT_PAGE_SIZE - (file_size % SEGMENT_PAGE_SIZE);
            let zero_bytes = vec![0u8; padding];
            file.write_all(&zero_bytes[..])?;

            page_start += padding;
        }

        let page = if write {
            Some([0u8; SEGMENT_PAGE_SIZE])
        } else {
            None
        };

        let segment = Segment {
            segno,
            file,
            page,
            page_allocated: 0,
            page_flushed: 0,
            page_start,
            capacity,
        };

        Ok(segment)
    }

    pub fn append<T>(&mut self, record: &T) -> Result<Option<LogPointer>>
    where
        T: Deref<Target = [u8]>,
    {
        match &mut self.page {
            None => Err(Error::InvalidState(
                "log segment is not writable".to_owned(),
            )),
            Some(page) => {
                let mut length = record.len();
                let mut offset = 0;

                if !Self::_sufficient_capacity(
                    self.page_allocated,
                    self.page_start,
                    self.capacity,
                    record.len(),
                ) {
                    return Ok(None);
                }

                let mut record_type = RecordHeaderType::None;

                while length > 0 {
                    if SEGMENT_PAGE_SIZE - self.page_allocated <= RECORD_HEADER_SIZE {
                        self.file
                            .write_all(&page[self.page_flushed..SEGMENT_PAGE_SIZE])?;

                        for i in page.iter_mut() {
                            *i = 0;
                        }

                        self.page_allocated = 0;
                        self.page_flushed = 0;
                        self.page_start += SEGMENT_PAGE_SIZE;
                    }

                    let chunk_size = std::cmp::min(
                        length,
                        SEGMENT_PAGE_SIZE - self.page_allocated - RECORD_HEADER_SIZE,
                    );
                    let last_chunk = chunk_size == length;

                    record_type = match record_type {
                        RecordHeaderType::None => {
                            if last_chunk {
                                RecordHeaderType::Full
                            } else {
                                RecordHeaderType::First
                            }
                        }
                        RecordHeaderType::First | RecordHeaderType::Middle => {
                            if last_chunk {
                                RecordHeaderType::Last
                            } else {
                                RecordHeaderType::Middle
                            }
                        }
                        _ => RecordHeaderType::None,
                    };

                    let chunk = &record[offset..offset + chunk_size];
                    let record_start = self.page_allocated;
                    // record type
                    page[self.page_allocated] = record_type as u8;
                    self.page_allocated += 1;
                    // chunk length
                    (&mut page[self.page_allocated..])
                        .write_u16::<LittleEndian>(chunk_size as u16)?;
                    self.page_allocated += 2;
                    // chunk data
                    page[self.page_allocated..self.page_allocated + chunk_size]
                        .copy_from_slice(chunk);
                    self.page_allocated += chunk_size;
                    // checksum
                    let crc = crc32::checksum_ieee(&page[record_start..self.page_allocated]);
                    (&mut page[self.page_allocated..]).write_u32::<LittleEndian>(crc as u32)?;
                    self.page_allocated += 4;

                    length -= chunk_size;
                    offset += chunk_size;
                }

                Ok(Some(self.current_lsn()))
            }
        }
    }

    pub fn flush_page(&mut self, reset: bool) -> Result<()> {
        match &mut self.page {
            None => Err(Error::InvalidState(
                "log segment is not writable".to_owned(),
            )),
            Some(page) => {
                let reset = reset || self.page_allocated + RECORD_HEADER_SIZE >= SEGMENT_PAGE_SIZE;

                if reset {
                    self.page_allocated = SEGMENT_PAGE_SIZE;
                }

                self.file
                    .write_all(&page[self.page_flushed..self.page_allocated])?;
                self.page_flushed = self.page_allocated;

                if reset {
                    for i in page.iter_mut() {
                        *i = 0;
                    }

                    self.page_allocated = 0;
                    self.page_flushed = 0;
                    self.page_start += SEGMENT_PAGE_SIZE;
                }

                Ok(())
            }
        }
    }

    pub fn segment_start(&self) -> LogPointer {
        ((self.segno as usize - 1) * self.capacity) as LogPointer
    }

    pub fn current_lsn(&self) -> LogPointer {
        self.segment_start() + (self.page_start + self.page_allocated) as LogPointer
    }

    pub fn flushed_lsn(&self) -> LogPointer {
        self.segment_start() + (self.page_start + self.page_flushed) as LogPointer
    }
    fn _sufficient_capacity(
        page_allocated: usize,
        page_start: usize,
        capacity: usize,
        record_size: usize,
    ) -> bool {
        let mut remaining = SEGMENT_PAGE_SIZE - page_allocated;
        remaining += (SEGMENT_PAGE_SIZE - RECORD_HEADER_SIZE)
            * ((capacity - page_start) / SEGMENT_PAGE_SIZE - 1);

        remaining >= record_size
    }
    pub fn sufficient_capacity(&self, record_size: usize) -> bool {
        self.page.map_or(false, |_| {
            Self::_sufficient_capacity(
                self.page_allocated,
                self.page_start,
                self.capacity,
                record_size,
            )
        })
    }

    pub fn dirty(&self) -> bool {
        self.page_allocated != self.page_flushed
    }
}
