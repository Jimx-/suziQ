use crate::{Error, Result};

use super::{segment::SegmentView, LogPointer};

use std::{fs, path::Path};

pub struct WalReader {
    start_segno: u32,
    start_pos: LogPointer,
    capacity: usize,
    segments: Vec<SegmentView>,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize, start_pos: LogPointer) -> Result<Self> {
        let start_segno = (start_pos as usize / capacity + 1) as u32;
        let mut last_segno = start_segno;
        let mut segments = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if !metadata.is_file() {
                return Err(Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                )));
            }

            let filename = entry.file_name().into_string().map_err(|_| {
                Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                ))
            })?;

            let segno = super::filename_to_segno(&filename)?;

            if segno >= start_segno {
                let segment = SegmentView::open(entry.path(), capacity)?;
                segments.push((segno, segment));

                if segno > last_segno {
                    last_segno = segno;
                }
            }
        }

        if segments.len() != (last_segno - start_segno + 1) as usize {
            return Err(Error::WrongObjectType(
                "missing segments in wal directory".to_owned(),
            ));
        }

        segments.sort_by(|(a, _), (b, _)| a.cmp(b));

        let reader = Self {
            start_segno,
            start_pos,
            capacity,
            segments: segments.into_iter().map(|a| a.1).collect(),
        };
        Ok(reader)
    }

    pub fn iter(&self) -> WalReaderIterator<'_> {
        (&self).into_iter()
    }

    fn pos_to_segment(&self, pos: LogPointer) -> Option<&SegmentView> {
        let segno = (pos as usize / self.capacity + 1) as u32;

        if segno < self.start_segno {
            return None;
        }

        let index = (segno - self.start_segno) as usize;

        if index >= self.segments.len() {
            return None;
        }

        Some(&self.segments[index])
    }

    pub fn read_record(&self, pos: LogPointer) -> Result<Option<(LogPointer, Vec<u8>)>> {
        match self.pos_to_segment(pos) {
            None => Ok(None),
            Some(segment) => {
                let segment_offset = pos as usize % self.capacity;

                match segment.read_record(segment_offset)? {
                    Some((recbuf, len)) => Ok(Some((pos + len as LogPointer, recbuf))),
                    None => {
                        if segment_offset == 0 {
                            Ok(None)
                        } else {
                            let pos = pos + (self.capacity - segment_offset) as LogPointer;
                            self.read_record(pos)
                        }
                    }
                }
            }
        }
    }
}

impl<'a> IntoIterator for &'a WalReader {
    type Item = Result<(LogPointer, Vec<u8>)>; // we could never return a log record unless we use streaming iterators
    type IntoIter = WalReaderIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        WalReaderIterator {
            reader: self,
            next_pos: self.start_pos,
        }
    }
}

pub struct WalReaderIterator<'a> {
    reader: &'a WalReader,
    next_pos: LogPointer,
}

impl<'a> Iterator for WalReaderIterator<'a> {
    type Item = Result<(LogPointer, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_record(self.next_pos) {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some((new_pos, recbuf))) => {
                self.next_pos = new_pos;
                Some(Ok((new_pos, recbuf)))
            }
        }
    }
}
