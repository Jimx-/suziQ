use crate::{
    concurrency::{is_invalid_xid, TransactionLogRecord, XID},
    Error, Result, DB,
};

use std::{
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    path::Path,
};

use lru::LruCache;

const TRANSACTION_PAGE_SIZE: usize = 4096;
const TABLE_CACHE_CAPACITY: usize = 128;

const BITS_PER_TXN: usize = 2;
const TXNS_PER_BYTE: usize = 8 / BITS_PER_TXN;
const TXNS_PER_PAGE: usize = TXNS_PER_BYTE * TRANSACTION_PAGE_SIZE;

#[inline(always)]
fn transaction_to_page_num(xid: XID) -> usize {
    xid as usize / TXNS_PER_PAGE
}

#[inline(always)]
fn transaction_to_page_index(xid: XID) -> usize {
    xid as usize % TXNS_PER_PAGE
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
    Error = 3,
}

impl From<u8> for TransactionStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::InProgress,
            1 => Self::Committed,
            2 => Self::Aborted,
            _ => Self::Error,
        }
    }
}

struct TransactionPage {
    page_num: usize,
    buffer: [u8; TRANSACTION_PAGE_SIZE],
    dirty: bool,
}

impl TransactionPage {
    fn new(page_num: usize) -> Self {
        Self {
            page_num,
            buffer: [0u8; TRANSACTION_PAGE_SIZE],
            dirty: false,
        }
    }
    fn zero_page(&mut self) {
        for b in self.buffer.iter_mut() {
            *b = 0;
        }
    }
}

/// Record the status of transactions
pub struct TransactionTable {
    last_page_num: usize,
    file: File,
    lru: LruCache<usize, TransactionPage>,
}

impl TransactionTable {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let file = if path.exists() {
            if !path.is_file() {
                return Err(Error::WrongObjectType(
                    "transaction table exists but is not a regular file".to_owned(),
                ));
            } else {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(path)?
            }
        } else {
            File::create(path)?
        };

        let metadata = file.metadata()?;

        if metadata.len() as usize % TRANSACTION_PAGE_SIZE != 0 {
            return Err(Error::DataCorrupted(
                "the last page of the transaction table is torn".to_owned(),
            ));
        }

        let last_page_num = metadata.len() as usize / TRANSACTION_PAGE_SIZE;

        let mut table = Self {
            last_page_num,
            file,
            lru: LruCache::new(TABLE_CACHE_CAPACITY),
        };

        if last_page_num == 0 {
            let page = table.new_page(0)?;
            table.write_page(0, &page)?;
            table.put_page(page);
        }

        Ok(table)
    }

    pub fn init_state(&mut self, xid: XID) {
        self.last_page_num = transaction_to_page_num(xid);
    }

    fn alloc_page(&mut self, page_num: usize) -> Result<TransactionPage> {
        if self.lru.len() < TABLE_CACHE_CAPACITY {
            Ok(TransactionPage::new(page_num))
        } else {
            match self.lru.pop_lru() {
                Some((page_num, mut page)) => {
                    if page.dirty {
                        self.write_page(page_num, &page)?;
                        page.dirty = false;
                    }
                    Ok(page)
                }
                _ => unreachable!(),
            }
        }
    }

    fn put_page(&mut self, page: TransactionPage) {
        self.lru.put(page.page_num, page);
    }

    fn read_page(&mut self, page_num: usize, page: &mut TransactionPage) -> Result<()> {
        self.file.seek(SeekFrom::Start(
            page_num as u64 * TABLE_CACHE_CAPACITY as u64,
        ))?;

        match self.file.read_exact(&mut page.buffer) {
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    Err(Error::DataCorrupted(format!(
                        "could not read page {} of the transaction table: unexpected EOF",
                        page_num,
                    )))
                } else {
                    Err(Error::FileAccess(format!(
                        "could not read page {} of the transaction table",
                        page_num,
                    )))
                }
            }
            _ => Ok(()),
        }
    }

    fn write_page(&mut self, page_num: usize, page: &TransactionPage) -> Result<()> {
        // XXX: flush the log?
        self.file.seek(SeekFrom::Start(
            page_num as u64 * TABLE_CACHE_CAPACITY as u64,
        ))?;

        match self.file.write_all(&page.buffer) {
            Err(_) => Err(Error::FileAccess(format!(
                "could not write page {} of the transaction table",
                page_num,
            ))),
            _ => Ok(()),
        }
    }

    fn new_page(&mut self, page_num: usize) -> Result<TransactionPage> {
        let mut page = self.alloc_page(page_num)?;
        page.zero_page();
        self.last_page_num = page_num;

        Ok(page)
    }

    fn fetch_page(&mut self, page_num: usize) -> Result<TransactionPage> {
        if page_num > self.last_page_num {
            return Err(Error::InvalidArgument(
                "page number > last page number".to_owned(),
            ));
        }

        match self.lru.pop(&page_num) {
            Some(page) => Ok(page),
            None => {
                let mut page = self.alloc_page(page_num)?;
                self.read_page(page_num, &mut page)?;
                Ok(page)
            }
        }
    }

    pub fn extend(&mut self, db: &DB, xid: XID) -> Result<()> {
        if is_invalid_xid(xid) || transaction_to_page_index(xid) != 0 {
            Ok(())
        } else {
            let page_num = transaction_to_page_num(xid);
            let page = self.new_page(page_num)?;
            self.put_page(page);

            // write log records only when extending the table
            // transaction statuses will be handled by commit log records
            let zero_page_log = TransactionLogRecord::create_transaction_zero_page_log(page_num);
            db.get_wal().append(0, zero_page_log)?;
            // no flush here because log records with XIDs in the extended page must be after this zero page record
            Ok(())
        }
    }

    pub fn get_transaction_status(&mut self, xid: XID) -> Result<TransactionStatus> {
        let page_num = transaction_to_page_num(xid);
        let page = self.fetch_page(page_num)?;

        let index = transaction_to_page_index(xid);
        let bytepos = index / TXNS_PER_BYTE;
        let byteoff = index % TXNS_PER_BYTE;

        let status =
            TransactionStatus::from((page.buffer[bytepos] >> (byteoff * BITS_PER_TXN)) & 3u8);
        self.put_page(page);

        Ok(status)
    }

    pub fn set_transaction_status(&mut self, xid: XID, status: TransactionStatus) -> Result<()> {
        let page_num = transaction_to_page_num(xid);
        let mut page = self.fetch_page(page_num)?;
        let index = transaction_to_page_index(xid);
        let bytepos = index / TXNS_PER_BYTE;
        let byteoff = index % TXNS_PER_BYTE;

        page.buffer[bytepos] &= !(3u8 << (byteoff * BITS_PER_TXN));
        page.buffer[bytepos] |= (status as u8) << (byteoff * BITS_PER_TXN);
        page.dirty = true;

        self.put_page(page);

        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        for (page_num, page) in self.lru.iter_mut() {
            if !page.dirty {
                continue;
            }

            self.file.seek(SeekFrom::Start(
                *page_num as u64 * TABLE_CACHE_CAPACITY as u64,
            ))?;

            if self.file.write_all(&page.buffer).is_err() {
                return Err(Error::FileAccess(format!(
                    "could not write page {} of the transaction table",
                    page_num,
                )));
            }

            page.dirty = false;
        }
        Ok(())
    }

    pub fn redo_table_zero_page(&mut self, page_num: usize) -> Result<()> {
        let page = self.new_page(page_num)?;
        self.write_page(page_num, &page)?;
        self.put_page(page);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_set_transaction_status() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut table = TransactionTable::open(file.path()).unwrap();

        for i in 0..100 {
            assert!(table
                .set_transaction_status(i as XID, TransactionStatus::from(i % 4))
                .is_ok());
        }

        for i in 0..100 {
            let status = table.get_transaction_status(i as XID).unwrap();
            assert_eq!(TransactionStatus::from(i % 4), status);
        }

        file.close().unwrap();
    }
}
