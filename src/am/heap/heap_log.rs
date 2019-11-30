use crate::{
    concurrency::XID,
    storage::{DiskPageReader, DiskPageWriter, ForkType, RelFileRef},
    wal::{LogPointer, LogRecord},
    Result, DB,
};

use super::{
    heap_page::{HeapPageReader, HeapPageViewMut},
    HeapTuple,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct HeapInsertLog<'a> {
    file_ref: RelFileRef,
    fork: ForkType,
    page_num: usize,
    offset: u16,
    #[serde(with = "serde_bytes")]
    tuple_data: &'a [u8],
}

impl<'a> HeapInsertLog<'a> {
    pub fn apply(self, db: &DB, xid: XID, lsn: LogPointer) -> Result<()> {
        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();

        // if this log record is written, then the storage must be created and extended to at least page_num pages
        // so we are safe to fetch the page and redo the insert there
        let shandle = smgr.open(self.file_ref)?;
        let page_ptr = bufmgr.fetch_page(db, &shandle, self.fork, self.page_num)?;

        page_ptr.with_write(|page| {
            let buffer = page.buffer_mut();
            let mut page_view = HeapPageViewMut::new(buffer);

            if page_view.get_lsn() >= lsn {
                // already done
                return Ok(());
            }

            if page_view.is_new() {
                page_view.init_page();
            }

            let RelFileRef { rel_id, .. } = self.file_ref;
            let mut htup = HeapTuple::new(rel_id, self.tuple_data).materialize();
            htup.min_xid = xid;
            let htup_buf = bincode::serialize(&htup).unwrap();

            page_view.put_tuple(&htup_buf, Some(self.offset as usize))?;

            page_view.set_lsn(lsn);
            page.set_dirty(true);
            Ok(())
        })?;

        bufmgr.release_page(page_ptr)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum HeapLogRecord<'a> {
    #[serde(borrow)]
    HeapInsert(HeapInsertLog<'a>),
}

impl<'a> HeapLogRecord<'a> {
    pub fn apply(self, db: &DB, xid: XID, lsn: LogPointer) -> Result<()> {
        match self {
            HeapLogRecord::HeapInsert(heap_insert_log) => heap_insert_log.apply(db, xid, lsn),
        }
    }

    pub fn create_heap_insert_log(
        file_ref: RelFileRef,
        fork: ForkType,
        page_num: usize,
        offset: usize,
        tuple_data: &[u8],
    ) -> LogRecord {
        let heap_insert_record = HeapInsertLog {
            file_ref,
            fork,
            page_num,
            offset: offset as u16,
            tuple_data,
        };
        LogRecord::create_heap_record(HeapLogRecord::HeapInsert(heap_insert_record))
    }
}
