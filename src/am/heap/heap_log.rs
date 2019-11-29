use crate::{
    wal::{LogPointer, LogRecord},
    Result, DB, OID,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct HeapInsertLog<'a> {
    heap_id: OID,
    page_num: usize,
    offset: u16,
    #[serde(with = "serde_bytes")]
    tuple_data: &'a [u8],
}

impl<'a> HeapInsertLog<'a> {
    pub fn apply(self, _db: &DB, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum HeapLogRecord<'a> {
    #[serde(borrow)]
    HeapInsert(HeapInsertLog<'a>),
}

impl<'a> HeapLogRecord<'a> {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        match self {
            HeapLogRecord::HeapInsert(heap_insert_log) => heap_insert_log.apply(db, lsn),
        }
    }

    pub fn create_heap_insert_log(
        heap_id: OID,
        page_num: usize,
        offset: usize,
        tuple_data: &[u8],
    ) -> LogRecord {
        let heap_insert_record = HeapInsertLog {
            heap_id,
            page_num,
            offset: offset as u16,
            tuple_data,
        };
        LogRecord::create_heap_record(HeapLogRecord::HeapInsert(heap_insert_record))
    }
}
