use crate::am::HeapLogRecord;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogRecord<'a> {
    #[serde(borrow)]
    Heap(HeapLogRecord<'a>),
}

impl<'a> LogRecord<'a> {
    pub fn create_heap_record(heap_log_record: HeapLogRecord) -> LogRecord {
        LogRecord::Heap(heap_log_record)
    }
}
