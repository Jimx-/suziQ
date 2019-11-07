use crate::{catalog::Schema, storage::ItemPointer, Relation, Result, DB};

use std::sync::Arc;

pub struct TableData {
    schema: Schema,
}

impl TableData {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ScanDirection {
    Forward,
    Backward,
}

pub trait TableScanIterator<'a> {
    fn next(&mut self, db: &DB, dir: ScanDirection) -> Result<bool>;

    fn get_data(&self) -> Option<&[u8]>;
}

pub trait Table: Relation {
    fn get_table_data(&self) -> &TableData;

    fn table_schema(&self) -> &Schema {
        &self.get_table_data().schema
    }

    fn insert_tuple(&self, db: &DB, tuple: &[u8]) -> Result<ItemPointer>;

    fn begin_scan<'a>(&'a self, db: &DB) -> Result<Box<dyn TableScanIterator<'a> + 'a>>;
}

pub type TablePtr = Arc<dyn Table>;
