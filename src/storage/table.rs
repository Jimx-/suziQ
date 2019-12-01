use crate::{
    catalog::Schema, concurrency::Transaction, storage::ItemPointer, Relation, Result, DB,
};

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

pub trait Tuple {
    fn get_data(&self) -> &[u8];
    /// Materialize the tuple so that it does not depend on any underlying resource
    fn materialize<'ret>(self: Box<Self>) -> Box<dyn Tuple + 'ret>;
}

pub trait TableScanIterator<'a> {
    fn next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<Box<dyn Tuple + 'a>>>;
}

pub trait Table: Relation {
    fn get_table_data(&self) -> &TableData;

    fn table_schema(&self) -> &Schema {
        &self.get_table_data().schema
    }

    fn insert_tuple(&self, db: &DB, txn: &Transaction, tuple: &[u8]) -> Result<ItemPointer>;

    fn begin_scan<'a>(
        &'a self,
        db: &DB,
        txn: &'a mut Transaction,
    ) -> Result<Box<dyn TableScanIterator<'a> + 'a>>;
}

pub type TablePtr = Arc<dyn Table>;
