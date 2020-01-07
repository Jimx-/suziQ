use crate::{
    concurrency::{Snapshot, Transaction, XID},
    storage::ItemPointer,
    Relation, Result, DB,
};

use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

pub trait Tuple {
    fn get_data(&self) -> &[u8];
    fn get_item_pointer(&self) -> Option<ItemPointer>;
    /// Materialize the tuple so that it does not depend on any underlying resource
    fn materialize<'ret>(self: Box<Self>) -> Box<dyn Tuple + 'ret>;
}

pub type TuplePtr<'a> = Box<dyn Tuple + 'a>;

pub trait TableScanIterator<'a> {
    fn next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<TuplePtr<'a>>>;
}

pub trait Table: Relation + Sync + Send {
    fn insert_tuple(&self, db: &DB, txn: &Transaction, tuple: &[u8]) -> Result<ItemPointer>;

    fn begin_scan<'a>(
        &'a self,
        db: &DB,
        txn: &'a mut Transaction,
    ) -> Result<Box<dyn TableScanIterator<'a> + 'a>>;

    fn fetch_tuple<'a>(
        &'a self,
        db: &'a DB,
        xid: XID,
        snapshot: &Snapshot,
        item_pointer: ItemPointer,
    ) -> Result<Option<TuplePtr<'a>>>;
}

pub type TablePtr = Arc<dyn Table>;
