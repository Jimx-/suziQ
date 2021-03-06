use crate::{
    concurrency::Transaction,
    storage::{ItemPointer, ScanDirection, Table, TuplePtr},
    Relation, Result, DB,
};

use std::sync::Arc;

pub trait IndexScanIterator<'a> {
    fn rescan(
        &mut self,
        db: &'a DB,
        start_key: Option<&[u8]>,
        predicate: IndexScanPredicate<'a>,
    ) -> Result<()>;
    fn next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<TuplePtr<'a>>>;
}

pub trait Index: Relation + Sync + Send {
    fn build_empty(&self, db: &DB) -> Result<()>;

    /// Insert an entry into the index
    ///
    /// We try to make the index general enough and leave the decoding and comparison completely to
    /// the frontend.
    fn insert<'a>(&'a self, db: &DB, key: &[u8], item_pointer: ItemPointer) -> Result<()>;

    fn begin_scan<'a>(
        &'a self,
        db: &DB,
        txn: &'a mut Transaction,
        table: &'a dyn Table,
    ) -> Result<Box<dyn IndexScanIterator<'a> + 'a>>;
}

pub type IndexPtr = Arc<dyn Index>;

pub struct IndexScanPredicate<'a>(Box<dyn Fn(&[u8]) -> Result<bool> + 'a>);

impl<'a> IndexScanPredicate<'a> {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&[u8]) -> Result<bool> + 'a,
    {
        Self(Box::new(f))
    }
}

impl<'a> std::ops::Deref for IndexScanPredicate<'a> {
    type Target = Box<dyn Fn(&[u8]) -> Result<bool> + 'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
