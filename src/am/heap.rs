mod heap_log;
mod heap_page;

use crate::{
    catalog::Schema,
    concurrency::{Snapshot, Transaction, TransactionStatus, XID},
    storage::{
        consts::PAGE_SIZE, BufferManager, DiskPageWriter, ForkType, ItemPointer, PinnedPagePtr,
        RelFileRef, RelationWithStorage, ScanDirection, StorageHandle, Table, TableData,
        TableScanIterator, Tuple,
    },
    Error, Relation, RelationEntry, RelationKind, Result, DB, OID,
};

use self::heap_page::{HeapPageReader, HeapPageView, HeapPageViewMut};

pub(crate) use self::heap_log::HeapLogRecord;

use std::{borrow::Cow, sync::Mutex};

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

fn tuple_size_limit() -> usize {
    PAGE_SIZE
}

bitflags! {
    struct HeapTupleFlags: u32 {
        const MIN_XID_COMMITTED = 0b00000001;
        const MAX_XID_COMMITTED = 0b00000010;
        const MIN_XID_INVALID = 0b00000100;
        const MAX_XID_INVALID = 0b00001000;
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct HeapTuple<'a> {
    #[serde(skip)]
    table_id: OID,
    #[serde(skip)]
    ptr: Option<ItemPointer>,

    flags: u32,
    min_xid: XID,
    max_xid: XID,
    #[serde(borrow)]
    data: Cow<'a, [u8]>,
}

impl<'a> HeapTuple<'a> {
    fn new(table_id: OID, data: &'a [u8]) -> Self {
        Self {
            table_id,
            ptr: None,
            flags: 0,
            min_xid: XID::default(),
            max_xid: XID::default(),
            data: data.into(),
        }
    }

    fn set_pointer(&mut self, ptr: ItemPointer) {
        self.ptr = Some(ptr);
    }

    fn materialize<'b>(&self) -> HeapTuple<'b> {
        HeapTuple {
            table_id: self.table_id,
            ptr: self.ptr,
            flags: 0,
            min_xid: self.min_xid,
            max_xid: self.max_xid,
            data: Cow::from(self.data.to_vec()),
        }
    }

    /// Test if the heap tuple is visible for the given snapshot
    fn is_visible(&self, db: &DB, snapshot: &Snapshot, current_xid: XID) -> Result<(bool, u32)> {
        let flags = HeapTupleFlags::from_bits_truncate(self.flags);
        let mut new_flags = HeapTupleFlags::empty();

        if !flags.contains(HeapTupleFlags::MIN_XID_COMMITTED) {
            if self.min_xid.is_invalid() {
                return Ok((false, 0));
            } else if self.min_xid == current_xid {
                if flags.contains(HeapTupleFlags::MAX_XID_INVALID) {
                    // not deleted
                    return Ok((true, 0));
                }

                if self.max_xid != current_xid {
                    // impossible (delete a tuple inserted by an in-progress transaction)
                    return Ok((false, HeapTupleFlags::MAX_XID_INVALID.bits()));
                }

                // the tuple is deleted by the current transaction
                return Ok((false, 0)); // XXX: determine whether the tuple is deleted before or after the scan
            } else if snapshot.is_xid_in_progress(self.min_xid) {
                // inserted by another in-progress transaction
                return Ok((false, 0));
            }
            // by here, the inserting transaction must be committed or aborted
            else if db
                .get_transaction_manager()
                .get_transaction_status(self.min_xid)?
                == TransactionStatus::Committed
            {
                new_flags |= HeapTupleFlags::MIN_XID_COMMITTED;
            } else {
                // the transaction that inserts the tuple must be aborted
                return Ok((false, HeapTupleFlags::MIN_XID_INVALID.bits()));
            }
        } else {
            // the transaction is marked committed but is in-progress according to the snapshot
            if snapshot.is_xid_in_progress(self.min_xid) {
                return Ok((false, 0));
            }
        }

        // by here, the inserting transaction is committed
        if flags.contains(HeapTupleFlags::MAX_XID_INVALID) {
            // the transaction that deletes the tuple is invalid or aborted
            return Ok((true, new_flags.bits()));
        }

        if !flags.contains(HeapTupleFlags::MAX_XID_COMMITTED) {
            if self.max_xid == current_xid {
                return Ok((false, new_flags.bits())); // XXX: determine whether the tuple is deleted before or after the scan
            }

            if snapshot.is_xid_in_progress(self.max_xid) {
                // the deleting transaction is still in-progress
                return Ok((true, new_flags.bits()));
            }

            if db
                .get_transaction_manager()
                .get_transaction_status(self.max_xid)?
                != TransactionStatus::Committed
            {
                // the deleting transaction is aborted
                return Ok((true, HeapTupleFlags::MAX_XID_INVALID.bits()));
            } else {
                return Ok((false, HeapTupleFlags::MAX_XID_COMMITTED.bits()));
            }
        } else {
            // the deleting transaction is committed but is in-progress in the snapshot
            if snapshot.is_xid_in_progress(self.max_xid) {
                return Ok((true, new_flags.bits()));
            }
        }

        // the deleteing transaction is committed
        return Ok((false, new_flags.bits()));
    }
}

struct BufferHeapTuple<'a> {
    tuple: HeapTuple<'a>,
    bufmgr: Option<&'a BufferManager>,
    page: Option<PinnedPagePtr>,
}

impl<'a> Tuple for BufferHeapTuple<'a> {
    fn get_data(&self) -> &[u8] {
        &self.tuple.data
    }
    fn materialize<'ret>(self: Box<Self>) -> Box<dyn Tuple + 'ret> {
        let tuple = BufferHeapTuple {
            tuple: self.tuple.materialize(),
            bufmgr: None,
            page: None,
        };

        Box::new(tuple)
    }
}

impl<'a> Drop for BufferHeapTuple<'a> {
    fn drop(&mut self) {
        let bufmgr = self.bufmgr.take();
        let page = self.page.take();
        match (bufmgr, page) {
            (Some(bufmgr), Some(page)) => {
                bufmgr.release_page(page).unwrap();
            }
            (None, None) => {}
            _ => unreachable!(),
        }
    }
}

pub struct Heap {
    rel_entry: RelationEntry,
    table_data: TableData,
    shandle: Mutex<Option<StorageHandle>>,
    insert_hint: Mutex<Option<usize>>,
}

impl Heap {
    pub fn new(rel_id: OID, db: OID, schema: Schema) -> Self {
        let rel_entry = RelationEntry::new(rel_id, db, RelationKind::Table);
        let table_data = TableData::new(schema);

        Self {
            rel_entry,
            table_data,
            shandle: Mutex::new(None),
            insert_hint: Mutex::new(None),
        }
    }

    fn prepare_heap_tuple_for_insert<'a>(&self, xid: XID, data: &'a [u8]) -> HeapTuple<'a> {
        let mut htup = HeapTuple::new(self.rel_id(), data).materialize();
        let flags = HeapTupleFlags::MAX_XID_INVALID;
        htup.min_xid = xid;
        htup.flags = flags.bits();
        htup
    }

    fn get_insert_hint(&self) -> Option<usize> {
        let guard = self.insert_hint.lock().unwrap();
        *guard
    }

    fn set_insert_hint(&self, hint: usize) {
        let mut guard = self.insert_hint.lock().unwrap();
        *guard = Some(hint);
    }

    fn with_page_for_tuple<F, R>(&self, db: &DB, tuple_len: usize, f: F) -> Result<R>
    where
        F: Copy + FnOnce(&mut HeapPageViewMut, usize) -> Result<(R, bool)>,
    {
        if tuple_len > tuple_size_limit() {
            return Err(Error::ProgramLimitExceed(format!(
                "tuple size {} exceeds limit {}",
                tuple_len,
                tuple_size_limit()
            )));
        }

        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();
        // try to use the page for the last insert
        let mut target_page_num = self.get_insert_hint();

        while let Some(page_num) = target_page_num {
            let page_ptr = self.with_storage(smgr, |storage| {
                bufmgr.fetch_page(db, storage, ForkType::Main, page_num)
            })?;

            let result = page_ptr.with_write(move |page| {
                let buffer = page.buffer_mut();
                let mut page_view = HeapPageViewMut::new(buffer);
                let mut dirty = page_view.is_new();

                if page_view.is_new() {
                    page_view.init_page();
                }

                let free_space = page_view.get_free_space();
                let result = if free_space >= tuple_len {
                    // enough space, go with this page
                    let (result, modified) = f(&mut page_view, page_num)?;
                    dirty = dirty || modified;

                    Some(result)
                } else {
                    None
                };

                if dirty {
                    page.set_dirty(true);
                }
                Ok(result)
            })?;

            bufmgr.release_page(page_ptr)?;

            match result {
                Some(r) => {
                    // record this page for later inserts
                    self.set_insert_hint(page_num);
                    return Ok(r);
                }
                None => {
                    // try again with an allocated page
                    target_page_num = None;
                }
            }
        }

        // need to extend the heap
        let page_ptr =
            self.with_storage(smgr, |storage| bufmgr.new_page(db, storage, ForkType::Main))?;

        let (result, page_num) = page_ptr.with_write(move |page| {
            let (_, _, page_num) = page.get_fork_and_num();
            let buffer = page.buffer_mut();
            let mut page_view = HeapPageViewMut::new(buffer);

            page_view.init_page();

            let (result, _) = f(&mut page_view, page_num)?;
            page.set_dirty(true);
            Ok((result, page_num))
        })?;

        bufmgr.release_page(page_ptr)?;

        self.set_insert_hint(page_num);

        Ok(result)
    }

    fn get_next_tuple<'a>(
        &'a self,
        db: &DB,
        iterator: &mut HeapScanIterator<'a>,
        dir: ScanDirection,
    ) -> Result<bool> {
        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();

        let mut offset: usize = 0;
        let mut remaining_tuples: usize;
        match dir {
            ScanDirection::Forward => {
                if !iterator.inited {
                    if iterator.heap_pages == 0 {
                        // empty heap, done
                        return Ok(false);
                    }

                    let page_num = iterator.start_page;
                    self.with_storage(smgr, |storage| iterator.fetch_page(db, storage, page_num))?;
                    offset = 0;
                    iterator.inited = true;
                } else {
                    // continue from last tuple
                    let line_ptr = iterator.tuple.ptr.expect("impossible");
                    let next_off = line_ptr.next_offset();
                    offset = next_off.offset;
                }

                match &iterator.cur_page {
                    None => {
                        return Err(Error::InvalidState(
                            "page not present for heap scan iterator".to_owned(),
                        ))
                    }
                    Some(page) => {
                        remaining_tuples = HeapPageView::with_page(page, |page_view| {
                            Ok(page_view.num_line_pointers() - offset)
                        })?;
                    }
                }
            }
            ScanDirection::Backward => {
                if !iterator.inited {
                    if iterator.heap_pages == 0 {
                        // empty heap, done
                        return Ok(false);
                    }

                    let page_num = if iterator.start_page > 0 {
                        iterator.start_page
                    } else {
                        iterator.heap_pages
                    } - 1;

                    self.with_storage(smgr, |storage| iterator.fetch_page(db, storage, page_num))?;
                    remaining_tuples = iterator.num_tuples;
                    offset = if remaining_tuples > 0 {
                        remaining_tuples - 1
                    } else {
                        0
                    };
                    iterator.inited = true;
                } else {
                    // continue from last tuple
                    let line_ptr = iterator.tuple.ptr.expect("impossible");
                    match line_ptr.prev_offset() {
                        None => {
                            remaining_tuples = 0;
                        }
                        Some(prev_off) => {
                            offset = prev_off.offset;
                            remaining_tuples = offset + 1;
                        }
                    }
                }
            }
        }

        loop {
            match &iterator.cur_page {
                Some(page) => {
                    match HeapPageViewMut::with_page(page, |page_view| {
                        let mut remaining_tuples = remaining_tuples;
                        let mut offset = offset;
                        let mut dirty = false;

                        while remaining_tuples > 0 {
                            let line_ptr = page_view.get_line_pointer(offset);

                            let valid = {
                                let item = page_view.get_item(line_ptr);
                                // deserialize the tuple to check visibility
                                let mut htup = match bincode::deserialize::<HeapTuple>(item) {
                                    Ok(htup) => htup,
                                    _ => {
                                        return Err(Error::DataCorrupted(
                                            "cannot deserialize heap tuple".to_owned(),
                                        ));
                                    }
                                };

                                let (valid, new_flags) =
                                    htup.is_visible(db, iterator.snapshot, iterator.xid)?;

                                if new_flags != 0 {
                                    // install the new hint bits to the page
                                    // XXX: If we set the hint bits that some transactions are
                                    //      committed, we should also set the page LSN to the
                                    //      latest commit LSNs of those transactions. This is
                                    //      to make sure that this page is written to disk
                                    //      only after the commit log records are written.
                                    //      Otherwise, the page may contain invalid bits if
                                    //      the transactions are marked committed but the
                                    //      commit log records are not written. (can this really
                                    //      happen?)
                                    htup.flags |= new_flags;
                                    let htup_buf = bincode::serialize(&htup).unwrap();
                                    page_view.set_item(&htup_buf, line_ptr)?;
                                    dirty = true;
                                }

                                valid
                            };

                            if valid {
                                let item = page_view.get_item(line_ptr);
                                let htup_buf = unsafe {
                                    // extend the lifetime of buf to 'a
                                    // this is ok because we keep a pinned page in the scan iterator
                                    // so the page buffer will be valid until the next iteration
                                    std::mem::transmute::<&[u8], &'a [u8]>(item)
                                };

                                let mut htup = match bincode::deserialize::<HeapTuple>(htup_buf) {
                                    Ok(htup) => htup,
                                    _ => {
                                        return Err(Error::DataCorrupted(
                                            "cannot deserialize heap tuple".to_owned(),
                                        ));
                                    }
                                };

                                htup.table_id = self.rel_id();
                                htup.set_pointer(ItemPointer::new(iterator.cur_page_num, offset));

                                return Ok((dirty, Some(htup)));
                            }

                            remaining_tuples -= 1;

                            match dir {
                                ScanDirection::Forward => {
                                    offset += 1;
                                }
                                ScanDirection::Backward => {
                                    offset -= 1;
                                }
                            }
                        }
                        // we've scanned all tuples on the current page, go to the next page
                        Ok((dirty, None))
                    })? {
                        Some(htup) => {
                            iterator.tuple = htup;
                            return Ok(true);
                        }

                        None => {
                            let mut finished;
                            let mut next_page;

                            match dir {
                                // move to the next page
                                ScanDirection::Forward => {
                                    next_page = iterator.cur_page_num;
                                    next_page += 1;

                                    if next_page >= iterator.heap_pages {
                                        next_page = 0;
                                    }

                                    finished = next_page == iterator.start_page;

                                    if let Some(limit) = &mut iterator.max_pages {
                                        if *limit == 0 {
                                            finished = true;
                                        } else {
                                            *limit -= 1;
                                        }
                                    }
                                }
                                ScanDirection::Backward => {
                                    finished = iterator.cur_page_num == iterator.start_page;

                                    if let Some(limit) = &mut iterator.max_pages {
                                        if *limit == 0 {
                                            finished = true;
                                        } else {
                                            *limit -= 1;
                                        }
                                    }

                                    next_page = if iterator.cur_page_num > 0 {
                                        iterator.cur_page_num
                                    } else {
                                        iterator.heap_pages
                                    } - 1;
                                }
                            }

                            if finished {
                                // no more pages
                                let page = iterator.cur_page.take();

                                if let Some(page) = page {
                                    bufmgr.release_page(page)?;
                                }

                                iterator.tuple = HeapTuple::new(self.rel_id(), &[]).materialize();
                                iterator.inited = false;

                                return Ok(false);
                            }

                            self.with_storage(smgr, |storage| {
                                iterator.fetch_page(db, storage, next_page)
                            })?;

                            remaining_tuples = iterator.num_tuples;

                            match dir {
                                ScanDirection::Forward => {
                                    offset = 0;
                                }
                                ScanDirection::Backward => {
                                    offset = remaining_tuples - 1;
                                }
                            }
                        }
                    }
                }
                None => {
                    return Ok(false);
                }
            }
        }
    }
}

impl Relation for Heap {
    fn get_relation_entry(&self) -> &RelationEntry {
        &self.rel_entry
    }
}

pub struct HeapScanIterator<'a> {
    heap: &'a Heap,
    xid: XID,
    snapshot: &'a Snapshot,
    inited: bool,
    tuple: HeapTuple<'a>,
    cur_page: Option<PinnedPagePtr>,
    cur_page_num: usize,
    num_tuples: usize,
    heap_pages: usize,
    start_page: usize,
    max_pages: Option<usize>,
}

impl<'a> HeapScanIterator<'a> {
    fn fetch_page(&mut self, db: &DB, shandle: &StorageHandle, page_num: usize) -> Result<()> {
        let bufmgr = db.get_buffer_manager();

        let old_page = self.cur_page.take();
        if let Some(page) = old_page {
            bufmgr.release_page(page)?;
        }

        let page = bufmgr.fetch_page(db, shandle, ForkType::Main, page_num)?;
        self.cur_page_num = page_num;

        self.num_tuples =
            HeapPageView::with_page(&page, |page_view| Ok(page_view.num_line_pointers()))?;

        self.cur_page = Some(page);

        Ok(())
    }
}

impl<'a> TableScanIterator<'a> for HeapScanIterator<'a> {
    fn next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<Box<dyn Tuple + 'a>>> {
        if self.heap.get_next_tuple(db, self, dir)? {
            let buffer_tuple = BufferHeapTuple {
                tuple: self.tuple.clone(),
                bufmgr: Some(db.get_buffer_manager()),
                page: self.cur_page.clone(),
            };
            Ok(Some(Box::new(buffer_tuple)))
        } else {
            Ok(None)
        }
    }
}

impl Table for Heap {
    fn get_table_data(&self) -> &TableData {
        &self.table_data
    }

    fn insert_tuple(&self, db: &DB, txn: &Transaction, tuple: &[u8]) -> Result<ItemPointer> {
        let htup = self.prepare_heap_tuple_for_insert(txn.xid(), tuple);
        let htup_buf = bincode::serialize(&htup).unwrap();
        let htup_len = htup_buf.len();

        let itemp = self.with_page_for_tuple(db, htup_len, |page_view, page_num| {
            let off = page_view.put_tuple(&htup_buf, None)?;
            // create insert log
            let insert_log = HeapLogRecord::create_heap_insert_log(
                RelFileRef {
                    db: self.rel_db(),
                    rel_id: self.rel_id(),
                },
                ForkType::Main,
                page_num,
                off,
                htup.flags,
                tuple,
            );
            let (_, lsn) = db.get_wal().append(txn.xid(), insert_log)?;
            page_view.set_lsn(lsn);
            Ok((ItemPointer::new(page_num, off), true))
        })?;
        Ok(itemp)
    }

    fn begin_scan<'a>(
        &'a self,
        db: &DB,
        txn: &'a mut Transaction,
    ) -> Result<Box<dyn TableScanIterator<'a> + 'a>> {
        let smgr = db.get_storage_manager();
        let heap_pages = self.get_size_in_page(smgr)?;
        let xid = txn.xid();
        let snapshot = db.get_transaction_manager().get_snapshot(txn)?;
        let heap_it = HeapScanIterator {
            heap: &self,
            xid,
            snapshot,
            inited: false,
            tuple: HeapTuple::new(self.rel_id(), &[]).materialize(),
            cur_page: None,
            cur_page_num: 0,
            num_tuples: 0,
            heap_pages,
            start_page: 0,
            max_pages: None,
        };

        Ok(Box::new(heap_it))
    }
}

impl RelationWithStorage for Heap {
    fn get_storage_handle(&self) -> &Mutex<Option<StorageHandle>> {
        &self.shandle
    }
}

#[cfg(test)]
mod tests {
    use crate::{catalog::Schema, storage::ScanDirection, test_util::get_temp_db};

    #[test]
    fn can_create_heap() {
        let (db, db_dir) = get_temp_db();
        assert!(db.create_table(0, 0, Schema::new()).is_ok());

        let mut rel_path = db_dir.path().to_path_buf();
        rel_path.push("base");
        rel_path.push("0");
        rel_path.push("0_0");

        assert!(rel_path.is_file());
        assert!(db_dir.close().is_ok());
    }

    #[test]
    fn can_insert_and_scan_heap() {
        let (db, db_dir) = get_temp_db();
        let mut txn = db.start_transaction().unwrap();
        let heap = db.create_table(0, 0, Schema::new()).unwrap();

        let data: &[u8] = &[1u8; 100];
        for _ in 0..100 {
            assert!(heap.insert_tuple(&db, &txn, data).is_ok());
        }

        {
            let mut iter = heap.begin_scan(&db, &mut txn).unwrap();

            let mut count = 0;
            while let Some(tuple) = iter.next(&db, ScanDirection::Forward).unwrap() {
                assert_eq!(tuple.get_data(), data);
                count += 1;
            }
            assert_eq!(count, 100);

            let mut count = 0;
            while let Some(tuple) = iter.next(&db, ScanDirection::Backward).unwrap() {
                assert_eq!(tuple.get_data(), data);
                count += 1;
            }
            assert_eq!(count, 100);
        }

        db.commit_transaction(txn).unwrap();

        assert!(db_dir.close().is_ok());
    }
}
