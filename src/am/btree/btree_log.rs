use crate::{
    concurrency::XID,
    storage::{
        DiskPageReader, DiskPageWriter, ForkType, ItemPageReader, ItemPageWriter, RelFileRef,
    },
    wal::{LogPointer, LogRecord},
    Result, DB,
};

use super::btree_page::{
    BTreeDataPageViewMut, BTreeMetaPageViewMut, BTreePageType, BTreePageWriter,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BTreeInsertLog<'a> {
    file_ref: RelFileRef,
    fork: ForkType,
    page_num: usize,
    offset: u16,
    #[serde(with = "serde_bytes")]
    tuple_data: &'a [u8],
}

impl<'a> BTreeInsertLog<'a> {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();

        let shandle = smgr.open(self.file_ref)?;
        let page_ptr = bufmgr.fetch_page(db, &shandle, self.fork, self.page_num)?;

        page_ptr.with_write(|page| {
            let buffer = page.buffer_mut();
            let mut page_view = BTreeDataPageViewMut::new(buffer);

            if page_view.get_lsn() >= lsn {
                return Ok(());
            }

            if page_view.is_new() {
                page_view.init_page();
            }

            page_view.put_item(self.tuple_data, Some(self.offset as usize), false)?;
            page_view.set_lsn(lsn);
            page.set_dirty(true);
            Ok(())
        })?;

        bufmgr.release_page(page_ptr)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BTreeNewRootLog {
    file_ref: RelFileRef,
    fork: ForkType,
    meta_page_num: usize,
    root_page_num: usize,
    level: u32,
    offset: u16,
    root_tuples: Vec<Vec<u8>>,
}

impl BTreeNewRootLog {
    pub fn apply(self, db: &DB, lsn: LogPointer) -> Result<()> {
        let smgr = db.get_storage_manager();
        let bufmgr = db.get_buffer_manager();

        let shandle = smgr.open(self.file_ref)?;
        let meta_page_ptr = bufmgr.fetch_page(db, &shandle, self.fork, self.meta_page_num)?;
        let root_page_ptr = bufmgr.fetch_page(db, &shandle, self.fork, self.root_page_num)?;

        // restore the root page
        root_page_ptr.with_write(|page| {
            let buffer = page.buffer_mut();
            let mut page_view = BTreeDataPageViewMut::new(buffer);

            if page_view.get_lsn() >= lsn {
                return Ok(());
            }

            if page_view.is_new() {
                page_view.init_page();
            }

            page_view.set_prev(0);
            page_view.set_next(0);
            page_view.set_level(self.level);
            page_view.set_page_type(if self.level == 0 {
                BTreePageType::Leaf
            } else {
                BTreePageType::Internal
            });
            page_view.set_as_root();

            for (i, tuple) in self.root_tuples.iter().enumerate() {
                page_view.put_item(tuple, Some(self.offset as usize + i), false)?;
            }

            page_view.set_lsn(lsn);
            page.set_dirty(true);
            Ok(())
        })?;

        meta_page_ptr.with_write(|page| {
            let buffer = page.buffer_mut();
            let mut page_view = BTreeMetaPageViewMut::new(buffer);

            page_view.set_root(self.root_page_num);

            page_view.set_lsn(lsn);
            page.set_dirty(true);
            Ok(())
        })?;

        bufmgr.release_page(root_page_ptr)?;
        bufmgr.release_page(meta_page_ptr)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum BTreeLogRecord<'a> {
    #[serde(borrow)]
    BTreeInsert(BTreeInsertLog<'a>),
    BTreeNewRoot(BTreeNewRootLog),
}

impl<'a> BTreeLogRecord<'a> {
    pub fn apply(self, db: &DB, _xid: XID, lsn: LogPointer) -> Result<()> {
        match self {
            BTreeLogRecord::BTreeInsert(btree_insert_log) => btree_insert_log.apply(db, lsn),
            BTreeLogRecord::BTreeNewRoot(btree_new_root_log) => btree_new_root_log.apply(db, lsn),
        }
    }

    pub fn create_btree_insert_log(
        file_ref: RelFileRef,
        fork: ForkType,
        page_num: usize,
        offset: usize,
        tuple_data: &[u8],
    ) -> LogRecord {
        let btree_insert_record = BTreeInsertLog {
            file_ref,
            fork,
            page_num,
            offset: offset as u16,
            tuple_data,
        };
        LogRecord::create_btree_record(BTreeLogRecord::BTreeInsert(btree_insert_record))
    }

    pub fn create_btree_new_root_log<'b>(
        file_ref: RelFileRef,
        fork: ForkType,
        meta_page_num: usize,
        root_page_num: usize,
        level: u32,
        offset: usize,
        root_tuples: Vec<Vec<u8>>,
    ) -> LogRecord<'b> {
        let btree_new_log_record = BTreeNewRootLog {
            file_ref,
            fork,
            meta_page_num,
            root_page_num,
            level,
            offset: offset as u16,
            root_tuples,
        };
        LogRecord::create_btree_record(BTreeLogRecord::BTreeNewRoot(btree_new_log_record))
    }
}
