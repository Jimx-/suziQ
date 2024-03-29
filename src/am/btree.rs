mod btree_log;
mod btree_page;

use crate::{
    am::{
        index::{IndexScanIterator, IndexScanPredicate},
        Index,
    },
    concurrency::{Snapshot, Transaction, XID},
    storage::{
        consts::PAGE_SIZE, DiskPageReader, DiskPageWriter, ForkType, ItemPageReader,
        ItemPageWriter, ItemPointer, PageReadGuard, PageWriteGuard, PinnedPagePtr, RelFileRef,
        RelationWithStorage, ScanDirection, StorageHandle, Table, TuplePtr,
    },
    Error, Relation, RelationEntry, RelationKind, Result, DB, OID,
};

pub(crate) use self::btree_log::BTreeLogRecord;

use self::btree_page::{views::*, BTreePageFlags, BTreePageType};

use ouroboros::self_referencing;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, sync::Mutex};

/// Own a pinned page and a read lock on that page
#[self_referencing]
struct OwningPageReadLock {
    page_ptr: PinnedPagePtr,
    #[borrows(page_ptr)]
    #[covariant]
    page_guard: PageReadGuard<'this>,
}

/// Own a pinned page and a write lock on that page
#[self_referencing]
struct OwningPageWriteLock {
    page_ptr: PinnedPagePtr,
    #[borrows(page_ptr)]
    #[covariant]
    page_guard: PageWriteGuard<'this>,
}

fn owning_page_read_lock(page_ptr: PinnedPagePtr) -> OwningPageReadLock {
    OwningPageReadLockBuilder {
        page_ptr,
        page_guard_builder: |page_ptr| page_ptr.read().unwrap(),
    }
    .build()
}

fn owning_page_write_lock(page_ptr: PinnedPagePtr) -> OwningPageWriteLock {
    OwningPageWriteLockBuilder {
        page_ptr,
        page_guard_builder: |page_ptr| page_ptr.write().unwrap(),
    }
    .build()
}

#[derive(Serialize, Deserialize)]
struct IndexTuple<'a> {
    #[serde(borrow)]
    key: Cow<'a, [u8]>,
    item_pointer: ItemPointer,
}

impl<'a> IndexTuple<'a> {
    fn get_downlink(&self) -> usize {
        self.item_pointer.page_num
    }

    fn set_downlink(&mut self, page_num: usize) {
        self.item_pointer.page_num = page_num;
    }

    fn materialize<'b>(&self) -> IndexTuple<'b> {
        IndexTuple {
            key: Cow::from(self.key.to_vec()),
            item_pointer: self.item_pointer,
        }
    }
}

type TreePath = Vec<ItemPointer>;

const BTREE_META_PAGE_NUM: usize = 0;

pub struct BTree<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    rel_entry: RelationEntry,
    shandle: Mutex<Option<StorageHandle>>,
    key_comparator: KCmp,
}

impl<KCmp> BTree<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    pub fn new(rel_id: OID, db: OID, key_comparator: KCmp) -> Self {
        let rel_entry = RelationEntry::new(rel_id, db, RelationKind::Index);

        Self {
            rel_entry,
            shandle: Mutex::new(None),
            key_comparator,
        }
    }

    // Basically, we need to implement everything twice, once for read and once or write...

    /// Get a page by page number for read.
    fn get_tree_page_read(&self, db: &DB, page_num: Option<usize>) -> Result<OwningPageReadLock> {
        self.with_storage(db.get_storage_manager(), |storage| match page_num {
            Some(page_num) => {
                let page_ptr =
                    db.get_buffer_manager()
                        .fetch_page(db, storage, ForkType::Main, page_num)?;
                Ok(owning_page_read_lock(page_ptr))
            }
            None => {
                let page_ptr = db
                    .get_buffer_manager()
                    .new_page(db, storage, ForkType::Main)?;
                page_ptr.with_write(|page| {
                    let mut page_view = BTreeDataPageViewMut::new(page.buffer_mut());
                    page_view.init_page();
                    Ok(())
                })?;

                Ok(owning_page_read_lock(page_ptr))
            }
        })
    }

    /// Get a page by page number for write.
    fn get_tree_page_write(&self, db: &DB, page_num: Option<usize>) -> Result<OwningPageWriteLock> {
        self.with_storage(db.get_storage_manager(), |storage| match page_num {
            Some(page_num) => {
                let page_ptr =
                    db.get_buffer_manager()
                        .fetch_page(db, storage, ForkType::Main, page_num)?;
                Ok(owning_page_write_lock(page_ptr))
            }
            None => {
                let page_ptr = db
                    .get_buffer_manager()
                    .new_page(db, storage, ForkType::Main)?;
                page_ptr.with_write(|page| {
                    let mut page_view = BTreeDataPageViewMut::new(page.buffer_mut());
                    page_view.init_page();
                    Ok(())
                })?;

                Ok(owning_page_write_lock(page_ptr))
            }
        })
    }

    /// Get the root page for read.
    fn get_root_page_read(&self, db: &DB) -> Result<OwningPageReadLock> {
        let bufmgr = db.get_buffer_manager();

        let meta_page_lock = self.get_tree_page_read(db, Some(BTREE_META_PAGE_NUM))?;
        let meta_page_view = BTreeMetaPageView::new(meta_page_lock.borrow_page_guard().buffer());
        let root_page_num = meta_page_view.get_root();

        if root_page_num == 0 {
            bufmgr.release_page(meta_page_lock.into_heads().page_ptr)?;

            Err(Error::InvalidState("root page not created".to_owned()))
        } else {
            let root_page_num = meta_page_view.get_root();
            bufmgr.release_page(meta_page_lock.into_heads().page_ptr)?;

            self.get_tree_page_read(db, Some(root_page_num))
        }
    }

    /// Get the root page for write. Create it if it does not exist.
    fn get_root_page_write(&self, db: &DB) -> Result<OwningPageWriteLock> {
        let bufmgr = db.get_buffer_manager();

        let mut meta_page_lock = self.get_tree_page_write(db, Some(BTREE_META_PAGE_NUM))?;
        let meta_page_view = BTreeMetaPageView::new(meta_page_lock.borrow_page_guard().buffer());
        let root_page_num = meta_page_view.get_root();

        if root_page_num == 0 {
            // no root page yet, create it
            let mut root_page_lock = self.get_tree_page_write(db, None)?;
            let (_, _, root_page_num) = root_page_lock.borrow_page_guard().get_fork_and_num();

            meta_page_lock.with_page_guard_mut::<Result<()>>(|meta_page_guard| {
                let mut meta_page_view = BTreeMetaPageViewMut::new(meta_page_guard.buffer_mut());

                root_page_lock.with_page_guard_mut::<Result<()>>(|root_page_guard| {
                    // initialize the root page
                    let mut root_page_view =
                        BTreeDataPageViewMut::new(root_page_guard.buffer_mut());
                    root_page_view.set_prev(0);
                    root_page_view.set_next(0);
                    root_page_view.set_level(0);
                    root_page_view.set_page_type(BTreePageType::Leaf);
                    root_page_view.set_as_root();

                    // update metadata
                    meta_page_view.set_root(root_page_num);

                    // WAL stuffs
                    let new_root_log = BTreeLogRecord::create_btree_new_root_log(
                        RelFileRef {
                            db: self.rel_db(),
                            rel_id: self.rel_id(),
                        },
                        ForkType::Main,
                        BTREE_META_PAGE_NUM,
                        root_page_num,
                        0,
                        0,
                        Vec::new(),
                    );
                    let (_, lsn) = db.get_wal().append(XID::default(), new_root_log)?;
                    meta_page_view.set_lsn(lsn);
                    root_page_view.set_lsn(lsn);

                    root_page_guard.set_dirty(true);
                    Ok(())
                })?;

                meta_page_guard.set_dirty(true);
                Ok(())
            })?;

            bufmgr.release_page(meta_page_lock.into_heads().page_ptr)?;

            Ok(root_page_lock)
        } else {
            let root_page_num = meta_page_view.get_root();
            bufmgr.release_page(meta_page_lock.into_heads().page_ptr)?;

            self.get_tree_page_write(db, Some(root_page_num))
        }
    }

    ///  Create a new root node with two children.
    fn new_root(
        &self,
        db: &DB,
        lchild: &OwningPageWriteLock,
        rchild: &OwningPageWriteLock,
    ) -> Result<OwningPageWriteLock> {
        let mut root_page_lock = self.get_tree_page_write(db, None)?;
        let (_, _, root_page_num) = root_page_lock.borrow_page_guard().get_fork_and_num();

        let mut meta_page_lock = self.get_tree_page_write(db, Some(BTREE_META_PAGE_NUM))?;

        meta_page_lock.with_page_guard_mut(|meta_page_guard| {
            let mut meta_page_view = BTreeMetaPageViewMut::new(meta_page_guard.buffer_mut());

            // create tuples for downlinks
            let (_, _, left_page_num) = lchild.borrow_page_guard().get_fork_and_num();
            let (_, _, right_page_num) = rchild.borrow_page_guard().get_fork_and_num();
            let mut left_tuple = IndexTuple {
                key: Cow::from(Vec::new()),
                item_pointer: ItemPointer::default(),
            };
            left_tuple.set_downlink(left_page_num);
            let left_tuple_buf = bincode::serialize(&left_tuple).unwrap();

            let left_page_view = BTreeDataPageView::new(lchild.borrow_page_guard().buffer());
            let high_key_buf = left_page_view.get_item(left_page_view.high_key_offset());
            let high_key = match bincode::deserialize::<IndexTuple>(high_key_buf) {
                Ok(itup) => itup.key,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ));
                }
            };
            let mut right_tuple = IndexTuple {
                key: high_key,
                item_pointer: ItemPointer::default(),
            };
            right_tuple.set_downlink(right_page_num);
            let right_tuple_buf = bincode::serialize(&right_tuple).unwrap();

            let level = left_page_view.get_level() + 1;

            root_page_lock.with_page_guard_mut::<Result<()>>(|root_page_guard| {
                // initialize the root page
                let mut root_page_view = BTreeDataPageViewMut::new(root_page_guard.buffer_mut());
                root_page_view.set_prev(0);
                root_page_view.set_next(0);
                root_page_view.set_level(level);
                root_page_view.set_page_type(BTreePageType::Internal);
                root_page_view.set_as_root();

                // update metadata
                meta_page_view.set_root(root_page_num);

                // insert the page pointers into the new root page
                let left_offset = root_page_view.high_key_offset();
                root_page_view.put_item(&left_tuple_buf, Some(left_offset), false)?;
                root_page_view.put_item(&right_tuple_buf, Some(left_offset + 1), false)?;

                // WAL stuffs
                let new_root_log = BTreeLogRecord::create_btree_new_root_log(
                    RelFileRef {
                        db: self.rel_db(),
                        rel_id: self.rel_id(),
                    },
                    ForkType::Main,
                    BTREE_META_PAGE_NUM,
                    root_page_num,
                    level,
                    left_offset,
                    vec![left_tuple_buf, right_tuple_buf],
                );
                let (_, lsn) = db.get_wal().append(XID::default(), new_root_log)?;
                meta_page_view.set_lsn(lsn);
                root_page_view.set_lsn(lsn);

                root_page_guard.set_dirty(true);

                Ok(())
            })?;

            meta_page_guard.set_dirty(true);

            Ok(())
        })?;

        db.get_buffer_manager()
            .release_page(meta_page_lock.into_heads().page_ptr)?;

        Ok(root_page_lock)
    }

    /// Search for the first leaf page containing the key and return the page with read lock.
    fn search_read(&self, db: &DB, key: &[u8]) -> Result<(OwningPageReadLock, TreePath)> {
        let mut page_lock = self.get_root_page_read(db)?;
        let mut path = Vec::new();

        loop {
            let (_, _, parent_page_num) = page_lock.borrow_page_guard().get_fork_and_num();
            let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());

            if page_view.page_type() == BTreePageType::Leaf {
                break;
            }

            let child_offset =
                self.binary_search_page(&page_view, key, ItemPointer::default(), false)?;
            let child_tuple_buf = page_view.get_item(child_offset);
            let child_tuple = match bincode::deserialize::<IndexTuple>(child_tuple_buf) {
                Ok(itup) => itup,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ))
                }
            };
            let child_page_num = child_tuple.get_downlink();
            let child_page_lock = self.get_tree_page_read(db, Some(child_page_num))?;

            // release the lock on current page after acquiring the lock on the child page
            db.get_buffer_manager()
                .release_page(page_lock.into_heads().page_ptr)?;

            path.push(ItemPointer::new(parent_page_num, child_offset));

            page_lock = child_page_lock;
        }

        Ok((page_lock, path))
    }

    /// Find the first or last leaf page in the tree and return the page with read lock.
    fn get_endpoint(&self, db: &DB, rightmost: bool) -> Result<OwningPageReadLock> {
        let mut page_lock = self.get_root_page_read(db)?;
        let mut path = Vec::new();

        loop {
            let (_, _, parent_page_num) = page_lock.borrow_page_guard().get_fork_and_num();
            let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());

            if page_view.page_type() == BTreePageType::Leaf {
                break;
            }

            let child_offset = if rightmost {
                page_view.num_line_pointers()
            } else {
                page_view.first_key_offset()
            };

            let child_tuple_buf = page_view.get_item(child_offset);
            let child_tuple = match bincode::deserialize::<IndexTuple>(child_tuple_buf) {
                Ok(itup) => itup,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ))
                }
            };
            let child_page_num = child_tuple.get_downlink();
            let child_page_lock = self.get_tree_page_read(db, Some(child_page_num))?;

            // release the lock on current page after acquiring the lock on the child page
            db.get_buffer_manager()
                .release_page(page_lock.into_heads().page_ptr)?;

            path.push(ItemPointer::new(parent_page_num, child_offset));

            page_lock = child_page_lock;
        }

        Ok(page_lock)
    }

    /// Search for the first leaf page containing the key and return the page with write lock.
    fn search_write(&self, db: &DB, key: &[u8]) -> Result<(OwningPageWriteLock, TreePath)> {
        let mut page_lock = self.get_root_page_write(db)?;
        let mut path = Vec::new();

        loop {
            let (_, _, parent_page_num) = page_lock.borrow_page_guard().get_fork_and_num();

            let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());
            if page_view.page_type() == BTreePageType::Leaf {
                break;
            }

            let child_offset =
                self.binary_search_page(&page_view, key, ItemPointer::default(), false)?;
            let child_tuple_buf = page_view.get_item(child_offset);
            let child_tuple = match bincode::deserialize::<IndexTuple>(child_tuple_buf) {
                Ok(itup) => itup,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ));
                }
            };
            let child_page_num = child_tuple.get_downlink();
            let child_page_lock = self.get_tree_page_write(db, Some(child_page_num))?;

            // release the lock on current page after acquiring the lock on the child page
            db.get_buffer_manager()
                .release_page(page_lock.into_heads().page_ptr)?;

            path.push(ItemPointer::new(parent_page_num, child_offset));

            page_lock = child_page_lock;
        }

        Ok((page_lock, path))
    }

    /// Compare the given key with a key on the page.
    fn compare_key<P>(
        &self,
        page_view: &P,
        key: &[u8],
        item_ptr: ItemPointer,
        offset: usize,
    ) -> Result<Ordering>
    where
        P: ItemPageReader + BTreePageReader,
    {
        if page_view.page_type() == BTreePageType::Internal
            && offset == page_view.first_key_offset()
        {
            // the leftmost key in internal nodes is considered to smaller than any other keys in the same node
            return Ok(Ordering::Greater);
        }

        let itup_buf = page_view.get_item(offset);
        let itup = match bincode::deserialize::<IndexTuple>(itup_buf) {
            Ok(itup) => itup,
            _ => {
                return Err(Error::DataCorrupted(
                    "cannot deserialize index tuple".to_owned(),
                ))
            }
        };

        match (self.key_comparator)(key, &itup.key)? {
            Ordering::Equal => Ok(item_ptr.cmp(&itup.item_pointer)),
            ord => Ok(ord),
        }
    }

    /// Do a binary search in the page to find the lower bound to insert the key.
    fn binary_search_page<P>(
        &self,
        page_view: &P,
        key: &[u8],
        item_ptr: ItemPointer,
        next_key: bool,
    ) -> Result<usize>
    where
        P: BTreeDataPageReader,
    {
        let mut low = page_view.first_key_offset();
        let mut high = page_view.num_line_pointers();
        high += 1;

        let cond = if next_key {
            Ordering::Equal
        } else {
            Ordering::Greater
        };

        if low > high {
            // use the first slot if there is no item in the page
            Ok(low)
        } else {
            while low < high {
                let mid = low + (high - low) / 2;

                if self.compare_key(page_view, key, item_ptr, mid)? >= cond {
                    // key > mid
                    low = mid + 1;
                } else {
                    // key <= mid
                    high = mid;
                }
            }

            if page_view.page_type() == BTreePageType::Leaf {
                Ok(low)
            } else {
                Ok(low - 1)
            }
        }
    }
    /// Get the best offset to split the page.
    fn get_split_location<P>(&self, page_view: &P) -> Result<usize>
    where
        P: BTreeDataPageReader,
    {
        Ok(page_view.num_line_pointers() / 2 + 1)
    }

    /// Find the page and offset to insert an index tuple.
    fn get_insert_location(
        &self,
        _db: &DB,
        key: &[u8],
        item_ptr: ItemPointer,
        start_page: OwningPageWriteLock,
    ) -> Result<(OwningPageWriteLock, usize)> {
        let page_lock = start_page;

        let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());
        let offset = self.binary_search_page(&page_view, key, item_ptr, false)?;
        Ok((page_lock, offset))
    }

    /// Split the target page into the left page and the right page, both write-locked and pinned.
    fn split_page(
        &self,
        db: &DB,
        tuple: &[u8],
        offset: usize,
        page: OwningPageWriteLock,
    ) -> Result<(OwningPageWriteLock, OwningPageWriteLock)> {
        let mut page_lock = page;
        let (_, _, page_num) = page_lock.borrow_page_guard().get_fork_and_num();
        let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());

        let first_right = self.get_split_location(&page_view)?;

        // allocate and initialize temp buffer for the left page
        let mut left_page_buffer = *page_lock.borrow_page_guard().buffer();
        let mut left_page_view = BTreeDataPageViewMut::new(&mut left_page_buffer);
        left_page_view.init_page();
        left_page_view.set_flags(page_view.get_flags());
        left_page_view.clear_flags(BTreePageFlags::IS_ROOT);
        left_page_view.set_prev(page_view.get_prev());

        left_page_view.set_lsn(page_view.get_lsn());

        // the high key for the left page is the first key in the right page
        let high_key = if first_right == offset {
            tuple
        } else {
            page_view.get_item(first_right)
        };

        // add the high key to the left page
        let mut left_offset = left_page_view.high_key_offset();
        left_page_view.put_item(high_key, Some(left_offset), false)?;
        left_offset += 1;

        // allocate and initialize the right page
        let mut right_page_lock = self.get_tree_page_write(db, None)?;
        let (_, _, right_page_num) = right_page_lock.borrow_page_guard().get_fork_and_num();
        left_page_view.set_next(right_page_num);

        right_page_lock.with_page_guard_mut::<Result<()>>(|page_guard| {
            let mut right_page_view = BTreeDataPageViewMut::new(page_guard.buffer_mut());
            right_page_view.set_flags(page_view.get_flags());
            right_page_view.clear_flags(BTreePageFlags::IS_ROOT);
            right_page_view.set_prev(page_num);
            right_page_view.set_next(page_view.get_next());

            // add the high key (if any) to the right page
            let mut right_offset = page_view.high_key_offset();
            if !page_view.is_rightmost() {
                let high_key = page_view.get_item(page_view.high_key_offset());
                right_page_view.put_item(high_key, Some(right_offset), false)?;
                right_offset += 1;
            }

            // copy keys into the two pages
            for i in page_view.first_key_offset()..=page_view.num_line_pointers() {
                let key = page_view.get_item(i);

                if i == offset {
                    if offset < first_right {
                        left_page_view.put_item(tuple, Some(left_offset), false)?;
                        left_offset += 1;
                    } else {
                        right_page_view.put_item(tuple, Some(right_offset), false)?;
                        right_offset += 1;
                    }
                }

                if i < first_right {
                    left_page_view.put_item(key, Some(left_offset), false)?;
                    left_offset += 1;
                } else {
                    right_page_view.put_item(key, Some(right_offset), false)?;
                    right_offset += 1;
                }
            }

            // add the new tuple if it is at the end
            if offset > page_view.num_line_pointers() {
                right_page_view.put_item(tuple, Some(right_offset), false)?;
            }

            // fetch the right sibling (if any) to update prev page number
            let mut right_sibling_lock = if page_view.is_rightmost() {
                None
            } else {
                Some(self.get_tree_page_write(db, Some(page_view.get_next()))?)
            };

            page_guard.set_dirty(true);

            // set the prev page number of the right sibling
            if let Some(lock) = &mut right_sibling_lock {
                lock.with_page_guard_mut(|page_guard| {
                    let mut rs_page_view = BTreeDataPageViewMut::new(page_guard.buffer_mut());
                    rs_page_view.set_prev(right_page_num);
                    page_guard.set_dirty(true);
                });
            }

            // TODO: WAL

            // release the right sibling
            if let Some(lock) = right_sibling_lock {
                db.get_buffer_manager()
                    .release_page(lock.into_heads().page_ptr)?;
            }

            Ok(())
        })?;

        // finalize the split
        page_lock.with_page_guard_mut(|page_guard| {
            page_guard
                .buffer_mut()
                .copy_from_slice(&left_page_buffer[..]);
            page_guard.set_dirty(true);
        });

        Ok((page_lock, right_page_lock))
    }

    /// Insert the tuple into the page at the given location.
    fn insert_into_page(
        &self,
        db: &DB,
        tuple: &[u8],
        offset: usize,
        page: OwningPageWriteLock,
        path: TreePath,
    ) -> Result<()> {
        let mut page_lock = page;
        let (_, _, page_num) = page_lock.borrow_page_guard().get_fork_and_num();
        let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());

        if page_view.get_free_space() < tuple.len() {
            // split
            let is_root = page_view.is_root();

            let (left_page_lock, right_page_lock) =
                self.split_page(db, tuple, offset, page_lock)?;

            self.insert_into_parent(db, path, left_page_lock, right_page_lock, is_root)
        } else {
            page_lock.with_page_guard_mut::<Result<()>>(|page_guard| {
                let mut page_view = BTreeDataPageViewMut::new(page_guard.buffer_mut());

                page_view.put_item(tuple, Some(offset), false)?;

                let insert_log = BTreeLogRecord::create_btree_insert_log(
                    RelFileRef {
                        db: self.rel_db(),
                        rel_id: self.rel_id(),
                    },
                    ForkType::Main,
                    page_num,
                    offset,
                    tuple,
                );
                let (_, lsn) = db.get_wal().append(XID::default(), insert_log)?;
                page_view.set_lsn(lsn);

                Ok(())
            })?;
            page_lock.with_page_guard_mut(|page_guard| page_guard.set_dirty(true));

            db.get_buffer_manager()
                .release_page(page_lock.into_heads().page_ptr)
        }
    }

    /// Walk one step up the path and re-acquire a write lock on the parent page.
    fn walk_up_path(
        &self,
        db: &DB,
        path: TreePath,
        child_page_num: usize,
    ) -> Result<(OwningPageWriteLock, TreePath, ItemPointer)> {
        let bufmgr = db.get_buffer_manager();
        let mut path = path;
        let tail = path.pop();

        match tail {
            Some(ItemPointer {
                mut page_num,
                mut offset,
            }) => loop {
                let parent_lock = self.get_tree_page_write(db, Some(page_num))?;
                let page_view = BTreeDataPageView::new(parent_lock.borrow_page_guard().buffer());
                let min_off = page_view.first_key_offset();
                let max_off = page_view.num_line_pointers();

                offset = std::cmp::max(offset, min_off);
                if offset > max_off {
                    offset = max_off + 1;
                }

                for i in offset..=max_off {
                    let tuple_buf = page_view.get_item(i);
                    let tuple = match bincode::deserialize::<IndexTuple>(tuple_buf) {
                        Ok(itup) => itup,
                        _ => {
                            return Err(Error::DataCorrupted(
                                "cannot deserialize index tuple".to_owned(),
                            ));
                        }
                    };

                    if tuple.get_downlink() == child_page_num {
                        return Ok((parent_lock, path, ItemPointer::new(page_num, i)));
                    }
                }

                for i in (min_off..offset).rev() {
                    let tuple_buf = page_view.get_item(i);
                    let tuple = match bincode::deserialize::<IndexTuple>(tuple_buf) {
                        Ok(itup) => itup,
                        _ => {
                            return Err(Error::DataCorrupted(
                                "cannot deserialize index tuple".to_owned(),
                            ));
                        }
                    };

                    if tuple.get_downlink() == child_page_num {
                        return Ok((parent_lock, path, ItemPointer::new(page_num, i)));
                    }
                }

                if page_view.is_rightmost() {
                    bufmgr.release_page(parent_lock.into_heads().page_ptr)?;
                    return Err(Error::DataCorrupted(format!(
                        "cannot re-find parent key for split page {}",
                        child_page_num
                    )));
                }

                page_num = page_view.get_next();
                offset = 0;
                bufmgr.release_page(parent_lock.into_heads().page_ptr)?;
            },
            _ => unreachable!(),
        }
    }

    /// Insert the split pages into the parent page.
    fn insert_into_parent(
        &self,
        db: &DB,
        path: TreePath,
        lchild_lock: OwningPageWriteLock,
        rchild_lock: OwningPageWriteLock,
        is_root: bool,
    ) -> Result<()> {
        let bufmgr = db.get_buffer_manager();

        if is_root {
            let root_page_lock = self.new_root(db, &lchild_lock, &rchild_lock)?;
            bufmgr.release_page(root_page_lock.into_heads().page_ptr)?;
            bufmgr.release_page(rchild_lock.into_heads().page_ptr)?;
            bufmgr.release_page(lchild_lock.into_heads().page_ptr)?;
            Ok(())
        } else {
            // prepare the downlink tuple for the right child
            let (_, _, left_page_num) = lchild_lock.borrow_page_guard().get_fork_and_num();
            let (_, _, right_page_num) = rchild_lock.borrow_page_guard().get_fork_and_num();
            let left_page_view = BTreeDataPageView::new(lchild_lock.borrow_page_guard().buffer());
            let high_key_buf = left_page_view.get_item(left_page_view.high_key_offset());
            let high_key = match bincode::deserialize::<IndexTuple>(high_key_buf) {
                Ok(itup) => itup.key,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ));
                }
            };
            let mut right_tuple = IndexTuple {
                key: high_key,
                item_pointer: ItemPointer::default(),
            };
            right_tuple.set_downlink(right_page_num);
            let right_tuple_buf = bincode::serialize(&right_tuple).unwrap();

            let (parent_lock, path, ItemPointer { offset, .. }) =
                self.walk_up_path(db, path, left_page_num)?;

            bufmgr.release_page(rchild_lock.into_heads().page_ptr)?;
            bufmgr.release_page(lchild_lock.into_heads().page_ptr)?;

            self.insert_into_page(db, &right_tuple_buf, offset + 1, parent_lock, path)
        }
    }

    fn scan_endpoint<'a>(
        &self,
        db: &DB,
        iterator: &mut BTreeScanIterator<'a, KCmp>,
        dir: ScanDirection,
    ) -> Result<Option<ItemPointer>> {
        let page_lock = self.get_endpoint(db, dir == ScanDirection::Backward)?;
        let (_, _, page_num) = page_lock.borrow_page_guard().get_fork_and_num();
        let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());
        let offset = match dir {
            ScanDirection::Forward => page_view.first_key_offset(),
            ScanDirection::Backward => page_view.num_line_pointers(),
        };

        iterator.read_page(&page_view, dir, offset)?;

        db.get_buffer_manager()
            .release_page(page_lock.into_heads().page_ptr)?;

        if iterator.items.is_empty() {
            // no items
            iterator.invalidate();
            self.step_page(db, iterator, dir)
        } else {
            iterator.cur_page_num = Some(page_num);
            let item_ptr = iterator.current_item_pointer();
            Ok(item_ptr)
        }
    }

    fn scan_first<'a>(
        &'a self,
        db: &DB,
        iterator: &mut BTreeScanIterator<'a, KCmp>,
        dir: ScanDirection,
    ) -> Result<Option<ItemPointer>> {
        let start_key = iterator.start_key.take();

        match start_key {
            Some(start_key) => {
                let (page_lock, _) = self.search_read(db, &start_key)?;
                let (_, _, page_num) = page_lock.borrow_page_guard().get_fork_and_num();
                let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());
                let offset =
                    self.binary_search_page(&page_view, &start_key, ItemPointer::default(), false)?;

                iterator.read_page(&page_view, dir, offset)?;

                db.get_buffer_manager()
                    .release_page(page_lock.into_heads().page_ptr)?;

                if iterator.items.is_empty() {
                    // no items
                    iterator.invalidate();
                    self.step_page(db, iterator, dir)
                } else {
                    iterator.cur_page_num = Some(page_num);
                    let item_ptr = iterator.current_item_pointer();
                    Ok(item_ptr)
                }
            }
            _ => self.scan_endpoint(db, iterator, dir),
        }
    }

    fn read_next_page(
        &self,
        db: &DB,
        iterator: &mut BTreeScanIterator<KCmp>,
        dir: ScanDirection,
        page_num: usize,
    ) -> Result<Option<ItemPointer>> {
        let mut page_num = page_num;
        match dir {
            ScanDirection::Forward => loop {
                if page_num == 0 {
                    iterator.invalidate();
                    return Ok(None);
                }

                let page_lock = self.get_tree_page_read(db, Some(page_num))?;
                let page_view = BTreeDataPageView::new(page_lock.borrow_page_guard().buffer());

                iterator.read_page(&page_view, dir, page_view.first_key_offset())?;

                if !iterator.items.is_empty() {
                    break;
                }

                page_num = page_view.get_next();
                db.get_buffer_manager()
                    .release_page(page_lock.into_heads().page_ptr)?;
            },
            ScanDirection::Backward => {
                return Ok(None);
            }
        }

        if iterator.items.is_empty() {
            // no items
            iterator.invalidate();
            self.step_page(db, iterator, dir)
        } else {
            iterator.cur_page_num = Some(page_num);
            let item_ptr = iterator.current_item_pointer();
            Ok(item_ptr)
        }
    }

    /// Step to the next page that contains valid data for a scan.
    fn step_page(
        &self,
        db: &DB,
        iterator: &mut BTreeScanIterator<KCmp>,
        dir: ScanDirection,
    ) -> Result<Option<ItemPointer>> {
        let next_page_num = match dir {
            ScanDirection::Forward => {
                let cur_page = iterator.cur_page.take();
                if let Some(page) = cur_page {
                    db.get_buffer_manager().release_page(page)?;
                }

                iterator.next_page
            }
            ScanDirection::Backward => match iterator.cur_page_num {
                Some(cur_page_num) => cur_page_num,
                _ => unreachable!(),
            },
        };

        self.read_next_page(db, iterator, dir, next_page_num)
    }
}

impl<KCmp> Relation for BTree<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    fn get_relation_entry(&self) -> &RelationEntry {
        &self.rel_entry
    }
}

impl<KCmp> RelationWithStorage for BTree<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    fn get_storage_handle(&self) -> &Mutex<Option<StorageHandle>> {
        &self.shandle
    }
}

impl<KCmp> Index for BTree<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    fn build_empty(&self, db: &DB) -> Result<()> {
        let smgr = db.get_storage_manager();
        self.with_storage(smgr, |storage| {
            let mut buffer = [0u8; PAGE_SIZE];
            let mut meta_view = BTreeMetaPageViewMut::new(&mut buffer);
            meta_view.init_page(0);

            smgr.write(storage, ForkType::Main, BTREE_META_PAGE_NUM, &buffer)?;
            smgr.sync(storage, ForkType::Main)
        })
    }

    fn insert<'a>(&'a self, db: &DB, key: &[u8], item_pointer: ItemPointer) -> Result<()> {
        let (page_lock, path) = self.search_write(db, key)?;

        let itup = IndexTuple {
            key: key.into(),
            item_pointer,
        };
        let itup_buf = bincode::serialize(&itup).unwrap();

        let (page_lock, offset) = self.get_insert_location(db, key, item_pointer, page_lock)?;

        self.insert_into_page(db, &itup_buf[..], offset, page_lock, path)
    }

    fn begin_scan<'a>(
        &'a self,
        db: &DB,
        txn: &'a mut Transaction,
        table: &'a dyn Table,
    ) -> Result<Box<dyn IndexScanIterator<'a> + 'a>> {
        let xid = txn.xid();
        let snapshot = db.get_transaction_manager().get_snapshot(txn)?;
        let btree_it = BTreeScanIterator {
            btree: &self,
            xid,
            snapshot,
            table,
            predicate: None,
            cur_page: None,
            cur_page_num: None,
            next_page: 0,
            start_key: None,
            items: Vec::new(),
            item_index: 0,
        };

        Ok(Box::new(btree_it))
    }
}

pub struct BTreeScanIterator<'a, KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    btree: &'a BTree<KCmp>,
    xid: XID,
    snapshot: &'a Snapshot,
    table: &'a dyn Table,
    predicate: Option<IndexScanPredicate<'a>>,
    cur_page: Option<PinnedPagePtr>,
    cur_page_num: Option<usize>,
    start_key: Option<Vec<u8>>,
    next_page: usize,

    // these members are valid when cur_page_num is not None
    items: Vec<IndexTuple<'a>>,
    item_index: usize,
}

impl<'a, KCmp> BTreeScanIterator<'a, KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    fn read_page<P>(&mut self, page_view: &P, dir: ScanDirection, offset: usize) -> Result<()>
    where
        P: BTreeDataPageReader,
    {
        self.items = Vec::new();
        let minoff = page_view.first_key_offset();
        let maxoff = page_view.num_line_pointers();

        self.next_page = page_view.get_next();

        let offsets = match dir {
            ScanDirection::Forward => std::cmp::max(minoff, offset)..=maxoff,
            ScanDirection::Backward => minoff..=std::cmp::min(maxoff, offset),
        };

        for off in offsets {
            let itup_buf = page_view.get_item(off);
            let itup = match bincode::deserialize::<IndexTuple>(itup_buf) {
                Ok(itup) => itup,
                _ => {
                    return Err(Error::DataCorrupted(
                        "cannot deserialize index tuple".to_owned(),
                    ))
                }
            };

            if self.check_predicate(&itup)? {
                self.items.push(itup.materialize());
            }
        }

        match dir {
            ScanDirection::Forward => {
                self.item_index = 0;
            }
            ScanDirection::Backward => {
                self.item_index = if self.items.is_empty() {
                    0
                } else {
                    self.items.len() - 1
                };
            }
        }

        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.cur_page_num.is_some()
    }

    fn invalidate(&mut self) {
        self.cur_page_num = None;
    }

    fn current_item_pointer(&self) -> Option<ItemPointer> {
        if !self.is_valid() || self.item_index >= self.items.len() {
            None
        } else {
            Some(self.items[self.item_index].item_pointer)
        }
    }

    fn scan_next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<ItemPointer>> {
        let step = match dir {
            ScanDirection::Forward => {
                self.item_index += 1;
                self.item_index >= self.items.len()
            }
            ScanDirection::Backward => {
                if self.item_index == 0 {
                    true
                } else {
                    self.item_index -= 1;
                    false
                }
            }
        };

        if step {
            self.btree.step_page(db, self, dir)
        } else {
            Ok(self.current_item_pointer())
        }
    }

    fn next_item_pointer(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<ItemPointer>> {
        if self.is_valid() {
            self.scan_next(db, dir)
        } else {
            self.btree.scan_first(db, self, dir)
        }
    }

    fn check_predicate(&self, tuple: &IndexTuple) -> Result<bool> {
        match &self.predicate {
            Some(predicate) => predicate(&tuple.key),
            _ => Err(Error::InvalidState(
                "index scan without predicate".to_owned(),
            )),
        }
    }
}

impl<'a, KCmp> IndexScanIterator<'a> for BTreeScanIterator<'a, KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Result<Ordering> + Sync + Send,
{
    fn rescan(
        &mut self,
        db: &'a DB,
        start_key: Option<&[u8]>,
        predicate: IndexScanPredicate<'a>,
    ) -> Result<()> {
        let cur_page = self.cur_page.take();
        if let Some(page_ptr) = cur_page {
            db.get_buffer_manager().release_page(page_ptr)?;
        }

        self.start_key = start_key.map(|key| key.to_vec());
        self.predicate = Some(predicate);
        Ok(())
    }

    fn next(&mut self, db: &'a DB, dir: ScanDirection) -> Result<Option<TuplePtr<'a>>> {
        loop {
            let item_pointer = match self.next_item_pointer(db, dir)? {
                Some(item_pointer) => item_pointer,
                _ => return Ok(None),
            };

            if let Some(tuple) =
                self.table
                    .fetch_tuple(db, self.xid, self.snapshot, item_pointer)?
            {
                return Ok(Some(tuple));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        am::index::IndexScanPredicate, concurrency::IsolationLevel, storage::ScanDirection,
        test_util::get_temp_db,
    };

    use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

    #[test]
    fn can_create_btree() {
        let (db, db_dir) = get_temp_db();
        let btree = db
            .create_index(0, 0, |_: &[u8], _: &[u8]| Ok(std::cmp::Ordering::Equal))
            .unwrap();
        btree.build_empty(&db).unwrap();

        let mut rel_path = db_dir.path().to_path_buf();
        rel_path.push("base");
        rel_path.push("0");
        rel_path.push("0_0");

        assert!(rel_path.is_file());
        assert!(db_dir.close().is_ok());
    }

    #[test]
    fn can_insert_and_scan_btree() {
        let (db, db_dir) = get_temp_db();
        let mut txn = db.start_transaction(IsolationLevel::ReadCommitted).unwrap();
        let heap = db.create_table(0, 0).unwrap();
        let btree = db
            .create_index(0, 1, |a: &[u8], b: &[u8]| {
                let a = LittleEndian::read_u32(a);
                let b = LittleEndian::read_u32(b);
                Ok(a.cmp(&b))
            })
            .unwrap();

        btree.build_empty(&db).unwrap();

        let make_key = |a| {
            let mut buf = [0u8; 4];
            (&mut buf[..]).write_u32::<LittleEndian>(a).unwrap();
            buf
        };

        let predicate = IndexScanPredicate::new(|a: &[u8]| {
            let a = LittleEndian::read_u32(a);
            Ok(a > 50)
        });

        for i in 0..300 {
            let key = make_key(300 - i);
            let item_ptr = heap.insert_tuple(&db, &txn, &key).unwrap();
            assert!(btree.insert(&db, &key, item_ptr).is_ok());
        }

        {
            let mut iter = btree.begin_scan(&db, &mut txn, &*heap).unwrap();
            iter.rescan(&db, None, predicate).unwrap();

            let mut count = 0;
            while let Some(tuple) = iter.next(&db, ScanDirection::Forward).unwrap() {
                let a = LittleEndian::read_u32(tuple.get_data());
                assert!(a > 50);
                count += 1;
            }
            assert_eq!(count, 250);
        }

        db.commit_transaction(txn).unwrap();

        assert!(db_dir.close().is_ok());
    }
}
