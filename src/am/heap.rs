use crate::{
    catalog::Schema,
    storage::{
        consts::PAGE_SIZE, ForkType, ItemPointer, RelationWithStorage, StorageHandle, Table,
        TableData,
    },
    Error, Relation, RelationEntry, RelationKind, Result, DB, OID,
};

use super::heap_page::{HeapPageReader, HeapPageViewMut};

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

fn tuple_size_limit() -> usize {
    PAGE_SIZE
}

#[derive(Serialize, Deserialize)]
struct HeapTuple {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
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

    fn prepare_heap_tuple_for_insert(&self, data: &[u8]) -> HeapTuple {
        HeapTuple {
            data: data.to_vec(),
        }
    }

    fn get_insert_hint(&self) -> Option<usize> {
        let guard = self.insert_hint.lock().unwrap();
        guard.clone()
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
                bufmgr.fetch_page(storage, ForkType::Main, page_num)
            })?;

            let result = page_ptr.with_write(move |page| {
                let buffer = page.buffer_mut();
                let mut page_view = HeapPageViewMut::new(buffer);
                let mut dirty = false;

                if page_view.is_new() {
                    page_view.init_page();
                    dirty = true;
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
            self.with_storage(smgr, |storage| bufmgr.new_page(storage, ForkType::Main))?;

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
}

impl Relation for Heap {
    fn get_relation_entry(&self) -> &RelationEntry {
        &self.rel_entry
    }
}

impl Table for Heap {
    fn get_table_data(&self) -> &TableData {
        &self.table_data
    }

    fn insert_tuple(&self, db: &DB, tuple: &[u8]) -> Result<ItemPointer> {
        let htup = self.prepare_heap_tuple_for_insert(tuple);
        let htup_buf = bincode::serialize(&htup).unwrap();
        let htup_len = htup_buf.len();

        let itemp = self.with_page_for_tuple(db, htup_len, |page_view, page_num| {
            let (off, len) = page_view.put_tuple(&htup_buf)?;
            Ok((ItemPointer::new(page_num, off as usize, len as usize), true))
        })?;
        Ok(itemp)
    }
}

impl RelationWithStorage for Heap {
    fn get_storage_handle(&self) -> &Mutex<Option<StorageHandle>> {
        &self.shandle
    }
}

#[cfg(test)]
mod tests {
    use crate::{catalog::Schema, test_util::get_temp_db};

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
    fn can_insert_tuples() {
        let (db, db_dir) = get_temp_db();
        let heap_result = db.create_table(0, 0, Schema::new());
        assert!(heap_result.is_ok());
        let heap = heap_result.unwrap();

        for _ in 0..100 {
            assert!(heap.insert_tuple(&db, &[1u8; 100]).is_ok());
        }
        assert!(db_dir.close().is_ok());
    }
}
