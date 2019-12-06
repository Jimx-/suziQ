use crate::{am::index::IndexPtr, storage::TablePtr, OID};

use std::{collections::HashMap, option::Option};

#[derive(Default)]
pub struct CatalogCache {
    table_cache: HashMap<OID, TablePtr>,
    index_cache: HashMap<OID, IndexPtr>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self {
            table_cache: HashMap::new(),
            index_cache: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: TablePtr) -> Option<TablePtr> {
        let rel_id = table.rel_id();
        self.table_cache.insert(rel_id, table)
    }
    pub fn lookup_table(&self, rel_id: OID) -> Option<TablePtr> {
        match self.table_cache.get(&rel_id) {
            Some(table) => Some(table.clone()),
            _ => None,
        }
    }

    pub fn add_index(&mut self, index: IndexPtr) -> Option<IndexPtr> {
        let rel_id = index.rel_id();
        self.index_cache.insert(rel_id, index)
    }
    pub fn lookup_index(&self, rel_id: OID) -> Option<IndexPtr> {
        match self.index_cache.get(&rel_id) {
            Some(index) => Some(index.clone()),
            _ => None,
        }
    }
}
