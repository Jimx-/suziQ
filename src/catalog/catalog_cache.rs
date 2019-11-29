use crate::storage::TablePtr;
use crate::*;

use std::{collections::HashMap, option::Option};

#[derive(Default)]
pub struct CatalogCache {
    table_cache: HashMap<OID, TablePtr>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self {
            table_cache: HashMap::new(),
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
}
