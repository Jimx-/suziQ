use crate::storage::TablePtr;
use crate::*;

use std::{collections::HashMap, option::Option, sync::RwLock};

#[derive(Default)]
pub struct CatalogCache {
    table_cache: RwLock<HashMap<OID, TablePtr>>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self {
            table_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_table(&self, table: TablePtr) -> Option<TablePtr> {
        let mut guard = self.table_cache.write().unwrap();
        let rel_id = table.rel_id();
        guard.insert(rel_id, table)
    }
    pub fn lookup_table(&self, rel_id: OID) -> Option<TablePtr> {
        let guard = self.table_cache.read().unwrap();
        match guard.get(&rel_id) {
            Some(table) => Some(table.clone()),
            _ => None,
        }
    }
}
