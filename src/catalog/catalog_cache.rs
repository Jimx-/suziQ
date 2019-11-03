use super::Schema;
use crate::*;
use crate::{am::Heap, storage::TablePtr};

use std::{
    collections::HashMap,
    option::Option,
    sync::{Arc, RwLock},
};

pub struct CatalogCache {
    table_cache: RwLock<HashMap<OID, TablePtr>>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self {
            table_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_heap(
        &mut self,
        db: OID,
        rel_id: OID,
        name: &str,
        schema: Schema,
    ) -> Result<TablePtr> {
        let mut guard = self.table_cache.write().unwrap();
        let entry = guard
            .entry(rel_id)
            .or_insert_with(|| Arc::new(Heap::new(rel_id, db, name, schema)));
        Ok(entry.clone())
    }

    pub fn lookup_table(&self, rel_id: OID) -> Option<TablePtr> {
        let guard = self.table_cache.read().unwrap();
        match guard.get(&rel_id) {
            Some(table) => Some(table.clone()),
            _ => None,
        }
    }
}
