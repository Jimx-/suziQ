use crate::{
    catalog::Schema,
    storage::{RelationWithStorage, StorageHandle, Table, TableData},
    Relation, RelationEntry, RelationKind, OID,
};

use std::sync::Mutex;

pub struct Heap {
    rel_entry: RelationEntry,
    table_data: TableData,
    shandle: Mutex<Option<StorageHandle>>,
}

impl Heap {
    pub fn new(rel_id: OID, db: OID, name: &str, schema: Schema) -> Self {
        let rel_entry = RelationEntry::new(rel_id, db, name, RelationKind::Table);
        let table_data = TableData::new(schema);

        Self {
            rel_entry,
            table_data,
            shandle: Mutex::new(None),
        }
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
}

impl RelationWithStorage for Heap {
    fn get_storage_handle(&self) -> &Mutex<Option<StorageHandle>> {
        &self.shandle
    }
}
