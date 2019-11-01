use crate::{
    catalog::Schema,
    storage::{Table, TableData},
    Relation, RelationEntry, RelationKind, OID,
};

pub struct Heap {
    rel_entry: RelationEntry,
    table_data: TableData,
}

impl Heap {
    pub fn new(rel_id: OID, name: &str, schema: Schema) -> Self {
        let rel_entry = RelationEntry::new(rel_id, name, RelationKind::Table);
        let table_data = TableData::new(schema);

        Self {
            rel_entry,
            table_data,
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
