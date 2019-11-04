use crate::{catalog::Schema, storage::ItemPointer, Relation, Result, DB};

use std::sync::Arc;

pub struct TableData {
    schema: Schema,
}

impl TableData {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

pub trait Table: Relation {
    fn get_table_data(&self) -> &TableData;

    fn table_schema(&self) -> &Schema {
        &self.get_table_data().schema
    }

    fn insert_tuple(&self, db: &DB, tuple: &[u8]) -> Result<ItemPointer>;
}

pub type TablePtr = Arc<dyn Table>;
