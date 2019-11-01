use crate::{catalog::Schema, Relation};

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
}

pub type TablePtr = Arc<dyn Table>;
