use crate::OID;

#[derive(Copy, Clone)]
pub enum RelationKind {
    Table,
    Index,
    View,
}

pub struct RelationEntry {
    id: OID,
    db: OID,
    name: String,
    kind: RelationKind,
}

impl RelationEntry {
    pub fn new(id: OID, db: OID, name: &str, kind: RelationKind) -> Self {
        Self {
            id,
            db,
            name: name.to_string(),
            kind,
        }
    }
}

pub trait Relation {
    fn get_relation_entry(&self) -> &RelationEntry;

    fn rel_id(&self) -> OID {
        self.get_relation_entry().id
    }

    fn rel_db(&self) -> OID {
        self.get_relation_entry().db
    }

    fn rel_name(&self) -> &String {
        &self.get_relation_entry().name
    }

    fn rel_kind(&self) -> RelationKind {
        self.get_relation_entry().kind
    }
}
