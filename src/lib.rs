pub mod am;
pub mod catalog;
pub mod storage;

extern crate lru;

#[cfg(test)]
extern crate tempfile;

mod config;
mod db;
mod relation;
mod result;

pub use self::{
    config::DBConfig,
    db::DB,
    relation::{Relation, RelationEntry, RelationKind},
    result::{Error, Result},
};

pub type OID = u64;
