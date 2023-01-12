pub mod am;
pub mod concurrency;
pub mod storage;
pub mod wal;

mod config;
mod db;
mod ffi;
mod relation;
mod result;
mod test_util;

pub use self::{
    config::DBConfig,
    db::DB,
    relation::{Relation, RelationEntry, RelationKind},
    result::{Error, Result},
};

pub type OID = u64;
