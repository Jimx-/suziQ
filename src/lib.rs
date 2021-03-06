pub mod am;
pub mod concurrency;
pub mod storage;
pub mod wal;

extern crate bincode;
extern crate bitflags;
extern crate byteorder;
extern crate fs2;
extern crate log;
extern crate lru;
extern crate serde;
extern crate serde_bytes;

#[macro_use]
extern crate rental;

#[cfg(test)]
extern crate tempfile;

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
