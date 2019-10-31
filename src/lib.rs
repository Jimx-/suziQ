pub mod storage;

extern crate lru;

#[cfg(test)]
extern crate tempfile;

mod config;
mod db;
mod result;

pub use self::{
    config::DBConfig,
    db::DB,
    result::{Error, Result},
};

pub type OID = u64;
