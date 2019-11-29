mod state_manager;
mod transaction;
mod transaction_log;
mod transaction_manager;

pub use self::{
    state_manager::StateManager, transaction::Transaction, transaction_log::TransactionLogRecord,
    transaction_manager::TransactionManager,
};

pub type XID = u64;

pub fn is_invalid_xid(xid: XID) -> bool {
    xid == 0
}
