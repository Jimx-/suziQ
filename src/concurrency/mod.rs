mod state_manager;
mod transaction_log;
mod transaction_manager;
mod transaction_table;

pub use self::{
    state_manager::StateManager,
    transaction_log::TransactionLogRecord,
    transaction_manager::TransactionManager,
    transaction_table::{TransactionStatus, TransactionTable},
};

pub type XID = u32;

pub fn is_invalid_xid(xid: XID) -> bool {
    xid == 0
}

pub fn compare_xid(a: XID, b: XID) -> std::cmp::Ordering {
    if is_invalid_xid(a) || is_invalid_xid(b) {
        a.cmp(&b)
    } else {
        let delta = (a - b) as i32;
        delta.cmp(&0)
    }
}

pub struct Transaction {
    xid: XID,
    // state: TransactionState,
}

impl Transaction {
    pub fn new(xid: XID) -> Self {
        Self {
            xid,
            // state: TransactionState::InProgress,
        }
    }

    pub fn xid(&self) -> XID {
        self.xid
    }

    // pub fn state(&self) -> TransactionState {
    //     self.state
    // }

    // pub fn set_state(&mut self, state: TransactionState) {
    //     self.state = state;
    // }
}
