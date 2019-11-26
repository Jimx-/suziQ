use crate::concurrency::XID;

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum TransactionState {
//     InProgress,
//     Commit,
//     Abort,
// }

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
