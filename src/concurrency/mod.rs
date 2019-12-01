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

use std::{cmp::Ordering, collections::HashSet, fmt, num::Wrapping};

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct XID(u32);

impl Default for XID {
    fn default() -> Self {
        Self(0)
    }
}

impl PartialOrd for XID {
    fn partial_cmp(&self, other: &XID) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for XID {
    fn cmp(&self, other: &XID) -> Ordering {
        if self.is_invalid() || other.is_invalid() {
            self.0.cmp(&other.0)
        } else {
            let delta = (Wrapping(self.0) - Wrapping(other.0)).0 as i32;
            delta.cmp(&0)
        }
    }
}

impl fmt::Display for XID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for XID {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Into<u64> for XID {
    fn into(self) -> u64 {
        self.0 as u64
    }
}

impl XID {
    pub fn is_invalid(self) -> bool {
        self.0 == 0
    }

    pub fn inc(self) -> Self {
        let mut xid = Wrapping(self.0);

        loop {
            xid += Wrapping(1);

            if xid.0 != 0 {
                break;
            }
        }

        Self(xid.0)
    }

    pub fn dec(self) -> Self {
        let mut xid = Wrapping(self.0);

        loop {
            xid -= Wrapping(1);

            if xid.0 != 0 {
                break;
            }
        }

        Self(xid.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum IsolationLevel {
    ReadUncommitted = 0,
    ReadCommitted = 1,
    RepeatableRead = 2,
    Serializable = 3,
}

impl IsolationLevel {}

pub struct Transaction {
    xid: XID,
    isolation_level: IsolationLevel,
    current_snapshot: Option<Snapshot>,
    // state: TransactionState,
}

impl Transaction {
    pub fn new(xid: XID, isolation_level: IsolationLevel) -> Self {
        Self {
            xid,
            isolation_level,
            current_snapshot: None,
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
    pub fn uses_transaction_snapshot(&self) -> bool {
        self.isolation_level >= IsolationLevel::RepeatableRead
    }

    pub fn is_serializable(&self) -> bool {
        self.isolation_level == IsolationLevel::Serializable
    }
}

pub struct Snapshot {
    // first active transaction
    min_xid: XID,
    // first unassigned XID
    max_xid: XID,
    // active XIDs at the time of snapshot
    xips: HashSet<XID>,
}

impl Snapshot {
    /// Is the XID in progress according to the snapshot
    pub fn is_xid_in_progress(&self, xid: XID) -> bool {
        if xid < self.min_xid {
            // txn must be committed or aborted
            return false;
        }

        if xid >= self.max_xid {
            return true;
        }

        self.xips.contains(&xid)
    }
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.min_xid,
            self.max_xid,
            self.xips
                .iter()
                .map(XID::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_compare_xid() {
        let xid = XID::from(1);
        assert!(!xid.dec().is_invalid());
        assert!(xid.dec() < xid);
        assert!(!xid.inc().is_invalid());
        assert!(xid.inc() > xid);

        let xid1 = xid.dec();
        assert!(!xid1.dec().is_invalid());
        assert!(xid1.dec() < xid1);
        assert!(!xid1.inc().is_invalid());
        assert!(xid1.inc() > xid1);
    }
}
