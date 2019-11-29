use crate::{wal::WalLogRecord, Result, DB, OID};

use std::sync::Mutex;

const OID_PREALLOC_COUNT: usize = 8192;
const NORMAL_OID_START: OID = 16384;

struct OidAllocator {
    next_oid: OID,
    oid_count: usize,
}

impl Default for OidAllocator {
    fn default() -> Self {
        Self {
            next_oid: 0,
            oid_count: 0,
        }
    }
}

pub struct StateManager {
    oid_allocator: Mutex<OidAllocator>,
}

impl StateManager {
    pub fn new() -> Self {
        let oid_allocator = Default::default();
        StateManager {
            oid_allocator: Mutex::new(oid_allocator),
        }
    }

    pub fn get_next_oid(&self, db: &DB) -> Result<OID> {
        let mut guard = self.oid_allocator.lock().unwrap();

        if guard.next_oid < NORMAL_OID_START {
            guard.next_oid = NORMAL_OID_START;
            guard.oid_count = 0;
        }

        if guard.oid_count == 0 {
            // get some more
            let wal = db.get_wal();
            let next_oid_log =
                WalLogRecord::create_next_oid_log(guard.next_oid + OID_PREALLOC_COUNT as OID);
            wal.append(&next_oid_log)?;
            guard.oid_count = OID_PREALLOC_COUNT;
        }

        let next_oid = guard.next_oid;
        guard.next_oid += 1;
        guard.oid_count -= 1;

        Ok(next_oid)
    }

    pub fn set_next_oid(&self, oid: OID) {
        let mut guard = self.oid_allocator.lock().unwrap();
        guard.next_oid = oid;
        guard.oid_count = 0;
    }
}
