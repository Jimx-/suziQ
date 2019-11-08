extern crate libc;

use crate::{catalog::Schema, storage::Table, DBConfig, DB, OID};

use libc::c_char;
use std::{ffi::CStr, path::PathBuf, sync::Arc};

#[no_mangle]
pub extern "C" fn sq_create_db(root_path: *const c_char) -> *const DB {
    let root_path = unsafe {
        assert!(!root_path.is_null());
        CStr::from_ptr(root_path)
    };
    let root_path_str = root_path.to_str().unwrap();
    let config = DBConfig::new().root_path(PathBuf::from(root_path_str));
    Arc::into_raw(Arc::new(DB::open(&config).unwrap()))
}

#[no_mangle]
pub extern "C" fn sq_free_db(ptr: *const DB) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        Arc::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn sq_create_table(db: *mut DB, db_oid: OID, rel_oid: OID) -> *const dyn Table {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let table = db.create_table(db_oid, rel_oid, Schema::new()).unwrap();
    Arc::into_raw(table)
}

#[no_mangle]
pub extern "C" fn sq_free_table(ptr: *const dyn Table) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        Arc::from_raw(ptr);
    }
}
