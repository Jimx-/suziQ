extern crate libc;

use crate::{catalog::Schema, storage::TablePtr, DBConfig, DB, OID};

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
pub extern "C" fn sq_free_db(db: *const DB) {
    if db.is_null() {
        return;
    }
    unsafe {
        Arc::from_raw(db);
    }
}

#[no_mangle]
pub extern "C" fn sq_create_table(db: *const DB, db_oid: OID, rel_oid: OID) -> *const TablePtr {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let table = db.create_table(db_oid, rel_oid, Schema::new()).unwrap();
    Box::into_raw(Box::new(table))
}

#[no_mangle]
pub extern "C" fn sq_free_table(table: *const TablePtr) {
    if table.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(table as *mut TablePtr);
    }
}

#[no_mangle]
pub extern "C" fn sq_table_insert_tuple(
    table: *const TablePtr,
    db: *const DB,
    data: *const u8,
    len: u64,
) {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };

    let tuple = unsafe { std::slice::from_raw_parts(data, len as usize) };

    table.insert_tuple(db, tuple).unwrap();
}
