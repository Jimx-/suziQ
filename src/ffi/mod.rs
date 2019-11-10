extern crate libc;

use crate::{
    catalog::Schema,
    storage::{ScanDirection, TablePtr, TableScanIterator, Tuple},
    DBConfig, Error, DB, OID,
};

use libc::{c_char, c_int};
use std::{cell::RefCell, ffi::CStr, path::PathBuf, sync::Arc};

// error handling code borrowed from https://michael-f-bryan.github.io/rust-ffi-guide/errors/return_types.html
thread_local! {
    static LAST_ERROR: RefCell<Option<Box<Error>>> = RefCell::new(None);
}

fn update_last_error(err: Error) {
    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = Some(Box::new(err));
    });
}

fn take_last_error() -> Option<Box<Error>> {
    LAST_ERROR.with(|prev| prev.borrow_mut().take())
}

#[no_mangle]
pub extern "C" fn sq_last_error_length() -> c_int {
    LAST_ERROR.with(|prev| match *prev.borrow() {
        Some(ref err) => err.to_string().len() as c_int + 1,
        None => 0,
    })
}

#[no_mangle]
pub unsafe extern "C" fn sq_last_error_message(buffer: *mut c_char, length: c_int) -> c_int {
    if buffer.is_null() {
        return -1;
    }

    let last_error = match take_last_error() {
        Some(err) => err,
        None => return 0,
    };

    let error_message = last_error.to_string();

    let buffer = std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if error_message.len() >= buffer.len() {
        return -1;
    }

    std::ptr::copy_nonoverlapping(
        error_message.as_ptr(),
        buffer.as_mut_ptr(),
        error_message.len(),
    );

    buffer[error_message.len()] = 0;

    error_message.len() as c_int
}

#[no_mangle]
pub extern "C" fn sq_create_db(root_path: *const c_char) -> *const DB {
    let root_path = unsafe {
        assert!(!root_path.is_null());
        CStr::from_ptr(root_path)
    };
    let root_path_str = root_path.to_str().unwrap();
    let config = DBConfig::new().root_path(PathBuf::from(root_path_str));
    let db = match DB::open(&config) {
        Ok(db) => db,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };
    Arc::into_raw(Arc::new(db))
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

    let table = match db.create_table(db_oid, rel_oid, Schema::new()) {
        Ok(table) => table,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

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
) -> c_int {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };

    let tuple = unsafe { std::slice::from_raw_parts(data, len as usize) };

    match table.insert_tuple(db, tuple) {
        Ok(_) => 1,
        Err(e) => {
            update_last_error(e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn sq_table_begin_scan<'a>(
    table: *const TablePtr,
    db: *const DB,
) -> *mut Box<dyn TableScanIterator<'a> + 'a> {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };

    let iterator = match table.begin_scan(db) {
        Ok(iterator) => iterator,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(iterator))
}

#[no_mangle]
pub extern "C" fn sq_free_table_scan_iterator<'a>(
    iterator: *mut Box<dyn TableScanIterator<'a> + 'a>,
) {
    if iterator.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(iterator);
    }
}

fn get_scan_direction(dir: c_int) -> ScanDirection {
    if dir == 0 {
        ScanDirection::Forward
    } else {
        ScanDirection::Backward
    }
}

#[no_mangle]
pub extern "C" fn sq_table_scan_next<'a>(
    iterator: *mut Box<dyn TableScanIterator<'a> + 'a>,
    db: *const DB,
    dir: c_int,
) -> *const Box<dyn Tuple + 'a> {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let iterator: &mut Box<dyn TableScanIterator<'a> + 'a> = unsafe {
        assert!(!iterator.is_null());
        &mut *iterator
    };

    let tuple = match iterator.next(db, get_scan_direction(dir)) {
        Ok(Some(tuple)) => tuple.materialize(),
        Ok(None) => {
            return std::ptr::null();
        }
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(tuple))
}

#[no_mangle]
pub extern "C" fn sq_free_tuple<'a>(tuple: *const Box<dyn Tuple + 'a>) {
    if tuple.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(tuple as *mut Box<dyn Tuple + 'a>);
    }
}

#[no_mangle]
pub extern "C" fn sq_tuple_get_data_len<'a>(tuple: *const Box<dyn Tuple + 'a>) -> c_int {
    let tuple = unsafe {
        assert!(!tuple.is_null());
        &*tuple
    };

    tuple.get_data().len() as c_int
}

#[no_mangle]
pub unsafe extern "C" fn sq_tuple_get_data<'a>(
    tuple: *const Box<dyn Tuple + 'a>,
    buffer: *mut c_char,
    length: c_int,
) -> c_int {
    if buffer.is_null() {
        return -1;
    }

    let tuple = {
        assert!(!tuple.is_null());
        &*tuple
    };

    let data = tuple.get_data();
    let buffer = std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if data.len() > buffer.len() {
        return -1;
    }

    std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr(), data.len());

    data.len() as c_int
}
