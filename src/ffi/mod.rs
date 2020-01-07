extern crate env_logger;
extern crate libc;

use crate::{
    am::index::{IndexPtr, IndexScanIterator, IndexScanPredicate},
    catalog::Schema,
    concurrency::{IsolationLevel, Transaction},
    storage::{ItemPointer, ScanDirection, TablePtr, TableScanIterator, Tuple},
    DBConfig, Error, Result, DB, OID,
};

use libc::{c_char, c_int, c_uint};
use std::{cell::RefCell, ffi::CStr, path::PathBuf, sync::Arc};

#[no_mangle]
pub extern "C" fn sq_init() {
    env_logger::init();
}

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

fn sq_get_isolation_level(isolation_level: c_int) -> Result<IsolationLevel> {
    match isolation_level {
        0 => Ok(IsolationLevel::ReadUncommitted),
        1 => Ok(IsolationLevel::ReadCommitted),
        2 => Ok(IsolationLevel::RepeatableRead),
        3 => Ok(IsolationLevel::Serializable),
        _ => Err(Error::InvalidArgument("unknown isolation level".to_owned())),
    }
}

#[no_mangle]
pub extern "C" fn sq_start_transaction(db: *const DB, isolation_level: c_int) -> *mut Transaction {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let isolation_level = match sq_get_isolation_level(isolation_level) {
        Ok(iso_level) => iso_level,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    let txn = match db.start_transaction(isolation_level) {
        Ok(txn) => txn,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(txn))
}

#[no_mangle]
pub extern "C" fn sq_free_transaction(_txn: *mut Transaction) {}

#[no_mangle]
pub extern "C" fn sq_commit_transaction(db: *const DB, txn: *mut Transaction) {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let txn = unsafe {
        assert!(!txn.is_null());
        Box::from_raw(txn)
    };

    match db.commit_transaction(*txn) {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
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
pub extern "C" fn sq_open_table(db: *const DB, db_oid: OID, rel_oid: OID) -> *const TablePtr {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let table = match db.open_table(db_oid, rel_oid) {
        Ok(Some(table)) => table,
        Ok(None) => {
            return std::ptr::null();
        }
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
    txn: *const Transaction,
    data: *const u8,
    len: u64,
) -> *const ItemPointer {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };
    let txn: &Transaction = unsafe {
        assert!(!txn.is_null());
        &*txn
    };

    let tuple = unsafe { std::slice::from_raw_parts(data, len as usize) };

    let item_pointer = match table.insert_tuple(db, txn, tuple) {
        Ok(ptr) => ptr,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(item_pointer))
}

#[no_mangle]
pub extern "C" fn sq_free_item_pointer(pointer: *const ItemPointer) {
    if pointer.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(pointer as *mut ItemPointer);
    }
}

#[no_mangle]
pub extern "C" fn sq_table_begin_scan<'a>(
    table: *const TablePtr,
    db: *const DB,
    txn: *mut Transaction,
) -> *mut Box<dyn TableScanIterator<'a> + 'a> {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };
    let txn: &mut Transaction = unsafe {
        assert!(!txn.is_null());
        &mut *txn
    };

    let iterator = match table.begin_scan(db, txn) {
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

#[no_mangle]
pub extern "C" fn sq_create_checkpoint(db: *const DB) {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    match db.create_checkpoint() {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    };
}

#[no_mangle]
pub extern "C" fn sq_get_next_oid(db: *const DB) -> OID {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    match db.get_next_oid() {
        Ok(oid) => oid,
        Err(e) => {
            update_last_error(e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn sq_create_index(
    db: *const DB,
    db_oid: OID,
    rel_oid: OID,
    key_comparator_func: *const (),
) -> *const IndexPtr {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let key_comparator_func: extern "C" fn(*const u8, c_uint, *const u8, c_uint) -> c_int =
        unsafe { std::mem::transmute(key_comparator_func) };

    let key_comparator = move |a: &[u8], b: &[u8]| {
        let result =
            key_comparator_func(a.as_ptr(), a.len() as c_uint, b.as_ptr(), b.len() as c_uint);

        match result {
            -1 => Ok(std::cmp::Ordering::Less),
            0 => Ok(std::cmp::Ordering::Equal),
            1 => Ok(std::cmp::Ordering::Greater),
            _ => Err(Error::InvalidArgument(
                "cannot compare index keys".to_owned(),
            )),
        }
    };

    let index = match db.create_index(db_oid, rel_oid, key_comparator) {
        Ok(index) => index,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(index))
}

#[no_mangle]
pub extern "C" fn sq_open_index(
    db: *const DB,
    db_oid: OID,
    rel_oid: OID,
    key_comparator_func: *const (),
) -> *const IndexPtr {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let key_comparator_func: extern "C" fn(*const u8, c_uint, *const u8, c_uint) -> c_int =
        unsafe { std::mem::transmute(key_comparator_func) };

    let key_comparator = move |a: &[u8], b: &[u8]| {
        let result =
            key_comparator_func(a.as_ptr(), a.len() as c_uint, b.as_ptr(), b.len() as c_uint);

        match result {
            -1 => Ok(std::cmp::Ordering::Less),
            0 => Ok(std::cmp::Ordering::Equal),
            1 => Ok(std::cmp::Ordering::Greater),
            _ => Err(Error::InvalidArgument(
                "cannot compare index keys".to_owned(),
            )),
        }
    };

    let index = match db.open_index(db_oid, rel_oid, key_comparator) {
        Ok(Some(index)) => index,
        Ok(None) => {
            return std::ptr::null();
        }
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(index))
}

#[no_mangle]
pub extern "C" fn sq_free_index(index: *const IndexPtr) {
    if index.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(index as *mut IndexPtr);
    }
}

#[no_mangle]
pub extern "C" fn sq_index_insert(
    index: *const IndexPtr,
    db: *const DB,
    key: *const u8,
    length: c_int,
    item_pointer: *const ItemPointer,
) {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let index = unsafe {
        assert!(!index.is_null());
        &*index
    };

    let item_pointer = unsafe {
        assert!(!item_pointer.is_null());
        *item_pointer
    };

    let key = unsafe { std::slice::from_raw_parts(key, length as usize) };

    match index.insert(db, key, item_pointer) {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    };
}

#[no_mangle]
pub extern "C" fn sq_index_begin_scan<'a>(
    index: *const IndexPtr,
    db: *const DB,
    txn: *mut Transaction,
    table: *const TablePtr,
) -> *mut Box<dyn IndexScanIterator<'a> + 'a> {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let index: &IndexPtr = unsafe {
        assert!(!index.is_null());
        &*index
    };
    let txn: &mut Transaction = unsafe {
        assert!(!txn.is_null());
        &mut *txn
    };
    let table: &TablePtr = unsafe {
        assert!(!table.is_null());
        &*table
    };

    let iterator = match index.begin_scan(db, txn, &**table) {
        Ok(iterator) => iterator,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(iterator))
}

#[no_mangle]
pub extern "C" fn sq_free_index_scan_iterator<'a>(
    iterator: *mut Box<dyn IndexScanIterator<'a> + 'a>,
) {
    if iterator.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(iterator);
    }
}

#[no_mangle]
pub extern "C" fn sq_index_rescan<'a>(
    iterator: *mut Box<dyn IndexScanIterator<'a> + 'a>,
    db: *const DB,
    start_key: *const u8,
    length: c_int,
    predicate_func: *const (),
) {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let iterator: &mut Box<dyn IndexScanIterator<'a> + 'a> = unsafe {
        assert!(!iterator.is_null());
        &mut *iterator
    };

    let start_key = unsafe {
        if start_key.is_null() {
            None
        } else {
            Some(std::slice::from_raw_parts(start_key, length as usize))
        }
    };

    let predicate_func: extern "C" fn(*const u8, c_uint) -> c_int =
        unsafe { std::mem::transmute(predicate_func) };

    let predicate = IndexScanPredicate::new(move |a: &[u8]| {
        let result = predicate_func(a.as_ptr(), a.len() as c_uint);

        match result {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::InvalidArgument(
                "cannot match keys with predicates".to_owned(),
            )),
        }
    });

    match iterator.rescan(db, start_key, predicate) {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    };
}

#[no_mangle]
pub extern "C" fn sq_index_scan_next<'a>(
    iterator: *mut Box<dyn IndexScanIterator<'a> + 'a>,
    db: *const DB,
    dir: c_int,
) -> *const Box<dyn Tuple + 'a> {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };
    let iterator: &mut Box<dyn IndexScanIterator<'a> + 'a> = unsafe {
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
