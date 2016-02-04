extern crate libc;

use rocksdb_ffi;
use rocksdb_ffi::{error_message};

use self::libc::{c_int, c_char, c_void};
use std::ffi::{CString};

#[derive(Copy, Clone)]
#[repr(C)]
pub struct rocksdb_backup_engine_t(*const c_void);

#[derive(Copy, Clone)]
#[repr(C)]
pub struct rocksdb_restore_options_t(*const c_void);

#[derive(Copy, Clone)]
#[repr(C)]
struct rocksdb_backup_engine_info_t(*const c_void);

unsafe impl Send for rocksdb_backup_engine_t {}
unsafe impl Sync for rocksdb_backup_engine_t {}

#[link(name = "rocksdb")]
extern {
    fn rocksdb_restore_options_create() -> rocksdb_restore_options_t;
    fn rocksdb_restore_options_destroy(opt: rocksdb_restore_options_t);
    fn rocksdb_restore_options_set_keep_log_files(opt: rocksdb_restore_options_t, v: c_int);

    fn rocksdb_backup_engine_open(
        options: rocksdb_ffi::DBOptions,
        path: *const c_char,
        err: *mut *const i8) -> rocksdb_backup_engine_t;
    fn rocksdb_backup_engine_create_new_backup(
        be: rocksdb_backup_engine_t,
        db: rocksdb_ffi::DBInstance,
        err: *mut *const i8);

    fn rocksdb_backup_engine_close(be: rocksdb_backup_engine_t);
    fn rocksdb_backup_engine_restore_db_from_latest_backup(
        be: rocksdb_backup_engine_t,
        db_dir: *const c_char,
        wal_dir: *const c_char,
        restore_options: rocksdb_restore_options_t,
        err: *mut *const i8);
}

pub fn restore_options_create() -> rocksdb_restore_options_t {
    unsafe { rocksdb_restore_options_create() }
}

pub fn restore_options_destroy(opt: rocksdb_restore_options_t) {
    unsafe { rocksdb_restore_options_destroy(opt) }
}

pub fn restore_options_set_keep_log_files(opt: rocksdb_restore_options_t, v: c_int) {
    unsafe { rocksdb_restore_options_set_keep_log_files(opt, v) }
}

pub fn backup_engine_open(db_options: rocksdb_ffi::DBOptions, path: &str) -> Result<rocksdb_backup_engine_t, String> {
    let cpath = match CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => return Err("Failed to convert path to CString when opening backup".to_string()),
    };

    let mut err: *const i8 = 0 as *const i8;
    let err_ptr: *mut *const i8 = &mut err;
    let back_up_engine: rocksdb_backup_engine_t;

    unsafe {
        back_up_engine = rocksdb_backup_engine_open(db_options, cpath.as_ptr(), err_ptr);
    }
    if !err.is_null() {
        return Err(error_message(err));
    }

    Ok(back_up_engine)
}

pub fn backup_engine_close(be: rocksdb_backup_engine_t) {
    unsafe { rocksdb_backup_engine_close(be) }
}

pub fn backup_engine_create_new_backup(be: rocksdb_backup_engine_t, db: rocksdb_ffi::DBInstance) -> Result<(),String> {
    let mut err: *const i8 = 0 as *const i8;
    let err_ptr: *mut *const i8 = &mut err;

    unsafe {
        rocksdb_backup_engine_create_new_backup(be, db, err_ptr)
    }

    if !err.is_null() {
        return Err(error_message(err));
    }

    Ok(())
}

pub fn backup_engine_restore_from_latest_backup(be: rocksdb_backup_engine_t, db_dir: &str, wal_dir: &str, keep_wal: bool) -> Result<(), String> {
    let c_db_dir = CString::new(db_dir.as_bytes()).unwrap();
    let c_wal_dir = CString::new(wal_dir.as_bytes()).unwrap();

    let mut err: *const i8 = 0 as *const i8;
    let err_ptr: *mut *const i8 = &mut err;

    unsafe {
        let restore_options = restore_options_create();

        if keep_wal {
            restore_options_set_keep_log_files(restore_options, 1)
        } else {
            restore_options_set_keep_log_files(restore_options, 0)
        }

        rocksdb_backup_engine_restore_db_from_latest_backup(be,
                                                            c_db_dir.as_ptr(),
                                                            c_wal_dir.as_ptr(),
                                                            restore_options,
                                                            err_ptr)
    }

    if !err.is_null() {
        return Err(error_message(err));
    }

    Ok(())
}
