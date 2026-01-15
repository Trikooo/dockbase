use std::{collections::HashMap, fs::File, path::PathBuf, sync::{Mutex, MutexGuard}};

use crate::common::{config::PageId, exception::Exception};

pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    db_io: Mutex<File>,
    log_io: Mutex<File>,
    metadata: Mutex<Metadata>,
}

struct Metadata {
    num_flushes: i32,
    num_writes: i32,
    num_deletes: i32,
    page_count: usize,
    page_capacity: usize,
    pages: HashMap<PageId, usize>,
    free_slots: Vec<usize>,
    flush_log: bool,
    flush_log_f: Option<()>,
}

impl DiskManager {
    pub fn new(_db_file_name: PathBuf) -> Result<Self, Exception> {
        unimplemented!()
    }

    pub fn shut_down(&self) -> Result<(), Exception> {
        unimplemented!()
    }

    pub fn write_page(&mut self, _page_id: PageId, _page_data: &[u8]) -> Result<(), Exception> {
        unimplemented!()
    }

    pub fn read_page(&mut self, _page_id: PageId, _page_data: &mut [u8]) {
        unimplemented!()
    }

    pub fn delete_page(&mut self, _page_id: PageId) {
        unimplemented!()
    }

    pub fn write_log(&mut self, _log_data: &[u8]) {
        unimplemented!()
    }

    pub fn read_log(&mut self, _log_data: &mut [u8], _size: usize, _offset: usize) -> bool {
        unimplemented!()
    }

    pub fn get_num_flushes(&self) -> i32 {
        unimplemented!()
    }

    pub fn get_flush_state(&self) -> bool {
        unimplemented!()
    }

    pub fn get_num_writes(&self) -> i32 {
        unimplemented!()
    }

    pub fn get_num_deletes(&self) -> i32 {
        unimplemented!()
    }

    pub fn set_flush_log_future(&mut self, _f: ()) {
        unimplemented!()
    }

    pub fn has_flush_log_future(&self) -> bool {
        unimplemented!()
    }

    pub fn get_log_file_name(&self) -> &PathBuf {
        unimplemented!()
    }

    pub fn get_db_file_size(&self) -> isize {
        unimplemented!()
    }

    fn get_file_size(&self, _file_name: &str) -> isize {
        unimplemented!()
    }

    fn allocate_page(
        &self,
        _metadata_guard: &mut MutexGuard<'_, Metadata>,
    ) -> Result<usize, Exception> {
        unimplemented!()
    }
}
