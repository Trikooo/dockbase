use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use crate::common::{
    config::{DEFAULT_DB_IO_SIZE, DOCKBASE_PAGE_SIZE, PageId},
    exception::Exception,
};

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
struct AllocationGuard<'a> {
    metadata: &'a Mutex<Metadata>,
    offset: usize,
    is_new: bool,
    active: bool,
}

impl DiskManager {
    pub fn new(db_file_name: PathBuf) -> Result<Self, Exception> {
        let stem = db_file_name
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or(Exception::Invalid("Invalid filename"))?;
        let log_file_name = format!("{stem}.log").into();

        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_name)?;
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_file_name)?;
        let _ = db_io.set_len(((DEFAULT_DB_IO_SIZE + 1) * DOCKBASE_PAGE_SIZE) as u64);
        Ok(Self {
            db_file_name,
            log_file_name,
            db_io: Mutex::new(db_io),
            log_io: Mutex::new(log_io),
            metadata: Mutex::new(Metadata {
                num_flushes: 0,
                num_writes: 0,
                num_deletes: 0,
                page_count: 0,
                page_capacity: DEFAULT_DB_IO_SIZE,
                pages: HashMap::new(),
                free_slots: Vec::new(),
                flush_log: false,
                flush_log_f: None,
            }),
        })
    }

    pub fn shut_down(&self) -> Result<(), Exception> {
        let db_guard = self.db_io.lock()?;
        drop(db_guard);
        let log_guard = self.log_io.lock()?;
        drop(log_guard);
        Ok(())
    }

    pub fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<(), Exception> {
        let mut metadata_guard = self.metadata.lock()?;
        let (offset, is_new) = match metadata_guard.pages.get(&page_id) {
            Some(&off) => (off, false),
            None => (self.allocate_page(&mut metadata_guard)?, true),
        };
        drop(metadata_guard);

        let mut cleanup_guard = AllocationGuard::new(&self.metadata, offset, is_new);

        let mut db_io_guard = self.db_io.lock()?;
        db_io_guard.seek(SeekFrom::Start(offset as u64))?;
        db_io_guard.write_all(page_data)?;
        db_io_guard.flush()?;

        let mut metadata_guard = self.metadata.lock()?;
        metadata_guard.pages.insert(page_id, offset);
        metadata_guard.num_writes += 1;

        cleanup_guard.commit();
        Ok(())
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
        metadata_guard: &mut MutexGuard<'_, Metadata>,
    ) -> Result<usize, Exception> {
        if let Some(offset) = metadata_guard.free_slots.pop() {
            return Ok(offset);
        }

        let offset = metadata_guard.page_count * DOCKBASE_PAGE_SIZE;
        metadata_guard.page_count += 1;

        if metadata_guard.page_count > metadata_guard.page_capacity {
            metadata_guard.page_capacity *= 2;
            let new_size = (metadata_guard.page_capacity * DOCKBASE_PAGE_SIZE) as u64;
            self.db_io.lock()?.set_len(new_size)?;
        }
        Ok(offset)
    }
}

impl<'a> AllocationGuard<'a> {
    fn new(metadata: &'a Mutex<Metadata>, offset: usize, is_new: bool) -> Self {
        Self {
            metadata,
            offset,
            is_new,
            active: true,
        }
    }
    fn commit(&mut self) {
        self.active = false;
    }
}

impl Drop for AllocationGuard<'_> {
    fn drop(&mut self) {
        if self.active && self.is_new {
            if let Ok(mut metadata_guard) = self.metadata.lock() {
                metadata_guard.free_slots.push(self.offset);
            }
        }
    }
}
