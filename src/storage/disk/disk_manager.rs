use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
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
        db_io.set_len(((DEFAULT_DB_IO_SIZE + 1) * DOCKBASE_PAGE_SIZE) as u64)?;
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

    pub fn write_page(&self, page_id: PageId, page_data: &[u8]) -> Result<(), Exception> {
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

    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8]) -> Result<(), Exception> {
        let metadata_guard = self.metadata.lock()?;
        let &offset = metadata_guard
            .pages
            .get(&page_id)
            .ok_or(Exception::Invalid("Page not found in disk mapping"))?;
        drop(metadata_guard);
        let mut db_io_guard = self.db_io.lock()?;
        let file_size = db_io_guard.metadata()?.len();
        if offset as u64 >= file_size {
            return Err(Exception::IO("Read offset past end of file"));
        }
        db_io_guard.seek(SeekFrom::Start(offset as u64))?;

        let mut bytes_total: usize = 0;
        while bytes_total < page_data.len() {
            let bytes = db_io_guard.read(&mut page_data[bytes_total..])?;
            if bytes == 0 {
                break; // EOF reached
            }
            bytes_total += bytes;
        }
        if bytes_total < DOCKBASE_PAGE_SIZE {
            page_data[bytes_total..].fill(0)
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> Result<(), Exception> {
        let mut metadata_guard = self.metadata.lock()?;
        if let Some(offset) = metadata_guard.pages.remove(&page_id) {
            metadata_guard.free_slots.push(offset);
            metadata_guard.num_deletes += 1;
        }
        Ok(())
    }

    pub fn write_log(&self, log_data: &[u8]) -> Result<(), Exception> {
        if log_data.is_empty() {
            return Ok(());
        }
        {
            let mut metadata_guard = self.metadata.lock()?;
            metadata_guard.flush_log = true;
        }

        let mut log_io_guard = self.log_io.lock()?;
        log_io_guard.write_all(log_data)?;
        log_io_guard.flush()?;

        let mut metadata_guard = self.metadata.lock()?;
        metadata_guard.num_flushes += 1;
        metadata_guard.flush_log = false;
        Ok(())
    }

    pub fn read_log(&self, log_data: &mut [u8], offset: usize) -> Result<bool, Exception> {
        let mut log_io_guard = self.log_io.lock()?;

        let file_size = log_io_guard.metadata()?.len() as usize;
        if offset >= file_size {
            return Ok(false);
        }

        log_io_guard.seek(SeekFrom::Start(offset as u64))?;

        let size = log_data.len();
        let mut bytes_total = 0;
        while bytes_total < size {
            let bytes = log_io_guard.read(&mut log_data[bytes_total..])?;
            if bytes == 0 {
                break;
            }
            bytes_total += bytes;
        }

        if bytes_total < size {
            log_data[bytes_total..].fill(0);
        }

        Ok(true)
    }
    pub fn get_num_flushes(&self) -> Result<i32, Exception> {
        Ok(self.metadata.lock()?.num_flushes)
    }

    pub fn get_log_flush_state(&self) -> Result<bool, Exception> {
        Ok(self.metadata.lock()?.flush_log)
    }

    pub fn get_num_writes(&self) -> Result<i32, Exception> {
        Ok(self.metadata.lock()?.num_writes)
    }

    pub fn get_num_deletes(&self) -> Result<i32, Exception> {
        Ok(self.metadata.lock()?.num_deletes)
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn setup(db_name: &str) -> (DiskManager, PathBuf, PathBuf) {
        let db_path = PathBuf::from(db_name);
        let log_path = PathBuf::from(format!(
            "{}.log",
            db_path.file_stem().unwrap().to_str().unwrap()
        ));
        let _ = fs::remove_file(&db_path);
        let _ = fs::remove_file(&log_path);
        (
            DiskManager::new(db_path.clone()).unwrap(),
            db_path,
            log_path,
        )
    }

    fn teardown(db_path: PathBuf, log_path: PathBuf) {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(log_path);
    }

    #[test]
    fn test_page_read_write() -> Result<(), Exception> {
        let (disk_manager, db_path, log_path) = setup("test_rw.db");
        let page_id = 10;
        let mut content = [0u8; DOCKBASE_PAGE_SIZE];
        content[0..5].copy_from_slice(b"hello");

        disk_manager.write_page(page_id, &content)?;
        let mut read_buffer = [0u8; DOCKBASE_PAGE_SIZE];
        disk_manager.read_page(page_id, &mut read_buffer)?;

        assert_eq!(content, read_buffer);
        assert_eq!(disk_manager.get_num_writes()?, 1);

        teardown(db_path, log_path);
        Ok(())
    }

    #[test]
    fn test_log_sequence() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_log.db");
        let entry1 = b"first_log_entry";
        let entry2 = b"second_entry";

        dm.write_log(entry1)?;
        let offset2 = entry1.len();
        dm.write_log(entry2)?;

        let mut buf1 = vec![0u8; entry1.len()];
        let mut buf2 = vec![0u8; entry2.len()];

        dm.read_log(&mut buf1, 0)?;
        dm.read_log(&mut buf2, offset2)?;

        assert_eq!(entry1, buf1.as_slice());
        assert_eq!(entry2, buf2.as_slice());
        assert_eq!(dm.get_num_flushes()?, 2);

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_delete_and_reuse() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_reuse.db");
        let data = [1u8; DOCKBASE_PAGE_SIZE];

        dm.write_page(1, &data)?;
        dm.delete_page(1)?;
        assert_eq!(dm.get_num_deletes()?, 1);

        dm.write_page(2, &data)?;
        let metadata = dm.metadata.lock().unwrap();
        assert_eq!(metadata.free_slots.len(), 0);
        assert_eq!(metadata.page_count, 1);

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_read_non_existent_page() {
        let (dm, db_p, log_p) = setup("test_err.db");
        let mut buf = [0u8; DOCKBASE_PAGE_SIZE];
        assert!(dm.read_page(99, &mut buf).is_err());
        teardown(db_p, log_p);
    }

    #[test]
    fn test_allocation_guard_rollback() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_rollback.db");

        let offset = {
            let mut metadata = dm.metadata.lock().unwrap();
            let offset = dm.allocate_page(&mut metadata)?;
            drop(metadata);
            let _guard = AllocationGuard::new(&dm.metadata, offset, true);
            offset
        };

        let metadata = dm.metadata.lock().unwrap();
        assert_eq!(metadata.free_slots.len(), 1);
        assert_eq!(metadata.free_slots[0], offset);

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_partial_read() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_partial.db");
        let page_id = 1;
        let mut content = [0u8; DOCKBASE_PAGE_SIZE];
        content[0..10].copy_from_slice(b"partial123");

        dm.write_page(page_id, &content)?;
        let mut read_buf = [0u8; DOCKBASE_PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf)?;

        assert_eq!(&read_buf[0..10], b"partial123");
        assert_eq!(&read_buf[10..], &[0u8; DOCKBASE_PAGE_SIZE - 10]);

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_log_read_beyond_eof() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_log_eof.db");
        let mut buf = [0u8; 10];
        let res = dm.read_log(&mut buf, 1000)?;
        assert!(!res);
        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_flush_log_flag() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_flush_flag.db");
        let log_data = b"flush_flag_test";

        {
            let metadata = dm.metadata.lock().unwrap();
            assert!(!metadata.flush_log);
        }

        dm.write_log(log_data)?;
        assert_eq!(dm.get_num_flushes()?, 1);
        assert_eq!(dm.get_log_flush_state()?, false);

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_shutdown() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_shutdown.db");
        dm.shut_down()?;
        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_concurrent_writes_reads() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_concurrent.db");
        let dm = Arc::new(dm); // Now using Arc directly because of &self
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for i in 0..4 {
            let dm_clone = Arc::clone(&dm);
            let barrier_clone = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut data = [0u8; DOCKBASE_PAGE_SIZE];
                data[0] = i as u8;
                barrier_clone.wait();

                dm_clone.write_page(i, &data).unwrap();

                let mut read_buf = [0u8; DOCKBASE_PAGE_SIZE];
                dm_clone.read_page(i, &mut read_buf).unwrap();
                assert_eq!(read_buf[0], i as u8);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        teardown(db_p, log_p);
        Ok(())
    }

    #[test]
    fn test_allocate_page_expansion() -> Result<(), Exception> {
        let (dm, db_p, log_p) = setup("test_expand.db");
        let mut metadata = dm.metadata.lock().unwrap();
        let initial_capacity = metadata.page_capacity;
        metadata.page_count = initial_capacity;
        drop(metadata);

        let offset = {
            let mut metadata = dm.metadata.lock().unwrap();
            dm.allocate_page(&mut metadata)?
        };

        let metadata = dm.metadata.lock().unwrap();
        assert!(metadata.page_capacity > initial_capacity);
        assert!(offset >= initial_capacity * DOCKBASE_PAGE_SIZE);

        teardown(db_p, log_p);
        Ok(())
    }
}
