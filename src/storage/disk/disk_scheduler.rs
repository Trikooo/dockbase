use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};

use crate::common::channel::Channel;
use crate::common::config::{DOCKBASE_PAGE_SIZE, PageId};
use crate::common::exception::Exception;
use crate::storage::disk::disk_manager::DiskManager;

pub enum RequestType {
    Read,
    Write,
}
pub struct DiskRequest {
    pub request_type: RequestType,
    pub data: *mut u8,
    pub page_id: PageId,
    pub callback: Sender<bool>,
}
pub struct DiskScheduler {
    disk_manager: Arc<DiskManager>,
    request_queue: Arc<Channel<Option<DiskRequest>>>,
    background_thread: Option<JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let request_queue = Arc::new(Channel::<Option<DiskRequest>>::new());

        let worker_disk_manager = disk_manager.clone();
        let worker_queue = request_queue.clone();

        let background_thread = thread::spawn(move || {
            Self::start_worker_thread(worker_disk_manager, worker_queue);
        });
        Self {
            disk_manager,
            request_queue,
            background_thread: Some(background_thread),
        }
    }
    pub fn schedule(&self, mut requests: Vec<DiskRequest>) -> Result<(), Exception> {
        for request in requests.drain(..) {
            self.request_queue.put(Some(request))?;
        }
        Ok(())
    }
    fn start_worker_thread(
        disk_manager: Arc<DiskManager>,
        queue: Arc<Channel<Option<DiskRequest>>>,
    ) {
        while let Ok(Some(request)) = queue.get() {
            let page_data =
                unsafe { std::slice::from_raw_parts_mut(request.data, DOCKBASE_PAGE_SIZE) };
            let result = match request.request_type {
                RequestType::Read => disk_manager.read_page(request.page_id, page_data),
                RequestType::Write => disk_manager.write_page(request.page_id, page_data),
            };
            let _ = request.callback.send(result.is_ok());
        }
    }
}
unsafe impl Send for DiskRequest {}
unsafe impl Sync for DiskRequest {}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        // We ignore the result because if the queue is poisoned,
        // the thread is likely already dead.
        let _ = self.request_queue.put(None);
        if let Some(handle) = self.background_thread.take() {
            let _ = handle.join();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, remove_file},
        path::PathBuf,
        sync::mpsc,
    };
    fn setup(db_name: &str) -> (DiskScheduler, PathBuf, PathBuf) {
        let db_path = PathBuf::from(db_name);
        let log_path = PathBuf::from(format!(
            "{}.log",
            db_path.file_stem().unwrap().to_str().unwrap()
        ));
        let _ = remove_file(&db_path);
        let _ = remove_file(&log_path);

        let disk_manager = Arc::new(DiskManager::new(db_path.clone()).unwrap());
        let disk_scheduler = DiskScheduler::new(disk_manager);
        (disk_scheduler, db_path, log_path)
    }

    fn teardown(db_path: PathBuf, log_path: PathBuf) {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(log_path);
    }

    #[test]
    fn test_basic_read_write() {
        let (disk_scheduler, db_path, log_path) = setup("test_scheduler_rw.db");
        let page_id: PageId = 10;
        let mut buffer = [0; DOCKBASE_PAGE_SIZE];
        let message = b"Hello Dockbase";
        buffer[..message.len()].copy_from_slice(message);
        let (tx, rx) = mpsc::channel::<bool>();
        let write_request = DiskRequest {
            request_type: RequestType::Write,
            data: buffer.as_mut_ptr(),
            page_id,
            callback: tx.clone(),
        };
        let _ = disk_scheduler.schedule(vec![write_request]);
        assert!(rx.recv().unwrap());
        let mut read_buffer = [0; DOCKBASE_PAGE_SIZE];
        let read_request = DiskRequest {
            request_type: RequestType::Read,
            data: read_buffer.as_mut_ptr(),
            page_id,
            callback: tx.clone(),
        };
        let _ = disk_scheduler.schedule(vec![read_request]);
        assert!(rx.recv().unwrap());
        assert_eq!(&read_buffer[..message.len()], message);
        teardown(db_path, log_path);
    }

    #[test]
    fn test_scheduler_stress() {
        let (disk_scheduler, db_path, log_path) = setup("test_scheduler_stress.db");
        let num_pages = 100000;
        let (tx, rx) = mpsc::channel::<bool>();

        let mut buffers = Vec::new();
        let mut requests = Vec::new();

        for i in 0..num_pages {
            let mut buffer = Box::new([0u8; DOCKBASE_PAGE_SIZE]);
            let msg = format!("Data for page {}", i);
            buffer[..msg.len()].copy_from_slice(msg.as_bytes());

            requests.push(DiskRequest {
                request_type: RequestType::Write,
                data: buffer.as_mut_ptr(),
                page_id: i as PageId,
                callback: tx.clone(),
            });
            buffers.push(buffer); // Keep buffers alive on the heap
        }

        disk_scheduler.schedule(requests).unwrap();

        for _ in 0..num_pages {
            assert!(rx.recv().unwrap());
        }

        // Read back a few random pages
        let test_pages = vec![0, 50, 99];
        for pid in test_pages {
            let mut read_buf = [0u8; DOCKBASE_PAGE_SIZE];
            let (read_tx, read_rx) = mpsc::channel();

            disk_scheduler
                .schedule(vec![DiskRequest {
                    request_type: RequestType::Read,
                    data: read_buf.as_mut_ptr(),
                    page_id: pid as PageId,
                    callback: read_tx,
                }])
                .unwrap();

            assert!(read_rx.recv().unwrap());
            let expected_msg = format!("Data for page {}", pid);
            assert_eq!(&read_buf[..expected_msg.len()], expected_msg.as_bytes());
        }

        teardown(db_path, log_path);
    }
    #[test]
    fn test_scheduler_concurrency_and_error() {
        let (disk_scheduler, db_path, log_path) = setup("test_concurrency.db");
        let disk_scheduler = Arc::new(disk_scheduler); // Shadow with Arc for thread sharing
        let num_threads = 8;
        let requests_per_thread = 50;
        let mut thread_handles = vec![];

        for t in 0..num_threads {
            let thread_scheduler = Arc::clone(&disk_scheduler);
            let handle = thread::spawn(move || {
                let (tx, rx) = mpsc::channel();
                let mut buffer = [0u8; DOCKBASE_PAGE_SIZE];

                for i in 0..requests_per_thread {
                    let page_id = (t * requests_per_thread + i) as PageId;

                    let is_error_case = i == 25;
                    let target_page_id = if is_error_case { 999_999_999 } else { page_id };

                    let request = DiskRequest {
                        request_type: RequestType::Write,
                        data: buffer.as_mut_ptr(),
                        page_id: target_page_id,
                        callback: tx.clone(),
                    };

                    thread_scheduler.schedule(vec![request]).unwrap();
                    let success = rx.recv().unwrap();

                    if !is_error_case {
                        assert!(success);
                    }
                }
            });
            thread_handles.push(handle);
        }

        for handle in thread_handles {
            handle.join().unwrap();
        }
        teardown(db_path, log_path);
    }

    #[test]
    fn test_scheduler_drop_cleanup() {
        let (db_path, log_path) = {
            let (disk_scheduler, db_path, log_path) = setup("test_drop.db");

            let (tx, rx) = mpsc::channel();
            let mut buffer = [0u8; DOCKBASE_PAGE_SIZE];
            let request = DiskRequest {
                request_type: RequestType::Write,
                data: buffer.as_mut_ptr(),
                page_id: 0,
                callback: tx,
            };

            disk_scheduler.schedule(vec![request]).unwrap();
            assert!(rx.recv().unwrap());

            // disk_scheduler dropped here
            (db_path, log_path)
        };

        teardown(db_path, log_path);
    }
}
