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
    pub fn schedule(&self, requests: Vec<DiskRequest>) -> Result<(), Exception>{
      for request in requests {
        self.request_queue.put(Some(request))?;
      }
      Ok(())
    }
    pub fn start_worker_thread(
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
