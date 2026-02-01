use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{JoinHandle};

use crate::common::channel::Channel;
use crate::common::config::{DOCKBASE_PAGE_SIZE, PageId};
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

