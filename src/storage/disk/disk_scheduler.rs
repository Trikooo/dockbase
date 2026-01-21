use std::sync::mpsc::Sender;
use std::thread::JoinHandle;

use crate::common::channel::Channel;
use crate::common::config::PageId;
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
    disk_manager: DiskManager,
    request_queue: Channel<Option<DiskRequest>>,
    background_thread: Option<JoinHandle<()>>,
}
