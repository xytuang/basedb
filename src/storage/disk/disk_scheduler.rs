use crate::{
    common::{constants::PAGE_SIZE, types::PageId},
    storage::disk::disk_manager::DiskManager,
};
use std::sync::Arc;

struct DiskRequest {
    is_write: bool,
    data: [u8; PAGE_SIZE],
    page_id: PageId,
}

pub struct DiskScheduler {
    disk_manager: Arc<DiskManager>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> DiskScheduler {
        DiskScheduler {
            disk_manager: disk_manager,
        }
    }
}
