use crate::{
    common::{
        config::{PageId, BASEDB_PAGE_SIZE},
        errors::{DiskSchedulerError, InternalError},
    },
    storage::disk::disk_manager::DiskManager,
};
use std::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

/// Represents a Read/Write request for the DiskManager to execute
pub enum DiskRequest {
    ReadWrite {
        /// Flag indicating if this is a read/write request
        is_write: bool,
        /// Area to read data into from disk, OR write to disk
        data: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>>,
        /// ID of page that is being read from/written to disk
        page_id: PageId,
        /// Callback to signal to request issuer when the request completes
        callback: Option<Sender<bool>>,
    },
    Shutdown,
}

/// Schedules read/write operations
/// A request can be scheduled using the schedule function.
/// The scheduler has a background thread that processes requests using the disk manager.
/// The background thread is created in new and destroyed in shutdown
pub struct DiskScheduler {
    /// Reference to DiskManager
    disk_manager: Arc<DiskManager>,
    /// Sender end of channel to schedule requests
    sender: Sender<DiskRequest>,
    /// Background thread responsible for issuing requests to DiskManager
    background_thread: Option<JoinHandle<()>>,
}

impl DiskScheduler {
    /// Creates a new DiskScheduler
    pub fn new(disk_manager: Arc<DiskManager>) -> DiskScheduler {
        let (sender, receiver) = channel();
        let disk_manager_clone = disk_manager.clone();
        let background_thread =
            thread::spawn(move || Self::start_background_thread(disk_manager_clone, receiver));
        DiskScheduler {
            disk_manager: disk_manager,
            sender: sender,
            background_thread: Some(background_thread),
        }
    }

    pub fn deallocate_page(&self, page_id: PageId) {
        self.disk_manager.delete_page(page_id).unwrap();
    }

    /// Destroys the DiskScheduler and stops the background thread inside it
    pub fn shutdown(&mut self) {
        let _ = self.sender.send(DiskRequest::Shutdown);
        if let Some(handle) = self.background_thread.take() {
            let _ = handle.join();
        }
    }

    /// Schedules a request for the DiskManager to execute
    pub fn schedule(&self, disk_request: DiskRequest) -> Result<(), DiskSchedulerError> {
        self.sender
            .send(disk_request)
            .map_err(|_| DiskSchedulerError::Internal(InternalError::ChannelError))
    }

    /// Called when DiskScheduler is created to spawn the background thread
    fn start_background_thread(disk_manager: Arc<DiskManager>, receiver: Receiver<DiskRequest>) {
        while let Ok(disk_request) = receiver.recv() {
            match disk_request {
                DiskRequest::ReadWrite {
                    is_write,
                    mut data,
                    page_id,
                    callback,
                } => {
                    let result = if is_write {
                        disk_manager.write_page(page_id, data)
                    } else {
                        disk_manager.read_page(page_id, data)
                    };
                    if let Some(cb) = callback {
                        let _ = cb.send(result.is_ok()); // is_ok converts the Ok result from read/write page to a bool
                    }
                }
                DiskRequest::Shutdown => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::disk::disk_scheduler::{DiskRequest, DiskScheduler};
    use crate::{common::config::BASEDB_PAGE_SIZE, storage::disk::disk_manager::DiskManager};
    use std::fs;
    use std::path::Path;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_disk_scheduler() {
        let buf: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>> =
            Arc::new(RwLock::new([0; BASEDB_PAGE_SIZE as usize]));
        let data: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>> =
            Arc::new(RwLock::new([0; BASEDB_PAGE_SIZE as usize]));
        let db_file_name = Path::new("test_diskscheduler.db");

        let test_string = b"A test string";
        {
            let mut guard = data.write().unwrap();
            guard[0..13].copy_from_slice(test_string);
        }

        let _dm = DiskManager::new(db_file_name).unwrap();
        let dm = Arc::new(_dm);
        let mut disk_scheduler = DiskScheduler::new(Arc::clone(&dm));

        let (sender_1, receiver_1) = channel::<bool>();
        let (sender_2, receiver_2) = channel::<bool>();
        let disk_request_1 = DiskRequest::ReadWrite {
            is_write: true,
            data: Arc::clone(&data),
            page_id: 0,
            callback: Some(sender_1),
        };
        let disk_request_2 = DiskRequest::ReadWrite {
            is_write: false,
            data: Arc::clone(&buf),
            page_id: 0,
            callback: Some(sender_2),
        };

        disk_scheduler.schedule(disk_request_1).unwrap();
        disk_scheduler.schedule(disk_request_2).unwrap();

        assert!(receiver_1.recv().unwrap());
        assert!(receiver_2.recv().unwrap());

        let buf_guard = buf.read().unwrap();
        let data_guard = data.read().unwrap();
        assert_eq!(*buf_guard, *data_guard);
        disk_scheduler.shutdown();
        let _ = fs::remove_file(dm.get_db_file_name());
        let _ = fs::remove_file(dm.get_log_file_name());
    }
}
