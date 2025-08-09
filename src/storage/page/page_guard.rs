use std::sync::{mpsc::channel, Arc, RwLock};

use crate::{
    buffer::{buffer_pool_manager::FrameHeader, lru_k_replacer::LRUKReplacer},
    common::{config::PageId, config::BASEDB_PAGE_SIZE},
    storage::disk::disk_scheduler::{DiskRequest, DiskScheduler},
};
/// Guarantees thread safe read access to a page of data.
/// With a ReadPageGuard there can be multiple threads that share access to a page's data.
/// However, the existence of any ReadPageGuard on a page implies that no thread can mutate that page's data.
pub struct ReadPageGuard {
    /// Page id of the page we are guarding.
    page_id: PageId,
    /// The frame that holds the page this guard is protecting.
    frame: Arc<FrameHeader>,
    /// A shared pointer to the buffer pool's replacer.
    ///
    /// Since the buffer pool cannot know when this page guard gets destroyed,
    /// we maintain a pointer to the replacer to set the frame as evictable on destruction
    replacer: Arc<LRUKReplacer>,
    /// A shared pointer to the disk scheduler.
    ///
    /// Used when flushing pages to disk.
    disk_scheduler: Arc<DiskScheduler>,
}

/// Guarantees thread safe write access to a page of data.
/// With a WritePageGuard there can be only 1 thread with access to a page's data.
/// However, the existence of any WritePageGuard on a page implies that there can be no other WritePageGuard or ReadPageGuard for this page.
pub struct WritePageGuard {
    /// Page id of the page we are guarding.
    page_id: PageId,
    /// The frame that holds the page this guard is protecting.
    frame: Arc<FrameHeader>,
    /// A shared pointer to the buffer pool's replacer.
    ///
    /// Since the buffer pool cannot know when this page guard gets destroyed,
    /// we maintain a pointer to the replacer to set the frame as evictable on destruction
    replacer: Arc<LRUKReplacer>,
    /// A shared pointer to the disk scheduler.
    ///
    /// Used when flushing pages to disk.
    disk_scheduler: Arc<DiskScheduler>,
}

impl ReadPageGuard {
    /// Creates a new ReadPageGuard
    pub fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        replacer: Arc<LRUKReplacer>,
        disk_scheduler: Arc<DiskScheduler>,
    ) -> ReadPageGuard {
        ReadPageGuard {
            page_id: page_id,
            frame: Arc::clone(&frame),
            replacer: Arc::clone(&replacer),
            disk_scheduler: Arc::clone(&disk_scheduler),
        }
    }

    /// Get the page id of the page we are guarding
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Get pointer to the data this page guard is protecting
    pub fn get_data(&self) -> Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>> {
        self.frame.get_data()
    }

    /// Returns true if this page is dirty ie modified but not flushed to disk
    pub fn is_dirty(&self) -> bool {
        self.frame.get_dirty()
    }

    /// Flushes this page's data safely to disk
    pub fn flush(&self) {
        let (sender, receiver) = channel();
        let disk_request = DiskRequest::ReadWrite {
            is_write: true,
            data: self.frame.get_data(),
            page_id: self.page_id,
            callback: Some(sender),
        };

        self.disk_scheduler.schedule(disk_request).unwrap();

        assert!(receiver.recv().unwrap())
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        let remaining = self.frame.unpin();
        if remaining == 0 {
            let _ = self.replacer.set_evictable(self.frame.get_frame_id(), true);
        }
    }
}

impl WritePageGuard {
    /// Creates a new ReadPageGuard
    pub fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        replacer: Arc<LRUKReplacer>,
        disk_scheduler: Arc<DiskScheduler>,
    ) -> WritePageGuard {
        WritePageGuard {
            page_id: page_id,
            frame: frame,
            replacer: replacer,
            disk_scheduler: disk_scheduler,
        }
    }

    /// Get the page id of the page we are guarding
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Get pointer to the data this page guard is protecting
    pub fn get_data(&self) -> Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>> {
        self.frame.get_data()
    }

    /// Returns true if this page is dirty ie modified but not flushed to disk
    pub fn is_dirty(&self) -> bool {
        self.frame.get_dirty()
    }

    pub fn set_dirty(&self) {
        self.frame.set_dirty(true);
    }

    /// Flushes this page's data safely to disk
    pub fn flush(&self) {
        let (sender, receiver) = channel();
        let disk_request = DiskRequest::ReadWrite {
            is_write: true,
            data: self.frame.get_data(),
            page_id: self.page_id,
            callback: Some(sender),
        };

        self.disk_scheduler.schedule(disk_request).unwrap();

        assert!(receiver.recv().unwrap())
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        let remaining = self.frame.unpin();
        if remaining == 0 {
            let _ = self.replacer.set_evictable(self.frame.get_frame_id(), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc};

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        common::config::PageId,
        recovery::log_manager::LogManager,
        storage::{
            disk::{self, disk_manager::DiskManager},
            page::page_guard::WritePageGuard,
        },
    };

    #[test]
    fn test_page_guard() {
        let FRAMES = 10;
        let K_DIST = 2;
        let db_file_name = Path::new("test_page_guard.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        );

        {
            let pid0 = bpm.new_page();
            let page0 = bpm.write_page(pid0).unwrap();

            assert_eq!(bpm.get_pin_count(pid0), Some(1));

            drop(page0);
            assert_eq!(bpm.get_pin_count(pid0), Some(0));
        }

        let pid1 = bpm.new_page();
        let pid2 = bpm.new_page();

        {
            let read_guarded_page = bpm.read_page(pid1);
            let write_guarded_page = bpm.read_page(pid2);

            assert_eq!(bpm.get_pin_count(pid1), Some(1));
            assert_eq!(bpm.get_pin_count(pid2), Some(1));

            drop(read_guarded_page);
            drop(write_guarded_page);

            assert_eq!(bpm.get_pin_count(pid1), Some(0));
            assert_eq!(bpm.get_pin_count(pid2), Some(0));
        }

        {
            let write_test1 = bpm.write_page(pid1);
            let write_test2 = bpm.write_page(pid2);
        }

        let mut page_ids: Vec<PageId> = Vec::new();
        {
            let mut guards: Vec<WritePageGuard> = Vec::new();

            for _ in 1..=FRAMES {
                let new_pid = bpm.new_page();
                guards.push(bpm.write_page(new_pid).unwrap());
                assert_eq!(bpm.get_pin_count(new_pid), Some(1));
                page_ids.push(new_pid);
            }
        } // Guards should be dropped here, pin count should be 0 for all pids

        for pid in page_ids {
            assert_eq!(bpm.get_pin_count(pid), Some(0));
        }

        // Get a new write page and edit it
        let mutable_pid = bpm.new_page();
        {
            let mutable_guard = bpm.write_page(mutable_pid).unwrap();
            let data = mutable_guard.get_data();
            let mut data_guard = data.write().unwrap();
            data_guard[0..4].copy_from_slice(b"data");
            mutable_guard.set_dirty();
            dbg!(&data_guard[0..4]);
        }

        {
            // Fill up the BPM again.
            let mut guards: Vec<WritePageGuard> = Vec::new();
            for _ in 1..=FRAMES {
                let new_pid = bpm.new_page();
                guards.push(bpm.write_page(new_pid).unwrap());
                assert_eq!(bpm.get_pin_count(new_pid), Some(1));
            }
        }

        let immutable_guard = bpm.read_page(mutable_pid).unwrap();
        {
            let data = immutable_guard.get_data();
            let data_guard = data.read().unwrap();
            dbg!(&data_guard[0..4]);
            assert_eq!(&data_guard[0..4], b"data");
        }

        let _ = fs::remove_file(disk_manager.get_db_file_name());
        let _ = fs::remove_file(disk_manager.get_log_file_name());
    }
}
