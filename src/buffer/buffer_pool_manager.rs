use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::common::constants::PAGE_SIZE;
use crate::common::types::FrameId;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::sync::{Arc, Mutex, RwLock};

/// Buffer pool manager for this database system
/// Responsible for moving physical pages of data between memory and disk
/// Functions as a cache for keeping frequently used pages in memory for faster access
pub struct BufferPoolManager {
    /// Number of frames in the buffer pool
    num_frames: usize,
    /// The id of the next page to be allocated
    next_page_id: AtomicU32,
    ///
    bpm_latch: Arc<Mutex<()>>,
    /// The frame headers of the frames being managed
    frame_headers: Vec<Arc<FrameHeader>>,
    /// Maps from pages to buffer pool frames
    page_table: HashMap<u32, u32>,
    /// Tracks frames that do not contain any data
    free_frames: Vec<u32>,
    /// Finds unpinned pages to evict
    replacer: Arc<LRUKReplacer>,
    /// Pointer to disk scheduler. Shared with page guards for flushing
    disk_scheduler: Arc<DiskScheduler>,
    /// Pointer to log manager
    log_manager: Arc<LogManager>,
}

/// Manages a frame of memory and related metadata
/// Represents headers for frames of memory. The actual frames are not stored inside FrameHeader,
/// but the FrameHeader has a pointer to the data
struct FrameHeader {
    /// The frame id of the frame that this header represents
    frame_id: FrameId,
    /// Readers/writers latch for this frame
    rwlatch: RwLock<()>,
    /// Number of pins on this frame. If 0, this frame can be evicted.
    pin_count: AtomicUsize,
    /// Indicates if the data in this frame was modified
    is_dirty: bool,
    /// Pointer to data for this frame
    data: [u8; PAGE_SIZE],
}

impl FrameHeader {
    /// Creates a new frame header and initializes all fields to default values
    ///
    /// # Arguments
    /// * `frame_id` - The frame id of the frame that this header represents
    fn new(frame_id: FrameId) -> FrameHeader {
        FrameHeader {
            frame_id: frame_id,
            rwlatch: RwLock::new(()),
            pin_count: AtomicUsize::new(0),
            is_dirty: false,
            data: [0; PAGE_SIZE],
        }
    }

    /// Returns data for this frame
    fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Returns data for this frame that is mutable
    fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    /// Resets the FrameHeader's fields
    fn reset(&mut self) {
        self.data = [0; PAGE_SIZE];
        self.pin_count = AtomicUsize::new(0);
        self.is_dirty = false;
    }
}

impl BufferPoolManager {
    /// Creates a new buffer pool manager and initializes all fields
    ///
    /// # Arguments
    /// * `num_frames` - The size of the buffer pool
    /// * `disk_managaer` - The disk manager
    /// * `k_dist` - The backward k distance for the LRU K replacer
    /// * `log_managaer` - The log manager
    pub fn new(
        num_frames: usize,
        disk_manager: Arc<DiskManager>,
        k_dist: usize,
        log_manager: Arc<LogManager>,
    ) -> BufferPoolManager {
        let bpm_latch = Arc::new(Mutex::new(()));
        let replacer = Arc::new(LRUKReplacer::new(num_frames, k_dist));
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let frame_headers = (0..num_frames)
            .map(|i| Arc::new(FrameHeader::new(i as FrameId)))
            .collect::<Vec<_>>();
        let free_frames = (0..num_frames as u32).collect::<Vec<_>>();
        let page_table = HashMap::with_capacity(num_frames);

        {
            // Equivalent to C++'s std::scoped_lock
            let _guard = bpm_latch.lock().unwrap();
            // Optional early locking logic
        }

        Self {
            num_frames,
            next_page_id: std::sync::atomic::AtomicU32::new(0),
            bpm_latch,
            replacer,
            disk_scheduler,
            log_manager,
            frame_headers,
            page_table,
            free_frames,
        }
    }
}
