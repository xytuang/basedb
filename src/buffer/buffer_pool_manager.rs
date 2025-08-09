use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::common::config::BASEDB_PAGE_SIZE;
use crate::common::config::{FrameId, PageId};
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::disk::disk_scheduler::{DiskRequest, DiskScheduler};
use crate::storage::page::page_guard::{ReadPageGuard, WritePageGuard};
use core::panic;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, AtomicUsize};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// Encoding for atomic pin_mode
const PIN_NONE: u8 = 0;
const PIN_READ: u8 = 1;
const PIN_WRITE: u8 = 2;

/// Per-frame header that contains metadata and the page bytes.
/// Manages a frame of memory and related metadata
/// Represents headers for frames of memory.
pub struct FrameHeader {
    /// Metadata that rarely changes can live behind this RwLock (page_id).
    meta: RwLock<FrameMeta>,
    /// The actual page bytes guarded by an RwLock so we can hand out read/write guards.
    pub data: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>>,
    /// Atomic pin count (fast to increment/decrement without locking `meta`).
    pin_count: AtomicUsize,
    /// Atomic pin mode: 0 = none, 1 = read, 2 = write.
    pin_mode: AtomicU8,
    /// Dirty flag as atomic.
    is_dirty: AtomicBool,
    /// Latch
    latch: RwLock<()>,
}

struct FrameMeta {
    frame_id: FrameId,
    page_id: PageId,
}

impl FrameHeader {
    pub fn new(frame_id: FrameId) -> Self {
        FrameHeader {
            meta: RwLock::new(FrameMeta {
                frame_id,
                page_id: 0,
            }),
            data: Arc::new(RwLock::new([0; BASEDB_PAGE_SIZE as usize])),
            pin_count: AtomicUsize::new(0),
            pin_mode: AtomicU8::new(PIN_NONE),
            is_dirty: AtomicBool::new(false),
            latch: RwLock::new(()),
        }
    }

    pub fn get_frame_id(&self) -> FrameId {
        self.meta.read().unwrap().frame_id
    }

    pub fn get_page_id(&self) -> PageId {
        self.meta.read().unwrap().page_id
    }

    pub fn set_page_id(&self, pid: PageId) {
        let mut m = self.meta.write().unwrap();
        m.page_id = pid;
    }

    pub fn get_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Acquire)
    }

    pub fn set_dirty(&self, dirty: bool) {
        self.is_dirty.store(dirty, Ordering::Release);
    }

    /// Try to pin the frame with requested mode non-blocking.
    /// Returns true if successful, false otherwise.
    pub fn try_pin(&self, want_write: bool) -> bool {
        let _guard = self.latch.write().unwrap();

        let before_pin_mode = self.pin_mode.load(Ordering::SeqCst);

        let success = if want_write && before_pin_mode == PIN_NONE {
            self.pin_mode.store(PIN_WRITE, Ordering::SeqCst);
            self.pin_count.fetch_add(1, Ordering::SeqCst);
            true
        } else if !want_write && before_pin_mode != PIN_WRITE {
            self.pin_mode.store(PIN_READ, Ordering::SeqCst);
            self.pin_count.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        };

        success
    }

    /// Unpin once. Returns new pin_count after decrement.
    pub fn unpin(&self) -> usize {
        let _guard = self.latch.write().unwrap();

        let old_count = self.pin_count.fetch_sub(1, Ordering::SeqCst);

        if old_count == 1 {
            self.pin_mode.store(PIN_NONE, Ordering::SeqCst);
        }

        if old_count == 1 {
            0
        } else if old_count > 1 {
            old_count - 1
        } else {
            self.pin_count.store(0, Ordering::SeqCst);
            self.pin_mode.store(PIN_NONE, Ordering::SeqCst);
            0
        }
    }

    /// Reset metadata & data. Do not call without ensuring nobody else uses this frame.
    pub fn reset(&self) {
        {
            let mut meta = self.meta.write().unwrap();
            meta.page_id = 0;
        }
        self.pin_count.store(0, Ordering::Release);
        self.pin_mode.store(PIN_NONE, Ordering::Release);
        self.is_dirty.store(false, Ordering::Release);
        let mut d = self.data.write().unwrap();
        d.fill(0);
    }

    /// Get arc to data (for scheduling I/O)
    pub fn get_data(&self) -> Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>> {
        Arc::clone(&self.data)
    }
}

/// Buffer pool manager for this database system
/// Responsible for moving physical pages of data between memory and disk
/// Functions as a cache for keeping frequently used pages in memory for faster access
pub struct BufferPoolManager {
    inner: Mutex<BufferPoolManagerInner>,
}

pub struct BufferPoolManagerInner {
    /// Number of frames in the buffer pool
    num_frames: usize,
    /// The id of the next page to be allocated
    next_page_id: AtomicU32,
    /// The frame headers of the frames being managed
    frame_headers: Vec<Arc<FrameHeader>>,
    /// Maps from pages to buffer pool frames
    page_table: HashMap<PageId, FrameId>,
    /// Tracks frames that do not contain any data
    free_frames: Vec<FrameId>,
    /// Finds unpinned pages to evict
    replacer: Arc<LRUKReplacer>,
    /// Pointer to disk scheduler. Shared with page guards for flushing
    disk_scheduler: Arc<DiskScheduler>,
    /// Pointer to log manager
    log_manager: Arc<LogManager>,
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
    ) -> Self {
        let replacer = Arc::new(LRUKReplacer::new(num_frames, k_dist));
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let frame_headers = (0..num_frames)
            .map(|i| Arc::new(FrameHeader::new(i as FrameId)))
            .collect::<Vec<_>>();
        let free_frames = (0..num_frames as FrameId).collect::<Vec<_>>();

        let inner = BufferPoolManagerInner {
            num_frames,
            next_page_id: AtomicU32::new(0),
            frame_headers,
            page_table: HashMap::with_capacity(num_frames),
            free_frames,
            replacer,
            disk_scheduler,
            log_manager,
        };

        BufferPoolManager {
            inner: Mutex::new(inner),
        }
    }

    /// Returns the number of frames managed by the buffer pool
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().num_frames
    }

    /// Allocates a new page on disk
    /// Returns the page id of the newly allocated page
    pub fn new_page(&self) -> PageId {
        let guard = self.inner.lock().unwrap();
        guard.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get_pin_count(&self, page_id: PageId) -> Option<usize> {
        let inner = self.inner.lock().unwrap();
        match inner.page_table.get(&page_id) {
            None => None,
            Some(&fid) => {
                let fh = &inner.frame_headers[fid];
                Some(fh.pin_count.load(Ordering::Acquire))
            }
        }
    }

    /// Internal helper: mark replacer evictable flag (non-blocking)
    fn set_evictable(&self, frame_id: FrameId, evictable: bool) {
        let inner = self.inner.lock().unwrap();
        let _ = inner.replacer.set_evictable(frame_id, evictable);
        // drop inner
    }

    /// Acquires an optional read-locked guard over a page of data.
    ///
    /// If it is not possible to bring the page of data into memory, this function will return None
    ///
    /// Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
    /// read guard or write guard depending on the mode in which they would like to access the data, which
    /// ensures that any access of data is thread-safe.
    ///
    /// There can be any number of read guards reading the same page of data at a time across different threads.
    /// However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
    /// write guard with write_page instead.
    ///
    /// # Arguments
    /// * page_id - Id of page to get write access to
    pub fn read_page(&self, page_id: PageId) -> Option<ReadPageGuard> {
        // 1) Lookup or allocate under metadata lock
        let (frame_arc, frame_id, replacer, disk_scheduler) = {
            let mut inner = self.inner.lock().unwrap();

            if let Some(&fid) = inner.page_table.get(&page_id) {
                // Try fast atomic pin in Read mode
                let fh = Arc::clone(&inner.frame_headers[fid]);
                if !fh.try_pin(false) {
                    return None;
                }
                // mark non-evictable while pinned
                let _ = inner.replacer.set_evictable(fid, false);
                // collect clones then drop metadata lock
                (
                    fh,
                    fid,
                    Arc::clone(&inner.replacer),
                    Arc::clone(&inner.disk_scheduler),
                )
            } else if let Some(free_fid) = inner.free_frames.pop() {
                // use a free frame
                let fh = Arc::clone(&inner.frame_headers[free_fid]);
                // pin it
                if !fh.try_pin(false) {
                    // shouldn't usually fail, but if it does, push back and return None
                    inner.free_frames.push(free_fid);
                    return None;
                }
                // mark non-evictable
                let _ = inner.replacer.set_evictable(free_fid, false);
                inner.page_table.insert(page_id, free_fid);
                (
                    fh,
                    free_fid,
                    Arc::clone(&inner.replacer),
                    Arc::clone(&inner.disk_scheduler),
                )
            } else {
                // Need to evict
                match inner.replacer.evict() {
                    None => {
                        return None;
                    }
                    Some(victim_id) => {
                        // Reserve/vacate the victim by pinning it (so it won't be taken by others)
                        let victim = Arc::clone(&inner.frame_headers[victim_id]);

                        if !victim.try_pin(false) {
                            // couldn't reserve victim; treat as failure
                            return None;
                        }

                        if victim.get_dirty() {
                            let (sender, receiver) = channel();
                            let sender_clone = sender.clone();
                            let disk_request = DiskRequest::ReadWrite {
                                is_write: true,
                                data: victim.get_data(),
                                page_id: victim.get_page_id(),
                                callback: Some(sender_clone),
                            };
                            // schedule (non-blocking)
                            inner.disk_scheduler.schedule(disk_request).unwrap();
                            // wait for completion
                            match receiver.recv() {
                                Ok(true) => {}
                                Ok(false) => panic!("Disk I/O returned false on read_page"),
                                Err(e) => panic!("Disk I/O channel disconnected: {}", e),
                            }
                        }

                        let (sender, receiver) = channel();
                        let sender_clone = sender.clone();
                        let disk_request = DiskRequest::ReadWrite {
                            is_write: false,
                            data: victim.get_data(),
                            page_id,
                            callback: Some(sender_clone),
                        };
                        // schedule (non-blocking)
                        inner.disk_scheduler.schedule(disk_request).unwrap();
                        // wait for completion
                        match receiver.recv() {
                            Ok(true) => {}
                            Ok(false) => panic!("Disk I/O returned false on read_page"),
                            Err(e) => panic!("Disk I/O channel disconnected: {}", e),
                        }

                        // remove victim mapping from table now
                        let old_pid = victim.get_page_id();
                        victim.set_page_id(page_id);
                        inner.page_table.remove(&old_pid);
                        // insert new mapping now so no races for this frame id
                        inner.page_table.insert(page_id, victim_id);
                        // mark non-evictable (we already pinned)
                        let _ = inner.replacer.set_evictable(victim_id, false);

                        // clone things to use outside lock
                        (
                            victim,
                            victim_id,
                            Arc::clone(&inner.replacer),
                            Arc::clone(&inner.disk_scheduler),
                        )
                        // NOTE: we do I/O after dropping inner below
                    }
                }
            }
        }; // drop metadata lock here
           // 2) If this frame was from eviction path (we pinned it in write mode earlier),
           //    ensure its content contains requested page by doing disk read.
           //    Our contract: when pin was acquired as write (try_pin(true)) above it means we reserved victim.
           //    We'll always perform read to load page_id into frame's bytes (overwriting).
           //    Use disk scheduler with callback channel as before.
           // Perform read I/O into frame.data (we expect DiskScheduler to write into the provided buffer)

        // 3) Acquire per-frame data read lock (blocks if there is an active writer)
        // let data_guard = frame_arc.data.read().unwrap();
        frame_arc.set_page_id(page_id);

        replacer.record_access(frame_id).unwrap();
        // 4) Build ReadPageGuard which will unpin on Drop and mark evictable when pin_count==0
        Some(ReadPageGuard::new(
            page_id,
            frame_arc,
            replacer,
            disk_scheduler,
        ))
    }

    /// Acquires an optional write-locked guard over a page of data.
    ///
    /// If it is not possible to bring the page of data into memory, this function will return None.
    ///
    /// Page data can only be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either
    /// read guard or write guard depending on the mode in which they would like to access the data, which
    /// ensures that any access of data is thread-safe.
    ///
    /// There can only be 1 write guard reading/writing a page at a time. This allows data access to be both immutable
    /// and mutable, meaning the thread that owns the write gaurd is allowed to manipulate the page's data however they
    /// want. If a user wants to have multiple threads reading the page at the same time, they must acquire a read guard
    /// with read_page instead.
    ///
    /// # Arguments
    /// * page_id - Id of page to get write access to
    pub fn write_page(&self, page_id: PageId) -> Option<WritePageGuard> {
        // similar flow to read_page but pins with write mode and takes a write latch
        let (frame_arc, frame_id, replacer, disk_scheduler) = {
            let mut inner = self.inner.lock().unwrap();

            if let Some(&fid) = inner.page_table.get(&page_id) {
                let fh = Arc::clone(&inner.frame_headers[fid]);
                // try to get exclusive pin
                if !fh.try_pin(true) {
                    return None;
                }
                let _ = inner.replacer.set_evictable(fid, false);
                (
                    fh,
                    fid,
                    Arc::clone(&inner.replacer),
                    Arc::clone(&inner.disk_scheduler),
                )
            } else if let Some(free_fid) = inner.free_frames.pop() {
                let fh = Arc::clone(&inner.frame_headers[free_fid]);
                if !fh.try_pin(true) {
                    inner.free_frames.push(free_fid);
                    return None;
                }
                let _ = inner.replacer.set_evictable(free_fid, false);
                inner.page_table.insert(page_id, free_fid);
                (
                    fh,
                    free_fid,
                    Arc::clone(&inner.replacer),
                    Arc::clone(&inner.disk_scheduler),
                )
            } else {
                match inner.replacer.evict() {
                    None => return None,
                    Some(victim_id) => {
                        let victim = Arc::clone(&inner.frame_headers[victim_id]);
                        // reserve victim exclusively
                        if !victim.try_pin(true) {
                            return None;
                        }

                        if victim.get_dirty() {
                            let (sender, receiver) = channel();
                            let sender_clone = sender.clone();
                            let disk_request = DiskRequest::ReadWrite {
                                is_write: true,
                                data: victim.get_data(),
                                page_id: victim.get_page_id(),
                                callback: Some(sender_clone),
                            };
                            // schedule (non-blocking)
                            inner.disk_scheduler.schedule(disk_request).unwrap();
                            // wait for completion
                            match receiver.recv() {
                                Ok(true) => {}
                                Ok(false) => panic!("Disk I/O returned false on read_page"),
                                Err(e) => panic!("Disk I/O channel disconnected: {}", e),
                            }
                        }

                        let (sender, receiver) = channel();
                        let sender_clone = sender.clone();
                        let disk_request = DiskRequest::ReadWrite {
                            is_write: false,
                            data: victim.get_data(),
                            page_id,
                            callback: Some(sender_clone),
                        };
                        // schedule (non-blocking)
                        inner.disk_scheduler.schedule(disk_request).unwrap();
                        // wait for completion
                        match receiver.recv() {
                            Ok(true) => {}
                            Ok(false) => panic!("Disk I/O returned false on read_page"),
                            Err(e) => panic!("Disk I/O channel disconnected: {}", e),
                        }

                        // flush dirty outside lock
                        let old_pid = victim.get_page_id();
                        inner.page_table.remove(&old_pid);
                        inner.page_table.insert(page_id, victim_id);
                        let _ = inner.replacer.set_evictable(victim_id, false);

                        (
                            victim,
                            victim_id,
                            Arc::clone(&inner.replacer),
                            Arc::clone(&inner.disk_scheduler),
                        )
                    }
                }
            }
        }; // drop inner

        frame_arc.set_page_id(page_id);

        // For write, we need exclusive access to the bytes
        // let data_guard = frame_arc.data.write().unwrap();
        replacer.record_access(frame_id).unwrap();

        // Return WritePageGuard that will set dirty flag on drop if mutated by caller.
        Some(WritePageGuard::new(
            page_id,
            frame_arc,
            replacer,
            disk_scheduler,
        ))
    }

    /// Flushes a page's data out to disk safely.
    ///
    /// This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
    /// function will return false.
    ///
    /// # Arguments
    /// * page_id - Id of page to flush to disk
    pub fn flush_page(&self, page_id: PageId) {
        let (frame_arc, disk_scheduler) = {
            let inner = self.inner.lock().unwrap();
            if let Some(&fid) = inner.page_table.get(&page_id) {
                (
                    Arc::clone(&inner.frame_headers[fid]),
                    Arc::clone(&inner.disk_scheduler),
                )
            } else {
                return;
            }
        };
        if frame_arc.get_dirty() {
            let (s, r) = channel();
            let req = DiskRequest::ReadWrite {
                is_write: true,
                data: frame_arc.get_data(),
                page_id: frame_arc.get_page_id(),
                callback: Some(s),
            };
            disk_scheduler.schedule(req).unwrap();
            assert!(r.recv().unwrap());
            frame_arc.set_dirty(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{
        fs,
        path::Path,
        sync::{atomic::AtomicBool, Arc, Condvar, Mutex},
        thread,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        common::config::BASEDB_PAGE_SIZE,
        recovery::log_manager::LogManager,
        storage::{disk::disk_manager::DiskManager, page::page_guard::WritePageGuard},
    };

    #[test]
    fn buffer_pool_manager_basic_test() {
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        );

        let pid = bpm.new_page();
        {
            let guard = bpm.write_page(pid).unwrap();
            let data = guard.get_data();

            let mut data_guard = data.write().unwrap();
            data_guard[0..4].copy_from_slice(b"data");

            guard.set_dirty();
        }
        {
            let guard = bpm.read_page(pid).unwrap();
            let data = guard.get_data();

            let data_guard = data.read().unwrap();
            assert_eq!(&data_guard[0..4], b"data");
        }

        let _ = fs::remove_file(disk_manager.get_db_file_name());
        let _ = fs::remove_file(disk_manager.get_log_file_name());
    }

    #[test]
    fn buffer_pool_manager_page_pin_easy_test() {
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        );
    }

    #[test]
    fn buffer_pool_manager_page_pin_med_test() {
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let bpm = BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        );

        let pid0 = bpm.new_page();
        let page0 = bpm.write_page(pid0).unwrap();
        {
            let data = page0.get_data();
            let mut data_guard = data.write().unwrap();
            data_guard[0..4].copy_from_slice(b"data");
            page0.set_dirty();
        }
        drop(page0);

        let mut pages: Vec<WritePageGuard> = Vec::new();

        // This loop should not panic as we have enough free frames and will completely fill up the buffer pool
        for _ in 0..FRAMES {
            let pid = bpm.new_page();
            let page = bpm.write_page(pid).unwrap();
            pages.push(page);
        }

        // All pages should be pinned exactly once
        for page in &pages {
            assert_eq!(bpm.get_pin_count(page.get_page_id()), Some(1))
        }

        // No page guards should be created for new pages as we cannot evict any frame
        for _ in 0..FRAMES {
            let pid = bpm.new_page();
            let page = bpm.write_page(pid);
            assert!(page.is_none());
        }

        // Drop the first 5 pages to unpin them
        for _ in 0..FRAMES / 2 {
            let pid = pages[0].get_page_id();
            assert_eq!(bpm.get_pin_count(pid), Some(1));
            let page = pages.remove(0);
            drop(page);
            assert_eq!(bpm.get_pin_count(pid), Some(0));
        }

        // Rest of the pages should still have pincount = 1
        for page in &pages {
            let pid = page.get_page_id();
            assert_eq!(bpm.get_pin_count(pid), Some(1));
        }

        // Should be able to create 4 new pages and bring them into memory.
        for _ in 0..(FRAMES / 2) - 1 {
            let pid = bpm.new_page();
            let page = bpm.write_page(pid).unwrap();
            pages.push(page);
        }

        // There should be one frame available, and we should be able to fetch the data in pid0
        {
            let original_page = bpm.read_page(pid0).unwrap();
            let data = original_page.get_data();
            let guard = data.read().unwrap();
            assert_eq!(&guard[0..4], b"data");
        }

        let last_pid = bpm.new_page();
        let last_page = bpm.read_page(last_pid);

        // Buffer pool should be completely full at this point, with all pages pinned.
        // Trying to read page 0 into memory should fail as there are no free frames
        let fail_guard = bpm.read_page(pid0);
        assert!(fail_guard.is_none());

        let _ = fs::remove_file(disk_manager.get_db_file_name());
        let _ = fs::remove_file(disk_manager.get_log_file_name());
    }

    #[test]
    fn buffer_pool_manager_page_access_test() {
        let rounds = 50;
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = Arc::new(BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        ));
        let mut bpm_clone = Arc::clone(&bpm);

        let pid = bpm.new_page();
        let mut buf: [u8; BASEDB_PAGE_SIZE as usize] = [0; BASEDB_PAGE_SIZE as usize];

        let writer_thread = thread::spawn(move || {
            for i in 0..rounds {
                thread::sleep(time::Duration::from_millis(5));
                let guard = bpm_clone.write_page(pid);
                match guard {
                    Some(_guard) => {
                        let data = _guard.get_data();
                        let mut data_guard = data.write().unwrap();
                        data_guard[0..4].copy_from_slice(&(i as i32).to_be_bytes());
                        _guard.set_dirty();
                    }
                    None => {}
                }
            }
        });

        for i in 0..rounds {
            thread::sleep(time::Duration::from_millis(10));
            let guard = bpm.read_page(pid);
            match guard {
                Some(_guard) => {
                    let data = _guard.get_data();
                    let data_guard = data.read().unwrap();
                    buf[0..BASEDB_PAGE_SIZE as usize].copy_from_slice(data_guard.as_slice());

                    // We should sleep while holding the guard on this page. Nothing should have changed while this thread is sleeping
                    thread::sleep(time::Duration::from_millis(10));

                    assert_eq!(buf, data_guard.as_slice());
                }
                None => {}
            };
        }
        writer_thread.join().unwrap();
    }

    #[test]
    fn contention_test() {
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = Arc::new(BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        ));
        let bpm_clone1 = Arc::clone(&bpm);
        let bpm_clone2 = Arc::clone(&bpm);
        let bpm_clone3 = Arc::clone(&bpm);
        let bpm_clone4 = Arc::clone(&bpm);

        let rounds = 1000000;
        let pid = bpm.new_page();

        let writer_thread1 = thread::spawn(move || {
            for i in 0..rounds {
                let guard = bpm_clone1.write_page(pid);

                match guard {
                    Some(_guard) => {
                        let data = _guard.get_data();
                        let mut data_guard = data.write().unwrap();
                        data_guard[0..4].copy_from_slice(&(i as i32).to_be_bytes());
                        _guard.set_dirty();
                    }
                    None => {}
                }
            }
        });

        let writer_thread2 = thread::spawn(move || {
            for i in 0..rounds {
                let guard = bpm_clone2.write_page(pid);
                match guard {
                    Some(_guard) => {
                        let data = _guard.get_data();
                        let mut data_guard = data.write().unwrap();
                        data_guard[0..4].copy_from_slice(&(i as i32).to_be_bytes());
                        _guard.set_dirty();
                    }
                    None => {}
                }
            }
        });

        let writer_thread3 = thread::spawn(move || {
            for i in 0..rounds {
                let guard = bpm_clone3.write_page(pid);
                match guard {
                    Some(_guard) => {
                        let data = _guard.get_data();
                        let mut data_guard = data.write().unwrap();
                        data_guard[0..4].copy_from_slice(&(i as i32).to_be_bytes());
                        _guard.set_dirty();
                    }
                    None => {}
                }
            }
        });

        let writer_thread4 = thread::spawn(move || {
            for i in 0..rounds {
                let guard = bpm_clone4.write_page(pid);
                match guard {
                    Some(_guard) => {
                        let data = _guard.get_data();
                        let mut data_guard = data.write().unwrap();
                        data_guard[0..4].copy_from_slice(&(i as i32).to_be_bytes());
                        _guard.set_dirty();
                    }
                    None => {}
                }
            }
        });

        writer_thread3.join().unwrap();
        writer_thread2.join().unwrap();
        writer_thread4.join().unwrap();
        writer_thread1.join().unwrap();
    }

    #[test]
    fn deadlock_test() {
        let FRAMES = 10;
        let K_DIST = 5;
        let db_file_name = Path::new("test_bpm.db");
        let disk_manager = Arc::new(DiskManager::new(db_file_name).unwrap());
        let log_manager = Arc::new(LogManager {});
        let mut bpm = Arc::new(BufferPoolManager::new(
            FRAMES,
            Arc::clone(&disk_manager),
            K_DIST,
            Arc::clone(&log_manager),
        ));
        let bpm_clone = Arc::clone(&bpm);

        let pid0 = bpm.new_page();
        let pid1 = bpm.new_page();

        let guard0 = bpm.write_page(pid0).unwrap();

        let start = Arc::new(AtomicBool::new(false));
        let start_clone = Arc::clone(&start);
        let child = thread::spawn(move || {
            start_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            let guard0 = bpm_clone.write_page(pid0);
            assert!(guard0.is_none());
        });

        while (!start.load(std::sync::atomic::Ordering::Relaxed)) {}

        thread::sleep(time::Duration::from_millis(1000));

        let guard1 = bpm.write_page(pid1).unwrap();

        drop(guard0);
        child.join().unwrap();
    }
}
