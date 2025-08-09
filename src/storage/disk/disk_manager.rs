use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Error, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use crate::common::{
    config::PageId,
    config::{BASEDB_PAGE_SIZE, DEFAULT_DB_IO_SIZE},
    errors::{DiskManagerError, InternalError},
};

/// Takes care of the allocation and deallocation of pages within a database.
/// It performs reading and writing of pages to and from disk, providing a logical file layer within the context of a database management system.
///
/// DiskManager uses lazy allocation, so it only allocates space on disk when accessed. It maintains a map from page ids to offsets in the database file.
/// When a page is deleted, it is marked free and can be reused by future allocations.
pub struct DiskManager {
    inner: Mutex<DiskManagerInner>,
}

struct DiskManagerInner {
    num_flushes: u32,
    num_writes: u32,
    num_deletes: u32,
    /// The capacity of the file used for storage on disk
    page_capacity: usize,
    /// Mapping from page id to offset in database file
    pages: HashMap<PageId, usize>,
    /// Records free slots/pages in the database file according to their offset
    free_slots: VecDeque<usize>,
    /// Path to database file
    db_file_name: String,
    /// Stream to write database file
    db_io: File,
    /// Path to log file
    log_file_name: String,
    /// Stream to write log file
    log_io: File,
}

impl DiskManager {
    /// Creates a new disk manager that writes to the specified database file
    ///
    /// # Arguments
    /// * `db_file_name` - The file name of the database file to write to
    pub fn new(db_file_name: &Path) -> Result<Self, DiskManagerError> {
        let mut log_path = PathBuf::from(db_file_name);
        log_path.set_extension("log");

        let log_io = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&log_path)
            .map_err(InternalError::IOError)?;

        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_name)
            .map_err(InternalError::IOError)?;

        let inner = DiskManagerInner {
            num_flushes: 0,
            num_writes: 0,
            num_deletes: 0,
            page_capacity: DEFAULT_DB_IO_SIZE as usize,
            pages: HashMap::new(),
            free_slots: VecDeque::new(),
            db_file_name: db_file_name.to_str().unwrap_or_default().to_string(),
            db_io,
            log_file_name: log_path.to_str().unwrap_or_default().to_string(),
            log_io,
        };

        Ok(DiskManager {
            inner: Mutex::new(inner),
        })
    }

    /// Write the contents of the specified page into the database file
    ///
    /// # Arguments
    /// * `page_id` - Identifies which page (and thus the location) to write to in the database file
    /// * `page_data` - Content to write to the database file
    pub fn write_page(
        &self,
        page_id: PageId,
        page_data: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>>,
    ) -> Result<(), DiskManagerError> {
        let mut inner = self.inner.lock().unwrap();
        let offset = if let Some(&offset) = inner.pages.get(&page_id) {
            offset
        } else {
            let offset = Self::allocate_page(&mut inner)?;
            inner.pages.insert(page_id, offset);
            offset
        };

        inner
            .db_io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(InternalError::IOError)?;
        let data_guard = page_data.read().unwrap();
        inner
            .db_io
            .write_all(&*data_guard)
            .map_err(InternalError::IOError)?;
        inner.db_io.flush().map_err(InternalError::IOError)?;
        inner.num_writes += 1;

        Ok(())
    }

    /// Read the contents of the specified page into the given memory area
    ///
    /// # Arguments
    /// * `page_id` - Identifies which page (and thus the location) to read from in the database file
    /// * `page_data` - The location to read data into
    pub fn read_page(
        &self,
        page_id: PageId,
        page_data: Arc<RwLock<[u8; BASEDB_PAGE_SIZE as usize]>>,
    ) -> Result<(), DiskManagerError> {
        let mut inner = self.inner.lock().unwrap();
        let offset = if let Some(&offset) = inner.pages.get(&page_id) {
            offset
        } else {
            let offset = Self::allocate_page(&mut inner)?;
            inner.pages.insert(page_id, offset);
            offset
        };

        let file_size = Self::get_file_size(&mut inner).map_err(InternalError::IOError)?;
        if (offset as u64) >= file_size {
            return Err(DiskManagerError::InvalidDBFile);
        }

        inner
            .db_io
            .seek(SeekFrom::Start(offset as u64))
            .map_err(InternalError::IOError)?;

        let mut data_guard = page_data.write().unwrap();
        let byte_count = inner
            .db_io
            .read(&mut *data_guard)
            .map_err(InternalError::IOError)?;

        if byte_count < BASEDB_PAGE_SIZE as usize {
            data_guard[byte_count..].fill(0);
        }

        Ok(())
    }

    /// Marks the offset for this page as a free slot that can be overwritten
    pub fn delete_page(&self, page_id: PageId) -> Result<(), DiskManagerError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(offset) = inner.pages.remove(&page_id) {
            inner.free_slots.push_back(offset);
            inner.num_deletes += 1;
        }
        Ok(())
    }

    /// Return the number of flushes that occurred
    pub fn get_num_flushes(&self) -> u32 {
        let inner = self.inner.lock().unwrap();
        inner.num_flushes
    }

    /// Return the number of writes that occurred
    pub fn get_num_writes(&self) -> u32 {
        let inner = self.inner.lock().unwrap();
        inner.num_writes
    }

    /// Return the number of deletes that occurred
    pub fn get_num_deletes(&self) -> u32 {
        let inner = self.inner.lock().unwrap();
        inner.num_deletes
    }

    /// Allocate a page in a free slot. If no free slot is available, append to the end of the file.
    /// Returns the offset of the allocated page
    fn allocate_page(inner: &mut DiskManagerInner) -> Result<usize, DiskManagerError> {
        if let Some(offset) = inner.free_slots.pop_back() {
            return Ok(offset);
        }

        let file_size = Self::get_file_size(inner).unwrap();
        let offset = file_size as usize;

        // Expand the file to accommodate the new page
        inner
            .db_io
            .set_len(file_size + BASEDB_PAGE_SIZE as u64)
            .map_err(InternalError::IOError)?;

        Ok(offset)
    }

    /// Get disk file size
    fn get_file_size(inner: &mut DiskManagerInner) -> Result<u64, Error> {
        match inner.db_io.metadata() {
            Err(e) => Err(e),
            Ok(m) => Ok(m.len()),
        }
    }

    /// Get db file name
    pub fn get_db_file_name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.db_file_name.clone()
    }

    /// Get log file name
    pub fn get_log_file_name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.log_file_name.clone()
    }
}
