use crate::common::types::FrameId;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Implements the LRU k replacement policy
///
/// Evicts a frame whose backward k distance is maximum of all frames
/// Backward k distance is the difference in time between current timestamp and timestamp of kth previous access
///
/// A frame with less than k accesses has infinite backward k distance.
/// When multiple frames have infinite backward k distance, classic LRU is used.
pub struct LRUKReplacer {
    /// Maps from a frame id to the history of the frame
    node_store: HashMap<FrameId, LRUKNode>,

    current_timestamp: i64,
    /// Number of frames that can be evicted
    curr_size: usize,
    /// Number of frames to keep track of
    replacer_size: usize,
    /// Number to calculate backward k distance
    k: usize,
    latch: Arc<Mutex<()>>,
}

/// Keeps track of access history for a single frame
struct LRUKNode {
    /// Last seen K timestamps for this page. Least recent timestamp is at the front
    history: Vec<i64>,
    /// Maximum number of timestamps to keep track of
    k: usize,
    /// The frame id being represented
    frame_id: FrameId,
    /// True if the frame being represented can be evicted
    is_evictable: bool,
}

/// Type of access being done on the frame
enum AccessType {
    UNKNOWN,
    LOOKUP,
    SCAN,
    INDEX,
}

impl LRUKReplacer {
    /// Creates a new LRU K replacer
    /// 
    /// # Arguments
    /// * `num_frames` - The size of the buffer pool and the number of frames the LRU K Replacer has to track
    /// * `k` - The backward k distance to use. Specifies the number of timestamps to track for frames
    pub fn new(num_frames: usize, k: usize) -> LRUKReplacer {
        LRUKReplacer {
            node_store: HashMap::new(),
            current_timestamp: Utc::now().timestamp() as i64,
            curr_size: 0,
            replacer_size: num_frames,
            k: k,
            latch: Arc::new(Mutex::new(())),
        }
    }

    /// Find the frame with the largest backward k distance and evict that frame. 
    /// Only frames that are marked evictable can be chosen
    /// 
    /// Frames with less than k accesses have infinite backward k distance.
    /// If there are multiple frames with infinite backward k distance, fall back to normal LRU
    /// 
    /// Successful eviction decreases the size of the replacer and removes the frame's access history
    /// 
    /// # Returns
    /// The frame id of the frame that was evicted or None if no frame could be evicted
    pub fn evict() -> Option<FrameId> {
        None
    }

    /// Record the event that the given frame id is accessed at current timestamp
    /// Create a new entry for access history if frame id has not been seen before
    /// If the frame id is invalid, throw an Exception
    /// 
    /// # Arguments
    /// * `frame_id` - Frame id of frame that was accessed
    /// * `access_type` - Access type for this frame
    pub fn record_access(frame_id: FrameId, access_type: AccessType) {}

    /// Toggle whether a frame is evictable or not.
    /// Affects the number of evictable frames in the replacer
    /// If frame was evictable and is set to non-evictable, number of evictable frames decreases. (and vice versa)
    /// If frame_id is invalid, throw an exception
    /// 
    /// # Arguments
    /// * `frame_id` - The frame for which we are modifying evictability
    /// * `set_evictable` - true to set evictable, false to set not evictable
    pub fn set_evictable(frame_id: FrameId, set_evictable: bool) {}

    /// Remove an evictable frame from the replacer with its access history
    /// 
    /// This is different from evicting where we evict the frame with the largest backward k distance.
    /// This function removes the specified frame regardless of backward k distance
    /// If called on an invalid frame (invalid frame_id or non-evictable frame), throw an exception
    /// 
    /// # Arguments
    /// * `frame_id` -> Frame to remove
    pub fn remove(frame_id: FrameId) {}

    /// Return the number of evictable frames
    pub fn size(&self) -> usize {
        self.replacer_size
    }
}
