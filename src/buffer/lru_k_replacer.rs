use crate::common::errors::ReplacerError;
use crate::common::time::get_current_time_f64;
use crate::common::types::FrameId;
use core::f64;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Implements the LRU k replacement policy
///
/// Evicts a frame whose backward k distance is maximum of all frames
/// Backward k distance is the difference in time between current timestamp and timestamp of kth previous access
///
/// A frame with less than k accesses has infinite backward k distance.
/// When multiple frames have infinite backward k distance, classic LRU is used.
pub struct LRUKReplacer {
    inner: Mutex<LRUKReplacerInner>,
}

struct LRUKReplacerInner {
    /// Maps from a frame id to the history of the frame
    node_store: HashMap<FrameId, LRUKNode>,
    /// Number of frames that can be evicted
    curr_size: usize,
    /// Number of frames to keep track of
    replacer_size: usize,
    /// Number to calculate backward k distance
    k: usize,
}

/// Keeps track of access history for a single frame
struct LRUKNode {
    /// Last seen K timestamps for this page. Least recent timestamp is at the front
    history: VecDeque<f64>,
    /// Maximum number of timestamps to keep track of
    k: usize,
    /// The frame id being represented
    frame_id: FrameId,
    /// True if the frame being represented can be evicted
    is_evictable: bool,
}

impl LRUKReplacer {
    /// Creates a new LRU K replacer
    ///
    /// # Arguments
    /// * `num_frames` - The size of the buffer pool and the number of frames the LRU K Replacer has to track
    /// * `k` - The backward k distance to use. Specifies the number of timestamps to track for frames
    pub fn new(num_frames: usize, k: usize) -> LRUKReplacer {
        let inner = LRUKReplacerInner {
            node_store: HashMap::new(),
            curr_size: 0,
            replacer_size: num_frames,
            k: k,
        };

        LRUKReplacer {
            inner: Mutex::new(inner),
        }
    }

    /// Find the frame with the largest backward k distance and evict that frame.
    /// Only frames that are marked evictable can be chosen
    ///
    /// Frames with less than k accesses have infinite backward k distance.
    /// If there are multiple frames with infinite backward k distance, evict the frame whose oldest timestamp is furthest in the past
    ///
    /// Successful eviction decreases the size of the replacer and removes the frame's access history
    ///
    /// # Returns
    /// The frame id of the frame that was evicted or None if no frame could be evicted
    pub fn evict(&self) -> Option<FrameId> {
        let current_timestamp = get_current_time_f64();
        let mut inner = self.inner.lock().unwrap();

        let mut best_candidate: Option<(FrameId, f64)> = None;
        let mut best_inf_candidate: Option<(FrameId, f64)> = None;

        for (frame_id, node) in &inner.node_store {
            if !node.is_evictable {
                continue;
            }

            let k_distance = if node.history.len() >= inner.k {
                current_timestamp - node.history[node.history.len() - inner.k]
            } else {
                f64::INFINITY
            };

            if k_distance.is_finite() {
                match best_candidate {
                    None => best_candidate = Some((*frame_id, k_distance)),
                    Some((_, dist)) => {
                        if k_distance > dist {
                            best_candidate = Some((*frame_id, k_distance))
                        }
                    }
                }
            } else {
                let oldest = node.history.front().copied().unwrap_or(f64::INFINITY);
                match best_inf_candidate {
                    None => best_inf_candidate = Some((*frame_id, oldest)),
                    Some((_, existing_oldest)) => {
                        if oldest < existing_oldest {
                            best_inf_candidate = Some((*frame_id, oldest))
                        }
                    }
                }
            }
        }

        let victim = if let Some((frame_id, _)) = best_inf_candidate {
            Some(frame_id)
        } else if let Some((frame_id, _)) = best_candidate {
            Some(frame_id)
        } else {
            None
        };

        if let Some(frame_id) = victim {
            inner.node_store.remove(&frame_id);
            inner.curr_size -= 1;
            return Some(frame_id);
        }

        return None;
    }

    /// Record the event that the given frame id is accessed at current timestamp
    /// Create a new entry for access history if frame id has not been seen before
    /// If the frame id is invalid, throw an Exception
    ///
    /// # Arguments
    /// * `frame_id` - Frame id of frame that was accessed
    /// * `access_type` - Access type for this frame
    pub fn record_access(&self, frame_id: FrameId) -> Result<(), ReplacerError> {
        let mut inner = self.inner.lock().unwrap();
        if frame_id >= inner.replacer_size {
            return Err(ReplacerError::InvalidFrameId);
        }

        let current_timestamp = get_current_time_f64();
        if let Some(lru_node) = inner.node_store.get_mut(&frame_id) {
            if lru_node.history.len() == lru_node.k {
                lru_node.history.pop_front();
            }
            lru_node.history.push_back(current_timestamp);
            return Ok(());
        } else {
            let mut new_lru_node = LRUKNode::new(frame_id, inner.k);
            new_lru_node.history.push_back(current_timestamp);
            inner.node_store.insert(frame_id, new_lru_node);
            return Ok(());
        }
    }

    /// Toggle whether a frame is evictable or not.
    /// Affects the number of evictable frames in the replacer
    /// If frame was evictable and is set to non-evictable, number of evictable frames decreases. (and vice versa)
    /// If frame_id is invalid, throw an exception
    ///
    /// # Arguments
    /// * `frame_id` - The frame for which we are modifying evictability
    /// * `set_evictable` - true to set evictable, false to set not evictable
    pub fn set_evictable(
        &self,
        frame_id: FrameId,
        set_evictable: bool,
    ) -> Result<(), ReplacerError> {
        let mut inner = self.inner.lock().unwrap();
        if frame_id >= inner.replacer_size {
            return Err(ReplacerError::InvalidFrameId);
        }

        let lru_node = match inner.node_store.get_mut(&frame_id) {
            Some(node) => node,
            None => return Err(ReplacerError::InvalidFrameId),
        };

        let was_evictable = lru_node.is_evictable;
        lru_node.is_evictable = set_evictable;

        if was_evictable && !set_evictable {
            inner.curr_size -= 1;
        } else if !was_evictable && set_evictable {
            inner.curr_size += 1;
        }
        Ok(())
    }

    /// Remove an evictable frame from the replacer with its access history
    ///
    /// This is different from evicting where we evict the frame with the largest backward k distance.
    /// This function removes the specified frame regardless of backward k distance
    /// If called on an invalid frame (invalid frame_id or non-evictable frame), throw an exception
    ///
    /// # Arguments
    /// * `frame_id` -> Frame to remove
    pub fn remove(&self, frame_id: FrameId) -> Result<(), ReplacerError> {
        let mut inner = self.inner.lock().unwrap();
        if frame_id >= inner.replacer_size {
            return Err(ReplacerError::InvalidFrameId);
        }

        if inner.node_store.get(&frame_id).is_none() {
            return Err(ReplacerError::InvalidFrameId);
        }

        let lru_node = inner.node_store.get_mut(&frame_id).unwrap();
        if !lru_node.is_evictable {
            return Err(ReplacerError::InvalidFrameId);
        }

        inner.curr_size -= 1;
        inner.node_store.remove(&frame_id);
        return Ok(());
    }

    /// Return the number of evictable frames
    pub fn size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.curr_size
    }
}

impl LRUKNode {
    fn new(frame_id: FrameId, k: usize) -> LRUKNode {
        LRUKNode {
            history: VecDeque::new(),
            k: k,
            frame_id: frame_id,
            is_evictable: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::lru_k_replacer::LRUKReplacer;

    #[test]
    fn test_lru_k_replacer() {
        let mut lru_replacer = LRUKReplacer::new(7, 2);

        let _ = lru_replacer.record_access(1);
        let _ = lru_replacer.record_access(2);
        let _ = lru_replacer.record_access(3);
        let _ = lru_replacer.record_access(4);
        let _ = lru_replacer.record_access(5);
        let _ = lru_replacer.record_access(6);
        let _ = lru_replacer.set_evictable(1, true);
        let _ = lru_replacer.set_evictable(2, true);
        let _ = lru_replacer.set_evictable(3, true);
        let _ = lru_replacer.set_evictable(4, true);
        let _ = lru_replacer.set_evictable(5, true);
        let _ = lru_replacer.set_evictable(6, false);

        assert_eq!(lru_replacer.size(), 5);

        let _ = lru_replacer.record_access(1);

        assert_eq!(lru_replacer.evict(), Some(2));
        assert_eq!(lru_replacer.evict(), Some(3));
        assert_eq!(lru_replacer.evict(), Some(4));
        assert_eq!(lru_replacer.size(), 2);

        let _ = lru_replacer.record_access(3);
        let _ = lru_replacer.record_access(4);
        let _ = lru_replacer.record_access(5);
        let _ = lru_replacer.record_access(4);
        let _ = lru_replacer.set_evictable(3, true);
        let _ = lru_replacer.set_evictable(4, true);

        assert_eq!(lru_replacer.size(), 4);

        assert_eq!(lru_replacer.evict(), Some(3));
        assert_eq!(lru_replacer.size(), 3);

        let _ = lru_replacer.set_evictable(6, true);

        assert_eq!(lru_replacer.size(), 4);
        assert_eq!(lru_replacer.evict(), Some(6));
        assert_eq!(lru_replacer.size(), 3);

        let _ = lru_replacer.set_evictable(1, false);

        assert_eq!(lru_replacer.size(), 2);
        assert_eq!(lru_replacer.evict(), Some(5));
        assert_eq!(lru_replacer.size(), 1);

        let _ = lru_replacer.record_access(1);
        let _ = lru_replacer.record_access(1);
        let _ = lru_replacer.set_evictable(1, true);

        assert_eq!(lru_replacer.size(), 2);

        assert_eq!(lru_replacer.evict(), Some(4));
        assert_eq!(lru_replacer.size(), 1);
        assert_eq!(lru_replacer.evict(), Some(1));
        assert_eq!(lru_replacer.size(), 0);

        let _ = lru_replacer.record_access(1);
        let _ = lru_replacer.set_evictable(1, false);
        assert_eq!(lru_replacer.size(), 0);

        let mut frame = lru_replacer.evict();
        assert!(frame.is_none());

        let _ = lru_replacer.set_evictable(1, true);
        assert_eq!(lru_replacer.size(), 1);
        assert_eq!(lru_replacer.evict(), Some(1));
        assert_eq!(lru_replacer.size(), 0);

        frame = lru_replacer.evict();
        assert!(frame.is_none());

        let _ = lru_replacer.set_evictable(6, false);
        let _ = lru_replacer.set_evictable(6, true);
    }
}
