use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};

use crate::common::config::{FrameId, PageId};
struct Node {
    frame_id: FrameId,
    is_evictable: bool,
}
pub struct ArcReplacer {
    replacer_size: usize,
    mru_target_size: usize,
    curr_size: usize,

    mru: VecDeque<PageId>,
    mfu: VecDeque<PageId>,
    mru_ghost: VecDeque<PageId>,
    mfu_ghost: VecDeque<PageId>,

    page_table: HashMap<PageId, Node>,

    latch: Mutex<()>,
}
