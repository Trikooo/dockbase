use std::{sync::atomic::AtomicBool, time::Duration};

pub static ENABLE_LOGGING: AtomicBool = AtomicBool::new(false);
pub const LOG_TIMEOUT: Duration = Duration::from_secs(1);

// type aliases
pub type FrameId = i32;
pub type PageId = i32;
pub type TxnId = i64;
pub type LsnId = i32;
pub type SlotOffset = usize;
pub type Oid = u16;

// Invalid identifiers
pub const INVALID_FRAME_ID: FrameId = -1;
pub const INVALID_PAGE_ID: PageId = -1;
pub const INVALID_TXN_ID: TxnId = -1;
pub const INVALID_LSN_ID: LsnId = -1;

pub const DOCKBASE_PAGE_SIZE: usize = 8192;
pub const BUFFER_POOL_SIZE: usize = 128;
pub const DEFAULT_DB_IO_SIZE: usize = 16;
pub const LOG_BUFFER_SIZE: usize = (BUFFER_POOL_SIZE + 1 ) * DOCKBASE_PAGE_SIZE;
pub const BUCKET_SIZE: usize = 50;
pub const LRUK_REPLACER_K: usize = 10;
pub const DOCKBASE_BATCH_SIZE: usize = 20;

// Transaction ID space
pub const TXN_START_ID: TxnId = 1i64 << 62;

// misc
pub const VAR_CHAR_LENGTH: usize = 128;

