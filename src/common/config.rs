pub const BASEDB_PAGE_SIZE: i32 = 4096;
pub const BUFFER_POOL_SIZE: i32 = 128;
pub const DEFAULT_DB_IO_SIZE: i32 = 16;
pub const LOG_BUFFER_SIZE: i32 = (BUFFER_POOL_SIZE + 1) * BASEDB_PAGE_SIZE;
pub const BUCKET_SIZE: i32 = 50;
pub const LRUK_REPLACER_K: i32 = 10;

pub const TXN_START_ID: TxnId = 1 << 62;
pub const VARCHAR_DEFAULT_LENGTH: i32 = 128;

pub const INVALID_FRAME_ID: i32 = -1;
pub const INVALID_PAGE_ID: i32 = -1;
pub const INVALID_TXN_ID: i32 = -1;
pub const INVALID_LSN: i32 = -1;

pub type PageId = u32;
pub type FrameId = usize;
pub type TxnId = u64;
pub type Lsn = u32;
pub type SlotOffset = usize;
pub type Oid = u16;
pub type TableOid = u32;
pub type IndexOid = u32;
