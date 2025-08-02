use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("BufferPool error: {0}")]
    BufferPool(#[from] BufferPoolError),
    #[error("DiskManager error: {0}")]
    Disk(#[from] DiskError),
}

#[derive(Debug, Error)]
pub enum BufferPoolError {
    #[error("ReplacerError")]
    Replacer(#[from] ReplacerError),
}

#[derive(Debug, Error)]
pub enum ReplacerError {
    #[error("Invalid frame id")]
    InvalidFrameId,
}

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("DiskManagerError")]
    DiskManager(#[from] DiskManagerError),
}

#[derive(Debug, Error)]
pub enum DiskManagerError {
    #[error("InvalidDBFile")]
    InvalidDBFile,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, Error)]
pub enum InternalError {
    #[error("Poisoned lock")]
    PoisonedLock,
    #[error("IOError {0}")]
    IOError(#[from] std::io::Error),
}
