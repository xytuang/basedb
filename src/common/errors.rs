use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("BufferPool error: {0}")]
    BufferPool(#[from] BufferPoolError),
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
