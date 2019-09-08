use std::io;

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;

/// Error type for kvs.
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Converting from bytes to String error.
    #[fail(display = "{}", _0)]
    FromUtf8(#[cause] std::string::FromUtf8Error),

    /// Slice conversion error.
    #[fail(display = "{}", _0)]
    TryFromSlice(#[cause] std::array::TryFromSliceError),

    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Unexpected command type error.
    ///
    /// This indicates a corrupted log or runtime bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<std::array::TryFromSliceError> for KvsError {
    fn from(err: std::array::TryFromSliceError) -> KvsError {
        KvsError::TryFromSlice(err)
    }
}

impl From<std::string::FromUtf8Error> for KvsError {
    fn from(err: std::string::FromUtf8Error) -> KvsError {
        KvsError::FromUtf8(err)
    }
}
