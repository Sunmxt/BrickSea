#[derive(PartialEq, Debug)]
pub enum Error {
    /* WAL Errors: */
    WALStreamNotFound,

    NotImplemented,
    BufferTooSmall,
    BufferOverflow,
}

pub type Result<T> = core::result::Result<T, Error>;