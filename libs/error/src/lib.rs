#[derive(PartialEq, Debug)]
pub enum Error {
    /* WAL Errors: */
    BlockSizeTooSmall,
    BadRecord,
    BadBlockHeader,
    BadChecksum,
    BadBlockWrite,
    BadRecordWrite,
    RecordTooLong,
    InvalidRecordType,
    InvalidRecordContentType,

    NotImplemented,
    BufferTooSmall,
    BufferTooLarge,
    BufferOverflow,
}

pub type Result<T> = core::result::Result<T, Error>;