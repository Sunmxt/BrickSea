use crate::wal::{LogStateSnapshotRecord, SerializableLogObject,
                 BlockWriteRecord, DataBlockDescriptionForRecord};
use crate::buffer::{MutBufferVector, SliceBufferReader};
use crate::error::Error;

/* LogStateSnapshotRecord tests */

#[test]
fn log_state_record_serialize() {
    let mut record = LogStateSnapshotRecord{
        sequence_ids: std::collections::BTreeMap::<u64, u64>::new(),
    };

    record.sequence_ids.insert(17, 288192);
    record.sequence_ids.insert(18, 288);
    record.sequence_ids.insert(281900, 779);

    const EXPECTED_SERIALIZED_SIZE: usize = 1 + 1 + 3 + 1 + 2 + 3 + 2;
    assert_eq!(record.size(), EXPECTED_SERIALIZED_SIZE);

    let mut buf_vec = MutBufferVector::<u8>::new();
    let mut buffer: [u8; EXPECTED_SERIALIZED_SIZE] = [0; EXPECTED_SERIALIZED_SIZE];
    buf_vec.append_buffer_slice(&mut buffer);
    let mut buf_writer = buf_vec.new_writer();

    let result = record.serialize(&mut buf_writer);
    assert!(result.is_ok());
    assert_eq!(buffer, [3,
                        17,
                        192, 203, 17,
                        18,
                        160, 2,
                        172, 154, 17,
                        139, 6]);
}


#[test]
fn log_state_record_serialize_buffer_too_small() {
    let mut record = LogStateSnapshotRecord{
        sequence_ids: std::collections::BTreeMap::<u64, u64>::new(),
    };

    record.sequence_ids.insert(17, 288192);
    record.sequence_ids.insert(18, 288);
    record.sequence_ids.insert(281900, 779);

    let expected_serialized_size = 1 + 1 + 3 + 1 + 2 + 3 + 2;
    assert_eq!(record.size(), expected_serialized_size);

    let mut buf_vec = MutBufferVector::<u8>::new();
    let mut buffer = Vec::<u8>::new();
    buffer.resize(expected_serialized_size - 1, 0);
    buf_vec.append_buffer_slice(&mut buffer);
    let mut buf_writer = buf_vec.new_writer();

    let result = record.serialize(&mut buf_writer);
    assert!(result.is_err());
}

#[test]
fn log_state_record_deserialize() {
    let buffer = [3, 
                         17,
                         192, 203, 17,
                         18,
                         160, 2,
                         172, 154, 17,
                         139, 6, 8, 10];

    let mut record = LogStateSnapshotRecord{
        sequence_ids: std::collections::BTreeMap::<u64, u64>::new(),
    };
    record.sequence_ids.insert(199, 288192);

    let mut reader = SliceBufferReader::<u8>::new(&buffer);
    let result = record.deserialize(&mut reader);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 13);
    assert_eq!(record.sequence_ids.len(), 3);

    let result = record.sequence_ids.get(&(17 as u64));
    assert!(result.is_some());
    assert_eq!(*result.unwrap(), 288192);

    let result = record.sequence_ids.get(&(18 as u64));
    assert!(result.is_some());
    assert_eq!(*result.unwrap(), 288);

    let result = record.sequence_ids.get(&(281900 as u64));
    assert!(result.is_some());
    assert_eq!(*result.unwrap(), 779);
}

#[test]
fn log_state_record_deserialize_data_partial_missing() {
    let buffer = [4, 
                         17,
                         192, 203, 17,
                         18,
                         160, 2,
                         172, 154, 17,
                         139, 6];

    let mut record = LogStateSnapshotRecord{
        sequence_ids: std::collections::BTreeMap::<u64, u64>::new(),
    };
    record.sequence_ids.insert(199, 288192);

    let mut reader = SliceBufferReader::<u8>::new(&buffer);
    let result = record.deserialize(&mut reader);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
}

#[test]
fn block_write_record_size_correct() {
    let mut record = BlockWriteRecord::new(18350178101100);
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192, size: 96, flags: 88});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 78, size: 4096, flags: 19});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 16384, size: 16781, flags: 254});

    assert_eq!(record.size(), 8 + 1
                              + (1 + 2 + 1) + 4096
                              + (2 + 1 + 1) + 96
                              + (3 + 3 + 1) + 16781)
}

#[test]
fn block_write_record_serialize() {
    let mut record = BlockWriteRecord::new(18350178101100);
    record.add_extent(&DataBlockDescriptionForRecord{offset: 78, size: 4096, flags: 19});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 16384, size: 16781, flags: 254});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192, size: 96, flags: 88});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192, size: 95, flags: 88});

    const EXPECTED_HEADER_SIZE: usize = 8 + 1 
                                        + (1 + 2 + 1)
                                        + (2 + 1 + 1)
                                        + (2 + 1 + 1)
                                        + (3 + 3 + 1);

    let mut buffer = [0; EXPECTED_HEADER_SIZE];
    let mut buf_vec = MutBufferVector::<u8>::new();
    buf_vec.append_buffer_slice(&mut buffer);
    let result = record.serialize(&mut buf_vec.new_writer());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), EXPECTED_HEADER_SIZE);
    assert_eq!(buffer[..EXPECTED_HEADER_SIZE], [0x6c, 0xe7, 0xd8, 0x7b, 0xb0, 0x10, 0x00, 0x00,
                                                04,
                                                78, 128, 32, 19,
                                                128, 64, 96, 88,
                                                128, 64, 95, 88,
                                                128, 128, 1, 141, 131, 1, 254])
}

#[test]
fn block_write_record_serialize_buffer_too_short() {
    let mut record = BlockWriteRecord::new(18350178101100);
    record.add_extent(&DataBlockDescriptionForRecord{offset: 16384, size: 16781, flags: 254});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 78, size: 4096, flags: 19});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192, size: 96, flags: 88});
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192, size: 95, flags: 88});

    const EXPECTED_HEADER_SIZE: usize = 8 + 1 
                                        + (1 + 2 + 1)
                                        + (2 + 1 + 1)
                                        + (2 + 1 + 1)
                                        + (3 + 3 + 1);

    let mut buffer = [0; EXPECTED_HEADER_SIZE - 2];
    let mut buf_vec = MutBufferVector::<u8>::new();
    buf_vec.append_buffer_slice(&mut buffer);
    let result = record.serialize(&mut buf_vec.new_writer());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
}

#[test]
fn block_write_record_deserialize() {
    let buffer = [0x6c, 0xe7, 0xd8, 0x7b, 0xb0, 0x10, 0x00, 0x00,
                          04,
                          78, 128, 32, 19,
                          128, 64, 96, 88,
                          128, 64, 95, 88,
                          128, 128, 1, 141, 131, 1, 254, 0x78];

    const EXPECTED_HEADER_SIZE: usize = 8 + 1 
                                        + (1 + 2 + 1)
                                        + (2 + 1 + 1)
                                        + (2 + 1 + 1)
                                        + (3 + 3 + 1);
    let mut record = BlockWriteRecord::new(0);
    record.add_extent(&DataBlockDescriptionForRecord{offset: 8192100, size: 96, flags: 88});
    let mut reader = SliceBufferReader::<u8>::new(&buffer);
    let result = record.deserialize(&mut reader);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), EXPECTED_HEADER_SIZE);
    assert_eq!(record.block_id, 18350178101100);
    assert_eq!(record.extents.len(), 4);
    assert_eq!(record.extents[0], DataBlockDescriptionForRecord{offset: 78, size: 4096, flags: 19});
    assert_eq!(record.extents[1], DataBlockDescriptionForRecord{offset: 8192, size: 96, flags: 88});
    assert_eq!(record.extents[2], DataBlockDescriptionForRecord{offset: 8192, size: 95, flags: 88});
    assert_eq!(record.extents[3], DataBlockDescriptionForRecord{offset: 16384, size: 16781, flags: 254});
}