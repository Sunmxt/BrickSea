use std::collections::BTreeMap;
use buffer::{BufferWriter, BufferReader};
use error::{Result, Error};
use encoding::{buffer_write_varuint, buffer_read_varuint, varuint_size,
               buffer_write_u64, buffer_read_u64,
               buffer_write_u32, buffer_read_u32,
               buffer_write_u8, buffer_read_u8};


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecordContentType {
    Unknown = 0,
    LogStateSnapshot = 1,
    BlockWrite = 2,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecordType {
    Full = 0,
    Starting = 1,
    Middle = 2,
    Ending = 3,
}

impl RecordType {
    pub fn to_raw_value(&self) -> u8 {
        *self as u8
    }

    pub fn from_raw_value(&mut self, raw_value: u8) -> Result<()> {
        *self = match raw_value {
            0 => RecordType::Full,
            1 => RecordType::Starting,
            2 => RecordType::Middle,
            3 => RecordType::Ending,
            _ => return Err(Error::InvalidRecordType),
        };

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecordCommonDescription {
    pub sequence_id: u64,
    pub block_id: u64,
    pub content_type: RecordContentType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
    pub record_type: RecordType,
    pub desc: RecordCommonDescription,
    pub size: u32,
}

impl Record {
    pub const fn header_size() -> usize {
        1 + 1 + 8 + 8 + 4
    }

    pub fn serailize(&self, buffer: &mut impl BufferWriter<u8>) -> Result<usize> {
        let mut written_size = 0;

        written_size += buffer_write_u8(buffer, self.record_type.to_raw_value())?;
        written_size += buffer_write_u8(buffer, self.desc.content_type.to_raw_value())?;
        written_size += buffer_write_u64(buffer, self.desc.block_id)?;
        written_size += buffer_write_u64(buffer, self.desc.sequence_id)?;
        written_size += buffer_write_u32(buffer, self.size)?;

        Ok(written_size)
    }

    pub fn deserialize(&mut self, buffer: &mut impl BufferReader<u8>) -> Result<usize> {
        let mut read_size = 0;

        let mut raw_value: u8 = 0;
        read_size += buffer_read_u8(buffer, &mut raw_value)?;
        self.record_type.from_raw_value(raw_value)?;

        read_size += buffer_read_u8(buffer, &mut raw_value)?;
        self.desc.content_type.from_raw_value(raw_value)?;

        read_size += buffer_read_u64(buffer, &mut self.desc.block_id)?;
        read_size += buffer_read_u64(buffer, &mut self.desc.sequence_id)?;
        read_size += buffer_read_u32(buffer, &mut self.size)?;
        
        Ok(read_size)
    }
}


impl RecordContentType {
    pub fn to_raw_value(&self) -> u8 {
        *self as u8
    }

    pub fn from_raw_value(&mut self, value: u8) -> Result<()> {
        *self = match value {
            0 => RecordContentType::Unknown,
            1 => RecordContentType::LogStateSnapshot,
            2 => RecordContentType::BlockWrite,
            _ => return Err(Error::InvalidRecordContentType),
        };

        Ok(())
    }
}

pub trait SerializableLogObject {
    fn serialize(&self, buffer: &mut impl BufferWriter<u8>) -> Result<usize>;
    fn deserialize(&mut self, buffer: &mut impl BufferReader<u8>) -> Result<usize>;
    fn size(&self) -> usize;
    fn content_type() -> RecordContentType;
}


//////////////////////////////////////////////////////////////////////////
// Implementation of log state snapshot record                          //
//                                                                      //
// Record format:                                                       //
//   +--------------+---------+----------+-----+---------+----------+   //
//   | num_of_entry | pv_id_1 | seq_id_1 | ... | pv_id_N | seq_id_N |   //
//   +--------------+---------+----------+-----+---------+----------+   //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

pub struct LogStateSnapshotRecord {
    pub sequence_ids: BTreeMap<u64, u64>,
}

impl SerializableLogObject for LogStateSnapshotRecord {
    fn serialize(&self, buffer: &mut impl BufferWriter<u8>) -> Result<usize> {
        let mut total_written_size: usize = 0;

        total_written_size += buffer_write_varuint(buffer, self.sequence_ids.len() as u64)?;

        for (pv_id, seq) in &self.sequence_ids {
            total_written_size += buffer_write_varuint(buffer, *pv_id)?;
            total_written_size += buffer_write_varuint(buffer, *seq)?;
        }

        Ok(total_written_size)
    }

    fn deserialize(&mut self, buffer: &mut impl BufferReader<u8>) -> Result<usize> {
        self.sequence_ids.clear();

        let mut pv_count: u64 = 0;
        let mut consumed: usize = 0;

        consumed += buffer_read_varuint(buffer, &mut pv_count)?;

        for _ in 0..pv_count {
            let mut pv_id: u64 = 0;
            consumed += buffer_read_varuint(buffer, &mut pv_id)?;

            let mut seq: u64 = 0;
            consumed += buffer_read_varuint(buffer, &mut seq)?;
            self.sequence_ids.insert(pv_id, seq);
        }


        Ok(consumed)
    }

    fn size(&self) -> usize {
        let mut total_size: usize = 0;

        total_size += varuint_size(self.sequence_ids.len() as u64);
        for (pv_id, seq) in &self.sequence_ids {
            total_size += varuint_size(*pv_id);
            total_size += varuint_size(*seq);
        }

        total_size
    }

    fn content_type() -> RecordContentType {
        RecordContentType::LogStateSnapshot
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementation of block write record                                                                          //
//                                                                                                               //
// Record format:                                                                                                //
//   +----------+----------------+---------------+-----+---------------+---------------+-----+---------------+   //
//   | block_id | num_of_extends | extent_desc_1 | ... | extent_desc_N | extent_data_1 | ... | extent_data_N |   //
//   +----------+----------------+---------------+-----+---------------+---------------+-----+---------------+   //
// Extent Desc format:                                                                                           //
//   +--------+------+-------+                                                                                   //
//   | offset | size | flags |                                                                                   //
//   +--------+------+-------+                                                                                   //
//                                                                                                               //
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct DataBlockDescriptionForRecord {
    pub offset: u32,
    pub size: u32,
    pub flags: u8,
}

const DATA_BLOCK_DESC_FLAG_EXTENT_ATTACHMENT: u8 = 0x01;

impl DataBlockDescriptionForRecord {
    pub fn new() -> DataBlockDescriptionForRecord {
        DataBlockDescriptionForRecord{
            offset: 0,
            size: 0,
            flags: 0,
        }
    }

    pub fn has_extent_attachment(&self) -> bool {
        self.flags & DATA_BLOCK_DESC_FLAG_EXTENT_ATTACHMENT != 0
    }

    pub fn config_extent_attachment(&mut self, enable: bool) {
        if enable {
            self.flags |= DATA_BLOCK_DESC_FLAG_EXTENT_ATTACHMENT;
        } else {
            self.flags &= !DATA_BLOCK_DESC_FLAG_EXTENT_ATTACHMENT;
        }
    }
}

pub struct BlockWriteRecord {
    pub block_id: u64,
    pub extents: Vec<DataBlockDescriptionForRecord>,
}

impl BlockWriteRecord {
    pub fn new(block_id: u64) -> BlockWriteRecord {
        BlockWriteRecord{
            block_id,
            extents: Vec::<DataBlockDescriptionForRecord>::new(),
        }
    }

    pub fn add_extent(&mut self, extent: &DataBlockDescriptionForRecord) {
        self.extents.push(*extent);
    }
}

impl SerializableLogObject for BlockWriteRecord {
    fn serialize(&self, buffer: &mut impl BufferWriter<u8>) -> Result<usize> {
        let mut total_written_size: usize = 0;

        // block id
        total_written_size += buffer_write_u64(buffer, self.block_id)?;

        // extents
        total_written_size += buffer_write_varuint(buffer, self.extents.len() as u64)?;
        let mut sorted_extents = Vec::<DataBlockDescriptionForRecord>::new();
        for extent in &self.extents {
            sorted_extents.push(*extent);
        }
        sorted_extents.sort_by( |a, b| a.offset.cmp(&b.offset));

        for extent in &sorted_extents {
            total_written_size += buffer_write_varuint(buffer, extent.offset)?;
            total_written_size += buffer_write_varuint(buffer, extent.size)?;
            total_written_size += buffer_write_u8(buffer, extent.flags)?;
        }

        Ok(total_written_size)
    }

    fn deserialize(&mut self, buffer: &mut impl BufferReader<u8>) -> Result<usize> {
        let mut total_consumed_size: usize = 0;

        // block id.
        total_consumed_size += buffer_read_u64(buffer, &mut self.block_id)?;

        // extents.
        let mut num_of_extents: u64 = 0;
        let mut x: u64 = 0;
        total_consumed_size += buffer_read_varuint(buffer, &mut num_of_extents)?;
        self.extents.clear();
        self.extents.reserve(num_of_extents as usize);
        for _ in 0..num_of_extents {
            let mut extent = DataBlockDescriptionForRecord::new();

            // extent.offset
            total_consumed_size += buffer_read_varuint(buffer, &mut x)?;
            if x > std::u32::MAX as u64 {
                return Err(Error::BadRecord)
            }
            extent.offset = x as u32;

            // extent.size
            total_consumed_size += buffer_read_varuint(buffer, &mut x)?;
            if x > std::u32::MAX as u64 {
                return Err(Error::BadRecord)
            }
            extent.size = x as u32;

            // extent.flags
            total_consumed_size += buffer_read_u8(buffer, &mut extent.flags)?;

            self.extents.push(extent);
        }

        Ok(total_consumed_size)
    }

    fn size(&self) -> usize {
        let mut total_size: usize = 8;

        total_size += varuint_size(self.extents.len() as u64);
        for extent in &self.extents {
            total_size += varuint_size(extent.offset);
            total_size += varuint_size(extent.size);
            total_size += 1;
            total_size += extent.size as usize;
        }

        total_size
    }

    fn content_type() -> RecordContentType {
        RecordContentType::BlockWrite
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer::{SliceBufferReader, MutBufferVector};

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

    /* block write record tests */
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
}