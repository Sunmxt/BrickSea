use crate::error::{Result, Error};
use std::collections::BTreeMap;
use std::vec::Vec;
use crate::encoding::{varuint_size, buffer_write_varuint, buffer_read_varuint,
                      buffer_read_u64, buffer_write_u64,
                      buffer_read_u8, buffer_write_u8};
use crate::buffer::{BufferWriter, BufferReader, ImmutRefBufferVector};

pub trait SerializableLogObject {
    fn serialize(&self, buffer: &mut impl BufferWriter<u8>) -> Result<usize>;
    fn deserialize(&mut self, buffer: &mut impl BufferReader<u8>) -> Result<usize>;
    fn size(&self) -> usize;
}

/////////////////////////////////////////////////
// Implementation of log state snapshot record //
/////////////////////////////////////////////////

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
}

/////////////////////////////////////////////////
// Implementation of block write record        //
/////////////////////////////////////////////////

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
}

/////////////////////////////////////////////////
// Implementation of wal block writer          //
/////////////////////////////////////////////////

pub struct BlockWriter {
}

impl BlockWriter {
    pub fn push_record(&mut self, record: &impl SerializableLogObject) -> Result<()> {
        Err(Error::NotImplemented)
    }

    pub fn push_write_block_record(&mut self, record: &BlockWriteRecord) -> Result<()> {
        Err(Error::NotImplemented)
    }
}

pub enum RecordContentType {
    LogStateSnapshot = 0,
    BlockWrite = 1,
}

pub enum RecordType {
    Full = 0,
    Starting = 1,
    Middle = 2,
    Ending = 3,
}

//pub enum EntryContent {
//    LogStateSnapshot(LogStateSnapshotRecord),
//    BlockWrite(BlockWriteRecord),
//}
//
//pub struct Record {
//    record_type: RecordType,
//    sequence_id: u64,
//    content_start_offset: u32,
//    size: u32,
//}
//
//pub struct Entry {
//    content: EntryContent,
//}
//
//pub struct Block {
//    block_id: u32,
//    block_size: u32,
//    used_size: u32,
//
//    data: Vec<u8>,
//    records: Vec<Record>,
//}
//
//impl Block {
//    pub fn new(block_id: u32, block_size: u32) -> Block {
//        Block{
//            block_id,
//            block_size,
//            used_size: 0,
//            data: Vec::<u8>::new(),
//            records: Vec::<Record>::new(),
//        }
//    }
//
//    pub fn max_sequence_id(&self) -> Option<u64> {
//        let last_record_index = self.records.len();
//        if last_record_index < 1 {
//            return None;
//        }
//
//        Some(self.records[last_record_index - 1].sequence_id)
//    }
//}
//
//pub struct LogReader {
//    block_size: u32,
//}
//
//impl LogReader {
//    pub fn new(input: impl BinaryInputStream, block_size_pow: u8) -> Result<LogReader> {
//        Err(Error::WALStreamNotFound)
//    }
//
//    pub fn current_block_id(&mut self) -> u32 {
//        0
//    }
//
//    pub fn seek_to_end_block(&mut self) -> Result<()> {
//        Ok(())
//    }
//
//    pub fn parse_current_block(&self) -> Result<Block> {
//        Ok(Block::new(0, self.block_size))
//    }
//}
//
//pub struct LogWriter {
//    append_start_block_id: u32,
//    current_block: Option<Block>,
//
//    block_size: u32,
//}
//
//impl LogWriter {
//   pub fn new(mut reader: LogReader, output: &mut impl LogOutputStream, block_size_pow: u8) -> Result<LogWriter> {
//       reader.seek_to_end_block()?;
//
//       let mut writer = LogWriter {
//           append_start_block_id: 0,
//           current_block: Some(reader.parse_current_block()?),
//           block_size: 1 << block_size_pow,
//       };
//
//       if let Some(block) = &writer.current_block {
//           // append-starting block id is current block id.
//           writer.append_start_block_id = block.block_id;
//       } else {
//           // no current block, we init a new WAL stream.
//           writer.reset_stream(output)?;
//       }
//
//       Ok(writer)
//   }
//
//   pub fn new_empty(mut output: impl LogOutputStream, block_size_pow: u8) -> Result<LogWriter> {
//       let block_size = (1 as u32) << block_size_pow;
//       let mut writer = LogWriter{
//           append_start_block_id: 0,
//           current_block: None,
//           block_size,
//       };
//
//       writer.reset_stream(&mut output)?;
//
//       Ok(writer)
//   }
//
//   pub fn last_sequence_id(&self) -> u64 {
//       let mut sequence_id: Option<u64> = None;
//
//       if let Some(block) = &self.current_block {
//           sequence_id = block.max_sequence_id();
//       }
//
//       if let Some(id) = sequence_id {
//           id
//       } else {
//           0
//       }
//   }
//
//   pub fn reset_stream(&mut self, output: &mut impl LogOutputStream) -> Result<()> {
//       self.append_start_block_id = 0;
//       self.current_block = Some(Block::new(0, self.block_size));
//
//       self.put_stream_head(self.last_sequence_id())
//   }
//
//   pub fn put_stream_head(&mut self, start_sequence_id: u64) -> Result<()> {
//       Err(Error::NotImplemented)
//   }
//
//   pub fn put_write_block_operation(&mut self, offset: u64, size: u64, data: Option<&[u8]>) -> Result<()> {
//       Err(Error::NotImplemented)
//   }
//
//   pub fn write_stream(&mut self, output: &mut impl LogOutputStream) -> Result<()> {
//       Err(Error::NotImplemented)
//   }
//   //pub fn finalize(self) -> (Vec<u8>, u8, u8) {
//   //}
//}

pub struct WALStore {
}