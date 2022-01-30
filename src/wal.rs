use crate::error::{Result, Error};
use std::collections::BTreeMap;
use crate::encoding::{varuint_size, buffer_write_varuint, buffer_read_varuint};
use crate::buffer::{BufferWriter, BufferReader};

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

pub struct BlockWriteRecord<'a> {
    pub block_id: u64,
    pub offset: u32,
    pub data: Option<&'a [u8]>,
}

impl<'a> BlockWriteRecord<'a> {
    pub fn new(block_id: u64, offset: u32, data: &[u8]) -> BlockWriteRecord {
        BlockWriteRecord{
            block_id,
            offset,
            data: Some(data),
        }
    }

    pub fn new_empty() -> BlockWriteRecord<'a> {
        BlockWriteRecord{
            block_id: 0,
            offset: 0,
            data: None,
        }
    }
}

/////////////////////////////////////////////////
// Implementation of wal block writer          //
/////////////////////////////////////////////////

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