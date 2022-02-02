use std::vec::Vec;

use error::{Result, Error};
use buffer::{BufferWriter, MutBufferVector,
             AbstractBufferVector, ImmutRefBufferVector,
             BufferVectorWriter, SliceBufferWriter};

use super::record::{SerializableLogObject, BlockWriteRecord,
                    RecordType, Record, RecordCommonDescription};
use super::block::Block;

#[derive(Debug)]
pub struct LogWriter {
    block_size: usize,

    active_block_buffer: Option<Vec<u8>>,
    active_block_write_cursor: usize,
    seal_blocks: Vec<(Vec<u8>, usize)>,
}

pub struct LogRecordBlockBuffer<'a> {
    block_size: usize,
    desc: RecordCommonDescription,
    first_block_slice: &'a mut [u8],
    first_block_consumed_size: usize,
    extra_full_blocks: Option<Vec<u8>>,
    extra_tail_block: Option<Vec<u8>>,
    tail_block_consumed_size: usize,
}

impl<'a> LogRecordBlockBuffer<'a> {
    fn new(first_block_slice: &'a mut [u8], block_size: usize, expected_size: usize, desc: RecordCommonDescription) -> LogRecordBlockBuffer {
        let mut block_buffer = LogRecordBlockBuffer{
            block_size, desc,
            first_block_slice,
            first_block_consumed_size: 0,
            extra_full_blocks: None,
            extra_tail_block: None,
            tail_block_consumed_size: 0,
        };

        // alloc first record.
        let mut need_size = expected_size;
        let first_block_slice_size = block_buffer.first_block_slice.len();
        let full_record_total_size = need_size + Record::header_size();
        if first_block_slice_size > full_record_total_size {
            // the first block can fully hold the record.
            need_size = 0;
            block_buffer.first_block_consumed_size = full_record_total_size;
        } else if Record::header_size() < first_block_slice_size && first_block_slice_size <= full_record_total_size {
            // the first block can hold small part of the record.
            need_size -= first_block_slice_size - Record::header_size();
            block_buffer.first_block_consumed_size = first_block_slice_size;
        } /* else {
            nothing to do since available slice of first block is too small to store any record content.
        }*/

        // when the first block cannot hold record, alloc more blocks.
        if need_size > 0 {
            let max_data_size_per_full_block = Block::max_free_size(block_size) - Record::header_size();
            let num_of_full_block = need_size / max_data_size_per_full_block;

            if num_of_full_block > 0 { // alloc full blocks.
                let mut extra_full_block_buffer = Vec::new();
                extra_full_block_buffer.resize(num_of_full_block * block_size, 0);
                block_buffer.extra_full_blocks = Some(extra_full_block_buffer);
                need_size -= num_of_full_block * max_data_size_per_full_block;
            }

            if need_size > 0 { // tail block.
                let mut extra_tail_block_buffer = Vec::new();
                extra_tail_block_buffer.resize(block_size, 0);
                block_buffer.extra_tail_block = Some(extra_tail_block_buffer);
                block_buffer.tail_block_consumed_size = Block::header_size() + Record::header_size() + need_size;
            };
        }

        block_buffer
    }

    fn buffer_vector_for_record_write(&mut self) -> Result<MutBufferVector<u8>> {
        let mut buf_vec = MutBufferVector::<u8>::new();
        
        // slice of first block.
        if self.first_block_consumed_size > Record::header_size() { 
            buf_vec.append_buffer_slice(&mut self.first_block_slice[Record::header_size()..self.first_block_consumed_size]);
        }

        // slice of full blocks.
        if let Some(full_block_buf) = self.extra_full_blocks.as_deref_mut() { 
            let num_of_blocks = full_block_buf.len() / self.block_size;
            assert_eq!(num_of_blocks * self.block_size, full_block_buf.len());

            let buf_ptr = full_block_buf.as_mut_ptr();
            for index in 0..num_of_blocks {
                let block_start_offset = index * self.block_size;
                let raw_block_buf = unsafe {
                    // SAFETY:
                    //   1. num_of_blocks * block_size == full_block_buf.len(), so refs must be valid.
                    //   2. all refs of data chunk aren't overlapped. 
                    //      it prevents duplicated mutable refeneces to same region.
                    //   3. refs will not outlive the buffer.
                    std::slice::from_raw_parts_mut(buf_ptr.add(block_start_offset), self.block_size)
                };
                let payload_buf = Block::payload_ref_mut(raw_block_buf)?;
                buf_vec.append_buffer_slice(&mut payload_buf[Record::header_size()..]);
            }
        }

        // slice of tail block.
        if let Some(tail_block_buf) = self.extra_tail_block.as_deref_mut() {
            assert!(self.tail_block_consumed_size > Block::header_size() + Record::header_size());
            let payload_buf = Block::payload_ref_mut(tail_block_buf)?;
            buf_vec.append_buffer_slice(&mut payload_buf[Record::header_size()..self.tail_block_consumed_size - Block::header_size()]);
        }

        Ok(buf_vec)
    }

    fn finalize_record_header(&mut self) -> Result<()> {
        if self.first_block_consumed_size >= Record::header_size() { // header of first record.
          let first_record_content_size = self.first_block_consumed_size - Record::header_size();
          if first_record_content_size > std::u32::MAX as usize {
              // TODO(chadmai): log error.
              return Err(Error::RecordTooLong);
          }
          let mut header_buf_writer = SliceBufferWriter::new(&mut self.first_block_slice[..Record::header_size()]);
          Record{
              desc: self.desc,
              size: first_record_content_size as u32,
              record_type: if self.extra_full_blocks.is_some() || self.extra_tail_block.is_some() {
                  RecordType::Starting
              } else {
                  RecordType::Full
              }
          }.serailize(&mut header_buf_writer)?;
          if header_buf_writer.cursor() != Record::header_size() {
              // TODO(chadmai): log error.
              return Err(Error::BadRecordWrite);
          }
        }

        // header of middle record.
        if let Some(full_block_buf) = self.extra_full_blocks.as_deref_mut() {
            let num_of_blocks = full_block_buf.len() / self.block_size;
            assert_eq!(num_of_blocks * self.block_size, full_block_buf.len());

            let data_size_per_record = Block::max_free_size(self.block_size) - Record::header_size();
            if data_size_per_record > std::u32::MAX as usize {
              // TODO(chadmai): log error.
              return Err(Error::RecordTooLong);
            }

            for index in 0..num_of_blocks {
                let block_start_offset = index * self.block_size;
                let payload_buf = Block::payload_ref_mut(&mut full_block_buf[block_start_offset..block_start_offset + self.block_size])?;
                let mut header_buf_writer = SliceBufferWriter::new(&mut payload_buf[..Record::header_size()]);

                let record_type = if self.first_block_consumed_size > Record::header_size() {
                    if index == num_of_blocks - 1 { // the last full block.
                        if self.tail_block_consumed_size > 0 { 
                            // has tail record, the last full block must have a middle record.
                            RecordType::Middle 
                        } else {
                            // has no tail record, the last full block has a ending record.
                            RecordType::Ending
                        }
                    } else {
                        // since the first slice buffer contains a starting record, this must be middle record.
                        RecordType::Middle 
                    }
                } else { // no first record.
                    if index == 0 {
                        if num_of_blocks < 2 && self.tail_block_consumed_size < 1 {
                            // no tail block and has only one full block. this full block contains a full record.
                            RecordType::Full
                        } else {
                            RecordType::Starting // has other records.
                        }
                    } else if index == num_of_blocks - 1 { // the last full block.
                        if self.tail_block_consumed_size < 1 {
                            RecordType::Ending
                        } else {
                            RecordType::Middle
                        }
                    } else {
                        RecordType::Middle
                    }
                };

                Record{
                    desc: self.desc,
                    size: data_size_per_record as u32,
                    record_type,
                }.serailize(&mut header_buf_writer)?;
                if header_buf_writer.cursor() != Record::header_size() {
                    // TODO(chadmai): log error.
                    return Err(Error::BadRecordWrite);
                }
            }
        }

        // header of end record.
        if let Some(tail_block_buf) = self.extra_tail_block.as_deref_mut() {
            assert!(self.tail_block_consumed_size > Block::header_size() + Record::header_size());
            let payload_buf = Block::payload_ref_mut(tail_block_buf)?;
            let mut header_buf_writer = SliceBufferWriter::new(&mut payload_buf[..Record::header_size()]);
            let data_size = self.tail_block_consumed_size - Block::header_size() - Record::header_size();
            if data_size > std::u32::MAX as usize {
              // TODO(chadmai): log error.
              return Err(Error::RecordTooLong);
            }
            Record{
                desc: self.desc,
                size: data_size as u32,
                record_type: RecordType::Ending,
            }.serailize(&mut header_buf_writer)?;
            if header_buf_writer.cursor() != Record::header_size() {
                // TODO(chadmai): log error.
                return Err(Error::BadRecordWrite);
            }
        }

        Ok(())
    }

    fn finalize(mut self) -> Result<(usize, Option<Vec<u8>>, Option<(Vec<u8>, usize)>)> {
        self.finalize_record_header()?;

        Ok((self.first_block_consumed_size, self.extra_full_blocks, match self.extra_tail_block {
            Some(buffer) => Some((buffer, self.tail_block_consumed_size)),
            None => None,
        }))
    }
}

impl LogWriter {
    pub fn new(block_size: usize) -> Result<Self> {
        if Block::header_size() + Record::header_size() >= block_size {
            return Err(Error::BlockSizeTooSmall);
        }

        let mut buf = Vec::new();
        buf.resize(block_size, 0);
        Ok(Self{
            block_size,
            active_block_buffer: Some(buf),
            active_block_write_cursor: Block::header_size(),
            seal_blocks: Vec::new(),
        })
    }

    fn encode_record(&mut self, record_data_size: usize, desc: RecordCommonDescription,
                     write_fn: impl Fn(&mut BufferVectorWriter<u8, MutBufferVector<u8>>) -> Result<()>) -> Result<()> {
        // prepare buffer.
        let first_block_buffer = self.active_block_buffer.as_mut();
        let first_block_slice  = first_block_buffer.unwrap().as_mut_slice();
        let mut record_block_buffer = LogRecordBlockBuffer::new(&mut first_block_slice[self.active_block_write_cursor..], self.block_size, record_data_size, desc);
        let mut buf_vec = record_block_buffer.buffer_vector_for_record_write()?;
        let mut writer = buf_vec.new_writer();

        write_fn(&mut writer)?; // serialize
        // For simpleness, the allocated buffer should be fully used.
        // If not, treat it a malformed serialization.
        if writer.cursor() != writer.capacity() {
            return Err(Error::BadRecordWrite);
        }

        // save serialized blocks.
        let (consumed_size_of_first_block, mut full_blocks, new_active_block) = record_block_buffer.finalize()?;
        let new_active_write_cursor = self.active_block_write_cursor + consumed_size_of_first_block;
        if new_active_write_cursor > first_block_slice.len() { // should not happen.
            // TODO(chadmai): log error.
            return Err(Error::BadBlockWrite);
        }

        if full_blocks.is_some() || new_active_block.is_some() {
            let old_active_block_buffer = self.active_block_buffer.take().unwrap();
            self.seal_blocks.push((old_active_block_buffer, new_active_write_cursor));
        } else {
            self.active_block_write_cursor = new_active_write_cursor;
        }

        if full_blocks.is_some() {
            let full_blocks = full_blocks.take().unwrap();
            self.seal_blocks.push((full_blocks, self.block_size));
        }

        if let Some((buffer, cursor)) = new_active_block {
            self.active_block_buffer = Some(buffer);
            self.active_block_write_cursor = cursor;
        }

        if self.block_size - self.active_block_write_cursor <= Record::header_size() {
            let old_active_block_buffer = self.active_block_buffer.take().unwrap();
            let old_active_block_write_cursor = self.active_block_write_cursor;
            self.seal_blocks.push((old_active_block_buffer, old_active_block_write_cursor));

            let mut new_buf = Vec::new();
            new_buf.resize(self.block_size, 0);
            self.active_block_buffer = Some(new_buf);
            self.active_block_write_cursor = Block::header_size();
        }

        Ok(())
    }

    pub fn put_record<T: SerializableLogObject>(&mut self, sequence_id: u64, block_id: u64, record: &T) -> Result<()> {
        let expected_record_size = record.size();

        self.encode_record(expected_record_size, RecordCommonDescription{
            sequence_id, block_id,
            content_type: T::content_type(),
        }, |writer| {
            let written_size = record.serialize(writer)?;
            assert_eq!(expected_record_size, written_size);
            Ok(())
        })
    }

    pub fn put_block_write_record(&mut self, sequence_id: u64, block_id: u64, record: &BlockWriteRecord,
                                   data_bufs: Option<&[(impl ImmutRefBufferVector<u8> + AbstractBufferVector)]>) -> Result<()> {
        let expected_record_size = record.size();
        let attachment_data_size = match data_bufs {
            None => 0,
            Some(data_bufs) => {
                let mut sum_data_size = 0;
                for data in data_bufs {
                    sum_data_size += data.capacity();
                }

                sum_data_size
            }
        };

        if attachment_data_size >= expected_record_size {
            // TODO(chadmai): log error
            return Err(Error::BadRecord);
        }

        self.encode_record(expected_record_size, RecordCommonDescription{
            sequence_id, block_id,
            content_type: BlockWriteRecord::content_type(),
        }, |writer| {
            let written_size = record.serialize(writer)?;
            if written_size + attachment_data_size != expected_record_size {
                // TODO(chadmai): log error
                return Err(Error::BadRecordWrite);
            }
            match data_bufs {
                None => {},
                Some(buf_vecs) => {
                    for buf_vec in buf_vecs {
                        for index in 0..buf_vec.total_vector_count() {
                            // TODO(chadmai): debug log.
                            writer.write(buf_vec.vec_ref(index))?;
                        }
                    }
                },
            }

            Ok(())
        })
    }

    pub fn collect_written_blocks(&mut self) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        // seal blocks with header.
        for (blocks, cursor_of_last_block) in self.seal_blocks.as_mut_slice() {
            assert!(*cursor_of_last_block <= self.block_size);

            assert!(blocks.len() % self.block_size == 0);
            if blocks.len() < 1 {
                // TODO(chadmai): warning.
                continue;
            }

            let num_of_block = blocks.len() / self.block_size;
            for block_index in 0..num_of_block - 1 {
                let start_offset = block_index * self.block_size;
                Block::seal_block(&mut blocks[start_offset..start_offset + self.block_size])?;
            }

            let start_offset = (num_of_block - 1) * self.block_size;
            Block::seal_block(&mut blocks[start_offset..start_offset + *cursor_of_last_block])?;
        }

        // move out seal blocks.
        let mut output_seal_blocks = Vec::with_capacity(self.seal_blocks.len());
        for (vec, _) in self.seal_blocks.drain(0..self.seal_blocks.len()) {
            output_seal_blocks.push(vec);
        }

        // seal active block.
        let output_active_block = match self.active_block_buffer.as_ref() {
            Some(block_buffer) => {
                let cursor = self.active_block_write_cursor;
                assert!(cursor >= Block::header_size());

                if cursor <= Block::header_size() {
                    None
                } else {
                    let mut cloned = Vec::with_capacity(self.block_size);

                    cloned.resize(self.block_size, 0);
                    cloned.clone_from_slice(block_buffer.as_slice());
                    assert!(cursor <= cloned.len());

                    Block::seal_block(&mut cloned[..cursor])?;

                    Some(cloned)
                }
            },
            None => None,
        };

        Ok((output_seal_blocks, output_active_block))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::record::RecordContentType;
    use crc::{Crc, CRC_32_ISCSI};

    #[test]
    fn log_record_block_buffer_alloc_only_first_slice() {
        let mut first_slice_buffer = [0; Record::header_size() + 100];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, 64 * 1024, 99, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, Record::header_size() + 99);
        assert!(buffer.extra_full_blocks.is_none());
        assert!(buffer.extra_tail_block.is_none());
        assert_eq!(buffer.tail_block_consumed_size, 0);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_alloc_exactly_only_first_slice() {
        let mut first_slice_buffer = [0; Record::header_size() + 100];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, 64 * 1024, 100, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, Record::header_size() + 100);
        assert!(buffer.extra_full_blocks.is_none());
        assert!(buffer.extra_tail_block.is_none());
        assert_eq!(buffer.tail_block_consumed_size, 0);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_alloc_need_extra_tail_block() {
        let mut first_slice_buffer = [0; Record::header_size() + 50];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, 64 * 1024, 100, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, Record::header_size() + 50);
        assert!(buffer.extra_full_blocks.is_none());
        assert!(buffer.extra_tail_block.is_some());
        assert_eq!(buffer.tail_block_consumed_size, Block::header_size() + Record::header_size() + 50);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_alloc_first_slice_cannot_hold_any() {
        let mut first_slice_buffer = [0; Record::header_size()];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, 64 * 1024, 100, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, 0);
        assert!(buffer.extra_full_blocks.is_none());
        assert!(buffer.extra_tail_block.is_some());
        assert_eq!(buffer.tail_block_consumed_size, Block::header_size() + Record::header_size() + 100);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_need_exactly_full_block() {
        let mut first_slice_buffer = [0; Record::header_size()];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let block_size = 64 * 1024;
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, block_size, 
                                                (block_size - Block::header_size() - Record::header_size()) * 2, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, 0);
        assert!(buffer.extra_full_blocks.is_some());
        assert_eq!(buffer.extra_full_blocks.as_ref().unwrap().len(), 2 * block_size);
        assert!(buffer.extra_tail_block.is_none());
        assert_eq!(buffer.tail_block_consumed_size, 0);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_need_exactly_full_block_1() {
        let mut first_slice_buffer = [0; Record::header_size()];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let block_size = 64 * 1024;
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, block_size, 
                                                block_size - Block::header_size() - Record::header_size(), record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, 0);
        assert!(buffer.extra_full_blocks.is_some());
        assert_eq!(buffer.extra_full_blocks.as_ref().unwrap().len(), block_size);
        assert!(buffer.extra_tail_block.is_none());
        assert_eq!(buffer.tail_block_consumed_size, 0);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    #[test]
    fn log_record_block_buffer_use_first_slice_and_need_full_block_and_tail_block() {
        let mut first_slice_buffer = [0; Record::header_size() + 100];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let block_size = 64 * 1024;
        let buffer = LogRecordBlockBuffer::new(&mut first_slice_buffer, block_size, 
                                                (block_size - Block::header_size() - Record::header_size()) * 2 + 110, record_desc);
        assert_eq!(buffer.block_size, 64 * 1024);
        assert_eq!(buffer.first_block_consumed_size, Record::header_size() + 100);
        assert!(buffer.extra_full_blocks.is_some());
        assert_eq!(buffer.extra_full_blocks.as_ref().unwrap().len(), 2 * block_size);
        assert!(buffer.extra_tail_block.is_some());
        assert_eq!(buffer.tail_block_consumed_size, Record::header_size() + Block::header_size() + 10);
        assert_eq!(buffer.desc.sequence_id, 1938199);
        assert_eq!(buffer.desc.block_id, 13182749189);
        assert_eq!(buffer.desc.content_type, RecordContentType::BlockWrite);
    }

    fn run_log_record_block_buffer_for_write_test(has_first_slice: bool, full_block_count: usize, has_tail_block: bool) {
        let mut first_slice_buffer = [0; Record::header_size() + 100];
        let record_desc = RecordCommonDescription{
            sequence_id: 1938199,
            block_id: 13182749189,
            content_type: RecordContentType::BlockWrite,
        };
        let block_size = 32 * 1024;
        let max_free_per_full_block = block_size - Block::header_size() - Record::header_size();
        let mut expected_size = full_block_count * max_free_per_full_block + if has_tail_block {
            10
        } else {
            0
        };
        let mut buffer = if has_first_slice {
            if full_block_count < 1 && !has_tail_block {
                expected_size += 98;
            } else {
                expected_size += 100;
            }
            LogRecordBlockBuffer::new(&mut first_slice_buffer, block_size, expected_size, record_desc)
        } else {
            LogRecordBlockBuffer::new(&mut first_slice_buffer[..Record::header_size() - 1], block_size, expected_size, record_desc)
        };

        if has_first_slice {
            if full_block_count < 1 && !has_tail_block {
                assert_eq!(buffer.first_block_consumed_size, 98 + Record::header_size());
            } else {
                assert_eq!(buffer.first_block_consumed_size, 100 + Record::header_size());
            }
        } else {
            assert_eq!(buffer.first_block_consumed_size, 0);
        }
        if full_block_count > 0 {
            assert!(buffer.extra_full_blocks.is_some());
            assert_eq!(buffer.extra_full_blocks.as_ref().unwrap().len(), block_size * full_block_count);
        } else {
            assert!(buffer.extra_full_blocks.is_none());
        }
        if has_tail_block {
            assert!(buffer.extra_tail_block.is_some());
            assert_eq!(buffer.tail_block_consumed_size, Record::header_size() + Block::header_size() + 10);
        } else {
            assert!(buffer.extra_tail_block.is_none());
        }
        assert_eq!(buffer.block_size, block_size);
        assert_eq!(buffer.desc, record_desc);

        let buf_vec = buffer.buffer_vector_for_record_write();
        assert!(buf_vec.is_ok());
        let buf_vec = buf_vec.unwrap();
        assert_eq!(buf_vec.capacity(), expected_size);
        fill_test_sequence_bytes_to_buffer_vector(buf_vec);

        let result = buffer.finalize();
        assert!(result.is_ok());
        let (first_slice_consumed, full_block_buffer, tail_block_info) = result.as_ref().unwrap();
        if has_first_slice {
            if full_block_count < 1 && !has_tail_block {
                assert_eq!(*first_slice_consumed, 98 + Record::header_size());
            } else {
                assert_eq!(*first_slice_consumed, 100 + Record::header_size());
            }
        } else {
            assert_eq!(*first_slice_consumed, 0);
        }
        if full_block_count > 0 {
            assert!(full_block_buffer.is_some());
        } else {
            assert!(full_block_buffer.is_none());
        }
        if has_tail_block {
            assert!(tail_block_info.is_some());
            let (_tail_block_buffer , tail_block_consumed) = tail_block_info.as_ref().unwrap();
            assert_eq!(*tail_block_consumed, 10 + Block::header_size() + Record::header_size());
        }

        verify_test_sequence_bytes_for_finalized_blocks(&first_slice_buffer, result.unwrap(), block_size);
    }

    fn verify_header(raw_buf: &[u8], expected: Record) {
        let mut record = Record{
            desc: RecordCommonDescription{
                block_id: 0, sequence_id: 0, content_type: RecordContentType::Unknown
            },
            size: 0, record_type: RecordType::Ending,
        };
        let mut reader = buffer::SliceBufferReader::new(raw_buf);
        let result = record.deserialize(&mut reader);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Record::header_size());
        assert_eq!(record, expected);
    }

    fn fill_test_sequence_bytes_to_buffer_vector(mut buf_vec: MutBufferVector<u8>) {
        let capacity = buf_vec.capacity();
        let mut writer = buf_vec.new_writer();
        let mut tmp_buf = [0; 1];
        for i in 0..capacity {
            tmp_buf[0] = (i % 256) as u8;
            let result = writer.write(&tmp_buf);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 1);
        }
    }

    fn verify_test_sequence_bytes_for_finalized_blocks(first_slice: &[u8], finalized_result: (usize, Option<Vec<u8>>, Option<(Vec<u8>, usize)>), block_size: usize) {
        let (first_slice_consumed, full_block_buffer, tail_block_info) = finalized_result;
        assert!(first_slice.len() >= first_slice_consumed);

        let mut accumulated_size = 0;
        if first_slice_consumed > 0 {
            assert!(first_slice_consumed > Record::header_size());

            // verify content of first slice.
            for i in 0..first_slice_consumed - Record::header_size() {
                assert_eq!(first_slice[i + Record::header_size()], (i % 256) as u8);
            }

            // verity record header in first slice.
            verify_header(&first_slice[..Record::header_size()], Record{
                desc: RecordCommonDescription{
                    sequence_id: 1938199,
                    block_id: 13182749189,
                    content_type: RecordContentType::BlockWrite
                },
                size: if full_block_buffer.is_none() && tail_block_info.is_none() {
                    98
                } else {
                    100
                }, record_type: if full_block_buffer.is_some() || tail_block_info.is_some() {
                    RecordType::Starting
                } else {
                    RecordType::Full
                }
            });

            accumulated_size += first_slice_consumed - Record::header_size();
        }

        if full_block_buffer.is_some() {
            // verify middle full blocks.
            let full_block_buffer = full_block_buffer.unwrap();
            let max_free_per_block = block_size - Block::header_size() - Record::header_size();

            assert!(full_block_buffer.len() % block_size == 0);
            let total_full_block_count = full_block_buffer.len() / block_size;
            for block_index in 0..total_full_block_count {
                for i in 0..max_free_per_block {
                    assert_eq!(full_block_buffer[i + block_size * block_index + Record::header_size() + Block::header_size()], ((accumulated_size + i) % 256) as u8);
                }

                let expected_record_type = if first_slice_consumed > 0 {
                    if block_index == total_full_block_count - 1 {
                        if tail_block_info.is_some() {
                            RecordType::Middle
                        } else {
                            RecordType::Ending
                        }
                    } else {
                        RecordType::Middle
                    }
                } else {
                    if block_index == 0 {
                        if total_full_block_count > 1 || tail_block_info.is_some() {
                            RecordType::Starting
                        } else {
                            RecordType::Full
                        }
                    } else if block_index == total_full_block_count - 1 {
                        if tail_block_info.is_some() {
                            RecordType::Middle
                        } else {
                            RecordType::Ending
                        }
                    } else {
                        RecordType::Middle
                    }
                };

                verify_header(&full_block_buffer[block_size * block_index + Block::header_size()..block_size * block_index + Block::header_size() + Record::header_size()], Record{
                    desc: RecordCommonDescription{
                        sequence_id: 1938199,
                        block_id: 13182749189,
                        content_type: RecordContentType::BlockWrite
                    },
                    size: max_free_per_block as u32, record_type: expected_record_type,
                });

                accumulated_size += max_free_per_block;
            }

        }

        // verify tail block.
        if tail_block_info.is_some() {
            let (tail_block_buffer , tail_block_consumed) = tail_block_info.unwrap();
            assert!(tail_block_consumed > Block::header_size() + Record::header_size());

            for i in 0..tail_block_consumed - Block::header_size() - Record::header_size() {
                assert_eq!(tail_block_buffer[i + Record::header_size() + Block::header_size()], ((accumulated_size + i) % 256) as u8);
            }
            verify_header(&tail_block_buffer[Block::header_size()..Block::header_size() + Record::header_size()], Record{
                desc: RecordCommonDescription{
                    sequence_id: 1938199,
                    block_id: 13182749189,
                    content_type: RecordContentType::BlockWrite,
                },
                size: (tail_block_consumed - Block::header_size() - Record::header_size()) as u32, record_type: RecordType::Ending,
            });
        }
    }

    #[test]
    fn log_record_block_buffer_write_correct() {
        run_log_record_block_buffer_for_write_test(false, 0, true);
        run_log_record_block_buffer_for_write_test(false, 1, false);
        run_log_record_block_buffer_for_write_test(false, 1, true);
        run_log_record_block_buffer_for_write_test(false, 2, false);
        run_log_record_block_buffer_for_write_test(false, 2, true);
        run_log_record_block_buffer_for_write_test(true, 0, false);
        run_log_record_block_buffer_for_write_test(true, 0, true);
        run_log_record_block_buffer_for_write_test(true, 1, false);
        run_log_record_block_buffer_for_write_test(true, 1, true);
        run_log_record_block_buffer_for_write_test(true, 2, false);
        run_log_record_block_buffer_for_write_test(true, 2, true);

        run_log_record_block_buffer_for_write_test(false, 3, false);
        run_log_record_block_buffer_for_write_test(false, 3, true);
        run_log_record_block_buffer_for_write_test(true, 3, false);
        run_log_record_block_buffer_for_write_test(true, 3, true);
    }

    /* LogWriter test */
    #[test]
    fn log_writer_init() {
        let block_size = 64 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let writer = result.unwrap();
        assert_eq!(writer.block_size, block_size);
        assert!(writer.active_block_buffer.is_some());
        assert_eq!(writer.active_block_write_cursor, Block::header_size());
        assert_eq!(writer.seal_blocks.len(), 0);
    }

    #[test]
    fn log_writer_init_block_size_too_small() {
        let result = LogWriter::new(Block::header_size() + Record::header_size());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BlockSizeTooSmall);
    }

    fn verify_block(buf: &[u8], expected_payload_len: usize) -> &[u8] {
        let result = Block::parse_block(buf);
        assert!(result.is_ok());
        let (parsed_crc, payload_ref) = result.unwrap();
        assert_eq!(payload_ref.len(), expected_payload_len);

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut hasher = crc.digest();
        hasher.update(&buf[4..Block::header_size() + payload_ref.len()]);
        let expected_crc = hasher.finalize();

        assert_eq!(expected_crc, parsed_crc);

        payload_ref
    }

    fn verify_collected_written_blocks(seal_blocks: Vec<Vec<u8>>, active_block: Option<Vec<u8>>, block_size: usize, expected_payload_lens: &[usize]) {
        let mut total_block_count = 0;
        if active_block.is_some() {
            total_block_count += 1;
        }
        for blocks in &seal_blocks {
            assert!(blocks.len() % block_size == 0);
            total_block_count += blocks.len() / block_size;
        }
        assert_eq!(expected_payload_lens.len(), total_block_count);

        if active_block.is_some() {
            assert_eq!(active_block.as_ref().unwrap().len(), block_size);
            verify_block(active_block.as_ref().unwrap().as_ref(), expected_payload_lens[expected_payload_lens.len() - 1]);
        }

        let mut block_no = 0;
        for blocks in &seal_blocks {
            for index in 0..blocks.len() / block_size {
                let start_offset = index * block_size;
                verify_block(&blocks[start_offset..start_offset + block_size], expected_payload_lens[block_no]);
                block_no += 1;
            }
        }
    }

    fn log_writer_put_record_for_test(writer: &mut LogWriter, record_size: usize) {
        let result = writer.encode_record(record_size, RecordCommonDescription{
            sequence_id: 101209,
            block_id: 1011,
            content_type: RecordContentType::Unknown,
        }, |writer| {
            let mut tmp_buf = [0; 1];
            for i in 0..record_size {
                tmp_buf[0] = (i % 256) as u8;
                let result = writer.write(&tmp_buf);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 1);
            }
            Ok(())
        });
        assert!(result.is_ok());
    }

    #[test]
    fn log_writer_encode_record_one_active_block() {
        let block_size = 32 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        for _i in 0..3 {
            log_writer_put_record_for_test(&mut writer, 100);
        }

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 0);
        assert!(active_block_buffer.is_some());
        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, &[(Record::header_size() + 100) * 3]);
    }

    #[test]
    fn log_writer_encode_record_autoseal_full_active_block() {
        let block_size = 8 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let record_content_size = block_size - Block::header_size() - Record::header_size() * 2;
        log_writer_put_record_for_test(&mut writer, record_content_size);

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 1);
        assert!(active_block_buffer.is_none());

        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, &[block_size - Record::header_size() - Block::header_size()]);
    }

    #[test]
    fn log_writer_encode_record_active_block_full() {
        let block_size = 8 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let record_content_size = block_size - Block::header_size() - Record::header_size();
        log_writer_put_record_for_test(&mut writer, record_content_size);

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 1);
        assert!(active_block_buffer.is_none());

        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, &[block_size - Block::header_size()]);
    }

    #[test]
    fn log_writer_encode_record_active_block_full_and_has_tail() {
        let block_size = 8 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let record_content_size = block_size - Block::header_size() - Record::header_size() + 10;
        log_writer_put_record_for_test(&mut writer, record_content_size);

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 1);
        assert!(active_block_buffer.is_some());

        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, 
                    &[block_size - Block::header_size(),
                                         10 + Record::header_size()]);
    }

    #[test]
    fn log_writer_encode_record_active_block_and_full_blocks_no_tail() {
        let block_size = 8 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let record_content_size = (block_size - Block::header_size() - Record::header_size()) * 4;
        log_writer_put_record_for_test(&mut writer, record_content_size);

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 2);
        assert!(active_block_buffer.is_none());

        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, 
                    &[block_size - Block::header_size(),
                                         block_size - Block::header_size(),
                                         block_size - Block::header_size(),
                                         block_size - Block::header_size()]);
    }

    #[test]
    fn log_writer_encode_record_active_block_and_full_blocks() {
        let block_size = 8 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let record_content_size = (block_size - Block::header_size() - Record::header_size()) * 4 + 1;
        log_writer_put_record_for_test(&mut writer, record_content_size);

        let result = writer.collect_written_blocks();
        assert!(result.is_ok());
        let (seal_block_buffers, active_block_buffer) = result.unwrap();
        assert_eq!(seal_block_buffers.len(), 2);
        assert!(active_block_buffer.is_some());

        verify_collected_written_blocks(seal_block_buffers, active_block_buffer, block_size, 
                    &[block_size - Block::header_size(),
                                         block_size - Block::header_size(),
                                         block_size - Block::header_size(),
                                         block_size - Block::header_size(),
                                         1 + Record::header_size()]);
    }

    #[test]
    fn log_writer_encode_record_malformed_record_write() {
        let block_size = 32 * 1024;
        let result = LogWriter::new(block_size);
        assert!(result.is_ok());
        let mut writer = result.unwrap();

        let result = writer.encode_record(110, RecordCommonDescription{
            sequence_id: 101209,
            block_id: 1011,
            content_type: RecordContentType::Unknown,
        }, |writer| {
            let mut tmp_buf = [0; 1];
            for i in 0..100 {
                tmp_buf[0] = (i % 256) as u8;
                let result = writer.write(&tmp_buf);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 1);
            }
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BadRecordWrite);
    }

}