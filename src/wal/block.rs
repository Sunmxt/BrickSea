use crc::{Crc, CRC_32_ISCSI};
use encoding::{write_u32, read_u32};
use error::{Result, Error};

//////////////////////////////////////////////////
// Block format:                                //
//   +-------+-----------+------------------+   //
//   | crc32 | used_size |      payload     |   //
//   +-------+-----------+------------------+   //
//                                              //
//////////////////////////////////////////////////

pub struct Block {
    pub crc: u32,
    pub used_size: usize,
}

impl Block {
    pub const fn header_size() -> usize {
        4 + 4 // crc32 + used_size(u32)
    }

    pub fn max_free_size(block_size: usize) -> usize {
        if block_size < Self::header_size() {
            return 0;
        }

        block_size - Self::header_size()
    }

    pub fn payload_ref_mut(buf: &mut [u8]) -> Result<&mut [u8]> {
        if buf.len() < Block::header_size() {
            return Err(Error::BufferTooSmall);
        }

        Ok(&mut buf[Self::header_size()..])
    }

    pub fn payload_ref(buf: &[u8]) -> Result<&[u8]> {
        if buf.len() < Block::header_size() {
            return Err(Error::BufferTooSmall);
        }

        Ok(&buf[Self::header_size()..])
    }

    pub fn seal_block(buf: &mut [u8]) -> Result<u32> {
        if buf.len() <= Block::header_size() {
            return Err(Error::BufferTooSmall);
        }

        let used_size = buf.len() - Self::header_size();
        if used_size > std::u32::MAX as usize {
            return Err(Error::BufferTooLarge);
        }
        let written_size = write_u32(&mut buf[4..8], used_size as u32)?;
        assert_eq!(written_size, 4);

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut hasher = crc.digest();
        //let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[4..]);
        let block_crc = hasher.finalize();
        let written_size = write_u32(&mut buf[..4], block_crc)?;
        assert_eq!(written_size, 4);

        Ok(block_crc)
    }

    pub fn parse_block(buf: &[u8]) -> Result<(u32, &[u8])> {
        if buf.len() <= Block::header_size() {
            return Err(Error::BufferTooSmall);
        }

        let mut used_size = 0;
        let read_size = read_u32::<u32>(&buf[4..8], &mut used_size)?;
        assert_eq!(read_size, 4);
        let used_size = used_size as usize;

        if used_size + Self::header_size() > buf.len() {
            return Err(Error::BadBlockHeader);
        }

        // verify crc.
        let mut expected_crc = 0;
        let read_size = read_u32::<u32>(&buf[0..4], &mut expected_crc)?;
        assert_eq!(read_size, 4);

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut hasher = crc.digest();
        hasher.update(&buf[4..used_size + Self::header_size()]);
        let calced_block_crc = hasher.finalize();
        if expected_crc != calced_block_crc {
            // TODO(chadmai): log error.
            return Err(Error::BadChecksum);
        }

        return Ok((expected_crc, &buf[Self::header_size()..Self::header_size() + used_size]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn block_header_size_correct() {
        assert_eq!(Block::header_size(), 4 + 4);
    }

    #[test]
    fn block_max_free_size_correct() {
        let random_block_size = (rand::random::<usize>() & 0xFF) + Block::header_size();
        assert_eq!(Block::max_free_size(random_block_size), random_block_size - Block::header_size());

        for block_size in 0..Block::header_size() {
            assert_eq!(Block::max_free_size(block_size), 0);
        }
    }

    #[test]
    fn block_payload_ref_correct() {
        let mut buf = [0; Block::header_size() + 4];
        for i in Block::header_size()..Block::header_size() + 4 {
            buf[i] = rand::random();
        }
        let result = Block::payload_ref(&buf);
        assert!(result.is_ok());
        assert_eq!(Ordering::Equal, result.unwrap().cmp(&buf[Block::header_size()..]));
        let result = Block::payload_ref_mut(&mut buf);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.len(), 4);
        let mut buf2 = [0; 4];
        buf2.clone_from_slice(result);
        assert_eq!(buf2, &buf[Block::header_size()..]);
    }

    #[test]
    fn block_seal_block() {
        let mut buf = [0 as u8; Block::header_size() + 256];
        for i in 0..256 {
            buf[Block::header_size() + i] = i as u8;
        }
        let result = Block::seal_block(&mut buf);
        assert!(result.is_ok());
        assert_eq!(buf[4..8], [0, 1, 0, 0]);
        assert_eq!(result.unwrap(), 0x790ec958);
        assert_eq!(buf[0..4], [0x58, 0xc9, 0x0e, 0x79]);
    }

    #[test]
    fn block_seal_block_block_too_small() {
        let mut buf = [0 as u8; Block::header_size()];
        let result = Block::seal_block(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    #[test]
    fn block_seal_block_block_too_large() {
        let mut buf = [0 as u8; Block::header_size()];
        let ptr = buf.as_mut_ptr();

        let invalid_buffer = unsafe {
            std::slice::from_raw_parts_mut(ptr, std::u32::MAX as usize + 1 + Block::header_size())
        };
        let result = Block::seal_block(invalid_buffer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooLarge);
    }

}