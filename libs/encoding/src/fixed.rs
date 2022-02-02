use error::{Result, Error};
use buffer::{SliceBufferWriter, SliceBufferReader,
             BufferWriter, BufferReader};

/////////////////////////////////////////////
// Implementation of u64 integer encoding  //
/////////////////////////////////////////////
pub fn write_u64<T: Into<u64>>(buf: &mut [u8], x: T) -> Result<usize> {
    let mut writer = SliceBufferWriter::<u8>::new(buf);

    buffer_write_u64(&mut writer, x)
}

pub fn buffer_write_u64<T: Into<u64>>(buffer: &mut impl BufferWriter<u8>, x: T) -> Result<usize> {
    let mut local_buf: [u8; 1] = [0; 1];
    let x = x.into();

    for i in 0..8 {
      local_buf[0] = (x >> (i << 3)) as u8;

      buffer.write(&local_buf)?;
    }

    Ok(8)
}

pub fn read_u64(buf: &[u8], x: &mut u64) -> Result<usize> {
    let mut reader = SliceBufferReader::<u8>::new(buf);

    buffer_read_u64(&mut reader, x)
}

pub fn buffer_read_u64(buffer: &mut impl BufferReader<u8>, x: &mut u64) -> Result<usize> {
    let mut local_buf: [u8; 1] = [0; 1];
  
    *x = 0;

    for i in 0..8 {
      if buffer.read(&mut local_buf)? < 1 {
        return Err(Error::BufferTooSmall)
      }

      *x |= (local_buf[0] as u64) << (i << 3);
    }

    Ok(8)
}

/////////////////////////////////////////////
// Implementation of u32 integer encoding  //
/////////////////////////////////////////////

pub fn write_u32<T: Into<u32>>(buf: &mut [u8], x: T) -> Result<usize> {
    let mut writer = SliceBufferWriter::<u8>::new(buf);

    buffer_write_u32(&mut writer, x)
}

pub fn buffer_write_u32<T: Into<u32>>(buffer: &mut impl BufferWriter<u8>, x: T) -> Result<usize> {
    let mut local_buf: [u8; 1] = [0; 1];
    let x = x.into();

    for i in 0..4 {
      local_buf[0] = (x >> (i << 3)) as u8;

      buffer.write(&local_buf)?;
    }

    Ok(4)
}

pub fn read_u32<T: Into<u32>>(buf: &[u8], x: &mut u32) -> Result<usize> {
    let mut reader = SliceBufferReader::<u8>::new(buf);

    buffer_read_u32(&mut reader, x)
}

pub fn buffer_read_u32(buffer: &mut impl BufferReader<u8>, x: &mut u32) -> Result<usize> {
    let mut local_buf: [u8; 1] = [0; 1];
  
    *x = 0;

    for i in 0..4 {
      if buffer.read(&mut local_buf)? < 1 {
        return Err(Error::BufferTooSmall)
      }

      *x |= (local_buf[0] as u32) << (i << 3);
    }

    Ok(4)
}

/////////////////////////////////////////////
// Implementation of u16 integer encoding  //
/////////////////////////////////////////////

pub fn write_u16<T: Into<u16>>(buf: &mut [u8], x: T) -> Result<usize> {
    let mut writer = SliceBufferWriter::<u8>::new(buf);

    buffer_write_u16(&mut writer, x)
}

pub fn buffer_write_u16<T: Into<u16>>(buffer: &mut impl BufferWriter<u8>, x: T) -> Result<usize> {
    let mut local_buf: [u8; 1] = [0; 1];
    let x = x.into();

    local_buf[0] = x as u8;
    buffer.write(&local_buf)?;

    local_buf[0] = (x >> 8) as u8;
    buffer.write(&local_buf)?;

    Ok(2)
}

/////////////////////////////////////////////
// Implementation of u8 integer encoding   //
/////////////////////////////////////////////
pub fn write_u8<T: Into<u8>>(buf: &mut [u8], x: T) -> Result<usize> {
    let mut writer = SliceBufferWriter::<u8>::new(buf);

    buffer_write_u8(&mut writer, x)
}

pub fn buffer_write_u8<T: Into<u8>>(buffer: &mut impl BufferWriter<u8>, x: T) -> Result<usize> {
    let local_buf = [x.into()];

    buffer.write(&local_buf)
}

pub fn read_u8(buf: &[u8], x: &mut u8) -> Result<usize> {
    let mut reader = SliceBufferReader::<u8>::new(buf);

    buffer_read_u8(&mut reader, x)
}

pub fn buffer_read_u8(buffer: &mut impl BufferReader<u8>, x: &mut u8) -> Result<usize> {
    let mut local_buf = [0; 1];

    let read_size = buffer.read(&mut local_buf)?;
    if read_size < 1 {
      return Err(Error::BufferTooSmall)
    }

    *x = local_buf[0];

    Ok(read_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    ////////////////////////////
    // tests for u64 encoding //
    ////////////////////////////

    #[test]
    fn write_u64_serialize() {
        let mut buffer: [u8; 8] = [0; 8];
        let result = write_u64(&mut buffer, 0x123456789abcdef0 as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 8);
        assert_eq!(buffer[..8], [0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn write_u64_serialize_buffer_too_short() {
        let mut buffer: [u8; 7] = [0; 7];
        let result = write_u64(&mut buffer, 0x123456789abcdef0 as u64);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    #[test]
    fn read_u64_deserialize() {
        let mut x: u64 = 0;
        let buffer = [0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12, 0x78];
        let result = read_u64(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 8);
        assert_eq!(x, 0x123456789abcdef0);
    }

    #[test]
    fn read_u64_deserialize_buffer_too_short() {
        let mut x: u64 = 0;
        let buffer = [0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34];
        let result = read_u64(&buffer, &mut x);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    ////////////////////////////
    // tests for u32 encoding //
    ////////////////////////////
    #[test]
    fn write_u32_serialize() {
        let mut buffer: [u8; 4] = [0; 4];
        let result = write_u32(&mut buffer, 0x12345678 as u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer[..4], [0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn write_u32_serialize_buffer_too_short() {
        let mut buffer: [u8; 3] = [0; 3];
        let result = write_u32(&mut buffer, 0x12345678 as u32);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    #[test]
    fn read_u32_deserialize() {
        let mut x: u32 = 0;
        let buffer = [0x78, 0x56, 0x34, 0x12, 0x78];
        let result = read_u32::<u32>(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);
        assert_eq!(x, 0x12345678);
    }

    #[test]
    fn read_u32_deserialize_buffer_too_short() {
        let mut x: u32 = 0;
        let buffer = [0x78, 0x56, 0x34];
        let result = read_u32::<u32>(&buffer, &mut x);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    ////////////////////////////
    // tests for u16 encoding //
    ////////////////////////////
    #[test]
    fn write_u16_serialize() {
        let mut buffer: [u8; 2] = [0; 2];
        let result = write_u16(&mut buffer, 0x1234 as u16);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
        assert_eq!(buffer[..2], [0x34, 0x12]);
    }

    #[test]
    fn write_u16_serialize_buffer_too_short() {
        let mut buffer: [u8; 1] = [0; 1];
        let result = write_u16(&mut buffer, 0x1234 as u16);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }


    ///////////////////////////
    // tests for u8 encoding //
    ///////////////////////////

    #[test]
    fn write_u8_serialize() {
        let mut buffer: [u8; 1] = [0; 1];
        let result = write_u8(&mut buffer, 0xf0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer[..1], [0xf0]);
    }

    #[test]
    fn write_u8_serialize_buffer_too_short() {
        let mut buffer: [u8; 0] = [0; 0];
        let result = write_u8(&mut buffer, 0x12);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

    #[test]
    fn read_u8_deserialize() {
        let mut x: u8 = 0;
        let buffer = [0xf0];
        let result = read_u8(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(x, 0xf0);
    }

    #[test]
    fn read_u8_deserialize_buffer_too_short() {
        let mut x: u8 = 0;
        let buffer = [];
        let result = read_u8(&buffer, &mut x);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }

}