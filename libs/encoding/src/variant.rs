use error::{Result, Error};
use buffer::{SliceBufferWriter, SliceBufferReader,
             BufferWriter, BufferReader};

////////////////////////////////////////////////////////
// Implementation of variant-length integer encoding  //
////////////////////////////////////////////////////////

pub const MAX_VARUINT_SIZE: usize = 10;

pub fn varuint_size<T: Into<u64>>(x: T) -> usize {
    let mut size: usize = 1;
    let mut x = x.into();
  
    while x >= 0x80 {
      x >>= 7;
      size += 1;
    }

    size
}

pub fn write_varuint<T: Into<u64>>(buf: &mut [u8], x: T) -> Result<usize> {
    let mut writer  = SliceBufferWriter::<u8>::new(buf);

    buffer_write_varuint(&mut writer, x)
}

pub fn buffer_write_varuint<T: Into<u64>>(buffer: &mut impl BufferWriter<u8>, x: T) -> Result<usize> {
    let mut x = x.into();
    let mut local_buf: [u8; 1] = [0; 1];
    let mut total_output: usize = 0;

    while x >= 0x80 {
      local_buf[0] = 0x80 | x as u8;

      buffer.write(&local_buf)?;

      x >>= 7;

      total_output += 1;
    }

    local_buf[0] = x as u8;
    buffer.write(&local_buf)?;

    Ok(total_output + 1)
}

pub fn read_varuint(buf: &[u8], x: &mut u64) -> Result<usize> {
    let mut reader = SliceBufferReader::<u8>::new(buf);

    buffer_read_varuint(&mut reader, x)
}

pub fn buffer_read_varuint(buffer: &mut impl BufferReader<u8>, x: &mut u64) -> Result<usize> {
    let mut shift_count: usize = 0;
    let mut local_buf: [u8; 1] = [0; 1];
    let mut total_consumed: usize = 0;

    *x = 0;
    loop {
      if shift_count >= 64 {
        return Err(Error::BufferOverflow);
      }

      let consumed = buffer.read(&mut local_buf)?;
      if consumed < 1 {
        return Err(Error::BufferTooSmall);
      }
      total_consumed += consumed;

      *x |= ((local_buf[0] & 0x7F) as u64) << shift_count;

      shift_count += 7;
      if local_buf[0] & 0x80 == 0 {
        break
      }
    }

    Ok(total_consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varuint_size_correct() {
        assert_eq!(varuint_size(0xFFFFFF as u64), 4);
        assert_eq!(varuint_size(0x6FFFFFFFFFFFFFFF as u64), 9);
        assert_eq!(varuint_size(0x8FFFFFFFFFFFFFFF as u64), 10);
    }
    
    #[test]
    fn write_varuint_serialize() {
        let mut buffer: [u8; MAX_VARUINT_SIZE] = [0; MAX_VARUINT_SIZE];
    
        let result = write_varuint(&mut buffer, 0x20 as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer[..1], [0x20]);
    
        let result = write_varuint(&mut buffer, 0x1891983 as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer[..4], [131, 179, 164, 12]);
    
        let result = write_varuint(&mut buffer, 0x1891983FFFFFFFFF as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 9);
        assert_eq!(buffer[..9], [255, 255, 255, 255, 255, 135, 230, 200, 24]);
    
        let result = write_varuint(&mut buffer, 0x8891983FFFFFFFFF as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(buffer[..10], [255, 255, 255, 255, 255, 135, 230, 200, 136, 1]);
    }
    
    #[test]
    fn write_varuint_serialize_buffer_too_small() {
        let mut buffer: [u8; 8] = [0; 8];
    
        let result = write_varuint(&mut buffer, 0x1891983 as u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer[..4], [131, 179, 164, 12]);
    
        let result = write_varuint(&mut buffer, 0x1891983FFFFFFFFF as u64);
        assert!(result.is_err());
    
        let result = write_varuint(&mut buffer, 0x8891983FFFFFFFFF as u64);
        assert!(result.is_err());
    }
    
    #[test]
    fn read_varuint_deserialize() {
        let mut x = 0;
    
        let buffer = [255, 255, 255, 255, 255, 135, 230, 200, 136, 1];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(x, 0x8891983FFFFFFFFF);
    
        let buffer = [255, 255, 255, 255, 255, 135, 230, 200, 136, 0x71];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(x, 0x8891983FFFFFFFFF);
    
        let buffer = [0x20];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(x, 0x20);
    
        let buffer = [131, 179, 164, 12];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);
        assert_eq!(x, 0x1891983);
    
        let buffer = [255, 255, 255, 255, 255, 135, 230, 200, 24];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 9);
        assert_eq!(x, 0x1891983FFFFFFFFF);
    }
    
    #[test]
    fn read_varuint_deserialize_invalid_binary() {
        let mut x = 0;
    
        let buffer = [255, 255, 255, 255, 255, 135, 230, 200, 136, 0x81];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferOverflow);
    
        let buffer = [255, 255, 255, 255, 255, 135, 230, 200, 136];
        let result = read_varuint(&buffer, &mut x);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Error::BufferTooSmall);
    }
    

}