use crate::error::{Result, Error};
use crate::buffer::{BufferReader, BufferWriter, SliceBufferWriter, SliceBufferReader};

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

pub fn write_u32<T: Into<u32>>(buf: &mut [u8], x: T) -> Result<usize> {
    if buf.len() < 4 {
      return Err(Error::BufferTooSmall);
    }

    let x = x.into();
  
    buf[3] = (x & 0xFF) as u8;
    buf[2] = ((x >> 8) & 0xFF) as u8;
    buf[1] = ((x >> 16) & 0xFF) as u8;
    buf[0] = ((x >> 24) & 0xFF) as u8;

    Ok(4)
}

pub fn read_u32<T: Into<u32>>(buf: &mut [u8], x: &mut u32) -> Result<usize> {
    if buf.len() < 4 {
      return Err(Error::BufferTooSmall);
    }

    *x = ((buf[0] as u32) << 24) 
        | ((buf[1] as u32) << 16)
        | ((buf[2] as u32) << 8)
        | buf[3] as u32;

    return Ok(4)
}

/////////////////////////////////////////////
// Implementation of u8 integer encoding  //
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