use error::{Result, Error};
use super::{BufferReader, BufferWriter};

///////////////////////////////////////////////////
// Implementation of reader for slice buffer     //
///////////////////////////////////////////////////

pub struct SliceBufferReader<'a, T> {
    slice_ref: &'a [T],
    cursor_: usize,
}

impl<'a, T> SliceBufferReader<'a, T> {
    pub fn new(buf: &[T]) -> SliceBufferReader<T> {
        SliceBufferReader::<T>{
            slice_ref: buf,
            cursor_: 0
        }
    }

    pub fn reset_cursor(&mut self) {
        self.cursor_ = 0
    }

    pub fn cursor(&self) -> usize {
        self.cursor_
    }
}

impl<'a, T> BufferReader<T> for SliceBufferReader<'a, T>
where T: Copy {
    fn read(&mut self, data: &mut [T]) -> Result<usize> {
        let mut offset: usize = 0;

        while offset < data.len() && self.cursor_ + offset < self.slice_ref.len() {
            data[offset] = self.slice_ref[self.cursor_ + offset];

            offset +=1;
        }

        self.cursor_ += offset;

        Ok(offset)
    }
}

///////////////////////////////////////////////////
// Implementation of writer for slice buffer     //
///////////////////////////////////////////////////
pub struct SliceBufferWriter<'a, T> {
    slice_ref: &'a mut [T],
    cursor_: usize,
}

impl<'a, T> SliceBufferWriter<'a, T> {
    pub fn new(buf: &mut [T]) -> SliceBufferWriter<T> {
        SliceBufferWriter::<T>{
            slice_ref: buf,
            cursor_: 0
        }
    }

    pub fn reset_cursor(&mut self) {
        self.cursor_ = 0
    }

    pub fn cursor(&self) -> usize {
        self.cursor_
    }
}

impl<'a, T> BufferWriter<T> for SliceBufferWriter<'a, T>
where T: Copy {
    fn write(&mut self, data: &[T]) -> Result<usize> {
        if data.len() + self.cursor_ > self.slice_ref.len() {
            return Err(Error::BufferTooSmall)
        }

        let mut offset: usize = 0;
        for _ in 0..data.len() {
            self.slice_ref[self.cursor_] = data[offset];

            offset += 1;
            self.cursor_ += 1;
        }

        Ok(offset)
    }
}
