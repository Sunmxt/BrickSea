use crate::error::{Result, Error};

/////////////////////////////////////////
// Abstraction of buffer reader/writer //
/////////////////////////////////////////

pub trait BufferWriter<T> {
    fn write(&mut self, data: &[T]) -> Result<usize>;
}

pub trait BufferReader<T> {
    fn read(&mut self, data: &mut [T]) -> Result<usize>;
}

pub trait AbstractBufferVector {
    fn total_vector_count(&self) -> usize;
    fn capacity(&self) -> usize;
}

pub trait ImmutRefBufferVector<T> {
    fn vec_ref(&self, index: usize) -> &[T];
}

pub trait MutRefBufferVector<T> {
    fn vec_ref_mut(&mut self, index: usize) -> &mut [T];
}

///////////////////////////////////////////////////////
// Implementation of buffer writer for buffer vector //
///////////////////////////////////////////////////////

pub struct BufferVectorWriter<'a, T, VecType>
where VecType: MutRefBufferVector<T> + AbstractBufferVector{
    vec: &'a mut VecType,
    cursor_: (usize, usize, usize),

    t_: std::marker::PhantomData<T>,
}

impl<'a, T, VecType> BufferVectorWriter<'a, T, VecType>
where VecType: MutRefBufferVector<T> + AbstractBufferVector {
    fn new(vec: &mut VecType) -> BufferVectorWriter<T, VecType> {
        BufferVectorWriter::<T, VecType>{
            vec,
            cursor_: (0, 0, 0),
            t_: std::marker::PhantomData::<T>{},
        }
    }

    pub fn cursor(&self) -> usize {
        self.cursor_.2
    }

    fn reset_cursor(&mut self) {
        self.cursor_ = (0, 0, 0)
    }

    fn capacity(&self) -> usize {
        self.vec.capacity()
    }
}

impl<'a, T, VecType> BufferWriter<T> for BufferVectorWriter<'a, T, VecType>
where T: Copy, VecType: MutRefBufferVector<T> + AbstractBufferVector {
    fn write(&mut self, data: &[T]) -> Result<usize> {
        if data.len() < 1 {
            return Ok(0)
        }

        let (mut vector_index, mut offset, mut logical_offset) = self.cursor_;

        let mut capacity: usize = 0;
        if vector_index < self.vec.total_vector_count() {
            let mut index = vector_index;
            let vec = self.vec.vec_ref_mut(index);
            if vec.len() > offset {
                capacity += vec.len() - offset;
            }
            index += 1;

            while index < self.vec.total_vector_count() {
                capacity += self.vec.vec_ref_mut(index).len();
                index += 1;
            }
        }
        if capacity < data.len() {
            return Err(Error::BufferTooSmall)
        }

        // TODO(chadmai): use memcpy.
        let mut vector = self.vec.vec_ref_mut(vector_index);
        let mut cursor: usize = 0;
        while cursor < data.len() {
            if offset >= vector.len() {
                vector_index += 1;

                vector = self.vec.vec_ref_mut(vector_index);
                offset = 0;
                continue;
            }

            vector[offset] = data[cursor];

            offset += 1;
            cursor += 1;
        }

        logical_offset += data.len();
        self.cursor_ = (vector_index, offset, logical_offset);

        Ok(data.len())
        
    }
}

///////////////////////////////////////////////////////
// Implementation of buffer reader for buffer vector //
///////////////////////////////////////////////////////

pub struct BufferVectorReader<T, VecType: ImmutRefBufferVector<T>> {
    vec: VecType,
    cursor_: (usize, usize, usize),

    t_: std::marker::PhantomData<T>,
}

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

/////////////////////////////////////////////
// Implementation of mutable buffer vector //
/////////////////////////////////////////////

pub struct MutBufferVector<'a, T: Copy> {
    pub vectors: Vec<&'a mut [T]>,
}

impl<'a, T> MutBufferVector<'a, T> 
where T: Copy {
    pub fn new() -> MutBufferVector<'a, T> {
        return MutBufferVector{
            vectors: Vec::<&'a mut [T]>::new(),
        }
    }

    pub fn append_buffer_slice(&mut self, buffer: &'a mut [T]) {
        self.vectors.push(buffer)
    }

    pub fn new_writer(&mut self) -> BufferVectorWriter<T, MutBufferVector<'a, T>> {
        BufferVectorWriter::<T, MutBufferVector<'a, T>>::new(self)
    }
}

impl<'a, T> AbstractBufferVector for MutBufferVector<'a, T>
where T: Copy {
    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn capacity(&self) -> usize {
        let mut total_len: usize = 0;

        for vec in &self.vectors {
            total_len += vec.len();
        }

        total_len
    }
}

impl<'a, T> MutRefBufferVector<T> for MutBufferVector<'a, T>
where T: Copy {
    fn vec_ref_mut(&mut self, index: usize) -> &mut [T] {
        self.vectors[index]
    }
}

impl<'a, T> ImmutRefBufferVector<T> for MutBufferVector<'a, T>
where T: Copy {
    fn vec_ref(&self, index: usize) -> &[T] {
        self.vectors[index]
    }
}

///////////////////////////////////////////////
// Implementation of immutable buffer vector //
///////////////////////////////////////////////
pub struct ImmutBufferVector<'a, T: Copy> {
    pub vectors: Vec<&'a [T]>,
}
