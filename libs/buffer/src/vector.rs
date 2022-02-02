use error::{Result, Error};
use super::{BufferReader, BufferWriter};

/////////////////////////////////////////
// Abstraction of buffer reader/writer //
/////////////////////////////////////////
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

////////////////////////////////////////////////
// Implementation of writer for buffer vector //
////////////////////////////////////////////////

pub struct CursorSnapshotForBufferVector {
    vector_index: usize,
    offset: usize,
    logical_offset: usize,
}

pub struct BufferVectorWriter<'a, T, VecType>
where VecType: MutRefBufferVector<T> + AbstractBufferVector {
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

    pub fn reset_cursor(&mut self) {
        self.cursor_ = (0, 0, 0)
    }

    pub fn restore_cursor(&mut self, snapshot: CursorSnapshotForBufferVector) {
        self.cursor_ = (
            snapshot.vector_index,
            snapshot.offset,
            snapshot.logical_offset,
        )
    }

    pub fn snapshot_cursor(&self) -> CursorSnapshotForBufferVector {
        CursorSnapshotForBufferVector{
            vector_index: self.cursor_.0,
            offset: self.cursor_.1,
            logical_offset: self.cursor_.2,
        }
    }

    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }

    pub fn skip(&mut self, skip_size: u32) -> usize {
        0
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

        // TODO(chadmai): copy_from_slice().
        let mut vector = self.vec.vec_ref_mut(vector_index);
        let mut cursor: usize = 0;
        while cursor < data.len() {
            if offset >= vector.len() {
                vector_index += 1;

                if vector_index >= self.vec.total_vector_count() {
                    break
                }
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

////////////////////////////////////////////////
// Implementation of reader for buffer vector //
////////////////////////////////////////////////

pub struct BufferVectorReader<'a, T, VecType>
where VecType: ImmutRefBufferVector<T> + AbstractBufferVector {
    vec: &'a VecType,
    cursor_: (usize, usize, usize),

    t_: std::marker::PhantomData<T>,
}

impl<'a, T, VecType> BufferVectorReader<'a, T, VecType>
where VecType: ImmutRefBufferVector<T> + AbstractBufferVector {
    fn new(vec: &VecType) -> BufferVectorReader<T, VecType> {
        BufferVectorReader::<T, VecType>{
            vec,
            cursor_: (0, 0, 0),
            t_: std::marker::PhantomData::<T>{},
        }
    }

    pub fn cursor(&self) -> usize {
        self.cursor_.2
    }

    pub fn reset_cursor(&mut self) {
        self.cursor_ = (0, 0, 0)
    }

    pub fn restore_cursor(&mut self, snapshot: CursorSnapshotForBufferVector) {
        self.cursor_ = (
            snapshot.vector_index,
            snapshot.offset,
            snapshot.logical_offset,
        )
    }

    pub fn snapshot_cursor(&self) -> CursorSnapshotForBufferVector {
        CursorSnapshotForBufferVector{
            vector_index: self.cursor_.0,
            offset: self.cursor_.1,
            logical_offset: self.cursor_.2,
        }
    }

    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }
}

impl<'a, T, VecType> BufferReader<T> for BufferVectorReader<'a, T, VecType>
where T: Copy, VecType: ImmutRefBufferVector<T> + AbstractBufferVector {
    fn read(&mut self, data: &mut [T]) -> Result<usize> {
        if data.len() < 1 {
            return Ok(0)
        }

        // TODO(chadmai): use memcpy.
        let (mut vector_index, mut offset, mut logical_offset) = self.cursor_;
        if vector_index >= self.vec.total_vector_count() {
            return Ok(0)
        }

        let mut vector = self.vec.vec_ref(vector_index);
        let mut cursor: usize = 0;
        while cursor < data.len() {
            if offset >= vector.len() {
                vector_index += 1;

                if vector_index >= self.vec.total_vector_count() {
                    break
                }
                vector = self.vec.vec_ref(vector_index);
                offset = 0;
                continue;
            }

            data[cursor] = vector[offset];

            offset += 1;
            cursor += 1;
        }

        logical_offset += cursor;
        self.cursor_ = (vector_index, offset, logical_offset);

        Ok(cursor)
    }
}

/////////////////////////////////////////////
// Implementation of mutable buffer vector //
/////////////////////////////////////////////

pub struct MutBufferVector<'a, T: Copy> {
    vectors: Vec<&'a mut [T]>,
    capacity_: usize,
}

impl<'a, T> MutBufferVector<'a, T> 
where T: Copy {
    pub fn new() -> MutBufferVector<'a, T> {
        return MutBufferVector{
            vectors: Vec::<&'a mut [T]>::new(),
            capacity_: 0,
        }
    }

    pub fn clone(&self) -> Self {
        let cloned = Self::new();

        cloned
    }

    pub fn append_buffer_slice(&mut self, buffer: &'a mut [T]) {
        self.capacity_ += buffer.len();

        self.vectors.push(buffer);
    }

    pub fn new_writer(&mut self) -> BufferVectorWriter<T, Self> {
        BufferVectorWriter::<T, Self>::new(self)
    }

    pub fn new_reader(&mut self) -> BufferVectorReader<T, Self> {
        BufferVectorReader::<T, Self>::new(self)
    }
}

impl<'a, T> AbstractBufferVector for MutBufferVector<'a, T>
where T: Copy {
    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn capacity(&self) -> usize {
        self.capacity_
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