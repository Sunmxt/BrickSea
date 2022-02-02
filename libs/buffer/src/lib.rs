mod vector;
mod slice;

use error::Result;

pub use slice::*;
pub use vector::*;
                    
/////////////////////////////////////////
// Abstraction of buffer reader/writer //
/////////////////////////////////////////

pub trait BufferWriter<T> {
    fn write(&mut self, data: &[T]) -> Result<usize>;
}

pub trait BufferReader<T> {
    fn read(&mut self, data: &mut [T]) -> Result<usize>;
}
