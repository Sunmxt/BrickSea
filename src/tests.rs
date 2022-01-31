mod wal;
mod buffer;
mod encoding;

use std::mem::size_of;

#[test]

fn global_type_assertions() {
    assert_eq!(size_of::<usize>(), size_of::<u64>());
}