use crate::encoding::{read_varuint, write_varuint, varuint_size, MAX_VARUINT_SIZE,
                      read_u64, write_u64, read_u8, write_u8};
use crate::error::Error;

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