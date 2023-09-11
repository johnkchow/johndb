use crate::page::Item;
use std::fmt::Debug;
use std::mem::size_of;

pub trait Key: Item + Ord + Copy + Clone + Debug {
    fn max_key() -> Self;
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Copy, Clone)]
pub struct KeyU32 {
    pub key: u32,
}

impl Key for KeyU32 {
    fn max_key() -> Self {
        Self { key: u32::MAX }
    }
}

impl Item for KeyU32 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn align() -> usize {
        std::mem::align_of::<Self>()
    }

    fn is_fixed_size() -> bool {
        true
    }

    unsafe fn write(&self, buffer: *mut u8) {
        *(buffer as *mut Self) = self.clone()
    }

    unsafe fn read(buffer: *const u8, size: usize) -> Self {
        assert!(
            size == size_of::<Self>(),
            "{} != {} ({})",
            size,
            size_of::<Self>(),
            "KeyU32",
        );

        (*(buffer as *mut Self)).clone()
    }
}
