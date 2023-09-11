use crate::page::Item;
use std::fmt::Debug;
use std::mem::size_of;

pub trait Value: Item + Ord + Copy + Clone + Debug {}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct ValueTupleId {
    pub page_no: u32,
    pub offset: u16,
}

impl Value for ValueTupleId {}

impl Item for ValueTupleId {
    fn size(&self) -> usize {
        size_of::<Self>()
    }

    fn align() -> usize {
        std::mem::align_of::<Self>()
    }

    fn is_fixed_size() -> bool {
        true
    }

    unsafe fn write(&self, buffer: *mut u8) {
        *(buffer as *mut ValueTupleId) = (*self).clone();
    }

    unsafe fn read(buffer: *const u8, size: usize) -> Self {
        assert!(
            size == size_of::<Self>(),
            "size {} != size_of::<Self> {}",
            size,
            size_of::<Self>(),
        );

        *(buffer as *mut Self).clone()
    }
}
