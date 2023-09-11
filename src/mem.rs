/// Given a `len`, provide the closest value that's a multiple of `align` >= `len`
/// `align` must be power of 2.
pub(crate) fn align_offset(len: usize, align: usize) -> usize {
    (len + ((align) - 1)) & !((align) - 1)
}

/// Similar to `align_offset` but finds closest value that's <= `len`.
/// `align` must be power of 2.
pub(crate) fn align_offset_down(len: usize, align: usize) -> usize {
    len & !((align) - 1)
}

#[cfg(test)]
mod tests {
    use super::align_offset;
    use super::align_offset_down;

    #[test]
    fn align_offset_test() {
        assert_eq!(align_offset(10, 2), 10);
        assert_eq!(align_offset(11, 2), 12);
        assert_eq!(align_offset(8, 8), 8);
        assert_eq!(align_offset(9, 8), 16);
        assert_eq!(align_offset(12, 2), 12);
    }

    #[test]
    fn align_offset_down_test() {
        assert_eq!(align_offset_down(10, 2), 10);
        assert_eq!(align_offset_down(11, 2), 10);
        assert_eq!(align_offset_down(8, 8), 8);
        assert_eq!(align_offset_down(7, 8), 0);
        assert_eq!(align_offset_down(12, 2), 12);
        assert_eq!(align_offset_down(13, 2), 12);
        assert_eq!(align_offset_down(14, 4), 12);
    }
}
