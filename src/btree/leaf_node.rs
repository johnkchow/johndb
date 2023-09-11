use super::key::Key;
use super::value::Value;
use super::BTreePageData;
use super::NodeType;
use crate::btree::PageFetcherTrait;
use crate::mem::align_offset;
use crate::page::Item;
use crate::page::Page;
use crate::page::PageItemIteratorV2;
use crate::page_fetcher::PagePtr;
use core::marker::PhantomData;
use log::debug;
use std::iter::Skip;
use std::mem::align_of;
use std::mem::size_of;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct LeafNodeItemData<K, V>
where
    K: Key,
    V: Value,
{
    pub key: K,
    pub value: V,
    // TODO: Need to figure out how to deal with string type with unboudn length (i.e. `VARCHAR`).
}

impl<K, V> Item for LeafNodeItemData<K, V>
where
    K: Key,
    V: Value,
{
    fn size(&self) -> usize {
        if Self::is_fixed_size() {
            size_of::<Self>()
        } else {
            // Unfortunately when we have dynamic width, we have 6 byte overhead.
            // TODO: Save 2 bytes in scenarios when either K or V is fixed size.
            let mut size: usize = 0;

            // key
            size += self.key.size();
            size = align_offset(size, V::align());

            // value
            size += self.value.size();
            size = align_offset(size, align_of::<u16>());

            // u16 representing size of key
            // u16 representing size of value
            // u16 representing offset for value
            size += 3 * size_of::<u16>();
            size
        }
    }

    fn align() -> usize {
        std::cmp::max(K::align(), V::align())
    }

    fn is_fixed_size() -> bool {
        K::is_fixed_size() && V::is_fixed_size()
    }

    unsafe fn write(&self, buffer: *mut u8) {
        if Self::is_fixed_size() {
            *(buffer as *mut Self) = self.clone();
        } else {
            // key
            self.key.write(buffer);

            // value
            let mut value_offset: usize = 0;
            value_offset += self.key.size();
            value_offset = align_offset(value_offset, V::align());
            self.value.write(buffer.offset(value_offset as isize));

            // key size
            let mut size_offset = value_offset;
            size_offset += self.value.size();
            size_offset = align_offset(size_offset, align_of::<u16>());
            let size_ptr = buffer.offset(size_offset as isize) as *mut u16;

            *size_ptr = self.key.size() as u16;
            *(size_ptr.offset(1)) = self.value.size() as u16;
            *(size_ptr.offset(2)) = value_offset as u16;
        }
    }

    unsafe fn read(buffer: *const u8, size: usize) -> Self {
        if Self::is_fixed_size() {
            (buffer as *mut Self).read()
        } else {
            let size_ptr = buffer.offset((size - 3 * size_of::<u16>()) as isize) as *mut u16;
            let key_size = *size_ptr;
            let value_size = *size_ptr.offset(1);
            let value_offset = *size_ptr.offset(2);
            debug!(
                "LeafNodeDataItem.read: key_size: {}, value_size: {}, value_offset: {}",
                key_size, value_size, value_offset
            );

            Self {
                key: K::read(buffer, key_size as usize),
                value: V::read(buffer.offset(value_offset as isize), value_size as usize),
            }
        }
    }
}

pub(super) fn fetch_page_write<'a, P, K, V>(
    page_fetcher: &'a P,
    page_no: u32,
) -> Option<LeafNodeWriteLock<'a, K, V>>
where
    P: PageFetcherTrait,
    K: Key,
    V: Value,
{
    page_fetcher
        .fetch_page_write(page_no)
        .map(|lock| from_write_lock(page_no, lock))
}

/// Initializes empty page. Note that the separator is not set here, so you'll need to do
/// `node.set_separator(&separator)`.
pub(super) fn new_page<'a, P, K, V>(
    page_fetcher: &'a P,
    right_sibling_page_no: u32,
) -> (u32, LeafNodeWriteLock<'a, K, V>)
where
    P: PageFetcherTrait,
    K: Key,
    V: Value,
{
    let (page_no, lock) = page_fetcher.new_page(BTreePageData {
        node_type: NodeType::Leaf,
        right_sibling_page_no,
    });

    (
        page_no,
        LeafNodeWriteLock {
            page_no,
            page: lock,
            phantom: PhantomData,
            phantom_value: PhantomData,
        },
    )
}

pub(super) fn from_write_lock<K, V>(
    page_no: u32,
    lock: RwLockWriteGuard<PagePtr>,
) -> LeafNodeWriteLock<K, V>
where
    K: Key,
    V: Value,
{
    assert!(matches!(
        lock.special_data::<BTreePageData>().node_type,
        NodeType::Leaf
    ));

    LeafNodeWriteLock {
        page_no,
        page: lock,
        phantom: PhantomData,
        phantom_value: PhantomData,
    }
}

pub(super) trait LeafNodeRead<K, V>
where
    K: Key,
    V: Value,
{
    fn page_ref(&self) -> &Page;

    fn item_iter(&self) -> Skip<PageItemIteratorV2<LeafNodeItemData<K, V>>> {
        // We skip the first element, because it's always the separator
        self.page_ref()
            .items_iter_v2::<LeafNodeItemData<K, V>>()
            .skip(1)
    }

    fn separator(&self) -> K {
        self.page_ref().get_item_v2::<K>(0)
    }

    fn special_data(&self) -> &BTreePageData {
        self.page_ref().special_data()
    }
}

pub struct LeafNodeReadLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    page_no: u32,
    page: RwLockReadGuard<'a, PagePtr>,
    phantom: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<'a, K, V> LeafNodeRead<K, V> for LeafNodeReadLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    #[inline]
    fn page_ref(&self) -> &Page {
        self.page.deref().deref()
    }
}

impl<'a, K, V> From<(u32, RwLockReadGuard<'a, PagePtr>)> for LeafNodeReadLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn from(value: (u32, RwLockReadGuard<'a, PagePtr>)) -> Self {
        Self {
            page_no: value.0,
            page: value.1,
            phantom: PhantomData,
            phantom_value: PhantomData,
        }
    }
}

pub(super) struct LeafNodeWriteLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    pub page_no: u32,
    page: RwLockWriteGuard<'a, PagePtr>,
    phantom: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<'a, K, V> LeafNodeRead<K, V> for LeafNodeWriteLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    #[inline]
    fn page_ref(&self) -> &Page {
        self.page.deref().deref()
    }
}

impl<'a, K, V> LeafNodeWriteLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    pub(super) fn add_item(&mut self, item: &LeafNodeItemData<K, V>) -> Result<(), &'static str> {
        if item.key > self.separator() {
            return Err(
                "We can't add due to item not fitting within this page's allowed key range",
            );
        }

        debug!(
            "[LeafNodeWriteLock.add_item ({})] Adding {:?}",
            self.page_no, item
        );

        self.page.add_item_v2(item)
    }

    pub(super) fn set_separator(&mut self, sep: &K) {
        assert_eq!(self.page.item_cnt(), 0);

        // TODO: handle error here
        self.page.add_item_v2(sep).unwrap();
    }

    pub fn special_data_mut(&mut self) -> &mut BTreePageData {
        self.page_ref_mut().special_data_mut()
    }

    pub(super) fn page_ref_mut(&mut self) -> &mut Page {
        self.page.deref_mut()
    }
}

impl<'a, K, V> Into<RwLockWriteGuard<'a, PagePtr>> for LeafNodeWriteLock<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn into(self) -> RwLockWriteGuard<'a, PagePtr> {
        self.page
    }
}

pub(super) fn find_move_right<'a, P, K, V>(
    page_fetcher: &'a P,
    mut leaf_no: u32,
    key: K,
) -> LeafNodeWriteLock<'a, K, V>
where
    P: PageFetcherTrait,
    K: Key,
    V: Value,
{
    debug!("[find_move_right] Starting leaf_no: {}", leaf_no);
    while leaf_no != 0 {
        // We release the leaf lock at the end of this while block, which means we're at most
        // holding one write lock at any given time within this function
        let leaf = fetch_page_write(page_fetcher, leaf_no).unwrap();

        if key < leaf.separator() {
            debug!("[find_move_right] Found leaf_no: {}", leaf_no);
            return leaf;
        } else {
            leaf_no = leaf.special_data().right_sibling_page_no;
        }
    }

    panic!("For some reason we couldn't find the child ptr containing key, probably bug somewhere here!");
}

#[cfg(test)]
#[cfg(target_arch = "aarch64")]
mod tests {
    use crate::btree::key::Key;
    use crate::btree::key::KeyU32;
    use crate::btree::leaf_node::LeafNodeRead;
    use crate::btree::value::ValueTupleId;
    use crate::page_fetcher::InMemoryPageFetcher;

    use super::new_page;
    use super::LeafNodeItemData;
    use crate::page::Item;
    use std::mem::align_of;
    use std::mem::align_of_val;
    use std::mem::size_of_val;

    #[repr(align(8))]
    #[derive(PartialEq, Eq)]
    struct AlignedBuffer([u8; 18]);

    #[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
    struct KeyDynamic {
        key: u8,
    }
    impl Key for KeyDynamic {
        fn max_key() -> Self {
            todo!("This function isn't used for tests")
        }
    }

    impl Item for KeyDynamic {
        fn size(&self) -> usize {
            size_of_val(&self.key)
        }
        fn align() -> usize {
            1
        }
        fn is_fixed_size() -> bool {
            false
        }
        unsafe fn write(&self, buffer: *mut u8) {
            *buffer = self.key
        }
        unsafe fn read(buffer: *const u8, size: usize) -> Self {
            Self { key: *buffer }
        }
    }

    #[test]
    fn leaf_node_data_item_fixed_size() {
        let mut buffer = AlignedBuffer([0; 18]);
        let key = KeyU32 { key: 34 };
        let value = ValueTupleId {
            page_no: 63,
            offset: 19,
        };
        println!("sizes: {}, {}", size_of_val(&key), size_of_val(&value));

        let leaf_data = LeafNodeItemData { key, value };
        println!(
            "size: {}, align: {}",
            size_of_val(&leaf_data),
            align_of_val(&leaf_data)
        );
        assert_eq!(leaf_data.size(), 12);
        assert_eq!(
            LeafNodeItemData::<KeyU32, ValueTupleId>::align(),
            align_of_val(&leaf_data)
        );

        unsafe {
            leaf_data.write(&mut buffer.0[0] as *mut u8);
        }
        assert_eq!(leaf_data, unsafe {
            LeafNodeItemData::<KeyU32, ValueTupleId>::read(&mut buffer.0[0] as *mut u8, 12)
        })
    }

    #[test]
    fn leaf_node_data_item_dynamic_size() {
        let mut buffer = AlignedBuffer([0; 18]);
        let key = KeyDynamic { key: 0x22 }; // 34
        let value = ValueTupleId {
            page_no: 0xFFFEFDFC,
            offset: 0x0016,
        };
        let expected_size = 18 as usize;
        println!("sizes: {}, {}", size_of_val(&key), size_of_val(&value));

        let leaf_data = LeafNodeItemData { key, value };
        println!(
            "size: {}, align: {}",
            size_of_val(&leaf_data),
            align_of_val(&leaf_data)
        );
        assert_eq!(leaf_data.size(), expected_size);
        assert_eq!(
            LeafNodeItemData::<KeyDynamic, ValueTupleId>::align(),
            align_of_val(&leaf_data)
        );
        assert_eq!(LeafNodeItemData::<KeyDynamic, ValueTupleId>::align(), 4,);
        unsafe {
            leaf_data.write(&mut buffer.0[0] as *mut u8);
        }
        println!("buffer: {:#04X?}", &buffer.0);
        #[rustfmt::skip]
        assert_eq!(&buffer.0, &([
             // key, 1 byte + 3 bytes of alignment padding
            0x22, 0, 0, 0,

            // value.page_no, u32
            0xFC, 0xFD, 0xFE, 0xFF,

            // value.offset, u16 + 2 byte padding
            0x16, 0x00, 0, 0,

            // u16 key size (1)
            0x01, 0x00, 

            // u16 val size (8)
            0x08, 0x00,

            // u16 val offset (4)
            0x04, 0x00,
        ] as [u8; 18]));

        assert_eq!(leaf_data, unsafe {
            LeafNodeItemData::<KeyDynamic, ValueTupleId>::read(
                &mut buffer.0[0] as *mut u8,
                expected_size,
            )
        })
    }

    #[test]
    fn leaf_node_separator() {
        let page_fetcher = InMemoryPageFetcher::new();
        let (_, mut leaf) = new_page::<_, KeyU32, ValueTupleId>(&page_fetcher, 0);

        let sep = KeyU32 { key: 34 };
        leaf.set_separator(&sep);
        assert_eq!(leaf.separator(), sep);
    }
}
