use super::key::Key;
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) struct InternalNodeItemData<K>
where
    K: Key,
{
    pub key: K,
    pub page_no: u32,
    // TODO: Need to figure out how to deal with string type with unboudn length (i.e. `VARCHAR`).
}

impl<K> Item for InternalNodeItemData<K>
where
    K: Key,
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
            size = align_offset(size, align_of::<u32>());

            // page_no (u32)
            size += size_of::<u32>();
            size = align_offset(size, align_of::<u16>());

            // u16 representing size of key
            // u16 representing offset for value
            size += 2 * size_of::<u16>();
            size
        }
    }

    fn is_fixed_size() -> bool {
        K::is_fixed_size()
    }

    fn align() -> usize {
        if Self::is_fixed_size() {
            std::mem::align_of::<Self>()
        } else {
            std::cmp::max(K::align(), std::mem::align_of::<u32>())
        }
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
            value_offset = align_offset(value_offset, align_of::<u32>());
            *(buffer as *mut u32) = self.page_no;

            // key size
            let mut size_offset = value_offset;
            size_offset += size_of::<u32>();
            size_offset = align_offset(size_offset, align_of::<u16>());
            let size_ptr = buffer.offset(size_offset as isize) as *mut u16;

            *size_ptr = self.key.size() as u16;
            *(size_ptr.offset(1)) = value_offset as u16;
        }
    }

    unsafe fn read(buffer: *const u8, size: usize) -> Self {
        if Self::is_fixed_size() {
            (buffer as *mut Self).read()
        } else {
            let size_ptr = buffer.offset((size - 3 * size_of::<u16>()) as isize) as *mut u16;
            let key_size = *size_ptr;
            let value_offset = *size_ptr.offset(1);

            debug!(
                "InternalNodeRead.read: key_size: {}, value_offset: {}",
                key_size, value_offset
            );

            Self {
                key: K::read(buffer, key_size as usize),
                page_no: *(buffer.offset(value_offset as isize) as *const u32),
            }
        }
    }
}

pub(super) trait InternalNodeRead<K>
where
    K: Key,
{
    fn page_ref(&self) -> &Page;
    fn page_no(&self) -> u32;

    /*
    fn internal_node_has_child_ptr(&self, child_page_no: u32) -> bool {
        let special_data = self.page_ref().special_data::<BTreePageData>();
        assert!(
            matches!(special_data.node_type, NodeType::Internal),
            "Only internal nodes should be used"
        );

        self.page_ref()
            .items_itr_typed::<InternalNodeItemData<K>>()
            .any(|i| i.to_data_ref().page_no == child_page_no)
    }
    */

    fn item_iter(&self) -> Skip<PageItemIteratorV2<InternalNodeItemData<K>>> {
        // We skip the first element, because it's always the separator
        self.page_ref()
            .items_iter_v2::<InternalNodeItemData<K>>()
            .skip(1)
    }

    fn separator(&self) -> K {
        self.page_ref()
            .get_item_v2::<InternalNodeItemData<K>>(0)
            .key
    }

    fn find_child_ptr(&self, key: K) -> Option<u32> {
        let mut child_ptr: u32 = 0;
        let mut child_key: K = K::max_key();
        for key_ptr in self.item_iter() {
            if key < key_ptr.key && key_ptr.key < child_key {
                child_ptr = key_ptr.page_no;
                child_key = key_ptr.key;
            }
        }

        if child_ptr != 0 {
            Some(child_ptr)
        } else {
            None
        }
    }

    fn special_data(&self) -> &super::BTreePageData {
        self.page_ref().special_data()
    }
}

pub(super) struct InternalNodeReadLock<'a, K>
where
    K: Key,
{
    page_no: u32,
    page: RwLockReadGuard<'a, PagePtr>,
    phantom: PhantomData<K>,
}

impl<'a, K> InternalNodeRead<K> for InternalNodeReadLock<'a, K>
where
    K: Key,
{
    #[inline]
    fn page_ref(&self) -> &Page {
        self.page.deref()
    }

    fn page_no(&self) -> u32 {
        self.page_no
    }
}

pub(super) struct InternalNodeWriteLock<'a, K>
where
    K: Key,
{
    page_no: u32,
    page: RwLockWriteGuard<'a, PagePtr>,
    phantom: PhantomData<K>,
}

impl<'a, K> InternalNodeRead<K> for InternalNodeWriteLock<'a, K>
where
    K: Key,
{
    #[inline]
    fn page_ref(&self) -> &Page {
        self.page.deref()
    }

    fn page_no(&self) -> u32 {
        self.page_no
    }
}

impl<'a, K> InternalNodeWriteLock<'a, K>
where
    K: Key,
{
    pub fn page_ref_mut(&mut self) -> &mut Page {
        self.page.deref_mut()
    }

    pub fn add_item(&mut self, item: InternalNodeItemData<K>) -> Result<(), &'static str> {
        if item.key > self.separator() {
            return Err(
                "We can't add due to item not fitting within this page's allowed key range",
            );
        }

        self.page.add_item_v2(&item)
    }

    pub fn update_item(&mut self, item: &InternalNodeItemData<K>) -> Result<(), &'static str> {
        let (idx, cur) = self
            .item_iter()
            .enumerate()
            .find(|(_idx, i)| i.page_no == item.page_no)
            .unwrap();

        if cur == *item {
            return Ok(());
        }

        // Note that the idx above "skips" the  the underlying page's first
        // item, which is reserved for the page's separator value
        self.page.update_item_v2(idx + 1, item);

        if self.separator() == cur.key {
            let max_key = self
                .item_iter()
                .max_by(|x, y| x.key.cmp(&y.key))
                .map(|i| i.key)
                .unwrap();

            self.page.update_item_v2(0, &max_key)
        }

        Ok(())
    }

    pub fn set_separator(&mut self, sep: &K) {
        assert_eq!(self.page.item_cnt(), 0);

        // TODO: handle error here
        self.page.add_item_v2(sep).unwrap();
    }
}

impl<'a, K> Into<RwLockWriteGuard<'a, PagePtr>> for InternalNodeWriteLock<'a, K>
where
    K: Key,
{
    fn into(self) -> RwLockWriteGuard<'a, PagePtr> {
        self.page
    }
}

pub(super) fn fetch_page_read<'a, P, K>(
    page_fetcher: &'a P,
    page_no: u32,
) -> Option<InternalNodeReadLock<'a, K>>
where
    P: PageFetcherTrait,
    K: Key,
{
    page_fetcher
        .fetch_page_read(page_no)
        .map(|lock| from_read_lock(page_no, lock))
}
pub(super) fn fetch_page_write<'a, P, K>(
    page_fetcher: &'a P,
    page_no: u32,
) -> Option<InternalNodeWriteLock<'a, K>>
where
    P: PageFetcherTrait,
    K: Key,
{
    page_fetcher
        .fetch_page_write(page_no)
        .map(|lock| from_write_lock(page_no, lock))
}

pub(super) fn new_page<'a, P, K>(
    page_fetcher: &'a P,
    right_sibling_page_no: u32,
) -> (u32, InternalNodeWriteLock<'a, K>)
where
    P: PageFetcherTrait,
    K: Key,
{
    let (page_no, lock) = page_fetcher.new_page(BTreePageData {
        node_type: NodeType::Internal,
        right_sibling_page_no,
    });

    (
        // TODO: Eliminate the `page_no` from being returned
        page_no,
        InternalNodeWriteLock {
            page_no,
            page: lock,
            phantom: PhantomData,
        },
    )
}

pub(super) fn from_read_lock<K>(
    page_no: u32,
    lock: RwLockReadGuard<PagePtr>,
) -> InternalNodeReadLock<K>
where
    K: Key,
{
    assert!(matches!(
        lock.special_data::<BTreePageData>().node_type,
        NodeType::Leaf
    ));

    InternalNodeReadLock {
        page_no,
        page: lock,
        phantom: PhantomData,
    }
}

pub(super) fn from_write_lock<K>(
    page_no: u32,
    lock: RwLockWriteGuard<PagePtr>,
) -> InternalNodeWriteLock<K>
where
    K: Key,
{
    assert!(matches!(
        lock.special_data::<BTreePageData>().node_type,
        NodeType::Leaf
    ));

    InternalNodeWriteLock {
        page_no,
        page: lock,
        phantom: PhantomData,
    }
}

/// Returns (internal_node_page_no, downlink_child_no)
pub(super) fn find_child_ptr_move_right_read_lock<'a, P, K>(
    page_fetcher: &P,
    page: InternalNodeReadLock<'a, K>,
    key: K,
) -> (u32, u32)
where
    P: PageFetcherTrait,
    K: Key,
{
    find_child_ptr_move_right(page, key, |page_no| fetch_page_read(page_fetcher, page_no))
}

pub(super) fn find_child_ptr_move_right_write_lock<'a, P, K>(
    page_fetcher: &P,
    page_no: u32,
    key: K,
) -> (u32, u32)
where
    P: PageFetcherTrait,
    K: Key,
{
    let page = fetch_page_write(page_fetcher, page_no).unwrap();
    find_child_ptr_move_right(page, key, |page_no| fetch_page_write(page_fetcher, page_no))
}

pub(super) fn find_node_with_entry_move_right_write_lock<'a, P, K>(
    page_fetcher: &'a P,
    page_no: u32,
    child_no: u32,
) -> InternalNodeWriteLock<'a, K>
where
    P: PageFetcherTrait,
    K: Key,
{
    let mut next = page_no;
    while next != 0 {
        // we want to drop read lock of current page prior to fetching the next page to reduce
        // overall lock contentions.
        let page = fetch_page_write(page_fetcher, next).unwrap();
        let child_ptr: Option<InternalNodeItemData<K>> =
            page.item_iter().find(|i| i.page_no == child_no);
        if child_ptr.is_some() {
            return page;
        } else {
            next = page.special_data().right_sibling_page_no;
        }
    }

    panic!("For some reason we couldn't find the child ptr containing key, probably bug here!");
}

/// Returns (internal_node_page_no, downlink_child_no)
fn find_child_ptr_move_right<'a, I, K, F>(page: I, key: K, fetch_page: F) -> (u32, u32)
where
    I: InternalNodeRead<K>,
    K: Key,
    F: Fn(u32) -> Option<I>,
{
    let mut child_ptr = page.find_child_ptr(key);

    if child_ptr.is_some() {
        return (page.page_no(), child_ptr.unwrap());
    }

    let mut next = page.special_data().right_sibling_page_no;
    // we want to drop the read lock prior entering the while loop. Otherwise, we will hold
    // onto two locks at any given time during the while loop execution.
    drop(page);
    while next != 0 {
        // we want to drop read lock of current page prior to fetching the next page to reduce
        // overall lock contentions.
        let page = fetch_page(next).unwrap();
        child_ptr = page.find_child_ptr(key);
        if child_ptr.is_some() {
            return (next, child_ptr.unwrap());
        } else {
            next = page.special_data().right_sibling_page_no;
        }
    }

    panic!("For some reason we couldn't find the child ptr containing key, probably bug here!");
}
