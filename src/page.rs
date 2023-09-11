use crate::mem::align_offset_down;

use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::addr_of;

const PAGE_SIZE: usize = 8192;
const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
pub const ITEM_POINTER_SIZE: usize = size_of::<ItemPointer>();

pub trait Item {
    fn size(&self) -> usize;
    fn align() -> usize;
    fn is_fixed_size() -> bool;
    unsafe fn write(&self, buffer: *mut u8);
    unsafe fn read(buffer: *const u8, size: usize) -> Self;
}

#[derive(Debug, Copy, Clone)]
// TODO: Figure out how we can make 8192 a const in the macro world.
#[repr(align(8192))]
// TODO: Make all fields private
pub struct Page {
    pub header: PageHeader,
    pub data: [u8; PAGE_DATA_SIZE],
}

impl Page {
    // TODO: Make sure special_size is aligned to 8bytes
    pub fn new(special_size: u32) -> Page {
        Page {
            header: PageHeader::new(special_size),
            data: [0; PAGE_DATA_SIZE],
        }
    }

    pub fn special_data<SpecialData>(&self) -> &SpecialData {
        assert!(
            std::mem::size_of::<SpecialData>() == self.header.special_size as usize,
            "Mismatch on SpecialData size (SpecialData: {}, PageHeader.special_size: {}",
            std::mem::size_of::<SpecialData>(),
            self.header.special_size
        );

        return unsafe {
            &*(&self.data[PAGE_DATA_SIZE - self.header.special_size as usize] as *const u8
                as *const SpecialData)
        };
    }

    pub fn special_data_mut<SpecialData>(&mut self) -> &mut SpecialData {
        assert!(
            std::mem::size_of::<SpecialData>() == self.header.special_size as usize,
            "Mismatch on SpecialData size (SpecialData: {}, PageHeader.special_size: {}",
            std::mem::size_of::<SpecialData>(),
            self.header.special_size
        );

        return unsafe {
            &mut *(&mut self.data[PAGE_DATA_SIZE - self.header.special_size as usize] as *mut u8
                as *mut SpecialData)
        };
    }

    pub fn items_iter_v2<I: Item>(&self) -> PageItemIteratorV2<I> {
        return PageItemIteratorV2::new(self);
    }

    pub fn item_cnt(&self) -> usize {
        self.header.item_cnt()
    }

    pub fn item_data_size(&self) -> usize {
        self.header.item_data_size()
    }

    pub fn zero_out_item_data(&mut self) {
        for i in 0..(PAGE_DATA_SIZE - (self.header.special_size as usize)) {
            self.data[i] = 0;
        }

        self.header = PageHeader::new(self.header.special_size);
    }

    #[deprecated]
    pub fn pop_item(&mut self) -> Result<(), &'static str> {
        if self.item_cnt() == 0 {
            return Err("No more left to pop");
        }

        let item_ptr = unsafe {
            &*(&self.data[self.header.item_upper as usize - ITEM_POINTER_SIZE] as *const u8
                as *const ItemPointer) as &ItemPointer
        };

        self.header.item_upper -= ITEM_POINTER_SIZE as u32;
        self.header.item_lower += item_ptr.size as u32;

        Ok(())
    }

    pub fn add_item_v2<T>(&mut self, item: &T) -> Result<(), &'static str>
    where
        T: Item,
    {
        let (ptr_offset, data_offset) = self.header.add_item_v2(item)?;

        let item_data = &mut self.data[data_offset as usize] as *mut u8;
        let item_ptr = (&mut self.data[ptr_offset as usize] as *mut u8) as *mut ItemPointer;

        unsafe {
            item.write(item_data);
            *item_ptr = ItemPointer {
                size: item.size() as u16,
                offset: data_offset as u16,
            };
        };

        Ok(())
    }

    pub fn get_item_v2<I>(&self, idx: usize) -> I
    where
        I: Item,
    {
        let data_idx = idx * ITEM_POINTER_SIZE;
        assert!(
            data_idx < self.header.item_upper as usize,
            "TODO: Make this return an Option/Result"
        );
        unsafe {
            let item_ptr = &*(addr_of!(self.data[data_idx]) as *const u8 as *const ItemPointer);

            I::read(
                addr_of!(self.data[item_ptr.offset as usize]),
                item_ptr.size as usize,
            )
        }
    }

    pub fn update_item_v2<T>(&mut self, idx: usize, item: &T)
    where
        T: Item,
    {
        // first, find existing item. if it's a larger item, just replace the data. otherwise,
        // we'll panic
        // TODO: Shift bytes around for dynamic sizing
        let data_idx = idx * ITEM_POINTER_SIZE;
        assert!(data_idx < self.header.item_upper as usize);
        let item_ptr = unsafe { &*(&self.data[data_idx] as *const u8 as *const ItemPointer) };
        assert_eq!(
            (item_ptr.size as usize),
            item.size(),
            "TODO: Need to shift bytes around!"
        );
        let data_ptr = &mut self.data[item_ptr.offset as usize] as *mut u8;

        unsafe { item.write(data_ptr) };
    }
}

#[deprecated]
pub struct ItemData<'a> {
    pub size: usize,
    pub raw_data_ptr: *const u8,
    // We want to bind ItemData's lifetime to at least the Page's lifetime
    phantom_page: PhantomData<&'a Page>,
}

impl<'a> ItemData<'a> {
    pub fn to_data_ref<T: Sized>(&self) -> &T {
        assert!(self.size == std::mem::size_of::<T>());

        unsafe { &*(self.raw_data_ptr as *const T) }
    }
}

#[deprecated]
pub struct PageItemIterator<'a> {
    page: &'a Page,
    curr: usize,
}

impl<'a> PageItemIterator<'a> {
    fn new(page: &'a Page) -> Self {
        PageItemIterator { page, curr: 0 }
    }
}

impl<'a> Iterator for PageItemIterator<'a> {
    type Item = ItemData<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr < self.page.header.item_cnt() {
            unsafe {
                let item_pointer: &ItemPointer = &*(&self.page.data[self.curr * ITEM_POINTER_SIZE]
                    as *const u8
                    as *const ItemPointer);

                Some(ItemData {
                    size: item_pointer.size as usize,
                    raw_data_ptr: &self.page.data[item_pointer.offset as usize] as *const u8,
                    phantom_page: PhantomData,
                })
            }
        } else {
            None
        }
    }
}

#[deprecated]
pub struct PageItemIteratorTyped<'a, Item: 'a>
where
    Item: Sized,
{
    page: &'a Page,
    forward: usize,
    back: usize,
    phantom: PhantomData<Item>,
}

impl<'a, Item: 'a> PageItemIteratorTyped<'a, Item>
where
    Item: Sized,
{
    fn new(page: &'a Page) -> Self {
        PageItemIteratorTyped {
            page,
            forward: 0,
            back: 0,
            phantom: PhantomData,
        }
    }
}

impl<'a, Item: 'a> Iterator for PageItemIteratorTyped<'a, Item> {
    type Item = ItemDataTyped<'a, Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.forward < self.page.header.item_cnt() {
            let item_pointer = unsafe {
                &*(&self.page.data[self.forward * ITEM_POINTER_SIZE] as *const u8
                    as *const ItemPointer)
            };

            self.forward += 1;

            Some(ItemDataTyped {
                size: item_pointer.size as usize,
                raw_data_ptr: &self.page.data[item_pointer.offset as usize] as *const u8,
                phantom_page: PhantomData,
                phantom_item: PhantomData,
            })
        } else {
            None
        }
    }
}

impl<'a, Item: 'a> DoubleEndedIterator for PageItemIteratorTyped<'a, Item> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.back < self.page.header.item_cnt() {
            unsafe {
                let item_pointer: &ItemPointer = &*(&self.page.data
                    [(self.page.header.item_cnt() - 1 - self.back) * ITEM_POINTER_SIZE]
                    as *const u8
                    as *const ItemPointer);

                self.back += 1;

                Some(ItemDataTyped {
                    size: item_pointer.size as usize,
                    raw_data_ptr: &self.page.data[item_pointer.offset as usize] as *const u8,
                    phantom_page: PhantomData,
                    phantom_item: PhantomData,
                })
            }
        } else {
            None
        }
    }
}

pub struct PageItemIteratorV2<'a, I>
where
    I: Item,
{
    page: &'a Page,
    forward: usize,
    back: usize,
    phantom: PhantomData<I>,
}

impl<'a, I> PageItemIteratorV2<'a, I>
where
    I: Item,
{
    fn new(page: &'a Page) -> Self {
        Self {
            page,
            forward: 0,
            back: 0,
            phantom: PhantomData,
        }
    }
}

impl<'a, I> Iterator for PageItemIteratorV2<'a, I>
where
    I: Item,
{
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.forward < self.page.header.item_cnt() {
            let item = self.page.get_item_v2(self.forward);
            self.forward += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, I> DoubleEndedIterator for PageItemIteratorV2<'a, I>
where
    I: Item,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.back < self.page.header.item_cnt() {
            let item = self.page.get_item_v2(self.back);
            self.back += 1;
            Some(item)
        } else {
            None
        }
    }
}

pub struct ItemDataTyped<'a, Item: Sized> {
    size: usize,
    raw_data_ptr: *const u8,
    // We want to bind ItemData's lifetime to at least the Page's lifetime
    phantom_page: PhantomData<&'a Page>,
    phantom_item: PhantomData<Item>,
}

impl<'a, Item: Sized> ItemDataTyped<'a, Item> {
    // TODO: Refactor this so that Item is just a smart pointer
    pub fn to_data_ref(&self) -> &Item {
        unsafe { &*(self.raw_data_ptr as *const Item) }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    /**
    "Top" of page's data. Starts at 0, and before it are the `ItemPointer`s.
    */
    item_upper: u32,
    /**
    "Bottom" of page's data. Starts at PAGE_DATA_SIZE-special_size, and references the beginning of the data.
    */
    item_lower: u32,
    special_size: u32,
}

impl PageHeader {
    pub fn new(special_size: u32) -> Self {
        PageHeader {
            item_upper: 0,
            // TODO: do idiomatic u32 conversion
            item_lower: PAGE_DATA_SIZE as u32 - special_size,
            special_size,
        }
    }

    fn item_cnt(&self) -> usize {
        (self.item_upper as usize) / ITEM_POINTER_SIZE
    }

    fn item_data_size(&self) -> usize {
        (PAGE_DATA_SIZE - (self.special_size as usize)) - (self.item_lower as usize)
    }

    fn can_add_item(&self, size: usize) -> bool {
        ((self.item_lower - self.item_upper) as usize) >= ITEM_POINTER_SIZE + size
    }

    fn add_item<Item: Sized>(&mut self) -> Result<(u32, u32), &'static str> {
        if !self.can_add_item(std::mem::size_of::<Item>()) {
            return Err("TODO: Can't add item");
        }
        let item_ptr_offset = self.item_upper;

        self.item_upper += ITEM_POINTER_SIZE as u32;
        self.item_lower -= std::mem::size_of::<Item>() as u32;

        Ok((item_ptr_offset, self.item_lower))
    }

    fn add_item_v2<I: Item>(&mut self, item: &I) -> Result<(u32, u32), &'static str> {
        let item_ptr_offset = self.item_upper;
        let new_item_upper = self.item_upper + ITEM_POINTER_SIZE as u32;
        let new_item_lower =
            align_offset_down(self.item_lower as usize - item.size(), I::align()) as u32;

        if new_item_upper > new_item_lower {
            return Err("TODO: Can't add item");
        }

        self.item_upper = new_item_upper;
        self.item_lower = new_item_lower;

        Ok((item_ptr_offset, self.item_lower))
    }
}

// Size is 4
struct ItemPointer {
    // from start of data
    offset: u16,
    size: u16,
}

#[cfg(test)]
mod tests {
    use super::Item;
    use super::Page;
    use log::debug;
    use std::mem::size_of;

    // Size is 12
    #[derive(Debug, PartialEq, Clone)]
    struct TestSpecialData {
        val: u32,
        data: [u8; 8],
    }

    // Size is 8
    #[derive(Debug, PartialEq, Clone)]
    struct TestItem {
        key: u32,
        val: u32,
    }

    impl Item for TestItem {
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
            assert!(size == std::mem::size_of::<Self>());

            (*(buffer as *mut Self)).clone()
        }
    }

    #[test]
    fn add_item_v2() {
        let (mut page, _special_data) = setup_page();

        // ItemPointer is 4bytes, TestItem is 8, and TestSpecialData is 12.
        // PAGE_DATA_SIZE is 8180. Max items we can store is 680.
        for i in 0..680 {
            let res = page.add_item_v2(&TestItem {
                key: i as u32,
                val: i as u32,
            });

            assert!(matches!(res, Ok(_)));
            assert_eq!(page.item_cnt(), i + 1);
        }

        assert_eq!(page.item_cnt(), 680);
        println!("{:?}", page.header);

        assert!(matches!(
            page.add_item_v2(&TestItem { key: 680, val: 680 }),
            Err(_)
        ));
    }

    #[test]
    fn iter_v2() {
        // Setup
        let (mut page, _special_data) = setup_page();

        for i in 0..680 {
            page.add_item_v2(&TestItem {
                key: i as u32,
                val: i + 1 as u32,
            })
            .unwrap();
        }

        // Test

        let iter = page.items_iter_v2::<TestItem>();
        assert_eq!(
            iter.map(|i| i.key).collect::<Vec<u32>>(),
            (0..680).collect::<Vec<u32>>(),
        );

        let iter = page.items_iter_v2::<TestItem>();
        assert_eq!(
            iter.map(|i| i.val).collect::<Vec<u32>>(),
            (1..681).collect::<Vec<u32>>(),
        );
    }

    #[test]
    fn update_and_get_item_v2() {
        let (mut page, _special_data) = setup_page();

        for i in 0..680 {
            page.add_item_v2(&TestItem {
                key: i as u32,
                val: i as u32,
            })
            .unwrap();
        }

        let item = TestItem { key: 681, val: 681 };

        page.update_item_v2(34, &item);
        assert_eq!(page.items_iter_v2::<TestItem>().nth(34).unwrap(), item);
        assert_eq!(page.get_item_v2::<TestItem>(34), item,);
    }

    fn setup_page() -> (Page, TestSpecialData) {
        let mut page = Page::new(std::mem::size_of::<TestSpecialData>() as u32);
        let special_data = TestSpecialData {
            val: 8,
            data: [0, 1, 2, 3, 4, 5, 6, 7],
        };
        assert_ne!(*page.special_data::<TestSpecialData>(), special_data);
        *page.special_data_mut() = special_data.clone();
        assert_eq!(*page.special_data::<TestSpecialData>(), special_data);

        (page, special_data)
    }
}
