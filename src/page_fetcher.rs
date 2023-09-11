use crate::page::Page;
use crate::page::PageHeader;
use log::debug;
use std::cell::Cell;

// TODO: Refactor to remove the <T> out.
#[derive(Debug)]
pub struct PagePtr {
    val: *mut Page,
}

impl PagePtr {
    fn new(val: *mut Page) -> Self {
        PagePtr { val }
    }
}

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

impl Deref for PagePtr {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.val }
    }
}

impl DerefMut for PagePtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.val }
    }
}

pub trait PageFetcher {
    // TODO: Replace PagePtr with a read-only smart ptr
    fn fetch_page_read(&self, page_no: u32) -> Option<RwLockReadGuard<PagePtr>>;
    fn fetch_page_write(&self, page_no: u32) -> Option<RwLockWriteGuard<PagePtr>>;

    fn new_page<T: Sized>(&self, special_data: T) -> (u32, RwLockWriteGuard<PagePtr>);
}

pub struct InMemoryPageFetcher {
    pub pages: Box<[Page; 16]>,
    pub used_cnt: Cell<usize>,
    pub rw_locks: Vec<RwLock<PagePtr>>,
}

impl InMemoryPageFetcher {
    pub fn new() -> Self {
        let mut pages = Box::new([Page::new(0); 16]);
        let mut rw_locks = Vec::with_capacity(10);
        for ele in pages.iter_mut() {
            rw_locks.push(RwLock::new(PagePtr::new(ele as *mut Page)));
        }
        InMemoryPageFetcher {
            pages,
            used_cnt: Cell::new(0),
            rw_locks,
        }
    }
}

impl<'a> PageFetcher for InMemoryPageFetcher {
    fn fetch_page_read(&self, page_no: u32) -> Option<RwLockReadGuard<PagePtr>> {
        if self.used_cnt.get() <= page_no as usize {
            return None;
        }

        debug!("Acquiring read lock for {}", page_no);
        self.rw_locks
            .get(page_no as usize)
            .map(|rw_lock| (*rw_lock).read().unwrap())
    }

    fn fetch_page_write(&self, page_no: u32) -> Option<RwLockWriteGuard<PagePtr>> {
        if self.used_cnt.get() <= page_no as usize {
            return None;
        }
        debug!("Acquiring write lock for {}", page_no);
        return self
            .rw_locks
            .get(page_no as usize)
            .map(|rw_lock| (*rw_lock).write().unwrap());
    }

    fn new_page<T: Sized>(&self, special_data: T) -> (u32, RwLockWriteGuard<PagePtr>) {
        if self.used_cnt.get() == self.pages.len() {
            panic!("TODO: Need to do more than this!")
        }
        self.used_cnt.set(self.used_cnt.get() + 1);

        let mut rw_lock = self
            .rw_locks
            .get(self.used_cnt.get() - 1)
            .map(|rw_lock| rw_lock.write().unwrap())
            .unwrap();

        rw_lock.header = PageHeader::new(std::mem::size_of::<T>() as u32);
        // Zero out the data just to be safe.
        rw_lock.data.iter_mut().for_each(|m| *m = 0);
        *rw_lock.special_data_mut::<T>() = special_data;
        let page_no = (self.used_cnt.get() - 1) as u32;

        debug!("Initializing new page {} with write lock", page_no);

        return (page_no, rw_lock);
    }
}
