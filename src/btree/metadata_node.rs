use super::key::KeyU32;
use crate::btree::BTreePageData;
use crate::btree::NodeType;
use crate::page::Page;
use crate::page_fetcher::PagePtr;
use std::ops::Deref;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

pub trait MetadataRead {
    fn page(&self) -> &Page;

    fn root_no(&self) -> Option<u32> {
        match self.page().item_cnt() {
            0 => None,
            1 => Some(self.page().get_item_v2::<KeyU32>(0).key),
            _ => panic!("Somehow we have multiple items in the metadata node!"),
        }
    }
}

pub struct MetadataReadLock<'a> {
    page: RwLockReadGuard<'a, PagePtr>,
}

impl<'a> MetadataRead for MetadataReadLock<'a> {
    fn page(&self) -> &Page {
        self.page.deref().deref()
    }
}

impl<'a> From<RwLockReadGuard<'a, PagePtr>> for MetadataReadLock<'a> {
    fn from(page: RwLockReadGuard<'a, PagePtr>) -> Self {
        assert!(matches!(
            page.special_data::<BTreePageData>().node_type,
            NodeType::Metadata
        ));
        Self { page }
    }
}

pub struct MetadataWriteLock<'a> {
    page: RwLockWriteGuard<'a, PagePtr>,
}

impl<'a> MetadataRead for MetadataWriteLock<'a> {
    fn page(&self) -> &Page {
        self.page.deref().deref()
    }
}

impl<'a> MetadataWriteLock<'a> {
    pub fn set_root_no(&mut self, root_no: u32) {
        match self.page.item_cnt() {
            0 => {
                self.page.add_item_v2(&KeyU32 { key: root_no });
            }
            1 => {
                self.page.update_item_v2(0, &KeyU32 { key: root_no });
            }
            _ => panic!("Somehow we have multiple items in the metadata node!"),
        };
    }
}

impl<'a> From<RwLockWriteGuard<'a, PagePtr>> for MetadataWriteLock<'a> {
    fn from(page: RwLockWriteGuard<'a, PagePtr>) -> Self {
        assert!(matches!(
            page.special_data::<BTreePageData>().node_type,
            NodeType::Metadata
        ));
        Self { page }
    }
}
