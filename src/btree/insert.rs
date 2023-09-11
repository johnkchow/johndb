use super::internal_node::InternalNodeRead;
use super::internal_node::InternalNodeWriteLock;
use super::key::Key;
use super::value::Value;
use crate::btree::leaf_node::LeafNodeRead;
use crate::btree::metadata_node::MetadataRead;
use crate::btree::metadata_node::MetadataReadLock;
use crate::btree::metadata_node::MetadataWriteLock;
use crate::page::Item;
use crate::page::Page;
use crate::page_fetcher::PageFetcher as PageFetcherTrait;
use crate::page_fetcher::PagePtr;
use log::debug;
use std::ops::DerefMut;
use std::sync::RwLockWriteGuard;

impl<PageFetcher> super::BTree<PageFetcher>
where
    PageFetcher: PageFetcherTrait,
{
    /// Returns the leaf page number where it was inserted.
    pub fn insert<K, V>(&mut self, key: K, value: V) -> u32
    where
        K: Key,
        V: Value,
    {
        debug!("[insert] Begin insert {:?}, {:?}", key, value);
        let mut leaf_node_no = {
            let metadata = MetadataReadLock::from(self.page_fetcher.fetch_page_read(0).unwrap());
            let root_no_opt = metadata.root_no();

            match root_no_opt {
                Some(root_no) => root_no,
                None => {
                    debug!(
                        "[insert.load_root] Root not found, acquiring write lock and initializing a new root)"
                    );
                    // Dropping read lock prior to acquiring the write lock
                    drop(metadata);
                    let mut metadata_w =
                        MetadataWriteLock::from(self.page_fetcher.fetch_page_write(0).unwrap());
                    let root_no_opt = metadata_w.root_no();
                    match root_no_opt {
                        Some(root_no) => root_no,
                        None => {
                            let (new_root_no, mut new_root_lock) =
                                super::leaf_node::new_page::<_, K, V>(&self.page_fetcher, 0);

                            new_root_lock.set_separator(&K::max_key());

                            // TODO: Add better error messsage unstead of unwrapping
                            // TODO: Create a new Metadata wrapper struct
                            metadata_w.set_root_no(new_root_no);
                            new_root_no
                        }
                    }
                }
            }
        };
        // We add zero here to indicate that the "parent" is the metadata, and therefore we'll want
        // to start from the top of the tree (in the very rare case that the "previous" root had
        // split from the time we started this method call to the bottom of this method where we're
        // walking up the tree to split pages.
        let mut traversed: Vec<u32> = vec![0];

        loop {
            debug!("[insert.traverse_down] Begin loop: {})", leaf_node_no);
            let current = self.page_fetcher.fetch_page_read(leaf_node_no).unwrap();
            let special_data = current.special_data::<super::BTreePageData>();
            match special_data.node_type {
                super::NodeType::Metadata => {
                    panic!("Somehow we encountered a metadata, this should never occur")
                }
                super::NodeType::Internal => {
                    let internal = super::internal_node::from_read_lock::<K>(leaf_node_no, current);
                    let (parent_node, child_node) =
                        super::internal_node::find_child_ptr_move_right_read_lock(
                            &self.page_fetcher,
                            internal,
                            key,
                        );
                    traversed.push(parent_node);
                    leaf_node_no = child_node;
                    debug!("[insert.traverse_down] Traversing to {}", child_node,);
                }
                super::NodeType::Leaf => {
                    debug!(
                        "[insert.traverse_down] Found child node {}, break",
                        leaf_node_no
                    );
                    // we've reached the leaf candidate, break;
                    break;
                }
            };
        }

        let mut leaf_lock = super::leaf_node::find_move_right::<PageFetcher, K, V>(
            &self.page_fetcher,
            leaf_node_no,
            key,
        );

        let leaf_data = super::leaf_node::LeafNodeItemData { key, value };
        match leaf_lock.add_item(&leaf_data) {
            Ok(()) => {
                return leaf_node_no;
            }
            Err(_err) => {
                // Not enough space to add item to this page, therefore we must split.
                debug!(
                    "[insert] Not enough space to add, now we're splitting leaf page {}",
                    leaf_lock.page_no,
                );

                // First, we split the leaf node into a new sibling page
                let prev_sibling_no = leaf_lock.special_data().right_sibling_page_no;
                let (new_sibling_no, mut new_sibling) =
                    super::leaf_node::new_page::<PageFetcher, K, V>(
                        &self.page_fetcher,
                        prev_sibling_no,
                    );
                leaf_lock.special_data_mut().right_sibling_page_no = new_sibling_no;

                split_node_data_v2::<super::leaf_node::LeafNodeItemData<K, V>, K, _>(
                    leaf_lock.page_ref_mut(),
                    new_sibling.page_ref_mut(),
                    |item| item.key,
                );

                debug!(
                    "[insert] Splitted leaf pages: page_no={:?} sep={:?}, NEW page_no={:?} sep={:?}",
                    leaf_lock.page_no,
                    leaf_lock.separator(),
                    new_sibling.page_no,
                    new_sibling.separator(),
                );

                let return_leaf_node_no: u32;
                if key <= leaf_lock.separator() {
                    return_leaf_node_no = leaf_node_no;
                    leaf_lock.add_item(&leaf_data).unwrap();
                } else {
                    return_leaf_node_no = new_sibling_no;
                    new_sibling.add_item(&leaf_data).unwrap();
                }

                // Then we begin the unwinding of the `traversed` stack to update the parent
                // linkage
                {
                    let mut orig_child = super::internal_node::InternalNodeItemData {
                        page_no: leaf_node_no,
                        key: leaf_lock.separator(),
                    };
                    let mut new_child = super::internal_node::InternalNodeItemData {
                        page_no: new_sibling_no,
                        key: new_sibling.separator(),
                    };
                    #[allow(unused_variables)]
                    let mut orig_child_lock: RwLockWriteGuard<PagePtr> = leaf_lock.into();

                    let mut split = true;

                    // We can drop the new page given that it's inaccessible while we hold a lock on
                    // the original sibling page
                    drop(new_sibling);

                    // now, we traverse up the tree to update the pointers and see if we need to split
                    // any internal nodes.
                    while split && traversed.len() > 0 {
                        let parent_node_no = traversed.pop().unwrap();
                        debug!(
                            "[insert.traverse_up] Begin loop: ORIG {:?}, NEW {:?}, parent_no: {}",
                            orig_child, new_child, parent_node_no,
                        );

                        if parent_node_no == 0 {
                            // in the scenario where we split the root, it's possible that the root had
                            // already splitted prior to reaching this code. thus, we want to start at
                            // the metadata page and traverse down until we find the root's parent (if
                            // there is one)
                            debug!("[insert.traverse_up] Arrived at metadata, meaning the root had split");
                            let mut metadata = MetadataWriteLock::from(
                                self.page_fetcher.fetch_page_write(0).unwrap(),
                            );

                            match metadata.root_no() {
                                Some(root_no) if root_no == orig_child.page_no => {
                                    // we initialize a new root, have the two roots point to the two pages,
                                    // and update the metadata, and we're done
                                    let (new_root_no, mut new_root_lock) =
                                        super::internal_node::new_page(&self.page_fetcher, 0);

                                    debug!(
                                        "[insert.traverse_up] Creating new root {}",
                                        new_root_no
                                    );

                                    new_root_lock.set_separator(&K::max_key());
                                    metadata.set_root_no(new_root_no);
                                    new_root_lock.add_item(orig_child).unwrap();
                                    new_root_lock.add_item(new_child).unwrap();
                                    split = false;
                                }
                                _ => {
                                    debug!(
                                        "[insert.traverse_up] Traversing down tree from metadata until we find the parent",
                                    );
                                    traversed.push(0);
                                    let mut page_no = metadata.root_no().unwrap();

                                    loop {
                                        let page = super::internal_node::fetch_page_read::<
                                            PageFetcher,
                                            K,
                                        >(
                                            &self.page_fetcher, page_no
                                        )
                                        .unwrap();
                                        let (candidate_no, downlink_no) =
                                        super::internal_node::find_child_ptr_move_right_read_lock(
                                            &self.page_fetcher,
                                            page,
                                            key,
                                        );
                                        if downlink_no == orig_child.page_no {
                                            traversed.push(candidate_no);
                                            break;
                                        } else {
                                            page_no = downlink_no;
                                        }
                                    }
                                    split = true;
                                }
                            }
                        } else {
                            let mut parent =
                                super::internal_node::find_node_with_entry_move_right_write_lock(
                                    &self.page_fetcher,
                                    parent_node_no,
                                    orig_child.page_no,
                                );

                            match update_child_ptr(
                                &self.page_fetcher,
                                &mut parent,
                                orig_child,
                                new_child,
                            ) {
                                None => {
                                    split = false;
                                }
                                Some(_res) => {
                                    orig_child = super::internal_node::InternalNodeItemData {
                                        page_no: parent_node_no,
                                        key: parent.separator(),
                                    };
                                    new_child = super::internal_node::InternalNodeItemData {
                                        page_no: parent_node_no,
                                        key: parent.separator(),
                                    };
                                    orig_child_lock = parent.into();
                                    split = true;
                                }
                            };
                        }
                    }

                    return_leaf_node_no
                }
            }
        }
    }
}

fn split_node_data_v2<I, S, F>(orig: &mut Page, new: &mut Page, separator_fn: F)
where
    I: Item + Ord,
    S: Key,
    F: Fn(&I) -> S,
{
    let separator = orig.get_item_v2::<S>(0);

    let mut sorted_rev = orig.items_iter_v2::<I>().skip(1).collect::<Vec<_>>();
    sorted_rev.sort();

    // First, add separator to the `new` Page. It's always guaranteed to be the first item in the
    // page.
    new.add_item_v2(&separator).unwrap();

    let item_data_size: usize = sorted_rev.iter().fold(0, |sum, i| sum + i.size());
    let mut added: usize = 0;
    let mut count: usize = 0;
    for (i, item) in sorted_rev.iter().enumerate() {
        // TODO: Make this not unwrap
        new.add_item_v2(item).unwrap();
        added += item.size();
        if added > item_data_size / 2 {
            count = i + 1;
            break;
        }
    }

    orig.zero_out_item_data();

    let sep = separator_fn(sorted_rev.get(count).unwrap());
    orig.add_item_v2(&sep).unwrap();

    for item in sorted_rev.iter().skip(count) {
        orig.add_item_v2(item).unwrap();
    }
}

fn update_child_ptr<'a, P, K>(
    page_fetcher: &'a P,
    parent: &mut InternalNodeWriteLock<'a, K>,
    orig: super::internal_node::InternalNodeItemData<K>,
    new: super::internal_node::InternalNodeItemData<K>,
) -> Option<(u32, InternalNodeWriteLock<'a, K>)>
where
    P: PageFetcherTrait,
    K: Key,
{
    parent.update_item(&orig).unwrap();

    match parent.add_item(new) {
        Ok(()) => None,
        Err(_err) => {
            // TODO: Log + handle error
            let (new_sibling_no, mut new_sibling_lock) = super::internal_node::new_page(
                page_fetcher,
                parent.special_data().right_sibling_page_no,
            );

            split_node_data_v2::<super::internal_node::InternalNodeItemData<K>, _, _>(
                parent.page_ref_mut(),
                new_sibling_lock.page_ref_mut(),
                |i| i.key,
            );

            if new.key < parent.separator() {
                parent.add_item(new).unwrap();
            } else {
                new_sibling_lock.add_item(new).unwrap();
            }

            Some((new_sibling_no, new_sibling_lock))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::key::KeyU32;
    use crate::btree::leaf_node::LeafNodeItemData;
    use crate::btree::leaf_node::LeafNodeRead;
    use crate::btree::leaf_node::LeafNodeReadLock;
    use crate::btree::metadata_node::MetadataRead;
    use crate::btree::metadata_node::MetadataReadLock;
    use crate::btree::value::ValueTupleId;
    use crate::btree::BTree;
    use crate::btree::BTreePageData;
    use crate::btree::NodeType;
    use crate::page::ITEM_POINTER_SIZE;
    use crate::page::PAGE_DATA_SIZE;
    use crate::page_fetcher::InMemoryPageFetcher;
    use crate::page_fetcher::PageFetcher;
    use log::debug;
    use std::mem::size_of;

    #[test]
    fn no_root() {
        let mut btree = setup_btree();

        let entry1 = (
            KeyU32 { key: 0 },
            ValueTupleId {
                page_no: 1,
                offset: 2,
            },
        );
        let entry2 = (
            KeyU32 { key: 2 },
            ValueTupleId {
                page_no: 3,
                offset: 4,
            },
        );

        assert_eq!(btree.insert(entry1.0, entry1.1), 1);
        assert_eq!(btree.insert(entry2.0, entry2.1), 1);
        let metadata = MetadataReadLock::from(btree.page_fetcher.fetch_page_read(0).unwrap());
        assert_eq!(metadata.root_no(), Some(1));
        let page = btree.page_fetcher.fetch_page_read(1).unwrap();
        assert_eq!(page.item_cnt(), 3); // 1 is separator, 2 are keys
                                        // let leaf = LeafNodeReadLock::<KeyU32, ValueTupleId>::from((1, page));
        let separator = page.get_item_v2::<KeyU32>(0);
        assert_eq!(separator.key, u32::MAX);

        let item = page.get_item_v2::<LeafNodeItemData<KeyU32, ValueTupleId>>(1);
        assert_eq!(item.key, entry1.0);
        assert_eq!(item.value, entry1.1);

        let item = page.get_item_v2::<LeafNodeItemData<KeyU32, ValueTupleId>>(2);
        assert_eq!(item.key, entry2.0);
        assert_eq!(item.value, entry2.1);
    }

    #[test]
    fn split_root_leaf() {
        let mut btree = setup_btree();
        let max_items_in_leaf = (PAGE_DATA_SIZE - size_of::<BTreePageData>())
            / (size_of::<LeafNodeItemData<KeyU32, ValueTupleId>>() + ITEM_POINTER_SIZE);

        for i in 0..max_items_in_leaf {
            let entry = (
                KeyU32 { key: i as u32 },
                ValueTupleId {
                    page_no: i as u32,
                    offset: i as u16,
                },
            );

            assert_eq!(btree.insert(entry.0, entry.1), 1);
        }

        let entry = (
            KeyU32 {
                key: max_items_in_leaf as u32,
            },
            ValueTupleId {
                page_no: max_items_in_leaf as u32,
                offset: max_items_in_leaf as u16,
            },
        );

        assert_eq!(btree.insert(entry.0, entry.1), 2);

        let leaf1 = LeafNodeReadLock::<KeyU32, ValueTupleId>::from((
            1,
            btree.page_fetcher.fetch_page_read(1).unwrap(),
        ));
        let leaf2 = LeafNodeReadLock::<KeyU32, ValueTupleId>::from((
            2,
            btree.page_fetcher.fetch_page_read(2).unwrap(),
        ));

        let mut items = leaf1.item_iter().collect::<Vec<_>>();
        items.extend(leaf2.item_iter());
        items.sort();
        items
            .iter()
            .enumerate()
            .for_each(|(i, leaf)| assert_eq!(i, leaf.key.key as usize));

        assert_eq!(items.len(), max_items_in_leaf + 1);
    }

    #[test]
    #[ignore]
    fn multi_internal_level() {
        todo!("TODO: Need to add this test!");
    }

    fn setup_btree() -> BTree<InMemoryPageFetcher> {
        let page_fetcher = InMemoryPageFetcher::new();
        {
            let (page_no, _lock) = page_fetcher.new_page(BTreePageData {
                node_type: NodeType::Metadata,
                right_sibling_page_no: 0,
            });
            assert_eq!(page_no, 0);
            debug!("{:?}", page_fetcher.pages[0]);
            debug!(
                "{:?}",
                page_fetcher.pages[0].special_data::<BTreePageData>()
            );
        }
        BTree { page_fetcher }
    }
}
