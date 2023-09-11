use super::internal_node::find_child_ptr_move_right_read_lock;
use super::internal_node::from_read_lock as from_read_lock_internal;
use super::key::Key;
use super::leaf_node::LeafNodeRead;
use super::leaf_node::LeafNodeReadLock;
use super::metadata_node::MetadataRead;
use super::value::Value;
use super::BTreePageData;
use super::NodeType;
use crate::btree::metadata_node::MetadataReadLock;
use crate::page_fetcher::PageFetcher as PageFetcherTrait;

#[derive(Debug, PartialEq)]
pub struct SearchResult<T> {
    pub leaf_page_no: u32,
    pub value: Option<T>,
}

impl<PageFetcher> super::BTree<PageFetcher>
where
    PageFetcher: PageFetcherTrait,
{
    pub fn search<K, V>(&self, key: K) -> SearchResult<V>
    where
        K: Key,
        V: Value,
    {
        let mut page_no = 0;

        loop {
            let node = self.page_fetcher.fetch_page_read(page_no).unwrap();
            let special_data = node.special_data::<BTreePageData>();
            let right_sibling_page_no = special_data.right_sibling_page_no;
            match special_data.node_type {
                NodeType::Leaf => {
                    let leaf = LeafNodeReadLock::<K, V>::from((page_no, node));
                    if key < leaf.separator() {
                        let found_row = leaf.item_iter().find(|item_data| key == item_data.key);

                        return match found_row {
                            Some(row) => SearchResult {
                                leaf_page_no: page_no,
                                value: Some(row.value),
                            },
                            // This indicates the scenario where page was splitted in between the release
                            // of the parent node's lock and the lock acquisition of current node
                            None => SearchResult {
                                leaf_page_no: page_no,
                                value: None,
                            },
                        };
                    } else if right_sibling_page_no == 0 {
                        return SearchResult {
                            leaf_page_no: page_no,
                            value: None,
                        };
                    } else {
                        page_no = right_sibling_page_no;
                    }
                }
                NodeType::Internal => {
                    let (_, child_no) = find_child_ptr_move_right_read_lock(
                        &self.page_fetcher,
                        from_read_lock_internal(page_no, node),
                        key,
                    );

                    page_no = child_no
                }
                NodeType::Metadata => {
                    let root_no = MetadataReadLock::from(node).root_no();
                    match root_no {
                        None => {
                            return SearchResult {
                                leaf_page_no: 0,
                                value: None,
                            };
                        }
                        Some(root_no) => page_no = root_no,
                    };
                }
            }
        }
    }
}
