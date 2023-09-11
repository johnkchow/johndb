use crate::page_fetcher::PageFetcher as PageFetcherTrait;

pub mod insert;
mod internal_node;
mod key;
mod leaf_node;
mod metadata_node;
mod search;
mod value;
/*
 * Running TODOs:
 *  * ? Sort items based on key for binary search?
 *    * pre-sorted: read: log(n), insert: nlog(n). Linear: read: n, insert: O(1)
 *  * Refactor LeafNodeItemData/InternalNodeItemData to store arbitrary key/value data types
 *  * Related to ^, refactor Page.update_item for dynamic item size
 *  * Store max key in special data and not iterate through items to find max key
 *  * Add left_sibling_no so we can traverse in both directions
 *  * Remove <T> from PagePtr<T>. Not necessary.
 *  * Audit all the `unwrap()` calls and add better error messaging/logging instead
 *  * Add error handling + logging in case PageFetcher.fetch* returns an Err(...)
 *
*/

/*
 * Btree
 */

pub struct BTree<PageFetcher>
where
    PageFetcher: PageFetcherTrait,
{
    page_fetcher: PageFetcher,
}

#[derive(Debug, Clone)]
enum NodeType {
    Metadata,
    Internal,
    Leaf,
}

trait DynamicSized {
    fn size(&self) -> usize;
}

#[derive(Debug, Clone)]
struct BTreePageData {
    node_type: NodeType,
    right_sibling_page_no: u32,
}

#[derive(Copy, Clone)]
#[deprecated]
struct InternalNodeItemData<K>
where
    K: Sized + Ord + Copy + Clone,
{
    page_no: u32,
    // TODO: Need to figure out how to deal with string type with unboudn length (i.e. `VARCHAR`).
    key: K,
}

#[cfg(test)]
mod tests {
    use super::key::KeyU32;
    use super::search::SearchResult;
    use super::value::ValueTupleId;
    use super::BTree;
    use crate::btree::leaf_node::LeafNodeRead;
    use crate::btree::leaf_node::LeafNodeReadLock;
    use crate::btree::BTreePageData;
    use crate::page_fetcher::InMemoryPageFetcher;
    use crate::page_fetcher::PageFetcher;
    use log::debug;

    #[test]
    fn basic_test() {
        let page_fetcher = InMemoryPageFetcher::new();
        {
            let (page_no, _lock) = page_fetcher.new_page(BTreePageData {
                node_type: super::NodeType::Metadata,
                right_sibling_page_no: 0,
            });
            assert_eq!(page_no, 0);
        }
        let mut btree = BTree { page_fetcher };
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
        let leaf = LeafNodeReadLock::<KeyU32, ValueTupleId>::from((
            1,
            btree.page_fetcher.fetch_page_read(1).unwrap(),
        ));
        leaf.item_iter().for_each(|i| debug!("{:?}", i));

        assert_eq!(
            btree.search::<_, ValueTupleId>(entry1.0),
            SearchResult {
                leaf_page_no: 1,
                value: Some(entry1.1),
            }
        );
        assert_eq!(
            btree.search::<_, ValueTupleId>(KeyU32 { key: 1 }),
            SearchResult {
                leaf_page_no: 1,
                value: None,
            }
        );
        assert_eq!(
            btree.search::<_, ValueTupleId>(entry2.0),
            SearchResult {
                leaf_page_no: 1,
                value: Some(entry2.1),
            }
        );
    }
}
