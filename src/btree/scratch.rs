fn split_node_data_v3<P, I, S, F>(orig: &mut P, new: &mut P, separator_fn: F)
where
    P: NodeWrite<I, S>,
    I: Item + Ord,
    S: Key,
    F: Fn(&I) -> S,
{
    let separator = orig.separator();

    let mut sorted_rev = orig.item_iter().collect::<Vec<_>>();
    sorted_rev.sort();

    // First, add separator to the `new` Page. It's always guaranteed to be the first item in the
    // page.
    new.set_separator(&separator);

    let item_data_size: usize = sorted_rev.iter().fold(0, |sum, i| sum + i.size());
    let mut added: usize = 0;
    let mut count: usize = 0;
    for (i, item) in sorted_rev.iter().enumerate() {
        // TODO: Make this not unwrap
        new.add_item(item).unwrap();
        added += item.size();
        if added > item_data_size / 2 {
            count = i + 1;
            break;
        }
    }

    orig.zero_out_item_data();

    let sep = separator_fn(sorted_rev.get(count).unwrap());
    orig.set_separator(&sep);

    for item in sorted_rev.iter().skip(count) {
        orig.add_item(item).unwrap();
    }
}

pub(super) trait NodeRead<I, S>
where
    I: Item + Ord,
    S: Key + Ord,
{
    fn page_ref(&self) -> &Page;

    fn item_iter(&self) -> Skip<PageItemIteratorV2<I>> {
        // We skip the first element, because it's always the separator
        self.page_ref().items_iter_v2::<I>().skip(1)
    }

    fn separator(&self) -> S {
        self.page_ref().get_item_v2::<S>(0)
    }

    fn special_data(&self) -> &BTreePageData {
        self.page_ref().special_data()
    }
}

pub(super) trait NodeWrite<I, S>: NodeRead<I, S>
where
    I: Item + Ord,
    S: Key + Ord,
{
    fn page_ref_mut(&mut self) -> &mut Page;

    fn gt_separator(&self, item: &I) -> bool;

    fn add_item(&mut self, item: &I) -> Result<(), &'static str> {
        if self.gt_separator(item) {
            return Err(
                "We can't add due to item not fitting within this page's allowed key range",
            );
        }

        self.page_ref_mut().add_item_v2(item)
    }

    fn set_separator(&mut self, sep: &S) {
        assert_eq!(self.page_ref().item_cnt(), 0);

        // TODO: handle error here
        self.page_ref_mut().add_item_v2(sep).unwrap();
    }

    fn zero_out_item_data(&mut self) {
        self.page_ref_mut().zero_out_item_data();
    }

    fn set_right_sibling_no(&mut self, right_sibling_no: u32) {
        self.page_ref_mut()
            .special_data_mut::<BTreePageData>()
            .right_sibling_page_no = right_sibling_no;
    }
}
