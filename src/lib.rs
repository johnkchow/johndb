// TODO: Figure out how to get rid of these dead code errors. Drives me crazy.

pub mod btree;
pub mod mem;
pub mod page;
pub mod page_fetcher;
extern crate log;

#[cfg(test)]
#[ctor::ctor]
fn init_log() {
    env_logger::init();
}
