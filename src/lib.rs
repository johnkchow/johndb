use byteorder::{BigEndian, ByteOrder};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;

const PAGE_SIZE_U64: u64 = 8192;
const PAGE_SIZE_USIZE: usize = 8192;

type PageOffset = u32;

struct BTree {
    file: File,
}

// Node types:
//
// Metadata
// Branch
// Leaf

// Header:
// type: 4 bits, but may change in future
// checksum: 32bits (although in PG they do 16 bit)
//
// Page offset size (32 bit)
// 8KB = 2^23
// NOTE: 1bit waste per 32bits in page = 3.125% of page
// 2^31 * 2^23 = 2^55, total DB memory = 16PB
//
// Branch
// 2^23 / 2^2 = ~ 2^21 =~ 2M root nodes
//
// Leaf
// Entries
//  Header
//  * key length: 8 bits
//  * type: 1 bit, inline vs TOAST
//  * if 0
//      * value length: 7 bits
//  * if 1
//      * 31 value length: page offset
//
// A Metadata simply points to the file offset where the root page is

type MetadataBlock = [u8; PAGE_SIZE_USIZE];

trait Metadata {
    fn root_offset(&self) -> PageOffset;
}

impl Metadata for MetadataBlock {
    fn root_offset(&self) -> PageOffset {
        // TODO: Handle header + checksum
        let slice: &[u8] = &(*self)[6..9];
        let page_addr = BigEndian::read_u32(slice);

        return page_addr;
    }
}

struct Header<'a> {
    buffer: &'a [u8],
}

impl BTree {
    fn open(path: String) -> BTree {
        let res = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path);
        let file = match res {
            Ok(v) => v,
            Err(err) => panic!(err),
        };

        BTree { file }
    }

    fn insert(&mut self, key: &String, value: String) {
        // reverse lookup to find the meta field
        let file_size = self.file.metadata().unwrap().len();
        let end: u64 = file_size - (file_size % PAGE_SIZE_U64);

        self.file
            .seek(SeekFrom::Start(end - (PAGE_SIZE_U64)))
            .expect("TODO: Cannot seek to position");
        let mut buffer: MetadataBlock = [0; PAGE_SIZE_USIZE];
        let len = self
            .file
            .read(&mut buffer)
            .expect("TODO: Cannot read file to buffer");

        if len != PAGE_SIZE_USIZE {
            panic!("TODO: Somehow we read less than PAGE_SIZE...")
        }

        let root_file_offset = buffer.root_offset() * PAGE_SIZE_U64;
    }

    // TODO: return err
    fn fetch_page(&self, page_offset: PageOffset, buffer: &[u8; PAGE_SIZE_USIZE]) {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
