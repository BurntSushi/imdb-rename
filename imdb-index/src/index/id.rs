use std::fs::File;
use std::io;
use std::path::Path;

use fst;

use error::{Error, Result};
use util::{fst_map_builder_file, fst_map_file};

/// An index that maps arbitrary length identifiers to 64-bit integers.
///
/// An ID index is often useful for mapping human readable identifiers or
/// "natural keys" to other more convenient forms, such as file offsets.
#[derive(Debug)]
pub struct IndexReader {
    idx: fst::Map,
}

impl IndexReader {
    /// Open's an ID index reader from the given file path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<IndexReader> {
        // We claim it is safe to open the following memory map because we
        // don't mutate them and no other process (should) either.
        Ok(IndexReader { idx: unsafe { fst_map_file(path)? } })
    }

    /// Return the integer associated with the given ID, if it exists.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        self.idx.get(key)
    }
}

/// An ID index writer that requires that identifiers are given in
/// lexicographically ascending order.
pub struct IndexSortedWriter<W> {
    wtr: fst::MapBuilder<W>,
}

impl IndexSortedWriter<io::BufWriter<File>> {
    /// Create an index writer that writes the index to the given file path.
    pub fn from_path<P: AsRef<Path>>(
        path: P,
    ) -> Result<IndexSortedWriter<io::BufWriter<File>>> {
        Ok(IndexSortedWriter {
            wtr: fst_map_builder_file(path)?,
        })
    }
}

impl<W: io::Write> IndexSortedWriter<W> {
    /// Associate the given identifier with the given integer.
    ///
    /// If the given key is not strictly lexicographically greater than the
    /// previous key, then an error is returned.
    pub fn insert(&mut self, key: &[u8], value: u64) -> Result<()> {
        self.wtr.insert(key, value).map_err(Error::fst)?;
        Ok(())
    }

    /// Finish writing the index.
    ///
    /// This must be called, otherwise the index will likely be unreadable.
    pub fn finish(self) -> Result<()> {
        self.wtr.finish().map_err(Error::fst)?;
        Ok(())
    }
}
