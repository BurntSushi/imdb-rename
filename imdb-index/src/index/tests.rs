use std::path::{Path, PathBuf};

use tempdir::TempDir;

/// A simple test context that makes it convenient to create an index.
///
/// Each test context has an IMDb data directory (which usually has only a
/// subset of the actual data) and an index directory (which starts empty by
/// default).
#[derive(Debug)]
pub struct TestContext {
    tmpdir: TempDir,
    data_dir: PathBuf,
    index_dir: PathBuf,
}

impl TestContext {
    /// Create a new test context using the test data set name given.
    ///
    /// Test data sets can be found in the `data/test` directory in this
    /// repository's root. Data set names are the names of sub-directories of
    /// `data`.
    pub fn new(name: &str) -> TestContext {
        let tmpdir = TempDir::new("imdb-rename-test-index").unwrap();
        let data_dir = PathBuf::from("../data/test").join(name);
        let index_dir = tmpdir.path().to_path_buf();
        TestContext { tmpdir, data_dir, index_dir }
    }

    /// Return the path to the data directory for this context.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the path to the index directory for this context.
    pub fn index_dir(&self) -> &Path {
        &self.index_dir
    }
}
