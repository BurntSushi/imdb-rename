use std::path::{Path, PathBuf};

/// Create an error from a format!-like syntax.
#[macro_export]
macro_rules! err {
    ($($tt:tt)*) => {
        Box::<dyn std::error::Error>::from(format!($($tt)*))
    }
}

/// A convenient result type alias.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// A simple test context that makes it convenient to create an index.
///
/// Each test context has an IMDb data directory (which usually has only a
/// subset of the actual data) and an index directory (which starts empty by
/// default).
#[derive(Debug)]
pub struct TestContext {
    _tmpdir: TempDir,
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
        TestContext { _tmpdir: tmpdir, data_dir, index_dir }
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

/// A simple wrapper for creating a temporary directory that is automatically
/// deleted when it's dropped.
///
/// We use this in lieu of tempfile because tempfile brings in too many
/// dependencies.
#[derive(Debug)]
pub struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

impl TempDir {
    /// Create a new empty temporary directory under the system's configured
    /// temporary directory.
    pub fn new(prefix: &str) -> Result<TempDir> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static TRIES: usize = 100;
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let tmpdir = std::env::temp_dir();
        for _ in 0..TRIES {
            let count = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = tmpdir.join(prefix).join(count.to_string());
            if path.is_dir() {
                continue;
            }
            std::fs::create_dir_all(&path).map_err(|e| {
                err!("failed to create {}: {}", path.display(), e)
            })?;
            return Ok(TempDir(path));
        }
        Err(err!("failed to create temp dir after {} tries", TRIES))
    }

    /// Return the underlying path to this temporary directory.
    pub fn path(&self) -> &Path {
        &self.0
    }
}
