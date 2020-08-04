use std::fmt;
use std::fs::File;
use std::io;
use std::path::Path;
use std::time;

use csv;
use fst;
use memmap::Mmap;

use crate::error::{Error, ErrorKind, Result};

/// The TSV file in the IMDb dataset that defines the canonical set of titles
/// available to us. Each record contains basic information about a title,
/// such as its IMDb identifier (e.g., `tt0096697`), primary title, start year
/// and type. This includes movies, TV shows, episodes and more.
pub const IMDB_BASICS: &str = "title.basics.tsv";

/// The TSV file in the IMDb dataset that defines alternate names for some of
/// the titles found in IMDB_BASICS. This includes, but is not limited to,
/// titles in different languages. This file uses the IMDb identifier as a
/// foreign key.
pub const IMDB_AKAS: &str = "title.akas.tsv";

/// The TSV file in the IMDb dataset that defines the season and episode
/// numbers for episodes in TV shows. Each record in this file corresponds to
/// a single episode. There are four columns: the first is the IMDb identifier
/// for the episode. The second is the IMDb identifier for the corresponding
/// TV show. The last two columns are the season and episode numbers. Both of
/// the IMDb identifiers are foreign keys that join the record to IMDB_BASICS.
pub const IMDB_EPISODE: &str = "title.episode.tsv";

/// The TSV file in the IMDb dataset that provides ratings for titles in
/// IMDB_BASICS. Each title has at most one rating, and a rating corresponds
/// to a rank (a decimal in the range 0-10) and the number of votes involved
/// in creating that rating (from the IMDb web site, presumably).
pub const IMDB_RATINGS: &str = "title.ratings.tsv";

/// A type that provides a Display impl for std::time::Duration.
#[derive(Debug)]
pub struct NiceDuration(pub time::Duration);

impl fmt::Display for NiceDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:0.4} secs", self.fractional_seconds())
    }
}

impl NiceDuration {
    /// Create a duration corresponding to the amount of time since the
    /// instant given.
    pub fn since(t: time::Instant) -> NiceDuration {
        NiceDuration(time::Instant::now().duration_since(t))
    }

    /// Returns the number of seconds in this duration in fraction form.
    /// The number to the left of the decimal point is the number of seconds,
    /// and the number to the right is the number of milliseconds.
    pub fn fractional_seconds(&self) -> f64 {
        let fractional = (self.0.subsec_nanos() as f64) / 1_000_000_000.0;
        self.0.as_secs() as f64 + fractional
    }
}

/// A function for creating a CSV reader builder that is pre-loaded with the
/// correct settings for reading all IMDb CSV files.
pub fn csv_reader_builder() -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(true).delimiter(b'\t').quoting(false);
    builder
}

/// Builds a CSV reader (using `csv_reader_builder`) that is backed by a
/// seekable memory map.
///
/// We use memory maps for this even though we could use a normal `File`, which
/// is also seekable, because seeking a memory map has very little overhead.
/// Seeking a `File`, on the other hand, requires a syscall.
pub unsafe fn csv_mmap<P: AsRef<Path>>(
    path: P,
) -> Result<csv::Reader<io::Cursor<Mmap>>> {
    let mmap = mmap_file(path)?;
    Ok(csv_reader_builder().from_reader(io::Cursor::new(mmap)))
}

/// Builds a CSV reader (using `csv_reader_builder`) that is backed by a file.
/// While this read can be seeked, it will be less efficient than using a
/// memory map. Therefore, this is useful for reading CSV data when no seeking
/// is needed.
pub fn csv_file<P: AsRef<Path>>(path: P) -> Result<csv::Reader<File>> {
    let path = path.as_ref();
    let rdr = csv_reader_builder().from_path(path).map_err(|e| {
        Error::new(ErrorKind::Csv(format!("{}: {}", path.display(), e)))
    })?;
    Ok(rdr)
}

/// Builds a file-backed memory map.
pub unsafe fn mmap_file<P: AsRef<Path>>(path: P) -> Result<Mmap> {
    let path = path.as_ref();
    let file = open_file(path)?;
    let mmap = Mmap::map(&file).map_err(|e| Error::io_path(e, path))?;
    Ok(mmap)
}

/// Creates a file and truncates it.
pub fn create_file<P: AsRef<Path>>(path: P) -> Result<File> {
    let path = path.as_ref();
    let file = File::create(path).map_err(|e| Error::io_path(e, path))?;
    Ok(file)
}

/// Opens a file for reading.
pub fn open_file<P: AsRef<Path>>(path: P) -> Result<File> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::io_path(e, path))?;
    Ok(file)
}

/// Creates an FST set builder for the given file path.
pub fn fst_set_builder_file<P: AsRef<Path>>(
    path: P,
) -> Result<fst::SetBuilder<io::BufWriter<File>>> {
    let path = path.as_ref();
    let wtr = io::BufWriter::new(create_file(path)?);
    let builder = fst::SetBuilder::new(wtr).map_err(|e| {
        Error::new(ErrorKind::Fst(format!("{}: {}", path.display(), e)))
    })?;
    Ok(builder)
}

/// Open an FST set file for the given file path as a memory map.
pub unsafe fn fst_set_file<P: AsRef<Path>>(path: P) -> Result<fst::Set<Mmap>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::io_path(e, path))?;
    let mmap = Mmap::map(&file).map_err(|e| Error::io_path(e, path))?;
    let set = fst::Set::new(mmap).map_err(|e| {
        Error::new(ErrorKind::Fst(format!("{}: {}", path.display(), e)))
    })?;
    Ok(set)
}

/// Creates an FST map builder for the given file path.
pub fn fst_map_builder_file<P: AsRef<Path>>(
    path: P,
) -> Result<fst::MapBuilder<io::BufWriter<File>>> {
    let path = path.as_ref();
    let wtr = io::BufWriter::new(create_file(path)?);
    let builder = fst::MapBuilder::new(wtr).map_err(|e| {
        Error::new(ErrorKind::Fst(format!("{}: {}", path.display(), e)))
    })?;
    Ok(builder)
}

/// Open an FST map file for the given file path as a memory map.
pub unsafe fn fst_map_file<P: AsRef<Path>>(path: P) -> Result<fst::Map<Mmap>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| Error::io_path(e, path))?;
    let mmap = Mmap::map(&file).map_err(|e| Error::io_path(e, path))?;
    let map = fst::Map::new(mmap).map_err(|e| {
        Error::new(ErrorKind::Fst(format!("{}: {}", path.display(), e)))
    })?;
    Ok(map)
}
