use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use csv;
use failure::ResultExt;
use memmap::Mmap;
use serde_json;

use error::{Error, ErrorKind, Result};
use record::{Episode, Rating, Title, TitleKind};
use scored::SearchResults;
use util::{
    IMDB_BASICS, NiceDuration, create_file, csv_file, csv_mmap, open_file,
};

pub use self::aka::AKARecordIter;
pub use self::names::{NameQuery, NameScorer, NgramType};

mod aka;
mod episode;
mod id;
mod names;
mod rating;
#[cfg(test)]
mod tests;
mod writer;

/// The version of the index format on disk.
///
/// Generally speaking, if the version of the index on disk doesn't exactly
/// match the version expected by this code, then the index won't be read.
/// The caller must then re-generate the index.
///
/// This version represents all indexing structures on disk in this module.
const VERSION: u64 = 1;

/// The name of the title file index.
///
/// This index represents a map from the IMDb title ID to the file offset
/// corresponding to that record in title.basics.tsv.
const TITLE: &str = "title.fst";

/// The name of the file containing the index configuration.
///
/// The index configuration is a JSON file with some meta data about this
/// index, such as its version.
const CONFIG: &str = "config.json";

/// A media entity is a title with optional episode and rating records.
///
/// A media entity makes it convenient to deal with the complete information
/// of an IMDb media record. This is the default value returned by search
/// routines such as what the [`Searcher`](struct.Searcher.html) provides, and
/// can also be cheaply constructed by an [`Index`](struct.Index.html) given a
/// [`Title`](struct.Title.html) or an IMDb ID.
#[derive(Clone, Debug)]
pub struct MediaEntity {
    title: Title,
    episode: Option<Episode>,
    rating: Option<Rating>,
}

impl MediaEntity {
    /// Return a reference to the underlying `Title`.
    pub fn title(&self) -> &Title {
        &self.title
    }

    /// Return a reference to the underlying `Episode`, if it exists.
    pub fn episode(&self) -> Option<&Episode> {
        self.episode.as_ref()
    }

    /// Return a reference to the underlying `Rating`, if it exists.
    pub fn rating(&self) -> Option<&Rating> {
        self.rating.as_ref()
    }
}

/// An index into IMDb titles and their associated data.
///
/// This index consists of a set of on disk index data structures in addition
/// to the uncompressed IMDb `tsv` files. The on disk index structures are used
/// to provide access to the records in the `tsv` files efficiently.
///
/// With this index, one can do the following things:
///
/// * Return a ranked list
///   [`Title`](struct.Title.html)
///   records matching a fuzzy name query.
/// * Access any `Title` record by ID in constant time.
/// * Access all
///   [`AKA`](struct.AKA.html)
///   records for any `Title` in constant time.
/// * Access the
///   [`Rating`](struct.Rating.html)
///   for any `Title` in constant time.
/// * Access the complete set of
///   [`Episode`](struct.Episode.html)
///   records for any TV show in constant time.
/// * Access the specific `Episode` given its ID in constant time.
#[derive(Debug)]
pub struct Index {
    /// The directory containing the IMDb tsv files.
    data_dir: PathBuf,
    /// The directory containing this crate's index structures.
    index_dir: PathBuf,
    /// A seekable reader for `title.basics.tsv`. The index structures
    /// typically return offsets that can be used to seek this reader to the
    /// beginning of any `Title` record.
    csv_basic: csv::Reader<io::Cursor<Mmap>>,
    /// The name index. This is what provides fuzzy queries.
    idx_names: names::IndexReader,
    /// The AKA index.
    idx_aka: aka::Index,
    /// The episode index.
    idx_episode: episode::Index,
    /// The rating index.
    idx_rating: rating::Index,
    /// The title index.
    idx_title: id::IndexReader,
}

#[derive(Debug, Deserialize, Serialize)]
struct Config {
    version: u64,
}

impl Index {
    /// Open an existing index using default settings. If the index does not
    /// exist, or if there was a problem opening it, then this returns an
    /// error.
    ///
    /// Generally, this method is cheap to call. It opens some file
    /// descriptors, but otherwise does no work.
    ///
    /// `data_dir` should be the directory containing decompressed IMDb
    /// `tsv` files. See: https://www.imdb.com/interfaces/
    ///
    /// `index_dir` should be the directory containing a previously created
    /// index using `Index::create`.
    pub fn open<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        IndexBuilder::new().open(data_dir, index_dir)
    }

    /// Create a new index using default settings.
    ///
    /// Calling this method is expensive, and one should expect this to take
    /// dozens of seconds or more to complete.
    ///
    /// `data_dir` should be the directory containing decompressed IMDb tsv`
    /// `files. See: https://www.imdb.com/interfaces/
    ///
    /// `index_dir` should be the directory containing a previously created
    /// index using `Index::create`.
    ///
    /// This will overwrite any previous index that may have existed in
    /// `index_dir`.
    pub fn create<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        IndexBuilder::new().create(data_dir, index_dir)
    }

    /// Attempt to clone this index, returning a distinct `Index`.
    ///
    /// This is as cheap to call as `Index::open` and returns an error if there
    /// was a problem reading the underlying index.
    ///
    /// This is useful when one wants to query the same `Index` on disk from
    /// multiple threads.
    pub fn try_clone(&self) -> Result<Index> {
        Index::open(&self.data_dir, &self.index_dir)
    }

    /// Search this index for `Title` records whose name matches the given
    /// query.
    ///
    /// The query controls the following things:
    ///
    /// * The name to search for.
    /// * The maximum number of results returned.
    /// * The scorer to use to rank results.
    ///
    /// The name can be any string. It is normalized and broken down into
    /// component pieces, which are then used to quickly search all existing
    /// titles quickly and fuzzily.
    ///
    /// This returns an error if there was a problem reading the index or the
    /// underlying CSV data.
    pub fn search(
        &mut self,
        query: &names::NameQuery,
    ) -> Result<SearchResults<Title>> {
        let mut results = SearchResults::new();
        // The name index gives us back scores with offsets. The offset can be
        // used to seek our `Title` CSV reader to the corresponding record and
        // read it in constant time.
        for result in self.idx_names.search(query) {
            let title = match self.read_record(*result.value())? {
                None => continue,
                Some(title) => title,
            };
            results.push(result.map(|_| title));
        }
        Ok(results)
    }

    /// Returns the `MediaEntity` for the given IMDb ID.
    ///
    /// An entity includes an [`Episode`](struct.Episode.html) and
    /// [`Rating`](struct.Rating.html) records if they exist for the title.
    ///
    /// This returns an error if there was a problem reading the underlying
    /// index. If no such title exists for the given ID, then `None` is
    /// returned.
    pub fn entity(&mut self, id: &str) -> Result<Option<MediaEntity>> {
        match self.title(id)? {
            None => Ok(None),
            Some(title) => self.entity_from_title(title).map(Some),
        }
    }

    /// Returns the `MediaEntity` for the given `Title`.
    ///
    /// This is like the `entity` method, except it takes a `Title` record as
    /// given.
    pub fn entity_from_title(&mut self, title: Title) -> Result<MediaEntity> {
        let episode = match title.kind {
            TitleKind::TVEpisode => self.episode(&title.id)?,
            _ => None,
        };
        let rating = self.rating(&title.id)?;
        Ok(MediaEntity { title, episode, rating })
    }

    /// Returns the `Title` record for the given IMDb ID.
    ///
    /// This returns an error if there was a problem reading the underlying
    /// index. If no such title exists for the given ID, then `None` is
    /// returned.
    pub fn title(&mut self, id: &str) -> Result<Option<Title>> {
        match self.idx_title.get(id.as_bytes()) {
            None => Ok(None),
            Some(offset) => self.read_record(offset),
        }
    }

    /// Returns an iterator over all `AKA` records for the given IMDb ID.
    ///
    /// If no AKA records exist for the given ID, then an empty iterator is
    /// returned.
    ///
    /// If there was a problem reading the index, then an error is returned.
    pub fn aka_records(&mut self, id: &str) -> Result<AKARecordIter> {
        self.idx_aka.find(id.as_bytes())
    }

    /// Returns the `Rating` associated with the given IMDb ID.
    ///
    /// If no rating exists for the given ID, then this returns `None`.
    ///
    /// If there was a problem reading the index, then an error is returned.
    pub fn rating(&mut self, id: &str) -> Result<Option<Rating>> {
        self.idx_rating.rating(id.as_bytes())
    }

    /// Returns all of the episodes for the given TV show. The TV show should
    /// be identified by its IMDb ID.
    ///
    /// If the given ID isn't a TV show or if the TV show doesn't have any
    /// episodes, then an empty list is returned.
    ///
    /// The episodes returned are sorted in order of their season and episode
    /// numbers. Episodes without a season or episode number are sorted after
    /// episodes with a season or episode number.
    ///
    /// If there was a problem reading the index, then an error is returned.
    pub fn seasons(&mut self, tvshow_id: &str) -> Result<Vec<Episode>> {
        self.idx_episode.seasons(tvshow_id.as_bytes())
    }

    /// Returns all of the episodes for the given TV show and season number.
    /// The TV show should be identified by its IMDb ID, and the season should
    /// be identified by its number. (Season numbers generally start at `1`.)
    ///
    /// If the given ID isn't a TV show or if the TV show doesn't have any
    /// episodes for the given season, then an empty list is returned.
    ///
    /// The episodes returned are sorted in order of their episode numbers.
    /// Episodes without an episode number are sorted after episodes with an
    /// episode number.
    ///
    /// If there was a problem reading the index, then an error is returned.
    pub fn episodes(
        &mut self,
        tvshow_id: &str,
        season: u32,
    ) -> Result<Vec<Episode>> {
        self.idx_episode.episodes(tvshow_id.as_bytes(), season)
    }

    /// Return the episode corresponding to the given IMDb ID.
    ///
    /// If the ID doesn't correspond to an episode, then `None` is returned.
    ///
    /// If there was a problem reading the index, then an error is returned.
    pub fn episode(&mut self, episode_id: &str) -> Result<Option<Episode>> {
        self.idx_episode.episode(episode_id.as_bytes())
    }

    /// Returns the data directory that this index returns results for.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the directory containing this index's files.
    pub fn index_dir(&self) -> &Path {
        &self.index_dir
    }

    /// Read the CSV `Title` record beginning at the given file offset.
    ///
    /// If no such record exists, then this returns `None`.
    ///
    /// If there was a problem reading the underlying CSV data, then an error
    /// is returned.
    ///
    /// If the given offset does not point to the start of a record in the CSV
    /// data, then the behavior of this method is unspecified.
    fn read_record(
        &mut self,
        offset: u64,
    ) -> Result<Option<Title>> {
        let mut pos = csv::Position::new();
        pos.set_byte(offset);
        self.csv_basic.seek(pos).map_err(Error::csv)?;

        let mut record = csv::StringRecord::new();
        if !self.csv_basic.read_record(&mut record).map_err(Error::csv)? {
            Ok(None)
        } else {
            let headers = self.csv_basic.headers().map_err(Error::csv)?;
            Ok(record.deserialize(Some(headers)).map_err(Error::csv)?)
        }
    }
}

/// A builder for opening or creating an `Index`.
#[derive(Debug)]
pub struct IndexBuilder {
    ngram_type: NgramType,
    ngram_size: usize,
}

impl IndexBuilder {
    /// Create a new builder with a default configuration.
    pub fn new() -> IndexBuilder {
        IndexBuilder {
            ngram_type: NgramType::default(),
            ngram_size: 3,
        }
    }

    /// Use the current configuration to open an existing index. If the index
    /// does not exist, or if there was a problem opening it, then this returns
    /// an error.
    ///
    /// Generally, this method is cheap to call. It opens some file
    /// descriptors, but otherwise does no work.
    ///
    /// `data_dir` should be the directory containing decompressed IMDb tsv`
    /// `files. See: https://www.imdb.com/interfaces/
    ///
    /// `index_dir` should be the directory containing a previously created
    /// index using `Index::create`.
    ///
    /// Note that settings for index creation are ignored.
    pub fn open<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        let data_dir = data_dir.as_ref();
        let index_dir = index_dir.as_ref();
        debug!("opening index {}", index_dir.display());

        let config_file = open_file(index_dir.join(CONFIG))?;
        let config: Config = serde_json::from_reader(config_file)
            .map_err(|e| Error::config(e.to_string()))?;
        if config.version != VERSION {
            return Err(Error::version(VERSION, config.version));
        }

        Ok(Index {
            data_dir: data_dir.to_path_buf(),
            index_dir: index_dir.to_path_buf(),
            // We claim it is safe to open the following memory map because we
            // don't mutate them and no other process (should) either.
            csv_basic: unsafe { csv_mmap(data_dir.join(IMDB_BASICS))? },
            idx_names: names::IndexReader::open(index_dir)?,
            idx_aka: aka::Index::open(data_dir, index_dir)?,
            idx_episode: episode::Index::open(index_dir)?,
            idx_rating: rating::Index::open(index_dir)?,
            idx_title: id::IndexReader::from_path(index_dir.join(TITLE))?,
        })
    }

    /// Use the current configuration to create a new index.
    ///
    /// Calling this method is expensive, and one should expect this to take
    /// dozens of seconds or more to complete.
    ///
    /// `data_dir` should be the directory containing decompressed IMDb tsv`
    /// `files. See: https://www.imdb.com/interfaces/
    ///
    /// `index_dir` should be the directory containing a previously created
    /// index using `Index::create`.
    ///
    /// This will overwrite any previous index that may have existed in
    /// `index_dir`.
    pub fn create<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        let data_dir = data_dir.as_ref();
        let index_dir = index_dir.as_ref();
        fs::create_dir_all(index_dir)
            .with_context(|_| ErrorKind::path(index_dir))?;
        info!("creating index at {}", index_dir.display());

        // Creating the rating and episode indices are completely independent
        // from the name/AKA indexes, so do them in a background thread. The
        // episode index takes long enough to build to justify this.
        let job = {
            let data_dir = data_dir.to_path_buf();
            let index_dir = index_dir.to_path_buf();
            thread::spawn(move || -> Result<()> {
                let start = Instant::now();
                rating::Index::create(&data_dir, &index_dir)?;
                info!("created rating index (took {})",
                      NiceDuration::since(start));

                let start = Instant::now();
                episode::Index::create(&data_dir, &index_dir)?;
                info!("created episode index (took {})",
                      NiceDuration::since(start));
                Ok(())
            })
        };

        let start = Instant::now();
        let mut aka_index = aka::Index::create(data_dir, index_dir)?;
        info!("created AKA index (took {})", NiceDuration::since(start));

        let start = Instant::now();
        create_name_index(
            &mut aka_index,
            data_dir,
            index_dir,
            self.ngram_type,
            self.ngram_size,
        )?;
        info!("created name index, ngram type: {}, ngram size: {} (took {})",
              self.ngram_type, self.ngram_size, NiceDuration::since(start));

        job.join().unwrap()?;

        // Write out our config.
        let config_file = create_file(index_dir.join(CONFIG))?;
        serde_json::to_writer_pretty(config_file, &Config {
            version: VERSION,
        }).map_err(|e| Error::config(e.to_string()))?;

        self.open(data_dir, index_dir)
    }

    /// Set the type of ngram generation to use.
    ///
    /// The default type is `Window`.
    pub fn ngram_type(&mut self, ngram_type: NgramType) -> &mut IndexBuilder {
        self.ngram_type = ngram_type;
        self
    }

    /// Set the ngram size on this index.
    ///
    /// When creating an index, ngrams with this size will be used.
    pub fn ngram_size(&mut self, ngram_size: usize) -> &mut IndexBuilder {
        self.ngram_size = ngram_size;
        self
    }
}

impl Default for IndexBuilder {
    fn default() -> IndexBuilder {
        IndexBuilder::new()
    }
}

/// Creates the name index from the title tsv data and an AKA index. The AKA
/// index is used to index additional names for each title record to improve
/// recall during search.
///
/// To avoid a second pass through the title records, this also creates the
/// title ID index, which provides an index for looking up a `Title` by its
/// ID in constant time.
fn create_name_index(
    aka_index: &mut aka::Index,
    data_dir: &Path,
    index_dir: &Path,
    ngram_type: NgramType,
    ngram_size: usize,
) -> Result<()> {
    // For logging.
    let (mut count, mut title_count) = (0u64, 0u64);

    let mut wtr = names::IndexWriter::open(index_dir, ngram_type, ngram_size)?;
    let mut twtr = id::IndexSortedWriter::from_path(index_dir.join(TITLE))?;

    let mut rdr = csv_file(data_dir.join(IMDB_BASICS))?;
    let mut record = csv::StringRecord::new();
    while rdr.read_record(&mut record).map_err(Error::csv)? {
        let pos = record.position().expect("position on row");
        let id = &record[0];
        let title = &record[2];
        let original_title = &record[3];
        count += 1;
        title_count += 1;

        twtr.insert(id.as_bytes(), pos.byte())?;
        // Index the primary name.
        wtr.insert(pos.byte(), title)?;
        if title != original_title {
            // Index the "original" name.
            wtr.insert(pos.byte(), original_title)?;
            count += 1;
        }
        // Now index all of the alternate names, if they exist.
        for result in aka_index.find(id.as_bytes())? {
            let akarecord = result?;
            if title != akarecord.title {
                wtr.insert(pos.byte(), &akarecord.title)?;
                count += 1;
            }
        }
    }
    wtr.finish()?;
    twtr.finish()?;

    info!("{} titles indexed", title_count);
    info!("{} total names indexed", count);
    Ok(())
}
