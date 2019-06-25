use std::io;
use std::iter;
use std::path::Path;

use csv;
use memmap::Mmap;

use crate::error::{Error, Result};
use crate::index::{csv_file, csv_mmap, id};
use crate::record::AKA;
use crate::util::IMDB_AKAS;

/// A name of the AKA record index file.
///
/// This index represents a map from IMDb title id to a 64-bit integer. The
/// 64-bit integer encodes two pieces of information: the number of alternate
/// names for the title (high 16 bits) and the file offset at which the records
/// appear in title.akas.tsv (low 48 bits).
const AKAS: &str = "akas.fst";

/// A handle to the AKA name index.
///
/// The AKA index maps IMDb identifiers to a list of AKA records.
///
/// This index assumes that the underlying AKA CSV file is sorted by IMDb ID.
#[derive(Debug)]
pub struct Index {
    akas: csv::Reader<io::Cursor<Mmap>>,
    idx: id::IndexReader,
}

impl Index {
    /// Open an AKA index using the corresponding data and index directories.
    /// The data directory contains the IMDb data set while the index directory
    /// contains the index data files.
    pub fn open<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        Ok(Index {
            // We claim it is safe to open the following memory map because we
            // don't mutate them and no other process (should) either.
            akas: unsafe { csv_mmap(data_dir.as_ref().join(IMDB_AKAS))? },
            idx: id::IndexReader::from_path(index_dir.as_ref().join(AKAS))?,
        })
    }

    /// Create an AKA index by reading the AKA data from the given data
    /// directory and writing the index to the corresponding index directory.
    pub fn create<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        let data_dir = data_dir.as_ref();
        let index_dir = index_dir.as_ref();

        let rdr = csv_file(data_dir.join(IMDB_AKAS))?;
        let mut wtr = id::IndexSortedWriter::from_path(index_dir.join(AKAS))?;
        let mut count = 0u64;
        for result in AKAIndexRecords::new(rdr) {
            let record = result?;
            wtr.insert(&record.id, (record.count << 48) | record.offset)?;
            count += record.count;
        }
        wtr.finish()?;

        info!("{} alternate names indexed", count);
        Index::open(data_dir, index_dir)
    }

    /// Return a (possibly empty) iterator over all AKA records for the given
    /// IMDb ID.
    pub fn find(&mut self, id: &[u8]) -> Result<AKARecordIter> {
        match self.idx.get(id) {
            None => Ok(AKARecordIter(None)),
            Some(v) => {
                let count = (v >> 48) as usize;
                let offset = v & ((1 << 48) - 1);

                let mut pos = csv::Position::new();
                pos.set_byte(offset);
                self.akas.seek(pos).map_err(Error::csv)?;

                Ok(AKARecordIter(Some(self.akas.deserialize().take(count))))
            }
        }
    }
}

/// An iterator over AKA records for a single IMDb title.
///
/// This iterator is constructed via the `aka::Index::find` method.
///
/// This iterator may yield no titles.
///
/// The lifetime `'r` refers to the lifetime of the underlying AKA index
/// reader.
pub struct AKARecordIter<'r>(
    Option<iter::Take<
        csv::DeserializeRecordsIter<'r, io::Cursor<Mmap>, AKA>,
    >>
);

impl<'r> Iterator for AKARecordIter<'r> {
    type Item = Result<AKA>;

    fn next(&mut self) -> Option<Result<AKA>> {
        let next = match self.0.as_mut().and_then(|it| it.next()) {
            None => return None,
            Some(next) => next,
        };
        match next {
            Ok(next) => Some(Ok(next)),
            Err(err) => Some(Err(Error::csv(err))),
        }
    }
}

/// An indexable AKA record.
///
/// Each indexable record represents a group of alternative titles in the
/// title.akas.tsv file.
#[derive(Clone, Debug, Eq, PartialEq)]
struct AKAIndexRecord {
    id: Vec<u8>,
    offset: u64,
    count: u64,
}

/// A streaming iterator over indexable AKA records.
///
/// Each indexable record is a triple, and consists of an IMDb title ID,
/// the number of alternate titles for that title, and the file offset in the
/// CSV file at which those records begin.
///
/// The `R` type parameter refers to the underlying `io::Read` type of the
/// CSV reader.
#[derive(Debug)]
struct AKAIndexRecords<R> {
    /// The underlying CSV reader.
    rdr: csv::Reader<R>,
    /// Scratch space for storing the byte record.
    record: csv::ByteRecord,
    /// Set to true when the iterator has been exhausted.
    done: bool,
}

impl<R: io::Read> AKAIndexRecords<R> {
    /// Create a new streaming iterator over indexable AKA records.
    fn new(rdr: csv::Reader<R>) -> AKAIndexRecords<R> {
        AKAIndexRecords {
            rdr: rdr,
            record: csv::ByteRecord::new(),
            done: false,
        }
    }
}

impl<R: io::Read> Iterator for AKAIndexRecords<R> {
    type Item = Result<AKAIndexRecord>;

    /// Advance to the next indexable record and return it. If no more
    /// records exist, return `None`.
    ///
    /// If there was a problem parsing or reading from the underlying CSV
    /// data, then an error is returned.
    fn next(&mut self) -> Option<Result<AKAIndexRecord>> {
        macro_rules! itry {
            ($e:expr) => {
                match $e {
                    Err(err) => return Some(Err(Error::csv(err))),
                    Ok(v) => v,
                }
            }
        }

        if self.done {
            return None;
        }
        // Only initialize the record if this is our first go at it.
        // Otherwise, previous call leaves next record in `AKAIndexRecord`.
        if self.record.is_empty() {
            if !itry!(self.rdr.read_byte_record(&mut self.record)) {
                return None;
            }
        }
        let mut irecord = AKAIndexRecord {
            id: self.record[0].to_vec(),
            offset: self.record.position().expect("position on row").byte(),
            count: 1,
        };
        while itry!(self.rdr.read_byte_record(&mut self.record)) {
            if irecord.id != &self.record[0] {
                break;
            }
            irecord.count += 1;
        }
        // If we've read the last record then we're done!
        if self.rdr.is_done() {
            self.done = true;
        }
        Some(Ok(irecord))
    }
}

#[cfg(test)]
mod tests {
    use crate::util::csv_reader_builder;
    use super::*;

    #[test]
    fn aka_index_records1() {
        let data =
r"titleId	ordering	title	region	language	types	attributes	isOriginalTitle
tt0117019	1	Hommes à l'huile	FR	\N	\N	\N	0
tt0117019	2	Männer in Öl	DE	\N	\N	\N	0
tt0117019	3	Men in Oil	XEU	en	festival	\N	0
tt0117019	4	Männer in Öl: Annäherungsversuche an die Malerin Susanne Hay	\N	\N	original	\N	1
tt0117019	5	Men in Oil	XWW	en	\N	\N	0
tt0117020	1	Mendigos sin fronteras	ES	\N	\N	\N	0
tt0117021	1	Menno's Mind	US	\N	\N	\N	0
tt0117021	2	Menno's Mind	\N	\N	original	\N	1
tt0117021	3	The Matrix 2	RU	\N	video	\N	0
tt0117021	4	Virtuális elme	HU	\N	imdbDisplay	\N	0
tt0117021	5	Power.com	US	\N	video	\N	0
tt0117021	6	La mente de Menno	ES	\N	\N	\N	0
tt0117021	7	Power.com	CA	en	video	\N	0
tt0117021	8	Terror im Computer	DE	\N	\N	\N	0
tt0117022	1	Menopause Song	CA	\N	\N	\N	0
tt0117023	1	Les menteurs	FR	\N	\N	\N	0";
        let rdr = csv_reader_builder().from_reader(data.as_bytes());
        let records: Vec<AKAIndexRecord> = AKAIndexRecords::new(rdr)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 5);

        assert_eq!(records[0].id, b"tt0117019");
        assert_eq!(records[0].count, 5);

        assert_eq!(records[1].id, b"tt0117020");
        assert_eq!(records[1].count, 1);

        assert_eq!(records[2].id, b"tt0117021");
        assert_eq!(records[2].count, 8);

        assert_eq!(records[3].id, b"tt0117022");
        assert_eq!(records[3].count, 1);

        assert_eq!(records[4].id, b"tt0117023");
        assert_eq!(records[4].count, 1);
    }

    #[test]
    fn aka_index_records2() {
        let data =
r"titleId	ordering	title	region	language	types	attributes	isOriginalTitle
tt0117019	1	Hommes à l'huile	FR	\N	\N	\N	0
tt0117019	2	Männer in Öl	DE	\N	\N	\N	0
tt0117019	3	Men in Oil	XEU	en	festival	\N	0
tt0117019	4	Männer in Öl: Annäherungsversuche an die Malerin Susanne Hay	\N	\N	original	\N	1
tt0117019	5	Men in Oil	XWW	en	\N	\N	0
tt0117020	1	Mendigos sin fronteras	ES	\N	\N	\N	0
tt0117021	1	Menno's Mind	US	\N	\N	\N	0
tt0117021	2	Menno's Mind	\N	\N	original	\N	1
tt0117021	3	The Matrix 2	RU	\N	video	\N	0
tt0117021	4	Virtuális elme	HU	\N	imdbDisplay	\N	0
tt0117021	5	Power.com	US	\N	video	\N	0
tt0117021	6	La mente de Menno	ES	\N	\N	\N	0
tt0117021	7	Power.com	CA	en	video	\N	0
tt0117021	8	Terror im Computer	DE	\N	\N	\N	0";
        let rdr = csv_reader_builder().from_reader(data.as_bytes());
        let records: Vec<AKAIndexRecord> = AKAIndexRecords::new(rdr)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 3);

        assert_eq!(records[0].id, b"tt0117019");
        assert_eq!(records[0].count, 5);

        assert_eq!(records[1].id, b"tt0117020");
        assert_eq!(records[1].count, 1);

        assert_eq!(records[2].id, b"tt0117021");
        assert_eq!(records[2].count, 8);
    }

    #[test]
    fn aka_index_records3() {
        let data =
r"titleId	ordering	title	region	language	types	attributes	isOriginalTitle
tt0117021	1	Menno's Mind	US	\N	\N	\N	0
tt0117021	2	Menno's Mind	\N	\N	original	\N	1
tt0117021	3	The Matrix 2	RU	\N	video	\N	0
tt0117021	4	Virtuális elme	HU	\N	imdbDisplay	\N	0
tt0117021	5	Power.com	US	\N	video	\N	0
tt0117021	6	La mente de Menno	ES	\N	\N	\N	0
tt0117021	7	Power.com	CA	en	video	\N	0
tt0117021	8	Terror im Computer	DE	\N	\N	\N	0";
        let rdr = csv_reader_builder().from_reader(data.as_bytes());
        let records: Vec<AKAIndexRecord> = AKAIndexRecords::new(rdr)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1);

        assert_eq!(records[0].id, b"tt0117021");
        assert_eq!(records[0].count, 8);
    }

    #[test]
    fn aka_index_records4() {
        let data =
r"titleId	ordering	title	region	language	types	attributes	isOriginalTitle
tt0117021	1	Menno's Mind	US	\N	\N	\N	0";
        let rdr = csv_reader_builder().from_reader(data.as_bytes());
        let records: Vec<AKAIndexRecord> = AKAIndexRecords::new(rdr)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1);

        assert_eq!(records[0].id, b"tt0117021");
        assert_eq!(records[0].count, 1);
    }
}
