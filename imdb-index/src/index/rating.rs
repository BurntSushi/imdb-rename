use std::path::Path;

use byteorder::{ByteOrder, BE};
use fst::{self, IntoStreamer, Streamer};

use crate::error::{Error, Result};
use crate::record::Rating;
use crate::util::{IMDB_RATINGS, csv_file, fst_set_builder_file, fst_set_file};

/// The name of the ratings index file.
///
/// The ratings index maps IMDb title ID to their average rating and number of
/// votes. The index is itself an FST set, where all keys begin with the IMDb
/// title ID, and also contain the average rating and number votes. Thus, a
/// lookup is accomplished via a range query on the title ID without needing
/// to consult the original CSV data.
const RATINGS: &str = "ratings.fst";

/// An index for ratings, which supports looking up ratings/votes for IMDb
/// titles efficiently.
#[derive(Debug)]
pub struct Index {
    idx: fst::Set,
}

impl Index {
    /// Open a rating index from the given index directory.
    pub fn open<P: AsRef<Path>>(index_dir: P) -> Result<Index> {
        Ok(Index {
            // We claim it is safe to open the following memory map because we
            // don't mutate them and no other process (should) either.
            idx: unsafe { fst_set_file(index_dir.as_ref().join(RATINGS))? },
        })
    }

    /// Create a rating index from the given IMDb data directory, and write it
    /// to the given index directory. If a rating index already exists, then it
    /// is overwritten.
    pub fn create<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        let data_dir = data_dir.as_ref();
        let index_dir = index_dir.as_ref();

        let mut buf = vec![];
        let mut count = 0u64;
        let mut idx = fst_set_builder_file(index_dir.join(RATINGS))?;
        let mut rdr = csv_file(data_dir.join(IMDB_RATINGS))?;
        for result in rdr.deserialize() {
            let record: Rating = result.map_err(Error::csv)?;

            buf.clear();
            write_rating(&record, &mut buf)?;
            idx.insert(&buf).map_err(Error::fst)?;
            count += 1;
        }
        idx.finish().map_err(Error::fst)?;

        log::info!("{} ratings indexed", count);
        Index::open(index_dir)
    }

    /// Return the rating information (which includes the actual rating and
    /// the number of votes associated with that rating) for the given IMDb
    /// identifier. If no rating information exists for the given ID, then
    /// `None` is returned.
    pub fn rating(&self, id: &[u8]) -> Result<Option<Rating>> {
        let mut upper = id.to_vec();
        upper.push(0xFF);

        let mut stream = self.idx.range().ge(id).le(upper).into_stream();
        while let Some(rating_bytes) = stream.next() {
            return Ok(Some(read_rating(rating_bytes)?));
        }
        Ok(None)
    }
}

fn read_rating(bytes: &[u8]) -> Result<Rating> {
    let nul = match bytes.iter().position(|&b| b == 0) {
        Some(nul) => nul,
        None => bug!("could not find nul byte"),
    };
    let id = match String::from_utf8(bytes[..nul].to_vec()) {
        Err(err) => bug!("rating id invalid UTF-8: {}", err),
        Ok(tvshow_id) => tvshow_id,
    };

    let i = nul + 1;
    Ok(Rating {
        id: id,
        rating: BE::read_f32(&bytes[i..]),
        votes: BE::read_u32(&bytes[i+4..]),
    })
}

fn write_rating(rat: &Rating, buf: &mut Vec<u8>) -> Result<()> {
    if rat.id.as_bytes().iter().any(|&b| b == 0) {
        bug!("unsupported rating id (with NUL byte) for {:?}", rat);
    }

    buf.extend_from_slice(rat.id.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&f32_to_bytes(rat.rating));
    buf.extend_from_slice(&u32_to_bytes(rat.votes));
    Ok(())
}

fn u32_to_bytes(n: u32) -> [u8; 4] {
    let mut buf = [0u8; 4];
    BE::write_u32(&mut buf, n);
    buf
}

fn f32_to_bytes(n: f32) -> [u8; 4] {
    let mut buf = [0u8; 4];
    BE::write_f32(&mut buf, n);
    buf
}

#[cfg(test)]
mod tests {
    use crate::index::tests::TestContext;
    use super::Index;

    #[test]
    fn basics() {
        let ctx = TestContext::new("small");
        let idx = Index::create(ctx.data_dir(), ctx.index_dir()).unwrap();

        let rat = idx.rating(b"tt0000001").unwrap().unwrap();
        assert_eq!(rat.rating, 5.8);
        assert_eq!(rat.votes, 1356);

        assert!(idx.rating(b"tt9999999").unwrap().is_none());
    }
}
