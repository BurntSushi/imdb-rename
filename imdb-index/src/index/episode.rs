use std::cmp;
use std::convert::TryInto;
use std::path::Path;
use std::u32;

use fst::{self, IntoStreamer, Streamer};
use memmap::Mmap;

use crate::error::{Error, Result};
use crate::index::csv_file;
use crate::record::Episode;
use crate::util::{fst_set_builder_file, fst_set_file, IMDB_EPISODE};

/// The name of the episode index file.
///
/// The episode index maps TV show ids to episodes. The index is constructed
/// in a way where either of the following things can be used as look up keys:
///
///   tvshow IMDb title ID
///   (tvshow IMDb title ID, season number)
///
/// In particular, the index itself stores the entire episode record, and it
/// can be re-constituted without re-visiting the original episode data file.
const SEASONS: &str = "episode.seasons.fst";

/// The name of the TV show index file.
///
/// The TV show index maps episode IMDb title IDs to tvshow IMDb title IDs.
/// This allows us to quickly look up the TV show corresponding to an episode
/// in search results.
///
/// The format of this index is an FST set, where each key corresponds to the
/// episode ID joined with the TV show ID by a `NUL` byte. This lets us do
/// a range query on the set when given the episode ID to find the TV show ID.
const TVSHOWS: &str = "episode.tvshows.fst";

/// An episode index that supports retrieving season and episode information
/// quickly.
#[derive(Debug)]
pub struct Index {
    seasons: fst::Set<Mmap>,
    tvshows: fst::Set<Mmap>,
}

impl Index {
    /// Open an episode index from the given index directory.
    pub fn open<P: AsRef<Path>>(index_dir: P) -> Result<Index> {
        let index_dir = index_dir.as_ref();
        // We claim it is safe to open the following memory map because we
        // don't mutate them and no other process (should) either.
        let seasons = unsafe { fst_set_file(index_dir.join(SEASONS))? };
        let tvshows = unsafe { fst_set_file(index_dir.join(TVSHOWS))? };
        Ok(Index { seasons, tvshows })
    }

    /// Create an episode index from the given IMDb data directory and write
    /// it to the given index directory. If an episode index already exists,
    /// then it is overwritten.
    pub fn create<P1: AsRef<Path>, P2: AsRef<Path>>(
        data_dir: P1,
        index_dir: P2,
    ) -> Result<Index> {
        let data_dir = data_dir.as_ref();
        let index_dir = index_dir.as_ref();

        let mut buf = vec![];
        let mut seasons = fst_set_builder_file(index_dir.join(SEASONS))?;
        let mut tvshows = fst_set_builder_file(index_dir.join(TVSHOWS))?;

        let mut episodes = read_sorted_episodes(data_dir)?;
        for episode in &episodes {
            buf.clear();
            write_episode(episode, &mut buf)?;
            seasons.insert(&buf).map_err(Error::fst)?;
        }

        episodes.sort_by(|e1, e2| {
            (&e1.id, &e1.tvshow_id).cmp(&(&e2.id, &e2.tvshow_id))
        });
        for episode in &episodes {
            buf.clear();
            write_tvshow(&episode, &mut buf)?;
            tvshows.insert(&buf).map_err(Error::fst)?;
        }

        seasons.finish().map_err(Error::fst)?;
        tvshows.finish().map_err(Error::fst)?;

        log::info!("{} episodes indexed", episodes.len());
        Index::open(index_dir)
    }

    /// Return a sequence of episodes for the given TV show IMDb identifier.
    ///
    /// The episodes are sorted in order of season number and episode number.
    /// Episodes without season/episode numbers are sorted after episodes with
    /// numbers.
    pub fn seasons(&self, tvshow_id: &[u8]) -> Result<Vec<Episode>> {
        let mut upper = tvshow_id.to_vec();
        upper.push(0xFF);

        let mut episodes = vec![];
        let mut stream =
            self.seasons.range().ge(tvshow_id).le(upper).into_stream();
        while let Some(episode_bytes) = stream.next() {
            episodes.push(read_episode(episode_bytes)?);
        }
        Ok(episodes)
    }

    /// Return a sequence of episodes for the given TV show IMDb identifier and
    /// season number.
    ///
    /// The episodes are sorted in order of episode number. Episodes without
    /// episode numbers are sorted after episodes with numbers.
    pub fn episodes(
        &self,
        tvshow_id: &[u8],
        season: u32,
    ) -> Result<Vec<Episode>> {
        let mut lower = tvshow_id.to_vec();
        lower.push(0x00);
        lower.extend_from_slice(&season.to_be_bytes());
        lower.extend_from_slice(&0u32.to_be_bytes());

        let mut upper = tvshow_id.to_vec();
        upper.push(0x00);
        upper.extend_from_slice(&season.to_be_bytes());
        upper.extend_from_slice(&u32::MAX.to_be_bytes());

        let mut episodes = vec![];
        let mut stream =
            self.seasons.range().ge(lower).le(upper).into_stream();
        while let Some(episode_bytes) = stream.next() {
            episodes.push(read_episode(episode_bytes)?);
        }
        Ok(episodes)
    }

    /// Return the episode information for the given episode IMDb identifier.
    ///
    /// If no episode information for the given ID exists, then `None` is
    /// returned.
    pub fn episode(&self, episode_id: &[u8]) -> Result<Option<Episode>> {
        let mut upper = episode_id.to_vec();
        upper.push(0xFF);

        let mut stream =
            self.tvshows.range().ge(episode_id).le(upper).into_stream();
        while let Some(tvshow_bytes) = stream.next() {
            return Ok(Some(read_tvshow(tvshow_bytes)?));
        }
        Ok(None)
    }
}

fn read_sorted_episodes(data_dir: &Path) -> Result<Vec<Episode>> {
    // We claim it is safe to open the following memory map because we don't
    // mutate them and no other process (should) either.
    let mut rdr = csv_file(data_dir.join(IMDB_EPISODE))?;
    let mut records = vec![];
    for result in rdr.deserialize() {
        let record: Episode = result.map_err(Error::csv)?;
        records.push(record);
    }
    records.sort_by(cmp_episode);
    Ok(records)
}

fn cmp_episode(ep1: &Episode, ep2: &Episode) -> cmp::Ordering {
    let k1 = (
        &ep1.tvshow_id,
        ep1.season.unwrap_or(u32::MAX),
        ep1.episode.unwrap_or(u32::MAX),
        &ep1.id,
    );
    let k2 = (
        &ep2.tvshow_id,
        ep2.season.unwrap_or(u32::MAX),
        ep2.episode.unwrap_or(u32::MAX),
        &ep2.id,
    );
    k1.cmp(&k2)
}

fn read_episode(bytes: &[u8]) -> Result<Episode> {
    let nul = match bytes.iter().position(|&b| b == 0) {
        Some(nul) => nul,
        None => bug!("could not find nul byte"),
    };
    let tvshow_id = match String::from_utf8(bytes[..nul].to_vec()) {
        Err(err) => bug!("tvshow_id invalid UTF-8: {}", err),
        Ok(tvshow_id) => tvshow_id,
    };

    let mut i = nul + 1;
    let season = from_optional_u32("season", &bytes[i..])?;

    i += 4;
    let episode = from_optional_u32("episode number", &bytes[i..])?;

    i += 4;
    let id = match String::from_utf8(bytes[i..].to_vec()) {
        Err(err) => bug!("episode id invalid UTF-8: {}", err),
        Ok(id) => id,
    };
    Ok(Episode { id, tvshow_id, season, episode })
}

fn write_episode(ep: &Episode, buf: &mut Vec<u8>) -> Result<()> {
    if ep.tvshow_id.as_bytes().iter().any(|&b| b == 0) {
        bug!("unsupported tvshow id (with NUL byte) for {:?}", ep);
    }
    buf.extend_from_slice(ep.tvshow_id.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&to_optional_season(ep)?.to_be_bytes());
    buf.extend_from_slice(&to_optional_epnum(ep)?.to_be_bytes());
    buf.extend_from_slice(ep.id.as_bytes());
    Ok(())
}

fn read_tvshow(bytes: &[u8]) -> Result<Episode> {
    let nul = match bytes.iter().position(|&b| b == 0) {
        Some(nul) => nul,
        None => bug!("could not find nul byte"),
    };
    let id = match String::from_utf8(bytes[..nul].to_vec()) {
        Err(err) => bug!("episode id invalid UTF-8: {}", err),
        Ok(tvshow_id) => tvshow_id,
    };

    let mut i = nul + 1;
    let season = from_optional_u32("season", &bytes[i..])?;

    i += 4;
    let episode = from_optional_u32("episode number", &bytes[i..])?;

    i += 4;
    let tvshow_id = match String::from_utf8(bytes[i..].to_vec()) {
        Err(err) => bug!("tvshow_id invalid UTF-8: {}", err),
        Ok(tvshow_id) => tvshow_id,
    };
    Ok(Episode { id, tvshow_id, season, episode })
}

fn write_tvshow(ep: &Episode, buf: &mut Vec<u8>) -> Result<()> {
    if ep.id.as_bytes().iter().any(|&b| b == 0) {
        bug!("unsupported episode id (with NUL byte) for {:?}", ep);
    }

    buf.extend_from_slice(ep.id.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&to_optional_season(ep)?.to_be_bytes());
    buf.extend_from_slice(&to_optional_epnum(ep)?.to_be_bytes());
    buf.extend_from_slice(ep.tvshow_id.as_bytes());
    Ok(())
}

fn from_optional_u32(
    label: &'static str,
    bytes: &[u8],
) -> Result<Option<u32>> {
    if bytes.len() < 4 {
        bug!("not enough bytes to read optional {}", label);
    }
    Ok(match u32::from_be_bytes(bytes[..4].try_into().unwrap()) {
        u32::MAX => None,
        x => Some(x),
    })
}

fn to_optional_season(ep: &Episode) -> Result<u32> {
    match ep.season {
        None => Ok(u32::MAX),
        Some(x) => {
            if x == u32::MAX {
                bug!("unsupported season number {} for {:?}", x, ep);
            }
            Ok(x)
        }
    }
}

fn to_optional_epnum(ep: &Episode) -> Result<u32> {
    match ep.episode {
        None => Ok(u32::MAX),
        Some(x) => {
            if x == u32::MAX {
                bug!("unsupported episode number {} for {:?}", x, ep);
            }
            Ok(x)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Index;
    use crate::index::tests::TestContext;
    use std::collections::HashMap;

    #[test]
    fn basics() {
        let ctx = TestContext::new("small");
        let idx = Index::create(ctx.data_dir(), ctx.index_dir()).unwrap();
        let eps = idx.seasons(b"tt0096697").unwrap();

        let mut counts: HashMap<u32, u32> = HashMap::new();
        for ep in eps {
            *counts.entry(ep.season.unwrap()).or_insert(0) += 1;
        }
        assert_eq!(counts.len(), 3);
        assert_eq!(counts[&1], 13);
        assert_eq!(counts[&2], 22);
        assert_eq!(counts[&3], 24);
    }

    #[test]
    fn by_season() {
        let ctx = TestContext::new("small");
        let idx = Index::create(ctx.data_dir(), ctx.index_dir()).unwrap();
        let eps = idx.episodes(b"tt0096697", 2).unwrap();

        let mut counts: HashMap<u32, u32> = HashMap::new();
        for ep in eps {
            *counts.entry(ep.season.unwrap()).or_insert(0) += 1;
        }
        println!("{:?}", counts);
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[&2], 22);
    }

    #[test]
    fn tvshow() {
        let ctx = TestContext::new("small");
        let idx = Index::create(ctx.data_dir(), ctx.index_dir()).unwrap();
        let ep = idx.episode(b"tt0701063").unwrap().unwrap();
        assert_eq!(ep.tvshow_id, "tt0096697");
    }
}
