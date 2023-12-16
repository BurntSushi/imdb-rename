/*!
This crate provides an on-disk indexing data structure for searching IMDb.
Searching is primarily done using information retrieval techniques, which
support fuzzy name queries and using TF-IDF-like ranking functions.
*/

#![deny(missing_docs)]

pub use crate::error::{Error, ErrorKind, Result};
pub use crate::index::{
    AKARecordIter, Index, IndexBuilder, MediaEntity, NameQuery, NameScorer,
    NgramType,
};
pub use crate::record::{Episode, Rating, Title, TitleKind, AKA};
pub use crate::scored::{Scored, SearchResults};
pub use crate::search::{Query, Searcher, Similarity};

// A macro that creates an error that represents a bug.
//
// This is typically used when reading index structures from disk. Since the
// data on disk is generally outside our control, we return an error using this
// macro instead of panicking (or worse, silently misinterpreting data).
macro_rules! bug {
    ($($tt:tt)*) => {{
        return Err($crate::error::Error::bug(format!($($tt)*)));
    }}
}

mod error;
mod index;
mod record;
mod scored;
mod search;
mod util;
