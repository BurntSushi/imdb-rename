use std::cmp;
use std::fmt;
use std::str::FromStr;

use csv;
use serde::{Deserialize, Deserializer};

use crate::error::Error;

/// An IMDb title record.
///
/// This is the primary type of an IMDb media entry. This record defines the
/// identifier of an IMDb title, which serves as a foreign key in other data
/// files (such as alternate names, episodes and ratings).
#[derive(Clone, Debug, Deserialize)]
pub struct Title {
    /// An IMDb identifier.
    ///
    /// Generally, this is a fixed width string beginning with the characters
    /// `tt`.
    #[serde(rename = "tconst")]
    pub id: String,
    /// The specific type of a title, e.g., movie, TV show, episode, etc.
    #[serde(rename = "titleType")]
    pub kind: TitleKind,
    /// The primary name of this title.
    #[serde(rename = "primaryTitle")]
    pub title: String,
    /// The "original" name of this title.
    #[serde(rename = "originalTitle")]
    pub original_title: String,
    /// Whether this title is classified as "adult" material or not.
    #[serde(rename = "isAdult", deserialize_with = "number_as_bool")]
    pub is_adult: bool,
    /// The start year of this title.
    ///
    /// Generally, things like movies or TV episodes have a start year to
    /// indicate their release year and no end year. TV shows also have a start
    /// year. TV shows that are still airing lack an end time, but TV shows
    /// that have stopped will typically have an end year indicating when it
    /// stopped airing.
    ///
    /// Note that not all titles have a start year.
    #[serde(rename = "startYear", deserialize_with = "csv::invalid_option")]
    pub start_year: Option<u32>,
    /// The end year of this title.
    ///
    /// This is typically used to indicate the ending year of a TV show that
    /// has stopped production.
    #[serde(rename = "endYear", deserialize_with = "csv::invalid_option")]
    pub end_year: Option<u32>,
    /// The runtime, in minutes, of this title.
    #[serde(
        rename = "runtimeMinutes",
        deserialize_with = "csv::invalid_option",
    )]
    pub runtime_minutes: Option<u32>,
    /// A comma separated string of genres.
    #[serde(rename = "genres")]
    pub genres: String,
}

/// The kind of a title. These form a partioning of all titles, where every
/// title has exactly one kind.
///
/// This type has a `FromStr` implementation that permits parsing a string
/// containing a title kind into this type. Note that parsing a title kind
/// recognizes all forms present in the IMDb data, and also addition common
/// sense forms. For example, `tvshow` and `tvSeries` are both accepted as
/// terms for the `TVSeries` variant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[allow(missing_docs)]
pub enum TitleKind {
    #[serde(rename = "movie")]
    Movie,
    #[serde(rename = "short")]
    Short,
    #[serde(rename = "tvEpisode")]
    TVEpisode,
    #[serde(rename = "tvMiniSeries")]
    TVMiniSeries,
    #[serde(rename = "tvMovie")]
    TVMovie,
    #[serde(rename = "tvSeries")]
    TVSeries,
    #[serde(rename = "tvShort")]
    TVShort,
    #[serde(rename = "tvSpecial")]
    TVSpecial,
    #[serde(rename = "video")]
    Video,
    #[serde(rename = "videoGame")]
    VideoGame,
}

impl TitleKind {
    /// Return a string representation of this title kind.
    ///
    /// This string representation is intended to be the same string
    /// representation used in the IMDb data files.
    pub fn as_str(&self) -> &'static str {
        use self::TitleKind::*;
        match *self {
            Movie => "movie",
            Short => "short",
            TVEpisode => "tvEpisode",
            TVMiniSeries => "tvMiniSeries",
            TVMovie => "tvMovie",
            TVSeries => "tvSeries",
            TVShort => "tvShort",
            TVSpecial => "tvSpecial",
            Video => "video",
            VideoGame => "videoGame",
        }
    }

    /// Returns true if and only if this kind represents a TV series.
    pub fn is_tv_series(&self) -> bool {
        use self::TitleKind::*;

        match *self {
            TVMiniSeries | TVSeries => true,
            _ => false,
        }
    }
}

impl fmt::Display for TitleKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Ord for TitleKind {
    fn cmp(&self, other: &TitleKind) -> cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for TitleKind {
    fn partial_cmp(&self, other: &TitleKind) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for TitleKind {
    type Err = Error;

    fn from_str(ty: &str) -> Result<TitleKind, Error> {
        use self::TitleKind::*;

        match &*ty.to_lowercase() {
            "movie" => Ok(Movie),
            "short" => Ok(Short),
            "tvepisode" | "episode" => Ok(TVEpisode),
            "tvminiseries" | "miniseries" => Ok(TVMiniSeries),
            "tvmovie" => Ok(TVMovie),
            "tvseries" | "tvshow" | "show" => Ok(TVSeries),
            "tvshort" => Ok(TVShort),
            "tvspecial" | "special" => Ok(TVSpecial),
            "video" => Ok(Video),
            "videogame" | "game" => Ok(VideoGame),
            unk => Err(Error::unknown_title(unk)),
        }
    }
}

/// A single alternate name.
///
/// Every title has one or more names, and zero or more alternate names. To
/// represent multiple names, AKA or "also known as" records are provided.
/// There may be many AKA records for a single title.
#[derive(Clone, Debug, Deserialize)]
pub struct AKA {
    /// The IMDb identifier that these AKA records describe.
    #[serde(rename = "titleId")]
    pub id:  String,
    /// The order in which an AKA record should be preferred.
    #[serde(rename = "ordering")]
    pub order: i32,
    /// The alternate name.
    #[serde(rename = "title")]
    pub title: String,
    /// A geographic region in which this alternate name applies.
    #[serde(rename = "region")]
    pub region: String,
    /// The language of this alternate name.
    #[serde(rename = "language")]
    pub language: String,
    /// A comma separated list of types for this name.
    #[serde(rename = "types")]
    pub types: String,
    /// A comma separated list of attributes for this name.
    #[serde(rename = "attributes")]
    pub attributes: String,
    /// A flag indicating whether this corresponds to the original title or
    /// not.
    #[serde(
        rename = "isOriginalTitle",
        deserialize_with = "optional_number_as_bool",
    )]
    pub is_original_title: Option<bool>,
}

/// A single episode record.
///
/// An episode record is an entry that joins two title records together, and
/// provides episode specific information, such as the season and episode
/// number. The two title records joined correspond to the title record for the
/// TV show and the title record for the episode.
#[derive(Clone, Debug, Deserialize)]
pub struct Episode {
    /// The IMDb title identifier for this episode.
    #[serde(rename = "tconst")]
    pub id: String,
    /// The IMDb title identifier for the parent TV show of this episode.
    #[serde(rename = "parentTconst")]
    pub tvshow_id: String,
    /// The season in which this episode is contained, if it exists.
    #[serde(
        rename = "seasonNumber",
        deserialize_with = "csv::invalid_option",
    )]
    pub season: Option<u32>,
    /// The episode number of the season in which this episode is contained, if
    /// it exists.
    #[serde(
        rename = "episodeNumber",
        deserialize_with = "csv::invalid_option",
    )]
    pub episode: Option<u32>,
}

/// A rating associated with a single title record.
#[derive(Clone, Debug, Deserialize)]
pub struct Rating {
    /// The IMDb title identifier for this rating.
    #[serde(rename = "tconst")]
    pub id: String,
    /// The rating, on a scale of 0 to 10, for this title.
    #[serde(rename = "averageRating")]
    pub rating: f32,
    /// The number of votes involved in this rating.
    #[serde(rename = "numVotes")]
    pub votes: u32,
}

fn number_as_bool<'de, D>(de: D) -> Result<bool, D::Error>
where D: Deserializer<'de>
{
    i32::deserialize(de).map(|n| n != 0)
}

fn optional_number_as_bool<'de, D>(de: D) -> Result<Option<bool>, D::Error>
where D: Deserializer<'de>
{
    Ok(i32::deserialize(de).map(|n| Some(n != 0)).unwrap_or(None))
}
