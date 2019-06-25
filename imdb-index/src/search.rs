use std::cmp;
use std::f64;
use std::fmt;
use std::result;
use std::str::FromStr;

use csv;
use failure::Fail;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strsim;

use crate::error::{Error, Result};
use crate::index::{MediaEntity, Index, NameQuery, NameScorer};
use crate::record::{Episode, Rating, Title, TitleKind};
use crate::scored::{Scored, SearchResults};
use crate::util::{IMDB_BASICS, csv_file};

/// A handle that permits searching IMDb media records with relevance ranking.
///
/// A searcher is constructed by providing it a handle to an IMDb
/// [`Index`](struct.Index.html). The `Index` is responsible for managing the
/// lower level data access, while the `Searcher` provides high level routines
/// for ranking results.
///
/// The primary interface to a `Searcher` is its `search` method, which takes
/// as input a [`Query`](struct.Query.html) and returns a ranked list of
/// [`MediaEntity`](struct.MediaEntity.html) as output.
#[derive(Debug)]
pub struct Searcher {
    idx: Index,
}

impl Searcher {
    /// Create a new searcher for the given `Index`.
    ///
    /// A single searcher can be used to execute many queries.
    ///
    /// An existing `Index` can be opened with `Index::open`, and a new `Index`
    /// can be created with `Index::create`.
    pub fn new(idx: Index) -> Searcher {
        Searcher { idx }
    }

    /// Execute a search with the given `Query`.
    ///
    /// Generally, the results returned are ranked in relevance order, where
    /// each result has a score associated with it. The score is between
    /// `0` and `1.0` (inclusive), where a score of `1.0` means "most similar"
    /// and a score of `0` means "least similar."
    ///
    /// Depending on the query, the behavior of search can vary:
    ///
    /// * When the query specifies a similarity function, then the results are
    ///   ranked by that function.
    /// * When the query contains a name to search by and a name scorer, then
    ///   results are ranked by the name scorer. If the query specifies a
    ///   similarity function, then results are first ranked by the name
    ///   scorer, and then re-ranked by the similarity function.
    /// * When no name or no name scorer are specified by the query, then
    ///   this search will do a (slow) exhaustive search over all media records
    ///   in IMDb. As a special case, if the query contains a TV show ID, then
    ///   only records in that TV show are searched, and this is generally
    ///   fast.
    /// * If the query is empty, then no results are returned.
    ///
    /// If there was a problem reading the underlying index or the IMDb data,
    /// then an error is returned.
    pub fn search(
        &mut self,
        query: &Query,
    ) -> Result<SearchResults<MediaEntity>> {
        if query.is_empty() {
            return Ok(SearchResults::new());
        }
        let mut results = match query.name_query() {
            None => self.search_exhaustive(query)?,
            Some(nameq) => self.search_with_name(query, &nameq)?,
        };
        results.trim(query.size);
        results.normalize();
        Ok(results)
    }

    /// Return a mutable reference to the underlying index for this searcher.
    pub fn index(&mut self) -> &mut Index {
        &mut self.idx
    }

    fn search_with_name(
        &mut self,
        query: &Query,
        name_query: &NameQuery,
    ) -> Result<SearchResults<MediaEntity>> {
        let mut results = SearchResults::new();
        for r in self.idx.search(name_query)? {
            if query.similarity.is_none() && results.len() >= query.size {
                break;
            }
            let (score, title) = r.into_pair();
            let entity = self.idx.entity_from_title(title)?;
            if query.matches(&entity) {
                results.push(Scored::new(entity).with_score(score));
            }
        }
        if !query.similarity.is_none() {
            results.rescore(|e| self.similarity(query, &e.title().title));
        }
        Ok(results)
    }

    fn search_exhaustive(
        &mut self,
        query: &Query,
    ) -> Result<SearchResults<MediaEntity>> {
        if let Some(ref tvshow_id) = query.tvshow_id {
            return self.search_with_tvshow(query, tvshow_id);
        }

        let mut rdr = csv_file(self.idx.data_dir().join(IMDB_BASICS))?;
        if !query.has_filters() {
            let mut nresults = SearchResults::new();
            let mut record = csv::StringRecord::new();
            while rdr.read_record(&mut record).map_err(Error::csv)? {
                let id_title = (record[0].to_string(), record[2].to_string());
                nresults.push(Scored::new(id_title));
            }
            nresults.rescore(|t| self.similarity(query, &t.1));

            let mut results = SearchResults::new();
            for nresult in nresults.into_vec().into_iter().take(query.size) {
                let (score, (id, _)) = nresult.into_pair();
                let entity = match self.idx.entity(&id)? {
                    None => continue,
                    Some(entity) => entity,
                };
                results.push(Scored::new(entity).with_score(score));
            }
            Ok(results)
        } else if query.needs_only_title() {
            let mut tresults = SearchResults::new();
            for result in rdr.deserialize() {
                let title: Title = result.map_err(Error::csv)?;
                if query.matches_title(&title) {
                    tresults.push(Scored::new(title));
                }
            }
            tresults.rescore(|t| self.similarity(query, &t.title));

            let mut results = SearchResults::new();
            for tresult in tresults.into_vec().into_iter().take(query.size) {
                let (score, title) = tresult.into_pair();
                let entity = self.idx.entity_from_title(title)?;
                results.push(Scored::new(entity).with_score(score));
            }
            Ok(results)
        } else {
            let mut results = SearchResults::new();
            for result in rdr.deserialize() {
                let title = result.map_err(Error::csv)?;
                let entity = self.idx.entity_from_title(title)?;
                if query.matches(&entity) {
                    results.push(Scored::new(entity));
                }
            }
            results.rescore(|e| self.similarity(query, &e.title().title));
            Ok(results)
        }
    }

    fn search_with_tvshow(
        &mut self,
        query: &Query,
        tvshow_id: &str,
    ) -> Result<SearchResults<MediaEntity>> {
        let mut results = SearchResults::new();
        for ep in self.idx.seasons(tvshow_id)? {
            let entity = match self.idx.entity(&ep.id)? {
                None => continue,
                Some(entity) => entity,
            };
            if query.matches(&entity) {
                results.push(Scored::new(entity));
            }
        }
        if !query.similarity.is_none() {
            results.rescore(|e| self.similarity(query, &e.title().title));
        }
        Ok(results)
    }

    fn similarity(&self, query: &Query, name: &str) -> f64 {
        match query.name {
            None => 0.0,
            Some(ref qname) => query.similarity.similarity(qname, name),
        }
    }
}

/// A query that can be used to search IMDb media records.
///
/// A query typically consists of a fuzzy name query along with zero or more
/// filters. If a query lacks a fuzzy name query, then this will generally
/// result in an exhaustive search of all IMDb media records, which can be
/// slow.
///
/// Filters are matched conjunctively. That is, a search result must satisfy
/// every filter on a query to match.
///
/// Empty queries always return no results.
///
/// The `Serialize` and `Deserialize` implementations for this type use the
/// free-form query syntax.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Query {
    name: Option<String>,
    name_scorer: Option<NameScorer>,
    similarity: Similarity,
    size: usize,
    kinds: Vec<TitleKind>,
    year: Range<u32>,
    votes: Range<u32>,
    season: Range<u32>,
    episode: Range<u32>,
    tvshow_id: Option<String>,
}

impl Default for Query {
    fn default() -> Query {
        Query::new()
    }
}

impl Query {
    /// Create a new empty query.
    pub fn new() -> Query {
        Query {
            name: None,
            name_scorer: Some(NameScorer::default()),
            similarity: Similarity::default(),
            size: 30,
            kinds: vec![],
            year: Range::none(),
            votes: Range::none(),
            season: Range::none(),
            episode: Range::none(),
            tvshow_id: None,
        }
    }

    /// Return true if and only if this query is empty.
    ///
    /// Searching with an empty query always yields no results.
    pub fn is_empty(&self) -> bool {
        self.name.as_ref().map_or(true, |n| n.is_empty())
        && self.kinds.is_empty()
        && self.year.is_none()
        && self.votes.is_none()
        && self.season.is_none()
        && self.episode.is_none()
        && self.tvshow_id.is_none()
    }

    /// Set the name to query by.
    ///
    /// The name given here is normalized and broken down into components
    /// automatically to facilitate fuzzy searching.
    ///
    /// Note that if no name is provided in a query, then it is possible that
    /// searching with the query will require exhaustively looking at every
    /// record in IMDb. This will be slower.
    pub fn name(mut self, name: &str) -> Query {
        self.name = Some(name.to_string());
        self
    }

    /// Set the scorer to use for name searches.
    ///
    /// The name scorer is used to rank results from searching the IMDb name
    /// index. If no name query is given, then this scorer is not used.
    ///
    /// If `None` is provided here, then the name index will not be used. This
    /// will likely cause an exhaustive search of all IMDb records, which can
    /// be slow. The use case for providing a name query without a name scorer
    /// is if you, for example, wanted to rank all of the records in IMDb
    /// by the Levenshtein distance between your query and every other record
    /// in IMDb. Normally, when the name index is used, only the (small number)
    /// of results returned by searching the name are ranked. Typically, these
    /// sorts of queries are useful for evaluation purposes, but not much else.
    pub fn name_scorer(mut self, scorer: Option<NameScorer>) -> Query {
        self.name_scorer = scorer;
        self
    }

    /// Set the similarity function.
    ///
    /// The similarity function can be selected from a predefined set of
    /// choices defined by the
    /// [`Similarity`](enum.Similarity.html) type.
    ///
    /// When a similarity function is used, then any results from searching
    /// the name index are re-ranked according to their similarity with the
    /// query.
    ///
    /// By default, no similarity function is used.
    pub fn similarity(mut self, sim: Similarity) -> Query {
        self.similarity = sim;
        self
    }

    /// Set the maximum number of results to be returned by a search.
    ///
    /// Note that setting this number too high (e.g., `> 10,000`) can impact
    /// performance. This is a normal restriction found in most information
    /// retrieval systems. That is, deep paging through result sets is
    /// expensive.
    pub fn size(mut self, size: usize) -> Query {
        self.size = size;
        self
    }

    /// Add a title kind to filter by.
    ///
    /// Multiple title kinds can be added to query, and search results must
    /// match at least one of them.
    ///
    /// Note that it is not possible to remove title kinds from an existing
    /// query. Instead, build a new query from scratch.
    pub fn kind(mut self, kind: TitleKind) -> Query {
        if !self.kinds.contains(&kind) {
            self.kinds.push(kind);
        }
        self
    }

    /// Set the lower inclusive bound on a title's year.
    ///
    /// This applies to either the title's start or end years.
    pub fn year_ge(mut self, year: u32) -> Query {
        self.year.start = Some(year);
        self
    }

    /// Set the upper inclusive bound on a title's year.
    ///
    /// This applies to either the title's start or end years.
    pub fn year_le(mut self, year: u32) -> Query {
        self.year.end = Some(year);
        self
    }

    /// Set the lower inclusive bound on a title's number of votes.
    pub fn votes_ge(mut self, votes: u32) -> Query {
        self.votes.start = Some(votes);
        self
    }

    /// Set the upper inclusive bound on a title's number of votes.
    pub fn votes_le(mut self, votes: u32) -> Query {
        self.votes.end = Some(votes);
        self
    }

    /// Set the lower inclusive bound on a title's season.
    ///
    /// This automatically limits all results to episodes.
    pub fn season_ge(mut self, season: u32) -> Query {
        self.season.start = Some(season);
        self
    }

    /// Set the upper inclusive bound on a title's season.
    ///
    /// This automatically limits all results to episodes.
    pub fn season_le(mut self, season: u32) -> Query {
        self.season.end = Some(season);
        self
    }

    /// Set the lower inclusive bound on a title's episode number.
    ///
    /// This automatically limits all results to episodes.
    pub fn episode_ge(mut self, episode: u32) -> Query {
        self.episode.start = Some(episode);
        self
    }

    /// Set the upper inclusive bound on a title's episode number.
    ///
    /// This automatically limits all results to episodes.
    pub fn episode_le(mut self, episode: u32) -> Query {
        self.episode.end = Some(episode);
        self
    }

    /// Restrict results to episodes belonging to the TV show given by its
    /// IMDb ID.
    ///
    /// This automatically limits all results to episodes.
    pub fn tvshow_id(mut self, tvshow_id: &str) -> Query {
        self.tvshow_id = Some(tvshow_id.to_string());
        self
    }

    /// Returns true if and only if the given entity matches this query.
    ///
    /// Note that this only applies filters in this query. e.g., The name
    /// aspect of the query, if one exists, is ignored.
    fn matches(&self, ent: &MediaEntity) -> bool {
        self.matches_title(&ent.title())
        && self.matches_rating(ent.rating())
        && self.matches_episode(ent.episode())
    }

    /// Returns true if and only if the given title matches this query.
    ///
    /// This ignores non-title filters.
    fn matches_title(&self, title: &Title) -> bool {
        if !self.kinds.is_empty() && !self.kinds.contains(&title.kind) {
            return false;
        }
        if !self.year.contains(title.start_year.as_ref())
            && !self.year.contains(title.end_year.as_ref())
        {
            return false;
        }
        true
    }

    /// Returns true if and only if the given rating matches this query.
    ///
    /// This ignores non-rating filters.
    ///
    /// If a rating filter is present and `None` is given, then this always
    /// returns `false`.
    fn matches_rating(&self, rating: Option<&Rating>) -> bool {
        if !self.votes.contains(rating.map(|r| &r.votes)) {
            return false;
        }
        true
    }

    /// Returns true if and only if the given episode matches this query.
    ///
    /// This ignores non-episode filters.
    ///
    /// If an episode filter is present and `None` is given, then this always
    /// returns `false`.
    fn matches_episode(&self, ep: Option<&Episode>) -> bool {
        if !self.season.contains(ep.and_then(|e| e.season.as_ref())) {
            return false;
        }
        if !self.episode.contains(ep.and_then(|e| e.episode.as_ref())) {
            return false;
        }
        if let Some(ref tvshow_id) = self.tvshow_id {
            if ep.map_or(true, |e| tvshow_id != &e.tvshow_id) {
                return false;
            }
        }
        true
    }

    /// Build a name query suitable for this query.
    ///
    /// The name query returned may request many more results than the result
    /// size maximum on this query.
    fn name_query(&self) -> Option<NameQuery> {
        let name = match self.name.as_ref() {
            None => return None,
            Some(name) => &**name,
        };
        let scorer = match self.name_scorer {
            None => return None,
            Some(scorer) => scorer,
        };
        // We want our name query to return a healthy set of results, even if
        // it's well beyond the result set size requested by the user. This is
        // primarily because a name search doesn't incorporate filters itself,
        // which simplifies the implementation. Therefore, we need to request
        // more results than what we need in case our filter is aggressive.
        let size = cmp::max(1000, self.size);
        Some(NameQuery::new(name).with_size(size).with_scorer(scorer))
    }

    /// Returns true if and only if this query has any filters.
    ///
    /// When a query lacks filters, then the result set can be completely
    /// determined by searching the name index and applying a similarity
    /// function, if present. This can make exhaustive searches, particularly
    /// the ones used during an evaluation, a bit faster.
    fn has_filters(&self) -> bool {
        self.needs_rating()
        || self.needs_episode()
        || !self.kinds.is_empty()
        || !self.year.is_none()
    }

    /// Returns true if and only this query has only title filters.
    ///
    /// When true, this can make exhaustive searches faster by avoiding the
    /// need to fetch the rating and/or episode for every title in IMDb.
    fn needs_only_title(&self) -> bool {
        !self.needs_rating() && !self.needs_episode()
    }

    /// Returns true if and only if this query has a rating filter.
    fn needs_rating(&self) -> bool {
        !self.votes.is_none()
    }

    /// Returns true if and only if this query has an episode filter.
    fn needs_episode(&self) -> bool {
        !self.season.is_none()
        || !self.episode.is_none()
        || !self.tvshow_id.is_none()
    }
}

impl Serialize for Query {
    fn serialize<S>(&self, s: S) -> result::Result<S::Ok, S::Error>
    where S: Serializer
    {
        s.serialize_str(&self.to_string())
    }
}

impl<'a> Deserialize<'a> for Query {
    fn deserialize<D>(d: D) -> result::Result<Query, D::Error>
    where D: Deserializer<'a>
    {
        use serde::de::Error;

        let querystr = String::deserialize(d)?;
        querystr.parse().map_err(|e: self::Error| {
            D::Error::custom(e.to_string())
        })
    }
}

impl FromStr for Query {
    type Err = Error;

    fn from_str(qstr: &str) -> Result<Query> {
        lazy_static! {
            // The 'directive', 'terms' and 'space' groups are all mutually
            // exclusive. When 'directive' matches, we parse it using DIRECTIVE
            // in a subsequent step. When 'terms' matches, we add them to the
            // name query. Then 'space' matches, we ignore it.
            static ref PARTS: Regex = Regex::new(
                r"\{(?P<directive>[^}]+)\}|(?P<terms>[^{}\s]+)|(?P<space>\s+)"
            ).unwrap();

            // Parse a directive of the form '{name:val}' or '{kind}'.
            static ref DIRECTIVE: Regex = Regex::new(
                r"^(?:(?P<name>[^:]+):(?P<val>.+)|(?P<kind>.+))$"
            ).unwrap();
        }
        let mut terms = vec![];
        let mut q = Query::new();
        for caps in PARTS.captures_iter(qstr) {
            if caps.name("space").is_some() {
                continue;
            } else if let Some(m) = caps.name("terms") {
                terms.push(m.as_str().to_string());
                continue;
            }

            let dcaps = DIRECTIVE.captures(&caps["directive"]).unwrap();
            if let Some(m) = dcaps.name("kind") {
                q = q.kind(m.as_str().parse()?);
                continue;
            }

            let (name, val) = (dcaps["name"].trim(), dcaps["val"].trim());
            match name {
                "size" => { q.size = val.parse().map_err(Error::number)?; }
                "year" => { q.year = val.parse()?; }
                "votes" => { q.votes = val.parse()?; }
                "season" => { q.season = val.parse()?; }
                "episode" => { q.episode = val.parse()?; }
                "tvseries" | "tvshow" | "show" => {
                    q.tvshow_id = Some(val.to_string());
                }
                "sim" | "similarity" => {
                    q.similarity = val.parse()?;
                }
                "scorer" => {
                    if val == "none" {
                        q.name_scorer = None;
                    } else {
                        q.name_scorer = Some(val.parse()?);
                    }
                }
                unk => return Err(Error::unknown_directive(unk)),
            }
        }
        if !terms.is_empty() {
            q = q.name(&terms.join(" "));
        }
        Ok(q)
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.name_scorer {
            None => f.write_str("{scorer:none}")?,
            Some(ref scorer) => write!(f, "{{scorer:{}}}", scorer)?,
        }
        write!(f, " {{sim:{}}}", self.similarity)?;
        write!(f, " {{size:{}}}", self.size)?;

        let mut kinds: Vec<&TitleKind> = self.kinds.iter().collect();
        kinds.sort();
        for kind in kinds {
            write!(f, " {{{}}}", kind)?;
        }
        if !self.year.is_none() {
            write!(f, " {{year:{}}}", self.year)?;
        }
        if !self.votes.is_none() {
            write!(f, " {{votes:{}}}", self.votes)?;
        }
        if !self.season.is_none() {
            write!(f, " {{season:{}}}", self.season)?;
        }
        if !self.episode.is_none() {
            write!(f, " {{episode:{}}}", self.episode)?;
        }
        if let Some(ref tvshow_id) = self.tvshow_id {
            write!(f, " {{show:{}}}", tvshow_id)?;
        }
        if let Some(ref name) = self.name {
            write!(f, " {}", name)?;
        }
        Ok(())
    }
}

/// A ranking function to use when searching IMDb records.
///
/// A similarity ranking function computes a score between `0.0` and `1.0` (not
/// including `0` but including `1.0`) for a query and a candidate result. The
/// score is determined by the corresponding names for a query and a candidate,
/// and a higher score indicates more similarity.
///
/// This ranking function can be used to increase the precision of a set
/// of results. In particular, when a similarity function is provided to
/// a [`Query`](struct.Query.html), then any results returned by querying
/// the IMDb name index will be rescored according to this function. If no
/// similarity function is provided, then the results will be ranked according
/// to scores produced by the name index.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Similarity {
    /// Do not use a similarity function.
    None,
    /// Computes the Levenshtein edit distance between two names and converts
    /// it to a similarity.
    Levenshtein,
    /// Computes the Jaro edit distance between two names and converts it to a
    /// similarity.
    Jaro,
    /// Computes the Jaro-Winkler edit distance between two names and converts
    /// it to a similarity.
    JaroWinkler,
}

impl Similarity {
    /// Returns a list of s trings representing the possible similarity
    /// function names.
    pub fn possible_names() -> &'static [&'static str] {
        &["none", "levenshtein", "jaro", "jarowinkler"]
    }

    /// Returns true if and only if no similarity function was selected.
    pub fn is_none(&self) -> bool {
        *self == Similarity::None
    }

    /// Computes the similarity between the given strings according to the
    /// underlying similarity function. If no similarity function is present,
    /// then this always returns `1.0`.
    ///
    /// The returned value is always in the range `(0, 1]`.
    pub fn similarity(&self, q1: &str, q2: &str) -> f64 {
        let sim = match *self {
            Similarity::None => 1.0,
            Similarity::Levenshtein => {
                let distance = strsim::levenshtein(q1, q2) as f64;
                // We do a simple conversion of distance to similarity. This
                // will produce very low scores even for very similar names,
                // but callers may normalize scores.
                //
                // We also add `1` to the denominator to avoid division by
                // zero. Incidentally, this causes the similarity of identical
                // strings to be exactly 1.0, which is what we want.
                1.0 / (1.0 + distance)
            }
            Similarity::Jaro => strsim::jaro(q1, q2),
            Similarity::JaroWinkler => strsim::jaro_winkler(q1, q2),
        };
        // Don't permit a score to actually be zero. This prevents division
        // by zero during normalization if all results have a score of zero.
        if sim < f64::EPSILON {
            f64::EPSILON
        } else {
            sim
        }
    }
}

impl Default for Similarity {
    fn default() -> Similarity {
        Similarity::None
    }
}

impl fmt::Display for Similarity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Similarity::None => write!(f, "none"),
            Similarity::Levenshtein => write!(f, "levenshtein"),
            Similarity::Jaro => write!(f, "jaro"),
            Similarity::JaroWinkler => write!(f, "jarowinkler"),
        }
    }
}

impl FromStr for Similarity {
    type Err = Error;

    fn from_str(s: &str) -> Result<Similarity> {
        match s {
            "none" => Ok(Similarity::None),
            "levenshtein" => Ok(Similarity::Levenshtein),
            "jaro" => Ok(Similarity::Jaro),
            "jarowinkler" | "jaro-winkler" => Ok(Similarity::JaroWinkler),
            unk => Err(Error::unknown_sim(unk)),
        }
    }
}

/// A range filter over any partially ordered type `T`.
///
/// This type permits either end of the range to be unbounded.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Range<T> {
    start: Option<T>,
    end: Option<T>,
}

impl<T> Range<T> {
    pub fn none() -> Range<T> {
        Range { start: None, end: None }
    }

    pub fn is_none(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }
}

impl<T: PartialOrd> Range<T> {
    pub fn contains(&self, t: Option<&T>) -> bool {
        let t = match t {
            None => return self.is_none(),
            Some(t) => t,
        };
        match (&self.start, &self.end) {
            (&None, &None) => true,
            (&Some(ref s), &None) => s <= t,
            (&None, &Some(ref e)) => t <= e,
            (&Some(ref s), &Some(ref e)) => s <= t && t <= e,
        }
    }
}

impl<T: fmt::Display + PartialEq> fmt::Display for Range<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (&self.start, &self.end) {
            (&None, &None) => write!(f, "-"),
            (&Some(ref s), &None) => write!(f, "{}-", s),
            (&None, &Some(ref e)) => write!(f, "-{}", e),
            (&Some(ref s), &Some(ref e)) if s == e => write!(f, "{}", s),
            (&Some(ref s), &Some(ref e)) => write!(f, "{}-{}", s, e),
        }
    }
}

impl<E: Fail, T: FromStr<Err=E>> FromStr for Range<T> {
    type Err = Error;

    fn from_str(range: &str) -> Result<Range<T>> {
        // One wonders what happens if we need to support ranges consisting
        // of negative numbers. Thankfully, it seems we needn't do that for
        // the IMDb data.
        let (start, end) = match range.find('-') {
            None => {
                // For no particular reason, parse it twice so that we don't
                // need a `Clone` bound.
                let start = range.parse().map_err(Error::number)?;
                let end = range.parse().map_err(Error::number)?;
                return Ok(Range { start: Some(start), end: Some(end) });
            }
            Some(i) => {
                let (start, end) = range.split_at(i);
                (start.trim(), end[1..].trim())
            }
        };
        Ok(match (start.is_empty(), end.is_empty()) {
            (true, true) => Range::none(),
            (true, false) => {
                Range {
                    start: None,
                    end: Some(end.parse().map_err(Error::number)?),
                }
            }
            (false, true) => {
                Range {
                    start: Some(start.parse().map_err(Error::number)?),
                    end: None,
                }
            }
            (false, false) => {
                Range {
                    start: Some(start.parse().map_err(Error::number)?),
                    end: Some(end.parse().map_err(Error::number)?),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn ranges() {
        let r: Range<u32> = "5-10".parse().unwrap();
        assert_eq!(r, Range { start: Some(5), end: Some(10) });

        let r: Range<u32> = "5-".parse().unwrap();
        assert_eq!(r, Range { start: Some(5), end: None });

        let r: Range<u32> = "-10".parse().unwrap();
        assert_eq!(r, Range { start: None, end: Some(10) });

        let r: Range<u32> = "5-5".parse().unwrap();
        assert_eq!(r, Range { start: Some(5), end: Some(5) });

        let r: Range<u32> = "5".parse().unwrap();
        assert_eq!(r, Range { start: Some(5), end: Some(5) });
    }

    #[test]
    fn query_parser() {
        let q: Query = "foo bar baz".parse().unwrap();
        assert_eq!(q, Query::new().name("foo bar baz"));

        let q: Query = "{movie}".parse().unwrap();
        assert_eq!(q, Query::new().kind(TitleKind::Movie));

        let q: Query = "{movie} {tvshow}".parse().unwrap();
        assert_eq!(q, Query::new()
            .kind(TitleKind::Movie).kind(TitleKind::TVSeries));

        let q: Query = "{movie}{tvshow}".parse().unwrap();
        assert_eq!(q, Query::new()
            .kind(TitleKind::Movie).kind(TitleKind::TVSeries));

        let q: Query = "foo {movie} bar {tvshow} baz".parse().unwrap();
        assert_eq!(q, Query::new()
            .name("foo bar baz")
            .kind(TitleKind::Movie)
            .kind(TitleKind::TVSeries));

        let q: Query = "{size:5}".parse().unwrap();
        assert_eq!(q, Query::new().size(5));

        let q: Query = "{ size : 5 }".parse().unwrap();
        assert_eq!(q, Query::new().size(5));

        let q: Query = "{year:1990}".parse().unwrap();
        assert_eq!(q, Query::new().year_ge(1990).year_le(1990));

        let q: Query = "{year:1990-}".parse().unwrap();
        assert_eq!(q, Query::new().year_ge(1990));

        let q: Query = "{year:-1990}".parse().unwrap();
        assert_eq!(q, Query::new().year_le(1990));

        let q: Query = "{year:-}".parse().unwrap();
        assert_eq!(q, Query::new());
    }

    #[test]
    fn query_parser_error() {
        assert!("{blah}".parse::<Query>().is_err());
        assert!("{size:a}".parse::<Query>().is_err());
        assert!("{year:}".parse::<Query>().is_err());
    }

    #[test]
    fn query_parser_weird() {
        let q: Query = "{movie".parse().unwrap();
        assert_eq!(q, Query::new().name("movie"));

        let q: Query = "movie}".parse().unwrap();
        assert_eq!(q, Query::new().name("movie"));
    }

    #[test]
    fn query_display() {
        let q = Query::new()
            .name("foo bar baz")
            .size(31)
            .season_ge(4).season_le(5)
            .kind(TitleKind::TVSeries)
            .kind(TitleKind::Movie)
            .similarity(Similarity::Jaro);
        let expected =
            "{scorer:okapibm25} {sim:jaro} {size:31} {movie} {tvSeries} {season:4-5} foo bar baz";
        assert_eq!(q.to_string(), expected);
    }

    #[test]
    fn query_serialize() {
        #[derive(Serialize)]
        struct Test {
            query: Query,
        }
        let query = Query::new()
            .name("foo bar baz")
            .name_scorer(None)
            .size(31)
            .season_ge(4).season_le(4);
        let got = serde_json::to_string(&Test { query }).unwrap();

        let expected = r#"{"query":"{scorer:none} {sim:none} {size:31} {season:4} foo bar baz"}"#;
        assert_eq!(got, expected);
    }

    #[test]
    fn query_deserialize() {
        let json = r#"{"query": "foo {size:30} bar {season:4} baz {show}"}"#;
        let expected =
            "{size:30} {season:4} {show} foo bar baz".parse().unwrap();

        #[derive(Deserialize)]
        struct Test {
            query: Query,
        }
        let got: Test = serde_json::from_str(json).unwrap();
        assert_eq!(got.query, expected);
    }
}
