use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use failure::{bail, format_err};
use imdb_index::{MediaEntity, Query, SearchResults, Searcher, TitleKind};
use lazy_static::lazy_static;
use regex::Regex;
use tinytemplate::TinyTemplate;

use crate::util::choose;
use crate::Result;

/// A proposal to rename a `src` file path to a `dst` file path.
#[derive(Clone, Debug)]
pub struct RenameProposal {
    src: PathBuf,
    dst: PathBuf,
    mkdir: bool,
    action: RenameAction,
}

/// The action to take when renaming a file.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RenameAction {
    /// This does a simple rename of the file.
    Rename,
    /// This creates a symlink to the given file.
    Symlink,
    /// This creates a hardlink to the given file.
    Hardlink,
}

impl fmt::Display for RenameAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RenameAction::Rename => "rename",
            RenameAction::Symlink => "symlink",
            RenameAction::Hardlink => "hardlink",
        }
        .fmt(f)
    }
}

impl RenameAction {
    fn is_link(&self) -> bool {
        match *self {
            RenameAction::Rename => false,
            RenameAction::Symlink | RenameAction::Hardlink => true,
        }
    }
}

impl RenameProposal {
    /// Create a new proposal with the given source and destination. The
    /// destination is constructed by joining `dst_parent` with `dst_name`.
    /// `dst_name` is sanitized to be safe as a file name.
    ///
    /// The given action determines whether to rename the source to the
    /// destination, create a symlink or create a hardlink.
    fn new(
        src: PathBuf,
        dst_parent: &Path,
        dst_name: &str,
        mkdir: bool,
        action: RenameAction,
    ) -> RenameProposal {
        lazy_static! {
            static ref RE_BAD_PATH_CHARS: Regex = Regex::new(r"[\x00/]",).unwrap();
        }
        let name = RE_BAD_PATH_CHARS.replace_all(dst_name, "_");

        RenameProposal {
            src,
            dst: dst_parent.join(&*name),
            mkdir,
            action,
        }
    }

    /// Execute this proposal according to `RenameAction`.
    pub fn rename(&self) -> Result<()> {
        if self.mkdir {
            if let Some(parent) = self.dst.parent() {
                fs::create_dir_all(parent)?;
            }
        }

        match self.action {
            RenameAction::Rename => {
                fs::rename(&self.src, &self.dst).map_err(|e| {
                    format_err!(
                        "error renaming '{}' to '{}': {}",
                        self.src.display(),
                        self.dst.display(),
                        e,
                    )
                })?;
            }
            #[cfg(not(unix))]
            RenameAction::Symlink => {
                bail!("symlinks are only supported for Unix")
            }
            #[cfg(unix)]
            RenameAction::Symlink => {
                use std::os::unix;

                unix::fs::symlink(&self.src, &self.dst).map_err(|e| {
                    format_err!(
                        "error symlinking '{}' to '{}': {}",
                        self.src.display(),
                        self.dst.display(),
                        e,
                    )
                })?;
            }
            RenameAction::Hardlink => {
                fs::hard_link(&self.src, &self.dst).map_err(|e| {
                    format_err!(
                        "error hardlinking '{}' to '{}': {}",
                        self.src.display(),
                        self.dst.display(),
                        e,
                    )
                })?;
            }
        }
        Ok(())
    }

    /// The `src` of this proposal.
    pub fn src(&self) -> &Path {
        &self.src
    }

    /// The `dst` of this proposal.
    ///
    /// Note that the destination is cleansed such that it is safe for
    /// renaming. e.g., If a `/` occurs in an IMDb title, then it is replaced
    /// with `_`.
    pub fn dst(&self) -> &Path {
        &self.dst
    }
}

/// A renamer generates file rename proposals based on IMDb.
///
/// Fundamentally, a renamer is an entity linker, which attempts to connect
/// file paths on your system that follow a prescribed pattern with canonical
/// entity entries in IMDb.
///
/// A renamer can be built via a `RenamerBuilder`, and proposals can be
/// generated via the `propose` method on `Renamer`. A `Renamer` itself never
/// touches the file system.
#[derive(Debug)]
pub struct Renamer {
    cache: Mutex<HashMap<Query, SearchResults<MediaEntity>>>,
    choose_cache: Mutex<HashMap<Query, MediaEntity>>,
    force: Option<MediaEntity>,
    min_votes: u32,
    good_threshold: f64,
    episode: Regex,
    season: Regex,
    year: Regex,
}

impl Renamer {
    /// Propose a set of renames, where each proposal proposes to rename a
    /// path in the slice given to a new path using its proper title according
    /// to IMDb. This never executes any changes to the file system.
    ///
    /// This returns an error if any two of the proposals recommend an exactly
    /// equivalent destination path. An error is also returned if a destination
    /// path already exists. Finally, the proposals are sorted in descending
    /// order of path length if any one of them is a directory, which should
    /// permit changing entries in a directory and a directory itself in one
    /// go.
    ///
    /// An optional destination can be given, which when present, is used as
    /// the directory in which renames/links are created. Similarly, the action
    /// given specifies whether the proposal should rename a file, symlink to
    /// it or hardlink to it.
    ///
    /// Note that this may log some types of errors to stderr but otherwise
    /// continue, which means that the set of proposals returned may not cover
    /// all paths given. Errors resulting from reading the index will cause an
    /// error to be returned.
    pub fn propose(
        &self,
        searcher: &mut Searcher,
        paths: &[PathBuf],
        dest: Option<PathBuf>,
        template: Option<&str>,
        mkdir: bool,
        action: RenameAction,
    ) -> Result<Vec<RenameProposal>> {
        let mut proposals = vec![];
        for path in paths {
            let result = self.propose_one(searcher, path, dest.as_deref(), template, mkdir, action);
            let proposal = match result {
                None => continue,
                Some(proposal) => proposal,
            };
            // If there's no change, then skip it.
            if proposal.src == proposal.dst {
                continue;
            }
            proposals.push(proposal);
        }

        // Check that we have no destination duplicates. If we permit them,
        // then it would be pretty easy to clobber the user's data. That's bad.
        //
        // We also make sure that the destination doesn't already exist. This
        // isn't atomic, but it's probably a fine approximation.
        let mut seen = HashSet::new();
        let mut any_dir = false;
        for p in &proposals {
            if seen.contains(&p.dst) {
                bail!("duplicate rename proposal for '{}'", p.dst.display());
            }
            seen.insert(p.dst.clone());
            if p.dst.exists() {
                bail!("file path '{}' already exists", p.dst.display());
            }
            any_dir = any_dir || p.src.is_dir();
        }
        // Finally, sort the proposals such that the longest ones come first.
        // This should cause child entries to get renamed before parent
        // entries.
        if any_dir {
            proposals.sort_by(|p1, p2| {
                let (p1, p2) = (p1.dst.as_os_str(), p2.dst.as_os_str());
                p1.len().cmp(&p2.len()).reverse()
            });
        }
        Ok(proposals)
    }

    /// Propose a single rename for the given path.
    ///
    /// If an error occurs while searching, or if searching yields no results,
    /// or if an unexpected condition was hit, then an error is logged to
    /// stderr and `None` is returned.
    fn propose_one(
        &self,
        searcher: &mut Searcher,
        path: &Path,
        dest: Option<&Path>,
        template: Option<&str>,
        mkdir: bool,
        action: RenameAction,
    ) -> Option<RenameProposal> {
        let candidate = match self.candidate(path) {
            Ok(candidate) => candidate,
            Err(err) => {
                eprintln!("[skipping] could not parse file path: {}", err);
                return None;
            }
        };
        let result = match candidate.kind {
            CandidateKind::Any(ref x) => self.find_any(searcher, x),
            CandidateKind::Episode(ref x) => self.find_episode(searcher, x),
            CandidateKind::Unknown => self.find_unknown(),
        };
        let ent = match result {
            Ok(ent) => ent,
            Err(err) => {
                eprintln!("[skipping] error searching for {}: {}", path.display(), err,);
                return None;
            }
        };

        // Setup our sources and destinations. They get tweaked depending on
        // what our rename action is and whether a destination directory was
        // explicitly given.
        let dest_path = match candidate.path.imdb_name(&ent, template) {
            Ok(name) => name,
            Err(err) => {
                eprintln!(
                    "[skipping] error rendering template for {}: {}",
                    path.display(),
                    err
                );
                return None;
            }
        };
        let mut src_path = path.to_path_buf();
        let mut dest_parent_dir = dest
            .map(|d| d.to_path_buf())
            .unwrap_or(candidate.path.parent);

        // A symlink was requested to be created in a destination presumably
        // different than the current directory. This means that the file
        // specified on the commandline will need to be an absolute path,
        // otherwise the symlink will not point to the correct place.
        if dest.is_some() && action == RenameAction::Symlink {
            src_path = match src_path.canonicalize() {
                Ok(src_path) => src_path,
                Err(err) => {
                    eprintln!(
                        "[skipping] error making {} an absolute path: {}",
                        src_path.display(),
                        err,
                    );
                    return None;
                }
            };
        }
        // A symlink or hardlink was requested to be created without a
        // destination specified. In this case, it only makes sense to place
        // the symlink in the current directory being executed from, otherwise
        // potentially relative file paths won't match up.
        if dest.is_none() && action.is_link() {
            dest_parent_dir = match std::env::current_dir() {
                Ok(cwd) => cwd,
                Err(err) => {
                    eprintln!("[skipping] error getting current directory: {}", err,);
                    return None;
                }
            };
        }

        if let Some(parent) = dest_path.parent() {
            dest_parent_dir.push(parent);
        }
        let dest_name = match dest_path.file_name().and_then(|os_str| os_str.to_str()) {
            Some(name) => name,
            None => {
                eprintln!(
                    "[skipping] destination describes a directory, not a file: {}",
                    dest_path.display()
                );
                return None;
            }
        };

        Some(RenameProposal::new(
            src_path,
            &dest_parent_dir,
            &dest_name,
            mkdir,
            action,
        ))
    }

    /// Search for any entity via its name and a year. In general, this is
    /// enough information to narrow down the results considerably for most
    /// movies.
    ///
    /// If an entity override is provided, then that is returned instead.
    fn find_any(&self, searcher: &mut Searcher, candidate: &CandidateAny) -> Result<MediaEntity> {
        // If we already have an entity override, then just use that to build
        // the proposal and skip any automatic searches.
        if let Some(ref ent) = self.force {
            return Ok(ent.clone());
        }

        // Otherwise, try to figure out the "right" name by constructing a
        // query from the candidate and searching IMDb.
        let query = self
            .name_query(&candidate.title)
            .year_ge(candidate.year)
            .year_le(candidate.year)
            // Basically include every kind except for episode and video games.
            // This helps filter out a lot of noise.
            .kind(TitleKind::Movie)
            .kind(TitleKind::Short)
            .kind(TitleKind::TVMiniSeries)
            .kind(TitleKind::TVMovie)
            .kind(TitleKind::TVSeries)
            .kind(TitleKind::TVShort)
            .kind(TitleKind::TVSpecial)
            .kind(TitleKind::Video)
            .votes_ge(self.min_votes);
        log::debug!("automatic 'any' query: {:?}", query);
        self.choose_one(searcher, &query)
    }

    /// Search for the episode entity corresponding to the episode information
    /// in the given candidate. If one couldn't be found, then an error is
    /// returned.
    ///
    /// This works by assuming the candidate episode's name is actually the
    /// TV show name. So we first look for the TV show entity, and then use
    /// that to find the corresponding episode.
    fn find_episode(
        &self,
        searcher: &mut Searcher,
        candidate: &CandidateEpisode,
    ) -> Result<MediaEntity> {
        let tvshow = self.find_tvshow_for_episode(searcher, candidate)?;
        let eps = searcher
            .index()
            .episodes(&tvshow.title().id, candidate.season)?;
        let ep = match eps
            .into_iter()
            .find(|ep| ep.episode == Some(candidate.episode))
        {
            Some(ep) => ep,
            None => bail!(
                "could not find S{:02}E{:02} for TV show {}",
                candidate.season,
                candidate.episode,
                tvshow.title().id,
            ),
        };
        match searcher.index().entity(&ep.id)? {
            Some(ent) => Ok(ent),
            None => bail!("could not find media entity for episode {}", ep.id),
        }
    }

    /// Search for the TV show entity corresponding to the episode information
    /// in the given candidate. If one couldn't be found, then an error is
    /// returned.
    ///
    /// If there is an entity override, then it is used instead. If the
    /// override isn't a TV show, then an error is returned.
    fn find_tvshow_for_episode(
        &self,
        searcher: &mut Searcher,
        candidate: &CandidateEpisode,
    ) -> Result<MediaEntity> {
        // If we already have an entity override, then just use that as the
        // TV show. If it isn't a TV show, then return an error.
        if let Some(ref ent) = self.force {
            if !ent.title().kind.is_tv_series() {
                bail!(
                    "expected TV show to rename episode, but found {}",
                    ent.title().kind
                );
            }
            return Ok(ent.clone());
        }

        // Otherwise, try to figure out the "right" TV show by constructing a
        // query from the candidate and searching IMDb.
        let query = self
            .name_query(&candidate.tvshow_title)
            .kind(TitleKind::TVMiniSeries)
            .kind(TitleKind::TVSeries)
            .votes_ge(self.min_votes);
        log::debug!("automatic 'tvshow for episode' query: {:?}", query);
        self.choose_one(searcher, &query)
    }

    /// Return an entity for a completely unknown candidate.
    ///
    /// This is invariant with respect to the source path, since we don't
    /// really know how to interpret it (and if we did, it shouldn't be
    /// unknown). Therefore, we always defer to the explicit override. If there
    /// is no override, then this returns an error.
    ///
    /// This is useful for renaming files like 'English.srt', where the path
    /// doesn't contain any useful information and an override is necessary
    /// anyway.
    fn find_unknown(&self) -> Result<MediaEntity> {
        match self.force {
            Some(ref ent) => Ok(ent.clone()),
            None => {
                bail!(
                    "could not parse file path and there is no override \
                       set via -q/--query"
                );
            }
        }
    }

    /// Produce a structured candidate for renaming from a source path.
    ///
    /// The candidate returned represents a heuristic analysis performed on
    /// the source path, and in particular, represents what we think the path
    /// represents. Principally, this consists of three categories: TV episode,
    /// any named title with a year, and then everything else. The type of
    /// candidate we have determines how we guess its canonical entry in IMDb.
    fn candidate(&self, path: &Path) -> Result<Candidate> {
        let cpath = CandidatePath::from_path(path)?;
        let name = cpath.base_name.clone();

        if let Some(cepisode) = self.episode_parts(&cpath)? {
            return Ok(Candidate {
                path: cpath,
                kind: CandidateKind::Episode(cepisode),
            });
        }

        let caps_year = match self.year.captures(&name) {
            None => {
                return Ok(Candidate {
                    path: cpath,
                    kind: CandidateKind::Unknown,
                })
            }
            Some(caps) => caps,
        };
        let mat_year = match caps_year.name("year") {
            None => bail!("missing 'year' group in: {}", self.year),
            Some(mat) => mat,
        };
        let year = mat_year.as_str().parse()?;
        let title = name[..mat_year.start()].to_string();
        Ok(Candidate {
            path: cpath,
            kind: CandidateKind::Any(CandidateAny { title, year }),
        })
    }

    /// Part episode information from the given candidate, if it exists.
    ///
    /// If a problem occurred (like detecting a match but missing an expected
    /// capture group name), then an error is returned. If no episode info
    /// could be found, then `None` is returned.
    fn episode_parts(&self, cpath: &CandidatePath) -> Result<Option<CandidateEpisode>> {
        let name = &cpath.base_name;
        let caps_season = match self.season.captures(name) {
            None => return Ok(None),
            Some(caps) => caps,
        };
        let caps_episode = match self.episode.captures(name) {
            None => return Ok(None),
            Some(caps) => caps,
        };
        let mat_season = match caps_season.name("season") {
            None => bail!("missing 'season' group in: {}", self.season),
            Some(mat) => mat,
        };
        let mat_episode = match caps_episode.name("episode") {
            None => bail!("missing 'episode' group in: {}", self.episode),
            Some(mat) => mat,
        };

        let title_end = caps_season.get(0).unwrap().start();
        Ok(Some(CandidateEpisode {
            tvshow_title: name[..title_end].to_string(),
            season: mat_season.as_str().parse()?,
            episode: mat_episode.as_str().parse()?,
        }))
    }

    /// Build a query and seed it with the given name, after sanitizing the
    /// name.
    fn name_query(&self, name: &str) -> Query {
        let name = name.replace('.', " ");
        let name = name.trim();
        log::debug!("automatic name query: {:?}", name);
        Query::new().name(name)
    }

    /// Execute a search against the given searcher with the given query and
    /// choose a single result from the search. If no obvious single result
    /// stands out, then prompt the user for an answer.
    ///
    /// If the given query has been executed before, then returned the cached
    /// answer.
    fn choose_one(&self, searcher: &mut Searcher, query: &Query) -> Result<MediaEntity> {
        let mut choose_cache = self.choose_cache.lock().unwrap();
        if let Some(ent) = choose_cache.get(query) {
            return Ok(ent.clone());
        }
        let results = self.search(searcher, query)?;
        let ent = choose(searcher, results.as_slice(), self.good_threshold)?;
        choose_cache.insert(query.clone(), ent.clone());
        Ok(ent)
    }

    /// Execute a search against the given searcher with the given query.
    ///
    /// If this exact query has been previously executed by this renamer, then
    /// a cache of results are returned.
    fn search(&self, searcher: &mut Searcher, query: &Query) -> Result<SearchResults<MediaEntity>> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(results) = cache.get(query) {
            return Ok(results.clone());
        }
        let results = searcher.search(query)?;
        cache.insert(query.clone(), results.clone());
        Ok(results)
    }
}

/// A candidate represents a source file path with additional structured
/// information that helps us guess what its corresponding canonical IMDb
/// entity is.
#[derive(Clone, Debug)]
struct Candidate {
    /// The original path that this candidate was drawn from. The path is
    /// split up into its parent, name and extension components.
    path: CandidatePath,
    /// The type of candidate, with potentially additional information
    /// depending on the type.
    kind: CandidateKind,
}

/// A representation of a source path that we'd like to rename.
///
/// It is split up into non-overlapping component pieces to make guessing
/// easier. In particular, the `parent` and `ext` fields generally aren't
/// involved in the guessing process, but are used for reassembling a final
/// proposed file path to rename to. In general, only the `base_name` is used
/// for guessing.
///
/// Note that it is not possible to split every possible path into these
/// component pieces. Generally, such paths aren't readily guessable, so they
/// are skipped (with an error message logged to stderr).
#[derive(Clone, Debug)]
struct CandidatePath {
    /// The parent component of the path. e.g., `/foo` in `/foo/bar.mkv`.
    parent: PathBuf,
    /// The base name of this path, minus the extention. e.g., `bar` in
    /// `/foo/bar.mkv`.
    base_name: String,
    /// The extension of this path, if it exists, minus the leading `.`.
    /// e.g., `mkv` in `/foo/bar.mkv`.
    ext: Option<String>,
}

/// Type of a candidate, including any additional type-specific information.
#[derive(Clone, Debug)]
enum CandidateKind {
    /// A general description of any candidate, with a minimal requirement:
    /// the source file path must contain a year.
    Any(CandidateAny),
    /// A description of a candidate that we believe to be an episode, which
    /// includes the TV show name, the season number and the episode number.
    Episode(CandidateEpisode),
    /// Anything else. Generally, these's nothing we can assume about this
    /// type, but if the user specifies an override, then we'll still be able
    /// to rename it. If no override is given, then a candidate with this type
    /// is skipped.
    Unknown,
}

/// A general description of any candidate with a name and a year. The name
/// is generally assumed to be all the text preceding the year in the base name
/// of a file path.
///
/// When we initiate a guess based on this candidate type, we assume it can
/// correspond to any entity in IMDb except for TV show episodes.
#[derive(Clone, Debug)]
struct CandidateAny {
    /// The presumed title.
    title: String,
    /// The presumed year.
    year: u32,
}

/// A description of a candidate that we believe to be an episode. This means
/// we have captured what we believe to be the TV show's name, along with the
/// season and episode numbers. The TV show's name is generally assumed to be
/// all the text preceding the season number in the base name of a file path.
#[derive(Clone, Debug)]
struct CandidateEpisode {
    /// The presumed TV show title.
    tvshow_title: String,
    /// The season number.
    season: u32,
    /// The episode number.
    episode: u32,
}

impl CandidatePath {
    /// Build a candidate path from a source file path. If a path could not
    /// be built, then an error is returned.
    fn from_path(path: &Path) -> Result<CandidatePath> {
        let parent = match path.parent() {
            None => bail!("{}: has no parent, cannot rename", path.display()),
            Some(parent) => parent.to_path_buf(),
        };
        let name_os = match path.file_name() {
            None => bail!("{}: missing file name", path.display()),
            Some(name_os) => name_os,
        };
        let name = match name_os.to_str() {
            None => bail!("{}: invalid UTF-8, cannot rename", path.display()),
            Some(name) => name,
        };
        let (base_name, ext) = if path.is_dir() {
            (name.to_string(), None)
        } else {
            match name.rfind('.') {
                None => (name.to_string(), None),
                Some(i) => (name[..i].to_string(), Some(name[i + 1..].to_string())),
            }
        };
        Ok(CandidatePath {
            parent,
            base_name,
            ext,
        })
    }

    /// Convert this candidate path to the desired name based on an IMDb
    /// entity. In general, this replaces the `base_name` of this candidate
    /// with the title found in the given entity.
    fn imdb_name(&self, ent: &MediaEntity, template: Option<&str>) -> Result<PathBuf> {
        let mut name = if let Some(template) = template {
            let mut tt = TinyTemplate::new();
            tt.add_template("template", template)?;
            tt.add_formatter("leading_zero", |value, output| match value {
                serde_json::Value::Number(num) => {
                    if num.is_i64() {
                        output.push_str(&format!("{:02}", num.as_i64().unwrap()));
                        Ok(())
                    } else {
                        tinytemplate::format(value, output)
                    }
                }
                _ => tinytemplate::format(value, output),
            });
            let res = tt.render("template", ent)?;
            let mut buf = PathBuf::new();
            buf.push(res);
            buf
        } else {
            match ent.episode() {
                Some(ep) => format!(
                    "S{:02}E{:02} - {}",
                    ep.season.unwrap_or(0),
                    ep.episode.unwrap_or(0),
                    ent.title().title,
                )
                .into(),
                None => match ent.title().start_year {
                    None => PathBuf::from(&ent.title().title),
                    Some(year) => format!("{} ({})", ent.title().title, year).into(),
                },
            }
        };

        match self.ext {
            None => Ok(name),
            Some(ref ext) => {
                name.set_extension(ext);
                Ok(name)
            }
        }
    }
}

/// A builder for configuring a renamer.
#[derive(Clone, Debug)]
pub struct RenamerBuilder {
    force: Option<MediaEntity>,
    min_votes: u32,
    good_threshold: f64,
    regex_episode: String,
    regex_season: String,
    regex_year: String,
}

impl RenamerBuilder {
    /// Create a `RenamerBuilder` with default settings.
    pub fn new() -> RenamerBuilder {
        RenamerBuilder {
            force: None,
            min_votes: 1000,
            good_threshold: 0.25,
            regex_episode: r"[Ee](?P<episode>[0-9]+)".into(),
            regex_season: r"[Ss](?P<season>[0-9]+)".into(),
            regex_year: r"\b(?P<year>[0-9]{4})\b".into(),
        }
    }

    /// Build a `Renamer` from the current configuration.
    pub fn build(&self) -> Result<Renamer> {
        Ok(Renamer {
            cache: Mutex::new(HashMap::new()),
            choose_cache: Mutex::new(HashMap::new()),
            force: self.force.clone(),
            min_votes: self.min_votes,
            good_threshold: self.good_threshold,
            episode: Regex::new(&self.regex_episode)?,
            season: Regex::new(&self.regex_season)?,
            year: Regex::new(&self.regex_year)?,
        })
    }

    /// Forcefully use the given entity when producing rename proposals.
    ///
    /// When an entity is given here, the renamer will never execute automatic
    /// queries based on the file name. Instead, it will rename every path
    /// given using this entity.
    ///
    /// If a path to be renamed is determined to be a TV episode, then this
    /// entity is assumed to be the entity corresponding to that episode's
    /// TV show. Otherwise, an error will be returned.
    pub fn force(&mut self, entity: MediaEntity) -> &mut RenamerBuilder {
        self.force = Some(entity);
        self
    }

    /// Set the minimum number of votes required for all search results from
    /// automatic queries. This is used when formulating queries based on file
    /// names that aren't TV episodes. The purpose of this is to heuristically
    /// filter out noise from the IMDb data.
    ///
    /// When this isn't specified, a non-zero default is used.
    pub fn min_votes(&mut self, min_votes: u32) -> &mut RenamerBuilder {
        self.min_votes = min_votes;
        self
    }

    /// Sets the "good" threshold for auto-selection.
    ///
    /// When running queries generated from file paths, it is often the case
    /// that multiple results will be returned. If the difference in score
    /// between the first result and second result is greater than or equal
    /// to this threshold, then the first result will be automatically chosen.
    /// Otherwise, a prompt will be shown to the end user requesting an
    /// explicit selection.
    pub fn good_threshold(&mut self, threshold: f64) -> &mut RenamerBuilder {
        self.good_threshold = threshold;
        self
    }

    /// Set the regex for detecting the episode number from a file path.
    ///
    /// Regexes are executed against the base name of a path. The episode
    /// number is extracted via the `episode` named capture group.
    pub fn regex_episode(&mut self, pattern: &str) -> &mut RenamerBuilder {
        self.regex_episode = pattern.to_string();
        self
    }

    /// Set the regex for detecting the season number from a file path.
    ///
    /// Regexes are executed against the base name of a path. The season
    /// number is extracted via the `season` named capture group.
    pub fn regex_season(&mut self, pattern: &str) -> &mut RenamerBuilder {
        self.regex_season = pattern.to_string();
        self
    }

    /// Set the regex for detecting the year from a file path.
    ///
    /// Regexes are executed against the base name of a path. The year is
    /// extracted via the `year` named capture group.
    pub fn regex_year(&mut self, pattern: &str) -> &mut RenamerBuilder {
        self.regex_year = pattern.to_string();
        self
    }
}

impl Default for RenamerBuilder {
    fn default() -> RenamerBuilder {
        RenamerBuilder::new()
    }
}
