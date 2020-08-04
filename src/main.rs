use std::env;
use std::ffi::OsStr;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;

use imdb_index::{Index, IndexBuilder, NgramType, Searcher};
use lazy_static::lazy_static;
use tabwriter::TabWriter;
use walkdir::WalkDir;

use crate::rename::{RenameAction, RenamerBuilder};
use crate::util::{choose, read_yesno, write_tsv};

mod download;
mod logger;
mod rename;
mod util;

fn main() {
    if let Err(err) = try_main() {
        // A pipe error occurs when the consumer of this process's output has
        // hung up. This is a normal event, and we should quit gracefully.
        if is_pipe_error(&err) {
            process::exit(0);
        }
        eprintln!("{:?}", err);
        process::exit(1);
    }
}

fn try_main() -> anyhow::Result<()> {
    logger::init()?;
    log::set_max_level(log::LevelFilter::Info);

    let args = Args::from_matches(&app().get_matches())?;
    if args.debug {
        log::set_max_level(log::LevelFilter::Debug);
    }

    // Forcefully update the data and re-index if requested.
    if args.update_data {
        args.download_all_update()?;
        args.create_index()?;
        return Ok(());
    }
    // Ensure that the necessary data exists.
    if args.download_all()? || args.update_index {
        args.create_index()?;
        if args.update_index {
            return Ok(());
        }
    }
    // Now ensure that the index exists.
    if !args.index_dir.exists() {
        args.create_index()?;
    }

    let mut searcher = args.searcher()?;
    let results = match args.query {
        None => None,
        Some(ref query) => Some(searcher.search(&query.parse()?)?),
    };
    if args.files.is_empty() {
        let results = match results {
            None => anyhow::bail!("run with a file to rename or --query"),
            Some(ref results) => results,
        };
        return write_tsv(io::stdout(), &mut searcher, results.as_slice());
    }

    let mut builder = RenamerBuilder::new();
    builder
        .min_votes(args.min_votes)
        .good_threshold(0.25)
        .regex_episode(&args.regex_episode)
        .regex_season(&args.regex_season)
        .regex_year(&args.regex_year);
    if let Some(ref results) = results {
        builder.force(choose(&mut searcher, results.as_slice(), 0.25)?);
    }
    let renamer = builder.build()?;
    let proposals = renamer.propose(
        &mut searcher,
        &args.files,
        args.dest_dir,
        args.rename_action,
    )?;
    if proposals.is_empty() {
        anyhow::bail!("no files to rename");
    }

    let mut stdout = TabWriter::new(io::stdout());
    for p in &proposals {
        writeln!(stdout, "{}\t->\t{}", p.src().display(), p.dst().display())?;
    }
    stdout.flush()?;

    if read_yesno(&format!(
        "Are you sure you want to {action} the above files? (y/n) ",
        action = &args.rename_action
    ))? {
        for p in &proposals {
            if let Err(err) = p.rename() {
                eprintln!("{}", err);
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    data_dir: PathBuf,
    dest_dir: Option<PathBuf>,
    debug: bool,
    files: Vec<PathBuf>,
    index_dir: PathBuf,
    ngram_size: usize,
    ngram_type: NgramType,
    query: Option<String>,
    regex_episode: String,
    regex_season: String,
    regex_year: String,
    update_data: bool,
    update_index: bool,
    min_votes: u32,
    rename_action: RenameAction,
}

impl Args {
    fn from_matches(matches: &clap::ArgMatches) -> anyhow::Result<Args> {
        let files = collect_paths(
            matches
                .values_of_os("file")
                .map(|it| it.collect())
                .unwrap_or(vec![]),
            matches.is_present("follow"),
        );
        let query = matches.value_of_lossy("query").map(|q| q.into_owned());
        let data_dir =
            matches.value_of_os("data-dir").map(PathBuf::from).unwrap();
        let dest_dir = matches.value_of_os("dest-dir").map(PathBuf::from);
        let index_dir = matches
            .value_of_os("index-dir")
            .map(PathBuf::from)
            .unwrap_or(data_dir.join("index"));
        let regex_episode =
            matches.value_of_lossy("re-episode").unwrap().into_owned();
        let regex_season =
            matches.value_of_lossy("re-season").unwrap().into_owned();
        let regex_year =
            matches.value_of_lossy("re-year").unwrap().into_owned();
        let min_votes = matches.value_of_lossy("votes").unwrap().parse()?;
        let rename_action = {
            if matches.is_present("symlink") {
                if !cfg!(unix) {
                    anyhow::bail!(
                        "--symlink currently supported only on Unix \
                         platforms, try hardlink (-H) instead"
                    );
                }
                RenameAction::Symlink
            } else if matches.is_present("hardlink") {
                RenameAction::Hardlink
            } else {
                RenameAction::Rename
            }
        };
        Ok(Args {
            data_dir: data_dir,
            dest_dir: dest_dir,
            debug: matches.is_present("debug"),
            files: files,
            index_dir: index_dir,
            ngram_size: matches
                .value_of_lossy("ngram-size")
                .unwrap()
                .parse()?,
            ngram_type: matches
                .value_of_lossy("ngram-type")
                .unwrap()
                .parse()?,
            query: query,
            regex_episode: regex_episode,
            regex_season: regex_season,
            regex_year: regex_year,
            update_data: matches.is_present("update-data"),
            update_index: matches.is_present("update-index"),
            min_votes: min_votes,
            rename_action: rename_action,
        })
    }

    fn create_index(&self) -> anyhow::Result<Index> {
        Ok(IndexBuilder::new()
            .ngram_size(self.ngram_size)
            .ngram_type(self.ngram_type)
            .create(&self.data_dir, &self.index_dir)?)
    }

    fn open_index(&self) -> anyhow::Result<Index> {
        Ok(Index::open(&self.data_dir, &self.index_dir)?)
    }

    fn searcher(&self) -> anyhow::Result<Searcher> {
        Ok(Searcher::new(self.open_index()?))
    }

    fn download_all(&self) -> anyhow::Result<bool> {
        download::download_all(&self.data_dir)
    }

    fn download_all_update(&self) -> anyhow::Result<()> {
        download::update_all(&self.data_dir)
    }
}

fn app() -> clap::App<'static, 'static> {
    use clap::{App, AppSettings, Arg};

    lazy_static! {
        // clap wants all of its strings tied to a particular lifetime, but
        // we'd really like to determine some default values dynamically. Using
        // a lazy_static here is one way of safely giving a static lifetime to
        // a value that is computed at runtime.
        //
        // An alternative approach would be to compute all of our default
        // values in the caller, and pass them into this function. It's nicer
        // to defined what we need here though. Locality of reference and all
        // that.
        static ref DATA_DIR: PathBuf = env::temp_dir().join("imdb-rename");
    }

    App::new("imdb-rename")
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .max_term_width(100)
        .setting(AppSettings::UnifiedHelpMessage)
        .arg(Arg::with_name("file")
             .multiple(true)
             .help("One or more files to rename."))
        .arg(Arg::with_name("data-dir")
             .long("data-dir")
             .env("IMDB_RENAME_DATA_DIR")
             .takes_value(true)
             .default_value_os(DATA_DIR.as_os_str())
             .help("The location to store IMDb data files."))
        .arg(Arg::with_name("dest-dir")
             .long("dest-dir")
             .short("d")
             .env("IMDB_RENAME_DEST_DIR")
             .takes_value(true)
             .help("The output directory of renamed files \
                    (or symlinks/hardlinks with the -s/-H options). \
                    By default, files are renamed in place."))
        .arg(Arg::with_name("debug")
             .long("debug")
             .help("Show debug messages. Use this when filing bugs."))
        .arg(Arg::with_name("follow")
             .long("follow")
             .short("f")
             .help("Follow directories and attempt to rename all child \
                    entries."))
        .arg(Arg::with_name("index-dir")
             .long("index-dir")
             .env("IMDB_RENAME_INDEX_DIR")
             .takes_value(true)
             .help("The location to store IMDb index files. \
                    When absent, the default is {data-dir}/index."))
        .arg(Arg::with_name("ngram-size")
             .long("ngram-size")
             .default_value("3")
             .help("Choose the ngram size for indexing names. This is only \
                    used at index time and otherwise ignored."))
        .arg(Arg::with_name("ngram-type")
             .long("ngram-type")
             .default_value("window")
             .possible_values(NgramType::possible_names())
             .help("Choose the type of ngram generation. This is only used \
                    used at index time and otherwise ignored."))
        .arg(Arg::with_name("query")
             .long("query")
             .short("q")
             .takes_value(true)
             .help("Setting an override query is necessary if the file \
                    path lacks sufficient information to find a matching \
                    title. For example, if a year could not be found. It \
                    is also useful for specifying a TV show when renaming \
                    multiple episodes at once."))
        .arg(Arg::with_name("re-episode")
             .long("re-episode")
             .takes_value(true)
             .default_value(r"[Ee](?P<episode>[0-9]+)")
             .help("A regex for matching episode numbers. The episode number \
                    is extracted by looking for a 'episode' capture group."))
        .arg(Arg::with_name("re-season")
             .long("re-season")
             .takes_value(true)
             .default_value(r"[Ss](?P<season>[0-9]+)")
             .help("A regex for matching season numbers. The season number \
                    is extracted by looking for a 'season' capture group."))
        .arg(Arg::with_name("re-year")
             .long("re-year")
             .takes_value(true)
             .default_value(r"\b(?P<year>[0-9]{4})\b")
             .help("A regex for matching the year. The year is extracted by \
                    looking for a 'year' capture group."))
        .arg(Arg::with_name("update-data")
             .long("update-data")
             .help("Forcefully refreshes the IMDb data and then exits."))
        .arg(Arg::with_name("votes")
             .long("votes")
             .default_value("1000")
             .help("The minimum number of votes required for results matching \
                    a query derived from existing file names. This is not \
                    applied to explicit queries via the -q/--query flag."))
        .arg(Arg::with_name("update-index")
             .long("update-index")
             .help("Forcefully re-indexes the IMDb data and then exits."))
        .arg(Arg::with_name("symlink")
             .long("symlink")
             .short("s")
             .conflicts_with("hardlink")
             .help("Create a symlink instead of renaming. \
                    (Unix only feature.)"))
        .arg(Arg::with_name("hardlink")
             .long("hardlink")
             .short("H")
             .conflicts_with("symlink")
             .help("Create a hardlink instead of renaming. \
                    This doesn't work when renaming directories."))
}

/// Collect all file paths from a sequence of OsStrings from the command line.
/// If `follow` is true, then any paths that are directories are expanded to
/// include all child paths, recursively.
///
/// If there is an error following a path, then it is logged to stderr and
/// otherwise skipped.
fn collect_paths(paths: Vec<&OsStr>, follow: bool) -> Vec<PathBuf> {
    let mut results = vec![];
    for path in paths {
        let path = PathBuf::from(path);
        if !follow || !path.is_dir() {
            results.push(path);
            continue;
        }
        for result in WalkDir::new(path) {
            match result {
                Ok(dent) => results.push(dent.path().to_path_buf()),
                Err(err) => eprintln!("{}", err),
            }
        }
    }
    results
}

/// Return true if and only if an I/O broken pipe error exists in the causal
/// chain of the given error.
fn is_pipe_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(ioerr) = cause.downcast_ref::<io::Error>() {
            if ioerr.kind() == io::ErrorKind::BrokenPipe {
                return true;
            }
        }
    }
    false
}
