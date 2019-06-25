#[macro_use]
extern crate clap;
extern crate csv;
#[macro_use]
extern crate failure;
extern crate imdb_index;
#[macro_use]
extern crate lazy_static;
extern crate log;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate toml;

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::result;
use std::str::FromStr;

use imdb_index::{NameScorer, NgramType, Similarity};

use crate::eval::Spec;

mod eval;
mod logger;

/// Our type alias for handling errors throughout imdb-eval.
type Result<T> = result::Result<T, failure::Error>;

fn main() {
    if let Err(err) = try_main() {
        // A pipe error occurs when the consumer of this process's output has
        // hung up. This is a normal event, and we should quit gracefully.
        if is_pipe_error(&err) {
            process::exit(0);
        }

        // Print the error, including all of its underlying causes.
        eprintln!("{}", pretty_error(&err));

        // If we get a non-empty backtrace (e.g., RUST_BACKTRACE=1 is set),
        // then show it.
        let backtrace = err.backtrace().to_string();
        if !backtrace.trim().is_empty() {
            eprintln!("{}", backtrace);
        }
        process::exit(1);
    }
}

fn try_main() -> Result<()> {
    logger::init()?;
    log::set_max_level(log::LevelFilter::Info);

    let args = Args::from_matches(&app().get_matches())?;
    if args.debug {
        log::set_max_level(log::LevelFilter::Debug);
    }
    if let Some(ref summarize) = args.summarize {
        return run_summarize(summarize);
    } else if args.dry_run {
        for spec in args.specs()? {
            println!("{}", spec);
        }
        return Ok(());
    }
    run_eval(
        &args.data_dir,
        &args.eval_dir,
        args.truth.as_ref().map(|p| p.as_path()),
        args.specs()?,
    )
}

/// Run an evaluation on the IMDb data in `data_dir`, and store any indexes
/// created for the evaluation in `eval_dir`. If a path to truth data is given,
/// then the information needs or "tasks" used for the evaluation are taken
/// from that file, otherwise, a built-in truth data set is used.
///
/// The specs given each describe the protocol for an evaluation. They each
/// represent a configuration for how an IMDb index is built and how queries
/// are constructed. The specification is fundamentally the thing we want to
/// evaluate. That is, we want to find the "best" specification.
fn run_eval(
    data_dir: &Path,
    eval_dir: &Path,
    truth_path: Option<&Path>,
    specs: Vec<Spec>,
) -> Result<()> {
    if !data_dir.exists() {
        bail!("data directory {} does not exist; please use \
               imdb-rename to create it", data_dir.display());
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());
    for spec in &specs {
        let results = match truth_path {
            None => spec.evaluate(data_dir, eval_dir)?,
            Some(p) => spec.evaluate_with(data_dir, eval_dir, p)?,
        };
        for result in results {
            wtr.serialize(result?)?;
            wtr.flush()?;
        }
    }
    Ok(())
}

/// Summarize the evaluation results at the given path.
fn run_summarize(summarize: &Path) -> Result<()> {
    let mut results: Vec<eval::TaskResult> = vec![];
    let mut rdr = csv::Reader::from_path(summarize)?;
    for result in rdr.deserialize() {
        results.push(result?);
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());
    for summary in eval::Summary::from_task_results(&results) {
        wtr.serialize(summary)?;
    }
    wtr.flush()?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    data_dir: PathBuf,
    debug: bool,
    dry_run: bool,
    eval_dir: PathBuf,
    ngram_sizes: Vec<usize>,
    ngram_types: Vec<NgramType>,
    result_sizes: Vec<usize>,
    scorers: Vec<Option<NameScorer>>,
    similarities: Vec<Similarity>,
    specs: Vec<String>,
    summarize: Option<PathBuf>,
    truth: Option<PathBuf>,
}

impl Args {
    /// Build a structured set of arguments from clap's matches.
    fn from_matches(matches: &clap::ArgMatches) -> Result<Args> {
        let data_dir = matches
            .value_of_os("data-dir")
            .map(PathBuf::from)
            .unwrap();
        let eval_dir = matches
            .value_of_os("eval-dir")
            .map(PathBuf::from)
            .unwrap();
        let specs = match matches.values_of_lossy("specs") {
            None => vec![],
            Some(specs) => specs,
        };
        let similarities = parse_many_lossy(matches, "sim", vec![
            Similarity::None,
            Similarity::Levenshtein,
            Similarity::Jaro,
            Similarity::JaroWinkler,
        ])?;
        let scorers = parse_many_lossy(matches, "scorer", vec![
            OptionalNameScorer::from(NameScorer::OkapiBM25),
            OptionalNameScorer::from(NameScorer::TFIDF),
            OptionalNameScorer::from(NameScorer::Jaccard),
            OptionalNameScorer::from(NameScorer::QueryRatio),
        ])?.into_iter().map(|s| s.0).collect();
        let ngram_types = parse_many_lossy(
            matches,
            "ngram-type",
            vec![NgramType::Window],
        )?;
        Ok(Args {
            data_dir: data_dir,
            debug: matches.is_present("debug"),
            dry_run: matches.is_present("dry-run"),
            eval_dir: eval_dir,
            ngram_sizes: parse_many_lossy(matches, "ngram-size", vec![3])?,
            ngram_types: ngram_types,
            result_sizes: parse_many_lossy(matches, "result-size", vec![30])?,
            scorers: scorers,
            similarities: similarities,
            specs: specs,
            summarize: matches.value_of_os("summarize").map(PathBuf::from),
            truth: matches.value_of_os("truth").map(PathBuf::from),
        })
    }

    /// Build all evaluation specifications as indicated by command line
    /// options.
    fn specs(&self) -> Result<Vec<Spec>> {
        // We want to build all possible permutations. We do this by
        // alternating between specs1 and specs2. Each additional parameter
        // combinatorially explodes the previous set of specifications.

        let (mut specs1, mut specs2) = (vec![], vec![]);
        for &ngram_size in &self.ngram_sizes {
            specs1.push(Spec::new().with_ngram_size(ngram_size)?);
        }
        for spec in specs1.drain(..) {
            for &result_size in &self.result_sizes {
                specs2.push(spec.clone().with_result_size(result_size)?);
            }
        }
        for spec in specs2.drain(..) {
            for sim in &self.similarities {
                specs1.push(spec.clone().with_similarity(sim.clone()));
            }
        }
        for spec in specs1.drain(..) {
            for scorer in &self.scorers {
                specs2.push(spec.clone().with_scorer(scorer.clone()));
            }
        }
        for spec in specs2.drain(..) {
            for ngram_type in &self.ngram_types {
                specs1.push(spec.clone().with_ngram_type(ngram_type.clone()));
            }
        }
        Ok(specs1)
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
        static ref DEFAULT_DATA_DIR: PathBuf =
            env::temp_dir().join("imdb-rename");
        static ref DEFAULT_EVAL_DIR: PathBuf =
            env::temp_dir().join("imdb-rename-eval");
        static ref POSSIBLE_SCORER_NAMES: Vec<&'static str> = {
            let mut names = NameScorer::possible_names().to_vec();
            names.insert(0, "none");
            names
        };
    }

    App::new("imdb-rename")
        .author(crate_authors!())
        .version(crate_version!())
        .max_term_width(100)
        .setting(AppSettings::UnifiedHelpMessage)
        .arg(Arg::with_name("data-dir")
             .long("data-dir")
             .env("IMDB_RENAME_DATA_DIR")
             .takes_value(true)
             .default_value_os(DEFAULT_DATA_DIR.as_os_str())
             .help("The location to store IMDb data files."))
        .arg(Arg::with_name("debug")
             .long("debug")
             .help("Show debug messages. Use this when filing bugs."))
        .arg(Arg::with_name("dry-run")
             .long("dry-run")
             .help("Show the evaluations that would be run and then exit \
                    without running them."))
        .arg(Arg::with_name("eval-dir")
             .long("eval-dir")
             .env("IMDB_RENAME_EVAL_DIR")
             .takes_value(true)
             .default_value_os(DEFAULT_EVAL_DIR.as_os_str())
             .help("The location to store evaluation index files."))
        .arg(Arg::with_name("ngram-size")
             .long("ngram-size")
             .takes_value(true)
             .multiple(true)
             .number_of_values(1)
             .help("Set the ngram size on which to perform an evaluation. \
                    An evaluation will be performed for each ngram size. \
                    If no ngram size is given, a default of 3 is used."))
        .arg(Arg::with_name("ngram-type")
             .long("ngram-type")
             .takes_value(true)
             .multiple(true)
             .number_of_values(1)
             .possible_values(NgramType::possible_names())
             .help("Set the ngram type on which to perform an evaluation. \
                    An evaluation will be performed for each ngram type. \
                    If no ngram type is given, it defaults to 'window'."))
        .arg(Arg::with_name("result-size")
             .long("result-size")
             .takes_value(true)
             .multiple(true)
             .number_of_values(1)
             .help("Set the result size on which to perform an evaluation. \
                    An evaluation will be performed for each result size. \
                    If no result size is given, a default of 30 is used."))
        .arg(Arg::with_name("scorer")
             .long("scorer")
             .takes_value(true)
             .multiple(true)
             .number_of_values(1)
             .possible_values(&POSSIBLE_SCORER_NAMES)
             .help("Set the name scorer function to use. An evaluation is \
                    performed for each name function given. By default, \
                    all name scorers are used, except for 'none'."))
        .arg(Arg::with_name("sim")
             .long("sim")
             .takes_value(true)
             .multiple(true)
             .number_of_values(1)
             .possible_values(Similarity::possible_names())
             .help("Set the similarity ranker function to use. An evaluation \
                    is performed for each ranker function given. By default, \
                    all ranker functions are used, including 'none'."))
        .arg(Arg::with_name("summarize")
             .long("summarize")
             .takes_value(true)
             .number_of_values(1)
             .help("Print summary statistics from an evaluation run."))
        .arg(Arg::with_name("truth")
             .long("truth")
             .takes_value(true)
             .help("A file path containing evaluation truth data. By default, \
                    an evaluation uses truth data embedded in imdb-rename."))
}

/// An optional name scorer is a `NameScorer` that may be absent.
///
/// We define a type for it to make parsing it easier.
#[derive(Debug)]
struct OptionalNameScorer(Option<NameScorer>);

impl FromStr for OptionalNameScorer {
    type Err = imdb_index::Error;

    fn from_str(
        s: &str,
    ) -> result::Result<OptionalNameScorer, imdb_index::Error> {
        let opt =
            if s == "none" {
                None
            } else {
                Some(s.parse()?)
            };
        Ok(OptionalNameScorer(opt))
    }
}

impl From<NameScorer> for OptionalNameScorer {
    fn from(scorer: NameScorer) -> OptionalNameScorer {
        OptionalNameScorer(Some(scorer))
    }
}

/// Parse a sequence of values from clap.
fn parse_many_lossy<E: failure::Fail, T: FromStr<Err=E>>(
    matches: &clap::ArgMatches,
    name: &str,
    default: Vec<T>,
) -> Result<Vec<T>> {
    let strs = match matches.values_of_lossy(name) {
        None => return Ok(default),
        Some(strs) => strs,
    };
    let mut values = vec![];
    for s in strs {
        values.push(s.parse()?);
    }
    Ok(values)
}

/// Return a prettily formatted error, including its entire causal chain.
fn pretty_error(err: &failure::Error) -> String {
    let mut pretty = err.to_string();
    let mut prev = err.as_fail();
    while let Some(next) = prev.cause() {
        pretty.push_str(": ");
        pretty.push_str(&next.to_string());
        prev = next;
    }
    pretty
}

/// Return true if and only if an I/O broken pipe error exists in the causal
/// chain of the given error.
fn is_pipe_error(err: &failure::Error) -> bool {
    for cause in err.iter_chain() {
        if let Some(ioerr) = cause.downcast_ref::<io::Error>() {
            if ioerr.kind() == io::ErrorKind::BrokenPipe {
                return true;
            }
        }
    }
    false
}
