use std::fmt;
use std::path::{Path, PathBuf};

use csv;
use fst;

/// A type alias for handling errors throughout imdb-index.
pub type Result<T> = std::result::Result<T, Error>;

/// An error that can occur while interacting with an IMDb index.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// Return a reference to the kind of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Transfer ownership of the kind of this error.
    pub fn into_kind(self) -> ErrorKind {
        self.kind
    }

    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error { kind }
    }

    pub(crate) fn unknown_title<T: AsRef<str>>(unk: T) -> Error {
        Error { kind: ErrorKind::UnknownTitle(unk.as_ref().to_string()) }
    }

    pub(crate) fn unknown_scorer<T: AsRef<str>>(unk: T) -> Error {
        Error { kind: ErrorKind::UnknownScorer(unk.as_ref().to_string()) }
    }

    pub(crate) fn unknown_ngram_type<T: AsRef<str>>(unk: T) -> Error {
        Error { kind: ErrorKind::UnknownNgramType(unk.as_ref().to_string()) }
    }

    pub(crate) fn unknown_sim<T: AsRef<str>>(unk: T) -> Error {
        Error { kind: ErrorKind::UnknownSimilarity(unk.as_ref().to_string()) }
    }

    pub(crate) fn unknown_directive<T: AsRef<str>>(unk: T) -> Error {
        Error { kind: ErrorKind::UnknownDirective(unk.as_ref().to_string()) }
    }

    pub(crate) fn bug<T: AsRef<str>>(msg: T) -> Error {
        Error { kind: ErrorKind::Bug(msg.as_ref().to_string()) }
    }

    pub(crate) fn config<T: AsRef<str>>(msg: T) -> Error {
        Error { kind: ErrorKind::Config(msg.as_ref().to_string()) }
    }

    pub(crate) fn version(expected: u64, got: u64) -> Error {
        Error { kind: ErrorKind::VersionMismatch { expected, got } }
    }

    pub(crate) fn csv(err: csv::Error) -> Error {
        Error { kind: ErrorKind::Csv(err.to_string()) }
    }

    pub(crate) fn fst(err: fst::Error) -> Error {
        Error { kind: ErrorKind::Fst(err.to_string()) }
    }

    pub(crate) fn io(err: std::io::Error) -> Error {
        Error { kind: ErrorKind::Io { err, path: None } }
    }

    pub(crate) fn io_path<P: AsRef<Path>>(
        err: std::io::Error,
        path: P,
    ) -> Error {
        Error {
            kind: ErrorKind::Io {
                err,
                path: Some(path.as_ref().to_path_buf()),
            },
        }
    }

    pub(crate) fn number<E: std::error::Error + Send + Sync + 'static>(
        err: E,
    ) -> Error {
        Error { kind: ErrorKind::Number(Box::new(err)) }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind {
            ErrorKind::Io { ref err, .. } => Some(err),
            ErrorKind::Number(ref err) => Some(&**err),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.kind.fmt(f)
    }
}

/// The specific kind of error that can occur.
#[derive(Debug)]
pub enum ErrorKind {
    /// An index version mismatch. This error occurs when the version of the
    /// index is different from the version supported by this version of
    /// imdb-index.
    ///
    /// Generally speaking, the versions must be exactly equivalent, otherwise
    /// this error is returned.
    VersionMismatch {
        /// The expected or supported index version.
        expected: u64,
        /// The actual version of the index on disk.
        got: u64,
    },
    /// An error parsing the type of a title.
    ///
    /// The data provided is the unrecognized title type.
    UnknownTitle(String),
    /// An error parsing the name of a scorer.
    ///
    /// The data provided is the unrecognized name.
    UnknownScorer(String),
    /// An error parsing the name of an ngram type.
    ///
    /// The data provided is the unrecognized name.
    UnknownNgramType(String),
    /// An error parsing the name of a similarity function.
    ///
    /// The data provided is the unrecognized name.
    UnknownSimilarity(String),
    /// An error parsing the name of a directive from a free-form query.
    ///
    /// The data provided is the unrecognized name.
    UnknownDirective(String),
    /// An unexpected error occurred while reading an index that should not
    /// have occurred. Generally, these errors correspond to bugs in this
    /// library.
    Bug(String),
    /// An error occurred while reading/writing the index config.
    Config(String),
    /// An error that occured while writing or reading CSV data.
    Csv(String),
    /// An error that occured while creating an FST index.
    Fst(String),
    /// An unexpected I/O error occurred.
    Io {
        /// The underlying I/O error.
        err: std::io::Error,
        /// A file path, if the I/O error occurred in the context of a named
        /// file.
        path: Option<PathBuf>,
    },
    /// An error occurred while parsing a number in a free-form query.
    Number(Box<dyn std::error::Error + Send + Sync>),
    /// Hints that destructuring should not be exhaustive.
    ///
    /// This enum may grow additional variants, so this makes sure clients
    /// don't count on exhaustive matching. (Otherwise, adding a new variant
    /// could break existing code.)
    #[doc(hidden)]
    __Nonexhaustive,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ErrorKind::VersionMismatch { expected, got } => write!(
                f,
                "index version mismatch: expected version {} \
                           but got version {}. Please rebuild the index.",
                expected, got
            ),
            ErrorKind::UnknownTitle(ref unk) => {
                write!(f, "unrecognized title type: '{}'", unk)
            }
            ErrorKind::UnknownScorer(ref unk) => {
                write!(f, "unrecognized scorer name: '{}'", unk)
            }
            ErrorKind::UnknownNgramType(ref unk) => {
                write!(f, "unrecognized ngram type: '{}'", unk)
            }
            ErrorKind::UnknownSimilarity(ref unk) => {
                write!(f, "unrecognized similarity function: '{}'", unk)
            }
            ErrorKind::UnknownDirective(ref unk) => {
                write!(f, "unrecognized search directive: '{}'", unk)
            }
            ErrorKind::Bug(ref msg) => {
                let report = "Please report this bug with a backtrace at \
                              https://github.com/BurntSushi/imdb-rename";
                write!(f, "BUG: {}\n{}", msg, report)
            }
            ErrorKind::Config(ref msg) => write!(f, "config error: {}", msg),
            ErrorKind::Csv(ref msg) => write!(f, "{}", msg),
            ErrorKind::Fst(ref msg) => write!(f, "fst error: {}", msg),
            ErrorKind::Io { path: None, .. } => write!(f, "I/O error"),
            ErrorKind::Io { path: Some(ref p), .. } => {
                write!(f, "{}", p.display())
            }
            ErrorKind::Number(_) => write!(f, "error parsing number"),
            ErrorKind::__Nonexhaustive => panic!("invalid error"),
        }
    }
}
