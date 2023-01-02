// This module defines a super simple logger that works with the `log` crate.
// We don't need anything fancy; just basic log levels and the ability to
// print to stderr. We therefore avoid bringing in extra dependencies just
// for this functionality.

use std::result;

use log::{self, Log};

use crate::Result;

/// Initialize a simple logger.
pub fn init() -> Result<()> {
    Ok(Logger::init()?)
}

/// The simplest possible logger that logs to stderr.
///
/// This logger does no filtering. Instead, it relies on the `log` crates
/// filtering via its global max_level setting.
#[derive(Debug)]
struct Logger(());

const LOGGER: &Logger = &Logger(());

impl Logger {
    /// Create a new logger that logs to stderr and initialize it as the
    /// global logger. If there was a problem setting the logger, then an
    /// error is returned.
    fn init() -> result::Result<(), log::SetLoggerError> {
        log::set_logger(LOGGER)
    }
}

impl Log for Logger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        // We set the log level via log::set_max_level, so we don't need to
        // implement filtering here.
        true
    }

    fn log(&self, record: &log::Record) {
        if !should_log(record) {
            return;
        }
        eprintln!("{}: {}", record.level(), record.args());
    }

    fn flush(&self) {
        // We use eprintln! which is flushed on every call.
    }
}

fn should_log(record: &log::Record) -> bool {
    let t = record.target();
    t.starts_with("imdb_rename") || t.starts_with("imdb_index")
}
