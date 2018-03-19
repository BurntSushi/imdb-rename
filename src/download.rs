use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use reqwest;

use Result;

/// The base URL to the IMDb data set.
///
/// It's not clear if this URL will remain free and open forever, although it
/// is provided by IMDb proper. If this goes away, we'll need to switch to s3.
const IMDB_BASE_URL: &'static str = "https://datasets.imdbws.com";

/// All of the data sets we care about.
///
/// We leave out cast/crew because we don't need them for renaming files.
const DATA_SETS: &'static [&'static str] = &[
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.episode.tsv.gz",
    "title.ratings.tsv.gz",
];

/// Download ensures that all of the IMDb data files exist and have non-zero
/// size in the given directory. Any path that does not meet these criteria
/// is fetched from IMDb. Other paths are left untouched.
///
/// Returns true if and only if at least one file was downloaded.
pub fn download_all<P: AsRef<Path>>(dir: P) -> Result<bool> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;

    let nonexistent = non_existent_data_sets(dir)?;
    for dataset in &nonexistent {
        download_one(dir, dataset)?;
    }
    Ok(nonexistent.len() > 0)
}

/// Update will update all data set files, regardless of whether they already
/// exist or not.
pub fn update_all<P: AsRef<Path>>(dir: P) -> Result<()> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;

    for dataset in DATA_SETS {
        download_one(dir, dataset)?;
    }
    Ok(())
}

/// Downloads a single data set, decompresses it and writes it to the
/// corresponding file path in the given directory.
fn download_one(outdir: &Path, dataset: &'static str) -> Result<()> {
    let outpath = dataset_path(outdir, dataset);
    let mut outfile = File::create(&outpath)?;

    let url = format!("{}/{}", IMDB_BASE_URL, dataset);
    info!("downloading {} to {}", url, outpath.display());
    let mut resp = GzDecoder::new(reqwest::get(&url)?.error_for_status()?);
    io::copy(&mut resp, &mut outfile)?;
    Ok(())
}

/// Gets a list of data sets that either don't exist in the current directory
/// or have zero size.
fn non_existent_data_sets(dir: &Path) -> Result<Vec<&'static str>> {
    let mut result = vec![];
    for &dataset in DATA_SETS {
        let path = dataset_path(dir, dataset);
        if fs::metadata(path).map(|md| md.len() == 0).unwrap_or(true) {
            result.push(dataset);
        }
    }
    Ok(result)
}

/// Build the path on disk for a dataset, given the directory and the dataset
/// name.
fn dataset_path(dir: &Path, name: &'static str) -> PathBuf {
    let mut path = dir.join(name);
    // We drop the gz extension since we decompress before writing to disk.
    path.set_extension("");
    path
}
