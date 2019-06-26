use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use failure::bail;
use flate2::read::GzDecoder;
use reqwest;

use crate::Result;

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
    log::info!("downloading {} to {}", url, outpath.display());
    let mut resp = GzDecoder::new(reqwest::get(&url)?.error_for_status()?);
    log::info!("sorting CSV records");
    write_sorted_csv_records(&mut resp, &mut outfile)?;
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

/// Read all CSV data into memory and sort the records in lexicographic order.
///
/// This is unfortunately necessary because the IMDb data is no longer sorted
/// in lexicographic order with respect to the `tt` identifiers. This appears
/// to be fallout as a result of adding 10 character identifiers (previously,
/// only 9 character identifiers were used).
fn write_sorted_csv_records<R: io::Read, W: io::Write>(
    rdr: R,
    wtr: W,
) -> Result<()> {
    use std::io::Write;
    use bstr::{io::BufReadExt, ByteSlice};

    // We actually only sort the raw lines here instead of parsing CSV records,
    // since parsing into CSV records has fairly substantial memory overhead.
    // Since IMDb CSV data never contains a record that spans multiple lines,
    // this transformation is okay.
    let rdr = io::BufReader::new(rdr);
    let mut lines = rdr.byte_lines().collect::<io::Result<Vec<_>>>()?;
    if lines.is_empty() {
        bail!("got empty CSV input");
    }
    // Keep the header record first.
    lines[1..].sort_unstable();

    let mut wtr = io::BufWriter::new(wtr);
    let mut prev = None;
    for (i, line) in lines.iter().enumerate() {
        // *sigh* ... Looks like the data downloaded is corrupt sometimes,
        // where there are duplicate rows.
        let first = match line.split_str("\t").next() {
            Some(first) => first,
            None => {
                bail!(
                    "expected to find one tab-delimited field in '{:?}'",
                    line.as_bstr(),
                )
            }
        };
        if i > 0 && prev == Some(first) {
            continue;
        }
        prev = Some(first);
        wtr.write_all(&line)?;
        wtr.write_all(b"\n")?;
    }
    wtr.flush()?;
    Ok(())
}
