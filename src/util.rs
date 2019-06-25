use std::io::{self, Write};

use imdb_index::{Episode, MediaEntity, Scored, Searcher, Title};
use tabwriter::TabWriter;

use crate::Result;

/// Make a choice among the search results given.
///
/// If there is no clear winner, then a prompt is shown to the end user, where
/// they must make a selection. If a selection is absent or invalid, then an
/// error is returned.
///
/// The threshold given determines the automatic selection criteria. Namely,
/// if the difference of scores between the first and second results is
/// greater than or equal to the given threshold, then the first result is
/// returned without prompted the end user.
pub fn choose(
    searcher: &mut Searcher,
    results: &[Scored<MediaEntity>],
    good_threshold: f64,
) -> Result<MediaEntity> {
    if results.is_empty() {
        bail!("no search results available for query");
    } else if results.len() == 1 {
        return Ok(results[0].clone().into_value());
    } else if (results[0].score() - results[1].score()) >= good_threshold {
        return Ok(results[0].clone().into_value());
    }

    write_tsv(io::stdout(), searcher, results)?;
    let choice = read_number(1, results.len())?;
    Ok(results[choice-1].clone().into_value())
}

/// Reads a number from stdin in the given inclusive range.
pub fn read_number(start: usize, end: usize) -> Result<usize> {
    let mut stdout = io::stdout();
    write!(stdout, "Please enter your choice [{}-{}]: ", start, end)?;
    stdout.flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    let choice: usize = response.trim().parse()?;
    if choice < start || choice > end {
        bail!("invalid choice: {} is not in range [{}-{}]",
              choice, start, end);
    }
    Ok(choice)
}

/// Reads a yes/no answer from stdin. This is flexible and recognizes
/// y, Y, yes, YES as 'yes' answers. Everything else is recognized as a 'no'
/// answer.
pub fn read_yesno(msg: &str) -> Result<bool> {
    let mut stdout = io::stdout();
    write!(stdout, "{}", msg)?;
    stdout.flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    let answer = response.trim().to_lowercase();
    Ok(answer == "y" || answer == "yes")
}

/// Write the given result set to the given writer.
///
/// If a result is an episode, then the index given is used to look up relevant
/// info about its TV show, if one could be found, and include that information
/// in the output.
pub fn write_tsv<W: io::Write>(
    wtr: W,
    searcher: &mut Searcher,
    results: &[Scored<MediaEntity>],
) -> Result<()> {
    let mut wtr = TabWriter::new(wtr).minwidth(4);
    writeln!(wtr, "#\tscore\tid\tkind\ttitle\tyear\ttv")?;
    for (i, sr) in results.iter().enumerate() {
        let (score, ent) = (sr.score(), sr.value());
        if let Some(ep) = ent.episode() {
            match searcher.index().title(&ep.tvshow_id)? {
                None => write_tsv_title(&mut wtr, i+1, score, ent)?,
                Some(tvshow) => {
                    write_tsv_episode(&mut wtr, i+1, score, ent, &tvshow, ep)?;
                }
            }
        } else {
            write_tsv_title(&mut wtr, i+1, score, ent)?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn write_tsv_title<W: io::Write>(
    mut wtr: W,
    position: usize,
    score: f64,
    ent: &MediaEntity,
) -> Result<()> {
    write!(
        wtr,
        "{}\t{:0.3}\t{}\t{}\t{}\t{}",
        position,
        score,
        ent.title().id,
        ent.title().kind,
        ent.title().title,
        ent.title().start_year
            .map(|y| y.to_string())
            .unwrap_or("N/A".to_string()),
    )?;
    write!(wtr, "\n")?;
    Ok(())
}

fn write_tsv_episode<W: io::Write>(
    mut wtr: W,
    position: usize,
    score: f64,
    ent: &MediaEntity,
    tvshow: &Title,
    ep: &Episode,
) -> Result<()> {
    let tvinfo = format!(
        "S{:02}E{:02} {}",
        ep.season.unwrap_or(0),
        ep.episode.unwrap_or(0),
        tvshow.title,
    );
    write!(
        wtr,
        "{}\t{:0.3}\t{}\t{}\t{}\t{}\t{}",
        position,
        score,
        ent.title().id,
        ent.title().kind,
        ent.title().title,
        ent.title().start_year
            .map(|y| y.to_string())
            .unwrap_or("N/A".to_string()),
        tvinfo,
    )?;
    write!(wtr, "\n")?;
    Ok(())
}
