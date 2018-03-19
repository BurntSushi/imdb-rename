imdb-rename
===========
A command line tool to rename media files based on titles from IMDb.
imdb-rename downloads the official IMDb data set and creates a local index to
use for fast fuzzy searching.

[![Linux build status](https://api.travis-ci.org/BurntSushi/imdb-rename.png)](https://travis-ci.org/BurntSushi/imdb-rename)
[![Windows build status](https://ci.appveyor.com/api/projects/status/github/BurntSushi/imdb-rename?svg=true)](https://ci.appveyor.com/project/BurntSushi/imdb-rename)
[![](http://meritbadge.herokuapp.com/imdb-rename)](https://crates.io/crates/imdb-rename)

Dual-licensed under MIT or the [UNLICENSE](http://unlicense.org).


### Installation

**[Archives of precompiled binaries for imdb-rename are available for Windows,
macOS and Linux.](https://github.com/BurntSushi/imdb-rename/releases)**

Otherwise, users are expected to compile imdb-rename from source:

```
$ git clone https://github.com/BurntSushi/imdb-rename
$ cd imdb-rename
$ cargo build --release
$ ./target/release/imdb-rename --help
```

Alternatively, if you have
[Cargo installed](https://rustup.rs),
then you can install imdb-rename directly from
[crates.io](https://crates.io):

```
$ cargo install imdb-rename
```


### Quick example

Ever since Season 1 of The Simpsons came out on DVD, I've been collecting them
and ripping them on to my hard drive. My process is somewhat manual, but I
wind up with a directory that looks like this:

```
S18E01.mkv  S18E05.mkv  S18E09.mkv  S18E13.mkv  S18E17.mkv  S18E21.mkv
S18E02.mkv  S18E06.mkv  S18E10.mkv  S18E14.mkv  S18E18.mkv  S18E22.mkv
S18E03.mkv  S18E07.mkv  S18E11.mkv  S18E15.mkv  S18E19.mkv
S18E04.mkv  S18E08.mkv  S18E12.mkv  S18E16.mkv  S18E20.mkv
```

It would be much nicer if these files had their proper episode titles.
imdb-rename can rename these files automatically using episode titles from
IMDb:

```
$ imdb-rename -q 'the simpsons {show}' *.mkv
```

This command ran a query with the `-q` flag to identify the TV show, provided
the files to rename, and... presto!

```
S18E01 - The Mook, the Chef, the Wife and Her Homer.mkv
S18E02 - Jazzy & The Pussycats.mkv
S18E03 - Please Homer, Don't Hammer 'Em.mkv
S18E04 - Treehouse of Horror XVII.mkv
S18E05 - G.I. (Annoyed Grunt).mkv
S18E06 - Moe'N'a Lisa.mkv
S18E07 - Ice Cream of Margie: With the Light Blue Hair.mkv
S18E08 - The Haw-Hawed Couple.mkv
S18E09 - Kill Gil, Vol. 1 & 2.mkv
S18E10 - The Wife Aquatic.mkv
S18E11 - Revenge Is a Dish Best Served Three Times.mkv
S18E12 - Little Big Girl.mkv
S18E13 - Springfield Up.mkv
S18E14 - Yokel Chords.mkv
S18E15 - Rome-old and Juli-eh.mkv
S18E16 - Homerazzi.mkv
S18E17 - Marge Gamer.mkv
S18E18 - The Boys of Bummer.mkv
S18E19 - Crook and Ladder.mkv
S18E20 - Stop or My Dog Will Shoot.mkv
S18E21 - 24 Minutes.mkv
S18E22 - You Kent Always Say What You Want.mkv
```


### Fancier example

imdb-rename isn't limited to just renaming TV episodes based on season/episode
numbers. It can also perform a fuzzy match based on the contents of the
file name. For example, given this file:

```
Thor.Ragnarok.2017.1080p.WEB-DL.DD5.1.H264-FGT.mkv
```

We can "clean it up" and rename it a nice title like so:

```
$ imdb-rename Thor.Ragnarok.2017.1080p.WEB-DL.DD5.1.H264-FGT.mkv
```

which gives us:

```
Thor: Ragnarok (2017).mkv
```


### Freeform searching

We can also use imdb-rename to search IMDb, which is the default behavior
when a `-q/--query` is provided without any file names:

```
$ imdb-rename -q 'homey loves flanders'
#     score  id         kind       title                   year  tv
1     1.000  tt0773646  tvEpisode  Homer Loves Flanders    1994  S05E16 The Simpsons
2     0.646  tt2101691  tvEpisode  Tiny Loves Flowers      N/A   S02E08 Dinosaur Train
3     0.568  tt3203408  tvEpisode  Courtney Loves Love     2014  S01E05 Courtney Loves Dallas
4     0.561  tt1722576  short      In Flanders Fields      2010
5     0.561  tt2253780  tvSeries   In Vlaamse Velden       2014
6     0.555  tt4528474  video      My Lovely Homeland      2011
7     0.551  tt0220646  tvMovie    Moll Flanders           1975
[... results truncated ...]
```

Notice that our query had a typo in it. imdb-rename does its best to find the
most relevant results. It is also fast. Even though the above query searches
through all 6 million names in IMDb, it runs in under 100ms. This is thanks to
using an inverted index memory mapped from disk.


### How does it work?

imdb-rename works by downloading
[approved datasets from IMDb](https://www.imdb.com/interfaces/),
and creating an inverted index based on ngrams extracted
from the names in IMDb's data. The inverted index provides a
quick way to search and rank results using techniques from
[information retrieval](https://nlp.stanford.edu/IR-book/)
such as
[Okapi-BM25](https://en.wikipedia.org/wiki/Okapi_BM25).


### Motivation

My motivation for building this tool is somewhat idiosyncratic, but three-fold:

1. I find it very convenient to have a tool to rename media files
   automatically. imdb-rename is my third iteration on this tool. The first was
   an unpublished hodge podge of Python scripts and a MySQL database. The
   second was a
   [Go program with a PostgreSQL database](https://github.com/BurntSushi/goim).
   The Go program served me well, but IMDb retired their old data format, which
   required me to build a new tool to adapt.
2. I've been working on a low-level information retrieval library off-and-on
   for a couple years, and initially built this tool on top of that library as
   a form of dogfooding. It didn't work out as well as I'd hoped, so I scrapped
   the generic library and built out a specific solution tailored to IMDb. I'm
   no longer dogfooding directly, but I've established a useful baseline.
3. I want more people to learn about information retrieval, and I believe this
   tool can serve to teach others. In particular, imdb-rename is a complete
   end-to-end information retrieval system that is fast, solves a real problem,
   is only a few thousand lines of code and comes with a built-in
   evaluation that is easy to run.

This tool is perhaps a bit over engineered, but I had fun with it. Believe it
or not, parts of imdb-rename are intentionally simple at the cost of both query
speed and size on disk!


### Evaluation

It is possible to run an evaluation to compare the various parameters available
for searching. The evaluation system is available as a separate tool called
imdb-eval, which is included in this repository. To use it, we must first build
it:

```
$ git clone https://github.com/BurntSushi/imdb-rename
$ cd imdb-rename
$ cargo build --release --all
$ ./target/release/imdb-eval --help
```

Running an evaluation is simple. We can run an evaluation on all combinations
of scorer and similarity function, along with ngram sizes of 3 and 4 like so:
(This will use truth data that is built into the `imdb-eval` binary.)

```
$ ./target/release/imdb-eval --ngram-size 3 --ngram-size 4 | tee eval.csv
```

This will output the results of running a search on every item in the truth
data. The results include the rank of the expected answer. The results can be
summarized into a single score called the
[Mean Reciprocal Rank](https://en.wikipedia.org/wiki/Mean_reciprocal_rank)
(which is itself a specific instance of MAP, or mean average precision)
with the `--summarize` flag like so:

```
$ ./target/release/imdb-eval --summarize eval.csv
```

If you have [xsv](https://github.com/BurntSushi/xsv) installed, then the
results can be easily sorted and formatted:

```
$ ./target/release/imdb-eval --summarize eval.csv | xsv sort -R -s mrr | xsv table
```

If you want to tweak the truth data, then you might consider starting with the
bundled truth data (assuming you're at the root of the imdb-rename repository):

```
$ $EDITOR data/eval/truth.toml
$ ./target/release/imdb-eval --ngram-size 3 --ngram-size 4 --truth data/eval/truth.toml
```


### What does this tool not do?

imdb-rename is tool for renaming media files, and to the extent that searching
IMDb facilitates renaming files, it is also a search tool. There is no
intent to develop this further to explore all IMDb data, such as cast/crew
information.

Folks interested in building a different type of IMDb tool may be interested
in the [`imdb-index`](https://docs.rs/imdb-index) crate, which provides
programmatic access to the index created by imdb-rename.


### IMDb licensing

The data used by imdb-rename is retrieved from
[IMDb datasets](https://www.imdb.com/interfaces/).
In particular, imdb-rename will never scrape imdb.com, and only uses the data
provided by IMDb in the `tsv` files.

Additionally, imdb-rename must only be used for non-commercial and personal
uses.
