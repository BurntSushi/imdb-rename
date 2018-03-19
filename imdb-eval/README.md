imdb-eval
=========
A command line tool for evaluating imdb-rename's search functionality.

[![Linux build status](https://api.travis-ci.org/BurntSushi/imdb-rename.png)](https://travis-ci.org/BurntSushi/imdb-rename)
[![Windows build status](https://ci.appveyor.com/api/projects/status/github/BurntSushi/imdb-rename?svg=true)](https://ci.appveyor.com/project/BurntSushi/imdb-rename)
[![](http://meritbadge.herokuapp.com/imdb-rename)](https://crates.io/crates/imdb-rename)


### Installation

No release binaries are provided for imdb-eval. Instead, users should compile
it from source:

```
$ git clone https://github.com/BurntSushi/imdb-rename
$ cd imdb-rename
$ cargo build --release --all
$ ./target/release/imdb-eval --help
```

For more details on how to use imdb-eval, please see imdb-rename's README.
