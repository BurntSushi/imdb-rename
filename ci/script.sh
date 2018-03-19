#!/bin/bash

# build, test and generate docs in this phase

set -ex

# shellcheck source=/dev/null
. "$(dirname $0)/utils.sh"

main() {
    # Test a normal debug build.
    cargo build --target "$TARGET" --verbose --all

    # sanity check the file type
    file target/"$TARGET"/debug/imdb-rename

    # Run tests for ripgrep and all sub-crates.
    cargo test --target "$TARGET" --verbose --all
}

main
