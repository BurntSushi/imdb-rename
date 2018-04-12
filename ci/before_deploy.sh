#!/bin/bash

# package the build artifacts

set -ex

# shellcheck source=/dev/null
. "$(dirname $0)/utils.sh"

# Generate artifacts for release
mk_artifacts() {
    cargo build --target "$TARGET" --release
}

mk_tarball() {
    local tmpdir name staging out_dir

    # Create a temporary dir that contains our staging area.
    # $tmpdir/$name is what eventually ends up as the deployed archive.
    tmpdir="$(mktemp -d)"
    name="${PROJECT_NAME}-${TRAVIS_TAG}-${TARGET}"
    staging="$tmpdir/$name"
    mkdir -p "$staging"/{complete,doc}
    # The deployment directory is where the final archive will reside.
    # This path is known by the .travis.yml configuration.
    out_dir="$(pwd)/deployment"
    mkdir -p "$out_dir"

    # Copy the imdb-rename binary and strip it.
    cp "target/$TARGET/release/imdb-rename" "$staging/imdb-rename"
    strip "$staging/imdb-rename"
    # Copy the licenses and README.
    cp {README.md,UNLICENSE,COPYING,LICENSE-MIT} "$staging/"

    (cd "$tmpdir" && tar czf "$out_dir/$name.tar.gz" "$name")
    rm -rf "$tmpdir"
}

main() {
    mk_artifacts
    mk_tarball
}

main
