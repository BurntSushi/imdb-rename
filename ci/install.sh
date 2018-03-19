#!/bin/bash

# install stuff needed for the `script` phase

# Where rustup gets installed.
export PATH="$PATH:$HOME/.cargo/bin"

set -ex

# shellcheck source=/dev/null
. "$(dirname $0)/utils.sh"

install_rustup() {
    curl https://sh.rustup.rs -sSf \
      | sh -s -- -y --default-toolchain="$TRAVIS_RUST_VERSION"
    rustc -V
    cargo -V
}

install_targets() {
    if [ "$(host)" != "$TARGET" ]; then
        rustup target add "$TARGET"
    fi
}

main() {
    install_rustup
    install_targets
}

main
