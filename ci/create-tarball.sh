#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

case "$CI_OS_NAME" in
osx)
  _cputype="$(uname -m)"
  if [[ $_cputype = arm64 ]]; then
    _cputype=aarch64
  fi
  TARGET=${_cputype}-apple-darwin
  ;;
linux)
  TARGET=x86_64-unknown-linux-gnu
  ;;
*)
  echo CI_OS_NAME unsupported
  exit 1
  ;;
esac

RELEASE_BASENAME="${RELEASE_BASENAME:="${GEYSER_PLUGIN_NAME}-release"}"
TARBALL_BASENAME="${TARBALL_BASENAME:="$RELEASE_BASENAME"}"

echo --- Creating release tarball
(
  set -x
  rm -rf "${RELEASE_BASENAME:?}"/
  mkdir "${RELEASE_BASENAME}"/

  COMMIT="$(git rev-parse HEAD)"

  (
    echo "channel: $CI_TAG"
    echo "commit: $COMMIT"
    echo "target: $TARGET"
  ) > "${RELEASE_BASENAME}"/version.yml

  # Make CHANNEL available to include in the software version information
  export CHANNEL

  source ci/rust-version.sh stable
  ci/cargo-install-all.sh stable "${RELEASE_BASENAME}"

  tar cvf "${TARBALL_BASENAME}"-$TARGET.tar "${RELEASE_BASENAME}"
  bzip2 "${TARBALL_BASENAME}"-$TARGET.tar
  cp "${RELEASE_BASENAME}"/version.yml "${TARBALL_BASENAME}"-$TARGET.yml
)

echo --- ok
