#!/usr/bin/env bash

set -e


usage() {
  exitcode=0
  if [[ -n "$1" ]]; then
    exitcode=1
    echo "Error: $*"
  fi
  cat <<EOF
usage: $0 [+<cargo version>] [--debug] <install directory>
EOF
  exit $exitcode
}

case "$CI_OS_NAME" in
osx)
  libExt=dylib
  ;;
linux)
  libExt=so
  ;;
*)
  echo CI_OS_NAME unsupported
  exit 1
  ;;
esac

maybeRustVersion=
installDir=
buildVariant=release
maybeReleaseFlag=--release

while [[ -n $1 ]]; do
  if [[ ${1:0:1} = - ]]; then
    if [[ $1 = --debug ]]; then
      maybeReleaseFlag=
      buildVariant=debug
      shift
    else
      usage "Unknown option: $1"
    fi
  elif [[ ${1:0:1} = \+ ]]; then
    maybeRustVersion=$1
    shift
  else
    installDir=$1
    shift
  fi
done

if [[ -z "$installDir" ]]; then
  usage "Install directory not specified"
  exit 1
fi

installDir="$(mkdir -p "$installDir"; cd "$installDir"; pwd)"

echo "Install location: $installDir ($buildVariant)"

cd "$(dirname "$0")"/..

SECONDS=0

mkdir -p "$installDir/lib"

(
  set -x
  # shellcheck disable=SC2086 # Don't want to double quote $rust_version
  cargo $maybeRustVersion build $maybeReleaseFlag --features scylladb
)

cp -fv "target/$buildVariant/${GEYSER_PLUGIN_LIB}.$libExt" "$installDir"/lib/

echo "Done after $SECONDS seconds"

