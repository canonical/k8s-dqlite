#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/dynamic-dqlite.sh"

# Default tags if TAGS environment variable is not set
TAGS="${TAGS:-libsqlite3}"

go install \
  -tags "${TAGS}" \
  -ldflags '-s -w -extldflags "-Wl,-rpath,$ORIGIN/lib -Wl,-rpath,$ORIGIN/../lib"' \
  "${@}"
