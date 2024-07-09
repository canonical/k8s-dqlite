#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/dynamic-dqlite.sh"

go build \
  -tags libsqlite3 \
  -ldflags '-s -w -extldflags "-Wl,-rpath,$ORIGIN/lib -Wl,-rpath,$ORIGIN/../lib"' \
  "${@}"
