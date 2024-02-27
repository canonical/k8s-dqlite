#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/dynamic-dqlite.sh"

go build \
  -tags dqlite,libsqlite3 \
  -ldflags '-s -w' \
  "${@}"
