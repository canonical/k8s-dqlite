#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/static-dqlite.sh"

go vet \
  -tags libsqlite3 \
  "${@}"
