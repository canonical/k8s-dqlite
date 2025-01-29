#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/static-dqlite.sh"

go build \
  -tags libsqlite3 \
  -ldflags '-linkmode "external" -extldflags "-static"' \
  "${@}"
