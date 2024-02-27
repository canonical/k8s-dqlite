#!/bin/bash -xeu

DIR="$(realpath `dirname "${0}"`)"

. "${DIR}/static-dqlite.sh"

go build \
  -tags dqlite,libsqlite3 \
  -ldflags '-s -w -linkmode "external" -extldflags "-static"' \
  "${@}"
