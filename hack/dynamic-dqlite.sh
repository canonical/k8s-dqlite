#!/bin/bash -xeu

DIR="${DIR:=$(realpath `dirname "${0}"`)}"

. "${DIR}/env.sh"

BUILD_DIR="${DIR}/.build/dynamic"
INSTALL_DIR="${DIR}/.deps/dynamic"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${INSTALL_DIR}/lib" "${INSTALL_DIR}/include"
BUILD_DIR="$(realpath "${BUILD_DIR}")"
INSTALL_DIR="$(realpath "${INSTALL_DIR}")"

"${DIR}/deps.sh"

# build libtirpc
if [ ! -f "${BUILD_DIR}/libtirpc/src/libtirpc.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libtirpc
    git clone "${REPO_LIBTIRPC}" --depth 1 --branch "${TAG_LIBTIRPC}" > /dev/null
    cd libtirpc
    chmod +x autogen.sh
    ./autogen.sh > /dev/null
    ./configure --disable-gssapi > /dev/null
    make -j > /dev/null
  )
fi

# build libnsl
if [ ! -f "${BUILD_DIR}/libnsl/src/libnsl.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libnsl
    git clone "${REPO_LIBNSL}" --depth 1 --branch "${TAG_LIBNSL}" > /dev/null
    cd libnsl
    ./autogen.sh > /dev/null
    autoreconf -i > /dev/null
    autoconf > /dev/null
    ./configure \
      CFLAGS="-I${BUILD_DIR}/libtirpc/tirpc" \
      LDFLAGS="-L${BUILD_DIR}/libtirpc/src" \
      TIRPC_CFLAGS="-I${BUILD_DIR}/libtirpc/tirpc" \
      TIRPC_LIBS="-L${BUILD_DIR}/libtirpc/src -ltirpc" \
      > /dev/null
    make -j > /dev/null
  )
fi

# build libuv
if [ ! -f "${BUILD_DIR}/libuv/libuv.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libuv
    git clone "${REPO_LIBUV}" --depth 1 --branch "${TAG_LIBUV}" > /dev/null
    cd libuv
    ./autogen.sh > /dev/null
    ./configure > /dev/null
    make -j > /dev/null
  )
fi

# build liblz4
if [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.a" ] || [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.so" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf lz4
    git clone "${REPO_LIBLZ4}" --depth 1 --branch "${TAG_LIBLZ4}" > /dev/null
    cd lz4
    make lib -j > /dev/null
  )
fi

# build sqlite3
if [ ! -f "${BUILD_DIR}/sqlite/libsqlite3.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf sqlite
    git clone "${REPO_SQLITE}" --depth 1 --branch "${TAG_SQLITE}" > /dev/null
    cd sqlite
    ./configure --disable-readline \
      > /dev/null
    make libsqlite3.la -j > /dev/null
  )
fi

# build dqlite
if [ ! -f "${BUILD_DIR}/dqlite/libdqlite.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf dqlite
    git clone "${REPO_DQLITE}" --depth 1 --branch "${TAG_DQLITE}" > /dev/null
    cd dqlite
    autoreconf -i > /dev/null
    ./configure --enable-build-raft \
      CFLAGS="-I${BUILD_DIR}/sqlite -I${BUILD_DIR}/libuv/include -I${BUILD_DIR}/lz4/lib -Werror=implicit-function-declaration" \
      LDFLAGS=" -L${BUILD_DIR}/libuv/.libs -L${BUILD_DIR}/lz4/lib -L${BUILD_DIR}/libnsl/src" \
      UV_CFLAGS="-I${BUILD_DIR}/libuv/include" \
      UV_LIBS="-L${BUILD_DIR}/libuv/.libs" \
      SQLITE_CFLAGS="-I${BUILD_DIR}/sqlite" \
      LZ4_CFLAGS="-I${BUILD_DIR}/lz4/lib" \
      LZ4_LIBS="-L${BUILD_DIR}/lz4/lib" \
      > /dev/null

    make -j > /dev/null
  )
fi

# collect libraries
(
  cd "${BUILD_DIR}"
  cp libuv/.libs/* "${INSTALL_DIR}/lib"
  cp lz4/lib/*.so* "${INSTALL_DIR}/lib"
  cp sqlite/.libs/* "${INSTALL_DIR}/lib"
  cp dqlite/.libs/* "${INSTALL_DIR}/lib"
)

# collect headers
(
  cd "${BUILD_DIR}"
  cp -r libuv/include/* "${INSTALL_DIR}/include"
  cp -r sqlite/*.h "${INSTALL_DIR}/include"
  cp -r dqlite/include/* "${INSTALL_DIR}/include"
)

export CGO_CFLAGS="-I${INSTALL_DIR}/include"
export CGO_LDFLAGS="-L${INSTALL_DIR}/lib -ldqlite -luv -llz4 -lsqlite3"
export LD_LIBRARY_PATH="${INSTALL_DIR}/lib"

echo "Libraries are in '${INSTALL_DIR}/lib'"
