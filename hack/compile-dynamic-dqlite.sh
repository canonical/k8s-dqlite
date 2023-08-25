#!/bin/bash -xe

### BEGIN CONFIGURATION
# TAG_MUSL="v1.2.3"
TAG_LIBTIRPC="upstream/1.3.3"
TAG_LIBNSL="v2.0.0"
TAG_LIBUV="v1.44.2"
TAG_LIBLZ4="v1.9.4"
TAG_RAFT="v0.17.1"
TAG_SQLITE="version-3.40.0"
TAG_DQLITE="v1.14.0"
### END CONFIGURATION

DIR="$(realpath `dirname "${0}"`)"
mkdir -p "${DIR}/../deps" "${DIR}/../build"
BUILD_DIR="$(realpath ${DIR}/../build/dynamic)"
INSTALL_DIR="$(realpath ${DIR}/../deps/dynamic)"

mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${INSTALL_DIR}/lib" "${INSTALL_DIR}/include"

# dependencies
sudo apt install -y build-essential automake libtool gettext autopoint tclsh tcl libsqlite3-dev pkg-config libsqlite3-dev git curl > /dev/null

# build libtirpc
if [ ! -f "${BUILD_DIR}/libtirpc/src/libtirpc.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libtirpc
    git clone https://salsa.debian.org/debian/libtirpc.git --depth 1 --branch "${TAG_LIBTIRPC}" > /dev/null
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
    git clone https://github.com/thkukuk/libnsl --depth 1 --branch "${TAG_LIBNSL}" > /dev/null
    cd libnsl
    ./autogen.sh > /dev/null
    autoreconf -i > /dev/null
    autoconf > /dev/null
    ./configure \
      CFLAGS="${CFLAGS} -I${BUILD_DIR}/libtirpc/tirpc" \
      LDFLAGS="${LDFLAGS} -L${BUILD_DIR}/libtirpc/src" \
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
    git clone https://github.com/libuv/libuv.git --depth 1 --branch "${TAG_LIBUV}" > /dev/null
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
    git clone https://github.com/lz4/lz4.git --depth 1 --branch "${TAG_LIBLZ4}" > /dev/null
    cd lz4
    make lib -j > /dev/null
  )
fi

# build raft
if [ ! -f "${BUILD_DIR}/raft/libraft.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf raft
    git clone https://github.com/canonical/raft.git --depth 1 --branch "${TAG_RAFT}" > /dev/null
    cd raft
    autoreconf -i > /dev/null
    ./configure \
      CFLAGS="${CFLAGS} -I${BUILD_DIR}/libuv/include -I${BUILD_DIR}/lz4/lib" \
      LDFLAGS="${LDFLAGS} -L${BUILD_DIR}/libuv/.libs -L${BUILD_DIR}/lz4/lib" \
      UV_CFLAGS="-I${BUILD_DIR}/libuv/include" \
      UV_LIBS="-L${BUILD_DIR}/libuv/.libs" \
      LZ4_CFLAGS="-I${BUILD_DIR}/lz4/lib" \
      LZ4_LIBS="-L${BUILD_DIR}/lz4/lib" \
      > /dev/null

    make -j > /dev/null
  )
fi

# build sqlite3
if [ ! -f "${BUILD_DIR}/sqlite/libsqlite3.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf sqlite
    git clone https://github.com/sqlite/sqlite.git --depth 1 --branch "${TAG_SQLITE}" > /dev/null
    cd sqlite
    ./configure --disable-readline \
      CFLAGS="${CFLAGS} -DSQLITE_ENABLE_DBSTAT_VTAB=1" \
      > /dev/null
    make libsqlite3.la -j > /dev/null
  )
fi

# build dqlite
if [ ! -f "${BUILD_DIR}/dqlite/libdqlite.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf dqlite
    git clone https://github.com/canonical/dqlite.git --depth 1 --branch "${TAG_DQLITE}" > /dev/null
    cd dqlite
    autoreconf -i > /dev/null
    ./configure \
      CFLAGS="${CFLAGS} -I${BUILD_DIR}/raft/include -I${BUILD_DIR}/sqlite -I${BUILD_DIR}/libuv/include -I${BUILD_DIR}/lz4/lib -Werror=implicit-function-declaration" \
      LDFLAGS="${LDFLAGS} -L${BUILD_DIR}/raft/.libs -L${BUILD_DIR}/libuv/.libs -L${BUILD_DIR}/lz4/lib -L${BUILD_DIR}/libnsl/src" \
      RAFT_CFLAGS="-I${BUILD_DIR}/raft/include" \
      RAFT_LIBS="-L${BUILD_DIR}/raft/.libs" \
      UV_CFLAGS="-I${BUILD_DIR}/libuv/include" \
      UV_LIBS="-L${BUILD_DIR}/libuv/.libs" \
      SQLITE_CFLAGS="-I${BUILD_DIR}/sqlite" \
      > /dev/null

    make -j > /dev/null
  )
fi

# collect libraries
(
  cd "${BUILD_DIR}"
  cp libuv/.libs/* "${INSTALL_DIR}/lib"
  cp lz4/lib/*.so* "${INSTALL_DIR}/lib"
  cp raft/.libs/* "${INSTALL_DIR}/lib"
  cp sqlite/.libs/* "${INSTALL_DIR}/lib"
  cp dqlite/.libs/* "${INSTALL_DIR}/lib"
)

# collect headers
(
  cd "${BUILD_DIR}"
  cp -r raft/include/* "${INSTALL_DIR}/include"
  cp -r sqlite/*.h "${INSTALL_DIR}/include"
  cp -r dqlite/include/* "${INSTALL_DIR}/include"
)
